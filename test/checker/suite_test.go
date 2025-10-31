package checker

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/clyso/chorus/pkg/api"
	"github.com/clyso/chorus/pkg/config"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/rclone"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/trace"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/service/worker"
	"github.com/clyso/chorus/service/worker/handler"
	"github.com/clyso/chorus/test/app"
	"github.com/clyso/chorus/test/env"
	"github.com/clyso/chorus/test/gen"
	"github.com/minio/madmin-go/v4"
	"github.com/minio/minio-go/v7"
	mcredentials "github.com/minio/minio-go/v7/pkg/credentials"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rs/xid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	CMinioInstance = "minio"
	CRedisInstance = "redis"

	CMinioProvider = "minio"

	CMinioUser = "user"
	CMinioPass = "userpass"

	CStorage1Key = "storage1"
	CStorage2Key = "storage2"
	CSyncUserKey = "syncuser"

	CBucket1 = "bucket1"
	CBucket2 = "bucket2"
)

var (
	testCtx           context.Context
	testCtxCancelFunc context.CancelFunc

	testRnd         *gen.Rnd
	testTree        *gen.Tree[*gen.GeneratedS3Object]
	testTreePicker  *gen.TreeRandomElementPicker[*gen.GeneratedS3Object]
	testMinioClient *minio.Client
	testAPIClient   pb.ChorusClient

	locations = []*pb.MigrateLocation{
		{
			Storage: CStorage1Key,
			Bucket:  CBucket1,
		},
		{
			Storage: CStorage2Key,
			Bucket:  CBucket2,
		},
	}
)

func TestEnv(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Consistency Checker Suite")
}

var _ = BeforeSuite(func() {
	ctx, cancelFunc := context.WithCancel(context.Background())

	var rnd *gen.Rnd

	if gen.CUseTestGenSeed {
		rnd = gen.NewRnd(gen.CTestGenSeed)
	} else {
		rnd = gen.NewRnd(GinkgoRandomSeed())
	}

	objGen := gen.NewS3ObjectGenerator(
		gen.WithVersioned(),
		gen.WithVersionRange(2, 10),
	)
	genOpts := []gen.TreeGeneratorOption[*gen.GeneratedS3Object]{
		gen.WithObjectGenerator(objGen),
		gen.WithRnd[*gen.GeneratedS3Object](rnd),
		gen.WithForceTargetDepth[*gen.GeneratedS3Object](),
	}
	treeGen, err := gen.NewTreeGenerator(genOpts...)
	Expect(err).NotTo(HaveOccurred())

	tree, err := treeGen.Generate()
	Expect(err).NotTo(HaveOccurred())

	picker := gen.NewTreeRandomElementPicker(tree, rnd)

	localTestEnv, err := env.NewTestEnvironment(ctx, map[string]env.ComponentCreationConfig{
		CMinioInstance: env.AsMinio(),
		CRedisInstance: env.AsRedis(),
	})
	Expect(err).NotTo(HaveOccurred())

	minioAccessConfig, err := localTestEnv.GetMinioAccessConfig(CMinioInstance)
	Expect(err).NotTo(HaveOccurred())

	minioS3Endpoint := fmt.Sprintf("%s:%d", minioAccessConfig.Host.Local, minioAccessConfig.S3Port.Forwarded)
	minioAdminClient, err := madmin.NewWithOptions(minioS3Endpoint, &madmin.Options{
		Creds:  mcredentials.NewStaticV4(minioAccessConfig.User, minioAccessConfig.Password, ""),
		Secure: false,
	})
	Expect(err).NotTo(HaveOccurred())

	err = minioAdminClient.AddUser(ctx, CMinioUser, CMinioPass)
	Expect(err).NotTo(HaveOccurred())

	_, err = minioAdminClient.AttachPolicy(ctx, madmin.PolicyAssociationReq{
		Policies: []string{"readwrite"},
		User:     CMinioUser,
	})
	Expect(err).NotTo(HaveOccurred())

	minioUserClient, err := minio.New(minioS3Endpoint, &minio.Options{
		Creds:  mcredentials.NewStaticV4(CMinioUser, CMinioPass, ""),
		Secure: false,
	})
	Expect(err).NotTo(HaveOccurred())

	grpcPort, err := env.RandomFreePort()
	Expect(err).NotTo(HaveOccurred())
	httpPort, err := env.RandomFreePort()
	Expect(err).NotTo(HaveOccurred())

	redisAccessConfig, err := localTestEnv.GetRedisAccessConfig(CRedisInstance)
	Expect(err).NotTo(HaveOccurred())

	s3Storages := map[string]s3.Storage{
		CStorage1Key: {
			Address: fmt.Sprintf("http://%s", minioS3Endpoint),
			Credentials: map[string]s3.CredentialsV4{
				CSyncUserKey: {
					AccessKeyID:     CMinioUser,
					SecretAccessKey: CMinioPass,
				},
			},
			Provider: CMinioProvider,
		},
		CStorage2Key: {
			Address: fmt.Sprintf("http://%s", minioS3Endpoint),
			Credentials: map[string]s3.CredentialsV4{
				CSyncUserKey: {
					AccessKeyID:     CMinioUser,
					SecretAccessKey: CMinioPass,
				},
			},
			Provider: CMinioProvider,
		},
	}
	mainStorage := CStorage1Key
	workerConf := &worker.Config{
		Common: config.Common{
			Features: &features.Config{
				ACL:        false,
				Tagging:    false,
				Versioning: true,
			},
			Log: &log.Config{
				Level: "debug",
			},
			Redis: &config.Redis{
				Addresses: []string{fmt.Sprintf("%s:%d", redisAccessConfig.Host.Local, redisAccessConfig.Port.Forwarded)},
			},
			Metrics: &metrics.Config{
				Enabled: false,
			},
			Trace: &trace.Config{
				Enabled: false,
			},
		},
		Concurrency: 10,
		Lock: &worker.Lock{
			Overlap: time.Second,
		},
		RClone: &rclone.Config{
			MemoryLimit: rclone.MemoryLimit{
				Enabled: false,
			},
			LocalFileLimit: ratelimit.SemaphoreConfig{
				Enabled: false,
			},
			GlobalFileLimit: ratelimit.SemaphoreConfig{
				Enabled: false,
			},
		},
		Worker: &handler.Config{
			SwitchRetryInterval: time.Millisecond * 500,
			PauseRetryInterval:  time.Millisecond * 500,
		},
		Storage: app.WorkerS3Config(mainStorage, s3Storages),
		Api: &api.Config{
			Enabled:  true,
			GrpcPort: grpcPort,
			HttpPort: httpPort,
		},
	}

	err = workerConf.Storage.Validate()
	Expect(err).NotTo(HaveOccurred())

	grpcAddr := fmt.Sprintf("%s:%d", "localhost", grpcPort)

	app := dom.AppInfo{
		Version: "test",
		App:     "worker",
		AppID:   xid.New().String(),
	}

	go func() {
		defer GinkgoRecover()
		err = worker.Start(ctx, app, workerConf)
		Expect(err).NotTo(HaveOccurred())
	}()

	grpcConn, err := grpc.NewClient(grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             time.Second,
			PermitWithoutStream: true,
		}),
	)
	Expect(err).NotTo(HaveOccurred())
	apiClient := pb.NewChorusClient(grpcConn)

	Eventually(func(g Gomega) {
		_, err := apiClient.GetAppVersion(ctx, &emptypb.Empty{})
		g.Expect(err).NotTo(HaveOccurred())
	}, 1*time.Minute, time.Second).Should(Succeed())

	testRnd = rnd
	testCtx = ctx
	testCtxCancelFunc = cancelFunc
	testTree = tree
	testTreePicker = picker
	testMinioClient = minioUserClient
	testAPIClient = apiClient
})

var _ = AfterSuite(func() {
	testCtxCancelFunc()
})

var _ = Describe("Consistency checker for unversioned buckets", func() {
	BeforeEach(func() {
		ctx := context.WithoutCancel(testCtx)

		err := testMinioClient.MakeBucket(ctx, CBucket1, minio.MakeBucketOptions{})
		Expect(err).NotTo(HaveOccurred())
		err = testMinioClient.MakeBucket(ctx, CBucket2, minio.MakeBucketOptions{})
		Expect(err).NotTo(HaveOccurred())

		filler := gen.NewS3Filler(testTree, testMinioClient)
		err = filler.FillLast(ctx, CBucket1)
		Expect(err).NotTo(HaveOccurred())
		err = filler.FillLast(ctx, CBucket2)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		ctx := context.WithoutCancel(testCtx)

		err := testMinioClient.RemoveBucketWithOptions(ctx, CBucket1, minio.RemoveBucketOptions{
			ForceDelete: true,
		})
		Expect(err).NotTo(HaveOccurred())

		err = testMinioClient.RemoveBucketWithOptions(ctx, CBucket2, minio.RemoveBucketOptions{
			ForceDelete: true,
		})
		Expect(err).NotTo(HaveOccurred())

		_, err = testAPIClient.DeleteConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{
			Locations: locations,
		})
		Expect(err).NotTo(HaveOccurred())
	})

	It("Should succeed", func() {
		ctx := context.WithoutCancel(testCtx)

		checkRequest := &pb.StartConsistencyCheckRequest{
			Locations: locations,
			User:      CSyncUserKey,
		}
		_, err := testAPIClient.StartConsistencyCheck(ctx, checkRequest)
		Expect(err).NotTo(HaveOccurred())

		var getCheckResponse *pb.GetConsistencyCheckReportResponse
		Eventually(func(g Gomega) {
			getCheckResponse, err = testAPIClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(getCheckResponse).NotTo(BeNil())
			g.Expect(getCheckResponse.Check).NotTo(BeNil())
			g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		Expect(getCheckResponse.Check.Consistent).To(BeTrue())
		Expect(getCheckResponse.Check.WithEtag).To(BeTrue())
		Expect(getCheckResponse.Check.WithSize).To(BeTrue())
		Expect(getCheckResponse.Check.Versioned).To(BeFalse())

		checkEntries, err := testAPIClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
			Locations: locations,
			PageSize:  10,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(checkEntries.Entries).To(HaveLen(0))
	})

	It("Should succeed, check only sizes", func() {
		ctx := context.WithoutCancel(testCtx)

		joint := testTreePicker.RandomJointValue()
		jointPath := joint.GetFullPath()

		objectPath := jointPath + "filewithdiffetag"

		_, err := testMinioClient.PutObject(ctx, CBucket1, objectPath, bytes.NewReader([]byte{1, 2, 3, 4}), 4, minio.PutObjectOptions{})
		Expect(err).NotTo(HaveOccurred())
		_, err = testMinioClient.PutObject(ctx, CBucket2, objectPath, bytes.NewReader([]byte{5, 6, 7, 8}), 4, minio.PutObjectOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			_, err := testMinioClient.StatObject(ctx, CBucket1, objectPath, minio.StatObjectOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			_, err = testMinioClient.StatObject(ctx, CBucket2, objectPath, minio.StatObjectOptions{})
			g.Expect(err).NotTo(HaveOccurred())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		checkRequest := &pb.StartConsistencyCheckRequest{
			Locations:   locations,
			User:        CSyncUserKey,
			IgnoreEtags: true,
		}
		_, err = testAPIClient.StartConsistencyCheck(ctx, checkRequest)
		Expect(err).NotTo(HaveOccurred())

		var getCheckResponse *pb.GetConsistencyCheckReportResponse
		Eventually(func(g Gomega) {
			getCheckResponse, err = testAPIClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(getCheckResponse).NotTo(BeNil())
			g.Expect(getCheckResponse.Check).NotTo(BeNil())
			g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		Expect(getCheckResponse.Check.Consistent).To(BeTrue())
		Expect(getCheckResponse.Check.WithEtag).To(BeFalse())
		Expect(getCheckResponse.Check.WithSize).To(BeTrue())
		Expect(getCheckResponse.Check.Versioned).To(BeFalse())

		checkEntries, err := testAPIClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
			Locations: locations,
			PageSize:  10,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(checkEntries.Entries).To(HaveLen(0))
	})

	It("Should succeed, check only list", func() {
		ctx := context.WithoutCancel(testCtx)

		joint := testTreePicker.RandomJointValue()
		jointPath := joint.GetFullPath()

		objectPath := jointPath + "filewithdiffetag"

		_, err := testMinioClient.PutObject(ctx, CBucket1, objectPath, bytes.NewReader([]byte{1, 2, 3, 4}), 4, minio.PutObjectOptions{})
		Expect(err).NotTo(HaveOccurred())
		_, err = testMinioClient.PutObject(ctx, CBucket2, objectPath, bytes.NewReader([]byte{5, 6, 7, 8, 9}), 5, minio.PutObjectOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			_, err := testMinioClient.StatObject(ctx, CBucket1, objectPath, minio.StatObjectOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			_, err = testMinioClient.StatObject(ctx, CBucket2, objectPath, minio.StatObjectOptions{})
			g.Expect(err).NotTo(HaveOccurred())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		checkRequest := &pb.StartConsistencyCheckRequest{
			Locations:   locations,
			User:        CSyncUserKey,
			IgnoreSizes: true,
		}
		_, err = testAPIClient.StartConsistencyCheck(ctx, checkRequest)
		Expect(err).NotTo(HaveOccurred())

		var getCheckResponse *pb.GetConsistencyCheckReportResponse
		Eventually(func(g Gomega) {
			getCheckResponse, err = testAPIClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(getCheckResponse).NotTo(BeNil())
			g.Expect(getCheckResponse.Check).NotTo(BeNil())
			g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		Expect(getCheckResponse.Check.Consistent).To(BeTrue())
		Expect(getCheckResponse.Check.WithEtag).To(BeFalse())
		Expect(getCheckResponse.Check.WithSize).To(BeFalse())
		Expect(getCheckResponse.Check.Versioned).To(BeFalse())

		checkEntries, err := testAPIClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
			Locations: locations,
			PageSize:  10,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(checkEntries.Entries).To(HaveLen(0))
	})

	It("Should fail, no object", func() {
		ctx := context.WithoutCancel(testCtx)

		leaf := testTreePicker.RandomLeafValue()
		leafPath := strings.TrimPrefix(leaf.GetFullPath(), "/")

		err := testMinioClient.RemoveObject(ctx, CBucket2, leafPath, minio.RemoveObjectOptions{
			ForceDelete: true,
		})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			_, err := testMinioClient.StatObject(ctx, CBucket2, leafPath, minio.StatObjectOptions{})
			g.Expect(err).To(HaveOccurred())
			minioErr, ok := err.(minio.ErrorResponse)
			g.Expect(ok).To(BeTrue())
			g.Expect(minioErr.StatusCode).To(Equal(http.StatusNotFound))
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		checkRequest := &pb.StartConsistencyCheckRequest{
			Locations: locations,
			User:      CSyncUserKey,
		}
		_, err = testAPIClient.StartConsistencyCheck(ctx, checkRequest)
		Expect(err).NotTo(HaveOccurred())

		var getCheckResponse *pb.GetConsistencyCheckReportResponse
		Eventually(func(g Gomega) {
			getCheckResponse, err = testAPIClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(getCheckResponse).NotTo(BeNil())
			g.Expect(getCheckResponse.Check).NotTo(BeNil())
			g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		Expect(getCheckResponse.Check.Consistent).To(BeFalse())
		Expect(getCheckResponse.Check.WithEtag).To(BeTrue())
		Expect(getCheckResponse.Check.WithSize).To(BeTrue())
		Expect(getCheckResponse.Check.Versioned).To(BeFalse())

		var entries []*pb.ConsistencyCheckReportEntry
		var cursor uint64
		for {
			checkEntries, err := testAPIClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
				Locations: locations,
				PageSize:  1000,
				Cursor:    cursor,
			})
			Expect(err).NotTo(HaveOccurred())

			entries = append(entries, checkEntries.Entries...)

			if checkEntries.Cursor == 0 {
				break
			}

			cursor = checkEntries.Cursor
		}

		Expect(entries).To(HaveLen(1))

		for _, entry := range entries {
			Expect(entry.Object).To(Equal(leafPath))
			for _, storageEntry := range entry.StorageEntries {
				Expect(storageEntry.Storage).To(Equal(CStorage1Key))
				Expect(storageEntry.VersionId).To(BeEmpty())
			}
		}
	})

	It("Should fail, no directory", func() {
		ctx := context.WithoutCancel(testCtx)

		jointSubtree := testTreePicker.RandomJointSubtree()
		jointRootObject, err := jointSubtree.DepthFirstValueIterator().Next()
		Expect(err).NotTo(HaveOccurred())

		jointPath := strings.TrimPrefix(jointRootObject.GetFullPath(), "/")
		err = testMinioClient.RemoveObject(ctx, CBucket2, jointPath, minio.RemoveObjectOptions{
			ForceDelete: true,
		})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			_, err := testMinioClient.StatObject(ctx, CBucket2, jointPath, minio.StatObjectOptions{})
			g.Expect(err).To(HaveOccurred())
			minioErr, ok := err.(minio.ErrorResponse)
			g.Expect(ok).To(BeTrue())
			g.Expect(minioErr.StatusCode).To(Equal(http.StatusNotFound))
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		checkRequest := &pb.StartConsistencyCheckRequest{
			Locations: locations,
			User:      CSyncUserKey,
		}
		_, err = testAPIClient.StartConsistencyCheck(ctx, checkRequest)
		Expect(err).NotTo(HaveOccurred())

		var getCheckResponse *pb.GetConsistencyCheckReportResponse
		Eventually(func(g Gomega) {
			getCheckResponse, err = testAPIClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(getCheckResponse).NotTo(BeNil())
			g.Expect(getCheckResponse.Check).NotTo(BeNil())
			g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		Expect(getCheckResponse.Check.Consistent).To(BeFalse())
		Expect(getCheckResponse.Check.WithEtag).To(BeTrue())
		Expect(getCheckResponse.Check.WithSize).To(BeTrue())
		Expect(getCheckResponse.Check.Versioned).To(BeFalse())

		var entries []*pb.ConsistencyCheckReportEntry
		var cursor uint64
		for {
			checkEntries, err := testAPIClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
				Locations: locations,
				PageSize:  1000,
				Cursor:    cursor,
			})
			Expect(err).NotTo(HaveOccurred())

			entries = append(entries, checkEntries.Entries...)

			if checkEntries.Cursor == 0 {
				break
			}

			cursor = checkEntries.Cursor
		}

		var elementCount uint64
		for range jointSubtree.WidthFirstValueIterator().Must() {
			elementCount += 1
		}

		Expect(entries).To(HaveLen(int(elementCount)))

		for _, entry := range entries {
			Expect(entry.Object).To(HavePrefix(jointPath))
			for _, storageEntry := range entry.StorageEntries {
				Expect(storageEntry.Storage).To(Equal(CStorage1Key))
			}
		}
	})

	It("Should fail, no empty dir", func() {
		ctx := context.WithoutCancel(testCtx)

		joint := testTreePicker.RandomJointValue()
		jointPath := joint.GetFullPath()

		emptyDirPath := jointPath + "emptydirectory/"

		_, err := testMinioClient.PutObject(ctx, CBucket1, emptyDirPath, bytes.NewReader([]byte{}), 0, minio.PutObjectOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			_, err := testMinioClient.StatObject(ctx, CBucket1, emptyDirPath, minio.StatObjectOptions{})
			g.Expect(err).NotTo(HaveOccurred())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		checkRequest := &pb.StartConsistencyCheckRequest{
			Locations: locations,
			User:      CSyncUserKey,
		}
		_, err = testAPIClient.StartConsistencyCheck(ctx, checkRequest)
		Expect(err).NotTo(HaveOccurred())

		var getCheckResponse *pb.GetConsistencyCheckReportResponse
		Eventually(func(g Gomega) {
			getCheckResponse, err = testAPIClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(getCheckResponse).NotTo(BeNil())
			g.Expect(getCheckResponse.Check).NotTo(BeNil())
			g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		Expect(getCheckResponse.Check.Consistent).To(BeFalse())
		Expect(getCheckResponse.Check.WithEtag).To(BeTrue())
		Expect(getCheckResponse.Check.WithSize).To(BeTrue())
		Expect(getCheckResponse.Check.Versioned).To(BeFalse())

		var entries []*pb.ConsistencyCheckReportEntry
		var cursor uint64
		for {
			checkEntries, err := testAPIClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
				Locations: locations,
				PageSize:  1000,
				Cursor:    cursor,
			})
			Expect(err).NotTo(HaveOccurred())

			entries = append(entries, checkEntries.Entries...)

			if checkEntries.Cursor == 0 {
				break
			}

			cursor = checkEntries.Cursor
		}

		Expect(entries).To(HaveLen(1))

		for _, entry := range entries {
			Expect(entry.Object).To(Equal(strings.TrimPrefix(emptyDirPath, "/")))
			for _, storageEntry := range entry.StorageEntries {
				Expect(storageEntry.Storage).To(Equal(CStorage1Key))
			}
		}
	})

	It("Should fail, wrong etag", func() {
		ctx := context.WithoutCancel(testCtx)

		joint := testTreePicker.RandomJointValue()
		jointPath := joint.GetFullPath()

		objectPath := jointPath + "filewithdiffetag"

		_, err := testMinioClient.PutObject(ctx, CBucket1, objectPath, bytes.NewReader([]byte{1, 2, 3, 4}), 4, minio.PutObjectOptions{})
		Expect(err).NotTo(HaveOccurred())
		_, err = testMinioClient.PutObject(ctx, CBucket2, objectPath, bytes.NewReader([]byte{5, 6, 7, 8}), 4, minio.PutObjectOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			_, err := testMinioClient.StatObject(ctx, CBucket1, objectPath, minio.StatObjectOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			_, err = testMinioClient.StatObject(ctx, CBucket2, objectPath, minio.StatObjectOptions{})
			g.Expect(err).NotTo(HaveOccurred())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		checkRequest := &pb.StartConsistencyCheckRequest{
			Locations: locations,
			User:      CSyncUserKey,
		}
		_, err = testAPIClient.StartConsistencyCheck(ctx, checkRequest)
		Expect(err).NotTo(HaveOccurred())

		var getCheckResponse *pb.GetConsistencyCheckReportResponse
		Eventually(func(g Gomega) {
			getCheckResponse, err = testAPIClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(getCheckResponse).NotTo(BeNil())
			g.Expect(getCheckResponse.Check).NotTo(BeNil())
			g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		Expect(getCheckResponse.Check.Consistent).To(BeFalse())
		Expect(getCheckResponse.Check.WithEtag).To(BeTrue())
		Expect(getCheckResponse.Check.WithSize).To(BeTrue())
		Expect(getCheckResponse.Check.Versioned).To(BeFalse())

		var entries []*pb.ConsistencyCheckReportEntry
		var cursor uint64
		for {
			checkEntries, err := testAPIClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
				Locations: locations,
				PageSize:  1000,
				Cursor:    cursor,
			})
			Expect(err).NotTo(HaveOccurred())

			entries = append(entries, checkEntries.Entries...)

			if checkEntries.Cursor == 0 {
				break
			}

			cursor = checkEntries.Cursor
		}

		Expect(entries).To(HaveLen(2))

		for _, entry := range entries {
			Expect(entry.Object).To(Equal(strings.TrimPrefix(objectPath, "/")))
			for _, storageEntry := range entry.StorageEntries {
				Expect(storageEntry.Storage).To(BeElementOf(CStorage1Key, CStorage2Key))
				Expect(storageEntry.VersionId).To(BeEmpty())
			}
		}
	})
})

var _ = Describe("Consistency checker for versioned buckets", func() {
	BeforeEach(func() {
		ctx := context.WithoutCancel(testCtx)

		err := testMinioClient.MakeBucket(ctx, CBucket1, minio.MakeBucketOptions{})
		Expect(err).NotTo(HaveOccurred())
		err = testMinioClient.EnableVersioning(ctx, CBucket1)
		Expect(err).NotTo(HaveOccurred())
		err = testMinioClient.MakeBucket(ctx, CBucket2, minio.MakeBucketOptions{})
		Expect(err).NotTo(HaveOccurred())
		err = testMinioClient.EnableVersioning(ctx, CBucket2)
		Expect(err).NotTo(HaveOccurred())

		filler := gen.NewS3Filler(testTree, testMinioClient)
		err = filler.Fill(ctx, CBucket1)
		Expect(err).NotTo(HaveOccurred())
		err = filler.Fill(ctx, CBucket2)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		ctx := context.WithoutCancel(testCtx)

		err := testMinioClient.RemoveBucketWithOptions(ctx, CBucket1, minio.RemoveBucketOptions{
			ForceDelete: true,
		})
		Expect(err).NotTo(HaveOccurred())

		err = testMinioClient.RemoveBucketWithOptions(ctx, CBucket2, minio.RemoveBucketOptions{
			ForceDelete: true,
		})
		Expect(err).NotTo(HaveOccurred())

		_, err = testAPIClient.DeleteConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{
			Locations: locations,
		})
		Expect(err).NotTo(HaveOccurred())
	})

	It("Should succeed", func() {
		ctx := context.WithoutCancel(testCtx)

		checkRequest := &pb.StartConsistencyCheckRequest{
			Locations: locations,
			User:      CSyncUserKey,
		}
		_, err := testAPIClient.StartConsistencyCheck(ctx, checkRequest)
		Expect(err).NotTo(HaveOccurred())

		var getCheckResponse *pb.GetConsistencyCheckReportResponse
		Eventually(func(g Gomega) {
			getCheckResponse, err = testAPIClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(getCheckResponse).NotTo(BeNil())
			g.Expect(getCheckResponse.Check).NotTo(BeNil())
			g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		Expect(getCheckResponse.Check.Consistent).To(BeTrue())
		Expect(getCheckResponse.Check.WithEtag).To(BeTrue())
		Expect(getCheckResponse.Check.WithSize).To(BeTrue())
		Expect(getCheckResponse.Check.Versioned).To(BeTrue())

		checkEntries, err := testAPIClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
			Locations: locations,
			PageSize:  10,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(checkEntries.Entries).To(HaveLen(0))
	})

	It("Should succeed, check only sizes", func() {
		ctx := context.WithoutCancel(testCtx)

		leaf := testTreePicker.RandomLeafValue()
		objectPath := leaf.GetFullPath()

		_, err := testMinioClient.PutObject(ctx, CBucket1, objectPath, bytes.NewReader([]byte{1, 2, 3, 4}), 4, minio.PutObjectOptions{})
		Expect(err).NotTo(HaveOccurred())
		_, err = testMinioClient.PutObject(ctx, CBucket2, objectPath, bytes.NewReader([]byte{5, 6, 7, 8}), 4, minio.PutObjectOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			stat1, err := testMinioClient.StatObject(ctx, CBucket1, objectPath, minio.StatObjectOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(stat1.Size).To(BeNumerically("==", 4))
			stat2, err := testMinioClient.StatObject(ctx, CBucket2, objectPath, minio.StatObjectOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(stat2.Size).To(BeNumerically("==", 4))
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		checkRequest := &pb.StartConsistencyCheckRequest{
			Locations:   locations,
			User:        CSyncUserKey,
			IgnoreEtags: true,
		}
		_, err = testAPIClient.StartConsistencyCheck(ctx, checkRequest)
		Expect(err).NotTo(HaveOccurred())

		var getCheckResponse *pb.GetConsistencyCheckReportResponse
		Eventually(func(g Gomega) {
			getCheckResponse, err = testAPIClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(getCheckResponse).NotTo(BeNil())
			g.Expect(getCheckResponse.Check).NotTo(BeNil())
			g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		Expect(getCheckResponse.Check.Consistent).To(BeTrue())
		Expect(getCheckResponse.Check.WithEtag).To(BeFalse())
		Expect(getCheckResponse.Check.WithSize).To(BeTrue())
		Expect(getCheckResponse.Check.Versioned).To(BeTrue())

		checkEntries, err := testAPIClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
			Locations: locations,
			PageSize:  10,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(checkEntries.Entries).To(HaveLen(0))
	})

	It("Should succeed, check only list", func() {
		ctx := context.WithoutCancel(testCtx)

		leaf := testTreePicker.RandomLeafValue()
		objectPath := leaf.GetFullPath()

		_, err := testMinioClient.PutObject(ctx, CBucket1, objectPath, bytes.NewReader([]byte{1, 2, 3, 4}), 4, minio.PutObjectOptions{})
		Expect(err).NotTo(HaveOccurred())
		_, err = testMinioClient.PutObject(ctx, CBucket2, objectPath, bytes.NewReader([]byte{5, 6, 7, 8, 9}), 5, minio.PutObjectOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			stat1, err := testMinioClient.StatObject(ctx, CBucket1, objectPath, minio.StatObjectOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(stat1.Size).To(BeNumerically("==", 4))
			stat2, err := testMinioClient.StatObject(ctx, CBucket2, objectPath, minio.StatObjectOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(stat2.Size).To(BeNumerically("==", 5))
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		checkRequest := &pb.StartConsistencyCheckRequest{
			Locations:   locations,
			User:        CSyncUserKey,
			IgnoreSizes: true,
		}
		_, err = testAPIClient.StartConsistencyCheck(ctx, checkRequest)
		Expect(err).NotTo(HaveOccurred())

		var getCheckResponse *pb.GetConsistencyCheckReportResponse
		Eventually(func(g Gomega) {
			getCheckResponse, err = testAPIClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(getCheckResponse).NotTo(BeNil())
			g.Expect(getCheckResponse.Check).NotTo(BeNil())
			g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		Expect(getCheckResponse.Check.Consistent).To(BeTrue())
		Expect(getCheckResponse.Check.WithEtag).To(BeFalse())
		Expect(getCheckResponse.Check.WithSize).To(BeFalse())
		Expect(getCheckResponse.Check.Versioned).To(BeTrue())

		checkEntries, err := testAPIClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
			Locations: locations,
			PageSize:  10,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(checkEntries.Entries).To(HaveLen(0))
	})

	It("Should succeed, check only last version", func() {
		ctx := context.WithoutCancel(testCtx)

		leaf := testTreePicker.RandomLeafValue()
		leafPath := leaf.GetFullPath()
		versionCount := leaf.GetVersionCount()
		Expect(versionCount).To(BeNumerically(">", 0))
		versionIdx := testRnd.IntInRange(1, int(versionCount)-1)

		objectVersionChan := testMinioClient.ListObjectsIter(ctx, CBucket2, minio.ListObjectsOptions{
			Prefix:       strings.TrimPrefix(leafPath, "/"),
			WithVersions: true,
		})

		versionId := ""
		currentVersionIdx := 0
		for version := range objectVersionChan {
			Expect(version.Err).NotTo(HaveOccurred())

			if currentVersionIdx == versionIdx {
				versionId = version.VersionID
				break
			}

			currentVersionIdx++
		}
		Expect(versionId).NotTo(BeEmpty())

		err := testMinioClient.RemoveObject(ctx, CBucket2, leafPath, minio.RemoveObjectOptions{
			VersionID: versionId,
		})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			_, err := testMinioClient.StatObject(ctx, CBucket2, leafPath, minio.StatObjectOptions{
				VersionID: versionId,
			})
			g.Expect(err).To(HaveOccurred())
			minioErr, ok := err.(minio.ErrorResponse)
			g.Expect(ok).To(BeTrue())
			g.Expect(minioErr.StatusCode).To(Equal(http.StatusNotFound))
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		checkRequest := &pb.StartConsistencyCheckRequest{
			Locations:             locations,
			User:                  CSyncUserKey,
			CheckOnlyLastVersions: true,
		}
		_, err = testAPIClient.StartConsistencyCheck(ctx, checkRequest)
		Expect(err).NotTo(HaveOccurred())

		var getCheckResponse *pb.GetConsistencyCheckReportResponse
		Eventually(func(g Gomega) {
			getCheckResponse, err = testAPIClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(getCheckResponse).NotTo(BeNil())
			g.Expect(getCheckResponse.Check).NotTo(BeNil())
			g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		Expect(getCheckResponse.Check.Consistent).To(BeTrue())
		Expect(getCheckResponse.Check.WithEtag).To(BeTrue())
		Expect(getCheckResponse.Check.WithSize).To(BeTrue())
		Expect(getCheckResponse.Check.Versioned).To(BeFalse())

		checkEntries, err := testAPIClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
			Locations: locations,
			PageSize:  10,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(checkEntries.Entries).To(HaveLen(0))
	})

	It("Should fail, no object", func() {
		ctx := context.WithoutCancel(testCtx)

		leaf := testTreePicker.RandomLeafValue()
		leafPath := strings.TrimPrefix(leaf.GetFullPath(), "/")

		err := testMinioClient.RemoveObject(ctx, CBucket2, leafPath, minio.RemoveObjectOptions{
			ForceDelete: true,
		})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			_, err := testMinioClient.StatObject(ctx, CBucket2, leafPath, minio.StatObjectOptions{})
			g.Expect(err).To(HaveOccurred())
			minioErr, ok := err.(minio.ErrorResponse)
			g.Expect(ok).To(BeTrue())
			g.Expect(minioErr.StatusCode).To(Equal(http.StatusNotFound))
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		checkRequest := &pb.StartConsistencyCheckRequest{
			Locations: locations,
			User:      CSyncUserKey,
		}
		_, err = testAPIClient.StartConsistencyCheck(ctx, checkRequest)
		Expect(err).NotTo(HaveOccurred())

		var getCheckResponse *pb.GetConsistencyCheckReportResponse
		Eventually(func(g Gomega) {
			getCheckResponse, err = testAPIClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(getCheckResponse).NotTo(BeNil())
			g.Expect(getCheckResponse.Check).NotTo(BeNil())
			g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		Expect(getCheckResponse.Check.Consistent).To(BeFalse())
		Expect(getCheckResponse.Check.WithEtag).To(BeTrue())
		Expect(getCheckResponse.Check.WithSize).To(BeTrue())
		Expect(getCheckResponse.Check.Versioned).To(BeTrue())

		var entries []*pb.ConsistencyCheckReportEntry
		var cursor uint64
		for {
			checkEntries, err := testAPIClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
				Locations: locations,
				PageSize:  1000,
				Cursor:    cursor,
			})
			Expect(err).NotTo(HaveOccurred())

			entries = append(entries, checkEntries.Entries...)

			if checkEntries.Cursor == 0 {
				break
			}

			cursor = checkEntries.Cursor
		}

		Expect(entries).To(HaveLen(int(leaf.GetVersionCount())))

		for _, entry := range entries {
			Expect(entry.Object).To(Equal(leafPath))
			for _, storageEntry := range entry.StorageEntries {
				Expect(storageEntry.Storage).To(Equal(CStorage1Key))
				Expect(storageEntry.VersionId).NotTo(BeEmpty())
			}
		}
	})

	It("Should fail, no directory", func() {
		ctx := context.WithoutCancel(testCtx)

		jointSubtree := testTreePicker.RandomJointSubtree()
		jointRootObject, err := jointSubtree.DepthFirstValueIterator().Next()
		Expect(err).NotTo(HaveOccurred())

		jointPath := strings.TrimPrefix(jointRootObject.GetFullPath(), "/")
		err = testMinioClient.RemoveObject(ctx, CBucket2, jointPath, minio.RemoveObjectOptions{
			ForceDelete: true,
		})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			_, err := testMinioClient.StatObject(ctx, CBucket2, jointPath, minio.StatObjectOptions{})
			g.Expect(err).To(HaveOccurred())
			minioErr, ok := err.(minio.ErrorResponse)
			g.Expect(ok).To(BeTrue())
			g.Expect(minioErr.StatusCode).To(Equal(http.StatusNotFound))
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		checkRequest := &pb.StartConsistencyCheckRequest{
			Locations: locations,
			User:      CSyncUserKey,
		}
		_, err = testAPIClient.StartConsistencyCheck(ctx, checkRequest)
		Expect(err).NotTo(HaveOccurred())

		var getCheckResponse *pb.GetConsistencyCheckReportResponse
		Eventually(func(g Gomega) {
			getCheckResponse, err = testAPIClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(getCheckResponse).NotTo(BeNil())
			g.Expect(getCheckResponse.Check).NotTo(BeNil())
			g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		Expect(getCheckResponse.Check.Consistent).To(BeFalse())
		Expect(getCheckResponse.Check.WithEtag).To(BeTrue())
		Expect(getCheckResponse.Check.WithSize).To(BeTrue())
		Expect(getCheckResponse.Check.Versioned).To(BeTrue())

		var entries []*pb.ConsistencyCheckReportEntry
		var cursor uint64
		for {
			checkEntries, err := testAPIClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
				Locations: locations,
				PageSize:  1000,
				Cursor:    cursor,
			})
			Expect(err).NotTo(HaveOccurred())

			entries = append(entries, checkEntries.Entries...)

			if checkEntries.Cursor == 0 {
				break
			}

			cursor = checkEntries.Cursor
		}

		var elementCount uint64
		for value := range jointSubtree.WidthFirstValueIterator().Must() {
			elementCount += value.GetVersionCount()
		}

		Expect(entries).To(HaveLen(int(elementCount)))

		for _, entry := range entries {
			Expect(entry.Object).To(HavePrefix(jointPath))
			for _, storageEntry := range entry.StorageEntries {
				Expect(storageEntry.Storage).To(Equal(CStorage1Key))
			}
		}
	})

	It("Should fail, no empty dir", func() {
		ctx := context.WithoutCancel(testCtx)

		joint := testTreePicker.RandomJointValue()
		jointPath := joint.GetFullPath()

		emptyDirPath := jointPath + "emptydirectory/"

		_, err := testMinioClient.PutObject(ctx, CBucket1, emptyDirPath, bytes.NewReader([]byte{}), 0, minio.PutObjectOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			_, err := testMinioClient.StatObject(ctx, CBucket1, emptyDirPath, minio.StatObjectOptions{})
			g.Expect(err).NotTo(HaveOccurred())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		checkRequest := &pb.StartConsistencyCheckRequest{
			Locations: locations,
			User:      CSyncUserKey,
		}
		_, err = testAPIClient.StartConsistencyCheck(ctx, checkRequest)
		Expect(err).NotTo(HaveOccurred())

		var getCheckResponse *pb.GetConsistencyCheckReportResponse
		Eventually(func(g Gomega) {
			getCheckResponse, err = testAPIClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(getCheckResponse).NotTo(BeNil())
			g.Expect(getCheckResponse.Check).NotTo(BeNil())
			g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		Expect(getCheckResponse.Check.Consistent).To(BeFalse())
		Expect(getCheckResponse.Check.WithEtag).To(BeTrue())
		Expect(getCheckResponse.Check.WithSize).To(BeTrue())
		Expect(getCheckResponse.Check.Versioned).To(BeTrue())

		var entries []*pb.ConsistencyCheckReportEntry
		var cursor uint64
		for {
			checkEntries, err := testAPIClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
				Locations: locations,
				PageSize:  1000,
				Cursor:    cursor,
			})
			Expect(err).NotTo(HaveOccurred())

			entries = append(entries, checkEntries.Entries...)

			if checkEntries.Cursor == 0 {
				break
			}

			cursor = checkEntries.Cursor
		}

		Expect(entries).To(HaveLen(1))

		for _, entry := range entries {
			Expect(entry.Object).To(Equal(strings.TrimPrefix(emptyDirPath, "/")))
			for _, storageEntry := range entry.StorageEntries {
				Expect(storageEntry.Storage).To(Equal(CStorage1Key))
			}
		}
	})

	It("Should fail, wrong etag", func() {
		ctx := context.WithoutCancel(testCtx)

		joint := testTreePicker.RandomJointValue()
		jointPath := joint.GetFullPath()

		objectPath := jointPath + "filewithdiffetag"

		_, err := testMinioClient.PutObject(ctx, CBucket1, objectPath, bytes.NewReader([]byte{1, 2, 3, 4}), 4, minio.PutObjectOptions{})
		Expect(err).NotTo(HaveOccurred())
		_, err = testMinioClient.PutObject(ctx, CBucket2, objectPath, bytes.NewReader([]byte{5, 6, 7, 8}), 4, minio.PutObjectOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			_, err := testMinioClient.StatObject(ctx, CBucket1, objectPath, minio.StatObjectOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			_, err = testMinioClient.StatObject(ctx, CBucket2, objectPath, minio.StatObjectOptions{})
			g.Expect(err).NotTo(HaveOccurred())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		checkRequest := &pb.StartConsistencyCheckRequest{
			Locations: locations,
			User:      CSyncUserKey,
		}
		_, err = testAPIClient.StartConsistencyCheck(ctx, checkRequest)
		Expect(err).NotTo(HaveOccurred())

		var getCheckResponse *pb.GetConsistencyCheckReportResponse
		Eventually(func(g Gomega) {
			getCheckResponse, err = testAPIClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(getCheckResponse).NotTo(BeNil())
			g.Expect(getCheckResponse.Check).NotTo(BeNil())
			g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		Expect(getCheckResponse.Check.Consistent).To(BeFalse())
		Expect(getCheckResponse.Check.WithEtag).To(BeTrue())
		Expect(getCheckResponse.Check.WithSize).To(BeTrue())
		Expect(getCheckResponse.Check.Versioned).To(BeTrue())

		var entries []*pb.ConsistencyCheckReportEntry
		var cursor uint64
		for {
			checkEntries, err := testAPIClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
				Locations: locations,
				PageSize:  1000,
				Cursor:    cursor,
			})
			Expect(err).NotTo(HaveOccurred())

			entries = append(entries, checkEntries.Entries...)

			if checkEntries.Cursor == 0 {
				break
			}

			cursor = checkEntries.Cursor
		}

		Expect(entries).To(HaveLen(2))

		for _, entry := range entries {
			Expect(entry.Object).To(Equal(strings.TrimPrefix(objectPath, "/")))
			for _, storageEntry := range entry.StorageEntries {
				Expect(storageEntry.Storage).To(BeElementOf(CStorage1Key, CStorage2Key))
				Expect(storageEntry.VersionId).NotTo(BeEmpty())
			}
		}
	})

	It("Should fail, wrong version count", func() {
		ctx := context.WithoutCancel(testCtx)

		leaf := testTreePicker.RandomLeafValue()
		leafPath := leaf.GetFullPath()
		versionCount := leaf.GetVersionCount()
		Expect(versionCount).To(BeNumerically(">", 0))
		versionIdx := testRnd.IntInRange(0, int(versionCount)-1)

		objectVersionChan := testMinioClient.ListObjectsIter(ctx, CBucket2, minio.ListObjectsOptions{
			Prefix:       strings.TrimPrefix(leafPath, "/"),
			WithVersions: true,
		})

		versionId := ""
		currentVersionIdx := 0
		for version := range objectVersionChan {
			Expect(version.Err).NotTo(HaveOccurred())

			if currentVersionIdx == versionIdx {
				versionId = version.VersionID
				break
			}

			currentVersionIdx++
		}
		Expect(versionId).NotTo(BeEmpty())

		leaf.GetVersionCount()
		err := testMinioClient.RemoveObject(ctx, CBucket2, leafPath, minio.RemoveObjectOptions{
			VersionID: versionId,
		})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			_, err := testMinioClient.StatObject(ctx, CBucket2, leafPath, minio.StatObjectOptions{
				VersionID: versionId,
			})
			g.Expect(err).To(HaveOccurred())
			minioErr, ok := err.(minio.ErrorResponse)
			g.Expect(ok).To(BeTrue())
			g.Expect(minioErr.StatusCode).To(Equal(http.StatusNotFound))
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		checkRequest := &pb.StartConsistencyCheckRequest{
			Locations: locations,
			User:      CSyncUserKey,
		}
		_, err = testAPIClient.StartConsistencyCheck(ctx, checkRequest)
		Expect(err).NotTo(HaveOccurred())

		var getCheckResponse *pb.GetConsistencyCheckReportResponse
		Eventually(func(g Gomega) {
			getCheckResponse, err = testAPIClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(getCheckResponse).NotTo(BeNil())
			g.Expect(getCheckResponse.Check).NotTo(BeNil())
			g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
		}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

		Expect(getCheckResponse.Check.Consistent).To(BeFalse())
		Expect(getCheckResponse.Check.WithEtag).To(BeTrue())
		Expect(getCheckResponse.Check.WithSize).To(BeTrue())
		Expect(getCheckResponse.Check.Versioned).To(BeTrue())

		var entries []*pb.ConsistencyCheckReportEntry
		var cursor uint64
		for {
			checkEntries, err := testAPIClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
				Locations: locations,
				PageSize:  1000,
				Cursor:    cursor,
			})
			Expect(err).NotTo(HaveOccurred())

			entries = append(entries, checkEntries.Entries...)

			if checkEntries.Cursor == 0 {
				break
			}

			cursor = checkEntries.Cursor
		}

		expectedDiffCount := (int(versionCount)-versionIdx)*2 - 1
		Expect(entries).To(HaveLen(expectedDiffCount))

		for _, entry := range entries {
			Expect(entry.Object).To(Equal(strings.TrimPrefix(leafPath, "/")))
			for _, storageEntry := range entry.StorageEntries {
				Expect(storageEntry.Storage).To(BeElementOf(CStorage1Key, CStorage2Key))
				Expect(storageEntry.VersionId).NotTo(BeEmpty())
			}
		}
	})
})
