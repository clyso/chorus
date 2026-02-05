package checker2

import (
	"bytes"
	"context"
	"fmt"
	"slices"

	"testing"
	"time"

	cadmin "github.com/ceph/go-ceph/rgw/admin"
	"github.com/clyso/chorus/pkg/api"
	"github.com/clyso/chorus/pkg/config"
	chorusctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/swift"
	"github.com/clyso/chorus/pkg/trace"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/service/worker"
	"github.com/clyso/chorus/service/worker/handler"
	"github.com/clyso/chorus/test/app"
	"github.com/clyso/chorus/test/env"
	"github.com/clyso/chorus/test/gen"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/projects"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/roles"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/users"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/containers"
	"github.com/gophercloud/gophercloud/v2/pagination"
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
	CMinioTestStorageBackend = "minio"
	CCephTestStorageBackend  = "ceph"
	CSwiftTestStorageBackend = "swift"

	CKeystoneInstance = "keystone"
	CMinioInstance    = "minio"
	CCephInstance     = "ceph"
	CSwiftInstance    = "swift"
	CRedisInstance    = "redis"

	CStorage1Key = "storage1"
	CStorage2Key = "storage2"
	CSyncUserKey = "syncuser"

	CKeystoneUrlTemplate = "http://%s:%d/v3"
)

var (
	testRnd        *gen.Rnd
	testTree       *gen.Tree[*gen.GeneratedObject]
	testTreePicker *gen.TreeRandomElementPicker[*gen.GeneratedObject]
	testCredGen    *gen.CredentialsGenerator
)

type TestDependencies struct {
	RedisConfig    *config.Redis
	ObjStoreConfig *objstore.Config
	ObjStoreClient objstore.Common
	Filler         gen.ContentFiller
}

type FakeMetricsSvc struct{}

func (r *FakeMetricsSvc) Count(_ chorusctx.Flow, _ string, _ string) {}

func (r *FakeMetricsSvc) Upload(_ chorusctx.Flow, _ string, _ string, _ int) {}

func (r *FakeMetricsSvc) Download(_ chorusctx.Flow, _ string, _ string, _ int) {}

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Diff Suite")
}

func setupEnvWithMinio(ctx context.Context, tree *gen.Tree[*gen.GeneratedObject], credGen *gen.CredentialsGenerator) (*TestDependencies, error) {
	localTestEnv, err := env.NewTestEnvironment(ctx, map[string]env.ComponentCreationConfig{
		CMinioInstance: env.AsMinio(),
		CRedisInstance: env.AsRedis(),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create test environment: %w", err)
	}

	minioAccessConfig, err := localTestEnv.GetMinioAccessConfig(CMinioInstance)
	if err != nil {
		return nil, fmt.Errorf("unable to get minio env config: %w", err)
	}

	minioS3Endpoint := fmt.Sprintf("%s:%d", minioAccessConfig.Host.Local, minioAccessConfig.S3Port.Forwarded)
	minioAdminClient, err := madmin.NewWithOptions(minioS3Endpoint, &madmin.Options{
		Creds:  mcredentials.NewStaticV4(minioAccessConfig.User, minioAccessConfig.Password, ""),
		Secure: false,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create minio admin config: %w", err)
	}

	ec2Creds := credGen.GenerateEC2Credentials()
	err = minioAdminClient.AddUser(ctx, ec2Creds.Access, ec2Creds.Secret)
	if err != nil {
		return nil, fmt.Errorf("unable to add user: %w", err)
	}

	_, err = minioAdminClient.AttachPolicy(ctx, madmin.PolicyAssociationReq{
		Policies: []string{"readwrite"},
		User:     ec2Creds.Access,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to attach user policy: %w", err)
	}

	minioUserClient, err := minio.New(minioS3Endpoint, &minio.Options{
		Creds:  mcredentials.NewStaticV4(ec2Creds.Access, ec2Creds.Secret, ""),
		Secure: false,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create user client: %w", err)
	}

	storageUrl := fmt.Sprintf("http://%s", minioS3Endpoint)
	storageAddress := s3.StorageAddress{
		Address:  storageUrl,
		Provider: s3.ProviderMinIO,
	}
	syncUserCredentials := s3.CredentialsV4{
		AccessKeyID:     ec2Creds.Access,
		SecretAccessKey: ec2Creds.Secret,
	}
	s3Storages := map[string]s3.Storage{
		CStorage1Key: {
			StorageAddress: storageAddress,
			Credentials: map[string]s3.CredentialsV4{
				CSyncUserKey: syncUserCredentials,
			},
		},
		CStorage2Key: {
			StorageAddress: storageAddress,
			Credentials: map[string]s3.CredentialsV4{
				CSyncUserKey: syncUserCredentials,
			},
		},
	}

	redisAccessConfig, err := localTestEnv.GetRedisAccessConfig(CRedisInstance)
	if err != nil {
		return nil, fmt.Errorf("unable to get redis config: %w", err)
	}

	redisAddress := fmt.Sprintf("%s:%d", redisAccessConfig.Host.Local, redisAccessConfig.Port.Forwarded)
	redisConfig := &config.Redis{
		Addresses: []string{redisAddress},
		Password:  redisAccessConfig.Password,
	}

	objstoreConfig := app.WorkerS3Config(CStorage1Key, s3Storages)
	filler := gen.NewS3Filler(tree, minioUserClient)

	metricsSvc := &FakeMetricsSvc{}
	s3ClientSet, err := s3client.NewClient(ctx, metricsSvc, storageAddress, syncUserCredentials, "", "")
	if err != nil {
		return nil, fmt.Errorf("unable to create s3 client set: %w", err)
	}

	return &TestDependencies{
		ObjStoreConfig: &objstoreConfig,
		ObjStoreClient: objstore.WrapS3common(s3ClientSet, s3.ProviderMinIO),
		Filler:         filler,
		RedisConfig:    redisConfig,
	}, nil
}

func setupEnvWithCeph(ctx context.Context, tree *gen.Tree[*gen.GeneratedObject], credGen *gen.CredentialsGenerator) (*TestDependencies, error) {
	localTestEnv, err := env.NewTestEnvironment(ctx, map[string]env.ComponentCreationConfig{
		CCephInstance:  env.AsCeph(),
		CRedisInstance: env.AsRedis(),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create test environment: %w", err)
	}

	cephAccessConfig, err := localTestEnv.GetCephAccessConfig(CCephInstance)
	if err != nil {
		return nil, fmt.Errorf("unable to get ceph config: %w", err)
	}

	cephAdminEndpoint := fmt.Sprintf("http://%s:%d", cephAccessConfig.Host.Local, cephAccessConfig.Port.Forwarded)
	cephAdminClient, err := cadmin.New(cephAdminEndpoint, cephAccessConfig.SystemUser, cephAccessConfig.SystemPassword, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to get ceph admin client: %w", err)
	}

	usernamePassword := credGen.GenerateUsernamePassword()
	ec2Creds := credGen.GenerateEC2Credentials()
	cephUser := cadmin.User{
		ID:          usernamePassword.Username,
		DisplayName: usernamePassword.Username,
		Keys: []cadmin.UserKeySpec{
			{
				AccessKey: ec2Creds.Access,
				SecretKey: ec2Creds.Secret,
			},
		},
	}
	_, err = cephAdminClient.CreateUser(ctx, cephUser)
	if err != nil {
		return nil, fmt.Errorf("unable to create user: %w", err)
	}

	minioS3Endpoint := fmt.Sprintf("%s:%d", cephAccessConfig.Host.Local, cephAccessConfig.Port.Forwarded)
	minioUserClient, err := minio.New(minioS3Endpoint, &minio.Options{
		Creds:  mcredentials.NewStaticV4(ec2Creds.Access, ec2Creds.Secret, ""),
		Secure: false,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create user client: %w", err)
	}

	storageURL := fmt.Sprintf("http://%s", minioS3Endpoint)
	storageAddress := s3.StorageAddress{
		Address:  storageURL,
		Provider: s3.ProviderCeph,
	}
	syncUserCredentials := s3.CredentialsV4{
		AccessKeyID:     ec2Creds.Access,
		SecretAccessKey: ec2Creds.Secret,
	}
	s3Storages := map[string]s3.Storage{
		CStorage1Key: {
			StorageAddress: storageAddress,
			Credentials: map[string]s3.CredentialsV4{
				CSyncUserKey: syncUserCredentials,
			},
		},
		CStorage2Key: {
			StorageAddress: storageAddress,
			Credentials: map[string]s3.CredentialsV4{
				CSyncUserKey: syncUserCredentials,
			},
		},
	}

	redisAccessConfig, err := localTestEnv.GetRedisAccessConfig(CRedisInstance)
	if err != nil {
		return nil, fmt.Errorf("unable to get redis config: %w", err)
	}

	redisAddress := fmt.Sprintf("%s:%d", redisAccessConfig.Host.Local, redisAccessConfig.Port.Forwarded)
	redisConfig := &config.Redis{
		Addresses: []string{redisAddress},
		Password:  redisAccessConfig.Password,
	}

	objstoreConfig := app.WorkerS3Config(CStorage1Key, s3Storages)
	filler := gen.NewS3Filler(tree, minioUserClient)

	metricsSvc := &FakeMetricsSvc{}
	s3ClientSet, err := s3client.NewClient(ctx, metricsSvc, storageAddress, syncUserCredentials, "", "")
	if err != nil {
		return nil, fmt.Errorf("unable to create s3 client set: %w", err)
	}

	return &TestDependencies{
		ObjStoreConfig: &objstoreConfig,
		ObjStoreClient: objstore.WrapS3common(s3ClientSet, s3.ProviderCeph),
		Filler:         filler,
		RedisConfig:    redisConfig,
	}, nil
}

func setupEnvWithSwift(ctx context.Context, tree *gen.Tree[*gen.GeneratedObject], credGen *gen.CredentialsGenerator) (*TestDependencies, error) {
	localTestEnv, err := env.NewTestEnvironment(ctx, map[string]env.ComponentCreationConfig{
		CKeystoneInstance: env.AsKeystone(),
		CSwiftInstance:    env.AsSwift(CKeystoneInstance),
		CRedisInstance:    env.AsRedis(),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create environment: %w", err)
	}

	keystoneAccessConfig, err := localTestEnv.GetKeystoneAccessConfig(CKeystoneInstance)
	if err != nil {
		return nil, fmt.Errorf("unable to create access config: %w", err)
	}
	swiftAccessConfig, err := localTestEnv.GetSwiftAccessConfig(CSwiftInstance)
	if err != nil {
		return nil, fmt.Errorf("unable to get swift config: %w", err)
	}

	keystoneUrl := fmt.Sprintf(CKeystoneUrlTemplate, keystoneAccessConfig.Host.Local, keystoneAccessConfig.ExternalPort.Forwarded)
	providerClient, err := openstack.AuthenticatedClient(ctx, gophercloud.AuthOptions{
		IdentityEndpoint: keystoneUrl,
		Username:         keystoneAccessConfig.User,
		Password:         keystoneAccessConfig.Password,
		DomainName:       keystoneAccessConfig.DefaultDomain.Name,
		TenantName:       keystoneAccessConfig.TenantName,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to get admin client: %w", err)
	}

	identityClient, err := openstack.NewIdentityV3(providerClient, gophercloud.EndpointOpts{})
	if err != nil {
		return nil, fmt.Errorf("unable to get identity client: %w", err)
	}

	newProjectName := credGen.GenerateKeystoneName()
	newProject, err := projects.Create(ctx, identityClient, projects.CreateOpts{
		DomainID: keystoneAccessConfig.DefaultDomain.ID,
		Name:     newProjectName,
	}).Extract()
	if err != nil {
		return nil, fmt.Errorf("unable to create project: %w", err)
	}

	newUsernamePassword := credGen.GenerateUsernamePassword()
	newUser, err := users.Create(ctx, identityClient, users.CreateOpts{
		Name:     newUsernamePassword.Username,
		Password: newUsernamePassword.Password,
		DomainID: keystoneAccessConfig.DefaultDomain.ID,
	}).Extract()
	if err != nil {
		return nil, fmt.Errorf("unable to create user: %w", err)
	}

	if err = roles.Assign(ctx, identityClient, swiftAccessConfig.Keystone.OperatorRole.ID, roles.AssignOpts{
		UserID:    newUser.ID,
		ProjectID: newProject.ID,
	}).ExtractErr(); err != nil {
		return nil, fmt.Errorf("unable to assign role: %w", err)
	}

	storageAddress := swift.StorageAddress{
		StorageEndpointName: swiftAccessConfig.Keystone.EndpointName,
		StorageEndpointType: swiftAccessConfig.Keystone.ServiceType,
		AuthURL:             keystoneUrl,
	}

	storageCredentials := swift.Credentials{
		Username:   newUsernamePassword.Username,
		Password:   newUsernamePassword.Password,
		DomainName: keystoneAccessConfig.DefaultDomain.Name,
		TenantName: newProjectName,
	}

	swiftUserClient, err := swift.NewClient(ctx, storageAddress, storageCredentials)
	if err != nil {
		return nil, fmt.Errorf("unable to create swift client: %w", err)
	}

	paginator := containers.List(swiftUserClient, containers.ListOpts{})
	paginator.EachPage(ctx, func(ctx context.Context, p pagination.Page) (bool, error) {
		return false, nil
	})

	swiftStorages := map[string]swift.Storage{
		CStorage1Key: {
			StorageAddress: storageAddress,
			Credentials: map[string]swift.Credentials{
				CSyncUserKey: storageCredentials,
			},
		},
		CStorage2Key: {
			StorageAddress: storageAddress,
			Credentials: map[string]swift.Credentials{
				CSyncUserKey: storageCredentials,
			},
		},
	}

	redisAccessConfig, err := localTestEnv.GetRedisAccessConfig(CRedisInstance)
	if err != nil {
		return nil, fmt.Errorf("unable to get redis config: %w", err)
	}

	redisAddress := fmt.Sprintf("%s:%d", redisAccessConfig.Host.Local, redisAccessConfig.Port.Forwarded)
	redisConfig := &config.Redis{
		Addresses: []string{redisAddress},
		Password:  redisAccessConfig.Password,
	}

	objstoreConfig := app.WorkerSwiftConfig(CStorage1Key, swiftStorages)
	filler := gen.NewSwiftFiller(tree, swiftUserClient)

	return &TestDependencies{
		ObjStoreConfig: &objstoreConfig,
		ObjStoreClient: objstore.WrapSwiftCommon(swiftUserClient),
		Filler:         filler,
		RedisConfig:    redisConfig,
	}, nil
}

func newWorkerConfig(objStoreConfig *objstore.Config, redisConfig *config.Redis, grpcPort int, httpPort int) *worker.Config {
	workerConf := &worker.Config{
		Common: config.Common{
			Features: &features.Config{
				ACL:        false,
				Tagging:    false,
				Versioning: true,
			},
			Log: &log.Config{
				Level: "warn",
			},
			Redis: redisConfig,
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
		Worker: &handler.Config{
			SwitchRetryInterval: time.Millisecond * 500,
			PauseRetryInterval:  time.Millisecond * 500,
		},
		Storage: *objStoreConfig,
		Api: &api.Config{
			Enabled:  true,
			GrpcPort: grpcPort,
			HttpPort: httpPort,
		},
	}

	return workerConf
}

var _ = BeforeSuite(func() {
	var rnd *gen.Rnd

	if gen.CUseTestGenSeed {
		rnd = gen.NewRnd(gen.CTestGenSeed)
	} else {
		rnd = gen.NewRnd(GinkgoRandomSeed())
	}

	objGen := gen.NewCommonObjectGenerator(
		gen.WithVersioned(),
		gen.WithVersionRange(2, 10),
	)
	genOpts := []gen.TreeGeneratorOption[*gen.GeneratedObject]{
		gen.WithObjectGenerator(objGen),
		gen.WithRnd[*gen.GeneratedObject](rnd),
		gen.WithForceTargetDepth[*gen.GeneratedObject](),
	}
	treeGen, err := gen.NewTreeGenerator(genOpts...)
	Expect(err).NotTo(HaveOccurred())

	tree, err := treeGen.Generate()
	Expect(err).NotTo(HaveOccurred())

	picker := gen.NewTreeRandomElementPicker(tree, rnd)

	credGen := gen.NewCredentialsGenerator(rnd)

	testRnd = rnd
	testTree = tree
	testTreePicker = picker
	testCredGen = credGen
})

var _ = Describe("Diff scenarious", Ordered, func() {
	testStorageBackends := []string{
		CMinioTestStorageBackend,
		// tests with followingg backends are taking significant amount of time
		// e.g. 30 minustes for swift
		// CCephTestStorageBackend,
		// CSwiftTestStorageBackend,
	}

	for _, testStorageBackend := range testStorageBackends {
		var ctx context.Context
		var cancelFunc context.CancelFunc
		var diffClient pb.DiffClient
		var chorusClient pb.ChorusClient
		var storeClient objstore.Common
		var contentFiller gen.ContentFiller

		BeforeAll(func() {
			ctx, cancelFunc = context.WithCancel(context.Background())

			var testDependencies *TestDependencies
			var err error
			switch testStorageBackend {
			case CMinioTestStorageBackend:
				testDependencies, err = setupEnvWithMinio(ctx, testTree, testCredGen)
			case CCephTestStorageBackend:
				testDependencies, err = setupEnvWithCeph(ctx, testTree, testCredGen)
			case CSwiftTestStorageBackend:
				testDependencies, err = setupEnvWithSwift(ctx, testTree, testCredGen)
			default:
				Fail(fmt.Sprintf("unrecognized storage type %s", testStorageBackend))
			}
			Expect(err).NotTo(HaveOccurred())
			Expect(testDependencies).NotTo(BeNil())
			storeClient = testDependencies.ObjStoreClient
			contentFiller = testDependencies.Filler

			grpcPort, err := env.RandomFreePort()
			Expect(err).NotTo(HaveOccurred())
			httpPort, err := env.RandomFreePort()
			Expect(err).NotTo(HaveOccurred())

			err = testDependencies.ObjStoreConfig.Validate()
			Expect(err).NotTo(HaveOccurred())

			workerConf := newWorkerConfig(testDependencies.ObjStoreConfig, testDependencies.RedisConfig, grpcPort, httpPort)

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
			diffClient = pb.NewDiffClient(grpcConn)
			chorusClient = pb.NewChorusClient(grpcConn)

			Eventually(func(g Gomega) {
				_, err := chorusClient.GetAppVersion(ctx, &emptypb.Empty{})
				g.Expect(err).NotTo(HaveOccurred())
			}, 1*time.Minute, time.Second).Should(Succeed())
		})

		AfterAll(func() {
			cancelFunc()
		})

		Context(fmt.Sprintf("For %s unversioned buckets", testStorageBackend), func() {
			var bucket1Name string
			var bucket2Name string
			var locations []*pb.MigrateLocation

			BeforeEach(func() {
				bucket1Name = testCredGen.GenerateBucketName()
				err := storeClient.CreateBucket(ctx, bucket1Name)
				Expect(err).NotTo(HaveOccurred())

				bucket2Name = testCredGen.GenerateBucketName()
				err = storeClient.CreateBucket(ctx, bucket2Name)
				Expect(err).NotTo(HaveOccurred())

				err = contentFiller.FillBucketWithLastVersions(ctx, bucket1Name)
				Expect(err).NotTo(HaveOccurred())
				err = contentFiller.FillBucketWithLastVersions(ctx, bucket2Name)
				Expect(err).NotTo(HaveOccurred())

				locations = []*pb.MigrateLocation{
					{
						Storage: CStorage1Key,
						Bucket:  bucket1Name,
					},
					{
						Storage: CStorage2Key,
						Bucket:  bucket2Name,
					},
				}
			})

			AfterEach(func() {
				_, err := diffClient.DeleteReport(ctx, &pb.ConsistencyCheckRequest{
					Locations: locations,
				})
				Expect(err).NotTo(HaveOccurred())
			})

			It("Should succeed", func() {
				checkRequest := &pb.StartConsistencyCheckRequest{
					Locations: locations,
					User:      CSyncUserKey,
				}
				_, err := diffClient.Start(ctx, checkRequest)
				Expect(err).NotTo(HaveOccurred())

				var getCheckResponse *pb.GetConsistencyCheckReportResponse
				Eventually(func(g Gomega) {
					getCheckResponse, err = diffClient.GetReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(getCheckResponse).NotTo(BeNil())
					g.Expect(getCheckResponse.Check).NotTo(BeNil())
					g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				Expect(getCheckResponse.Check.Consistent).To(BeTrue())
				Expect(getCheckResponse.Check.WithEtag).To(BeTrue())
				Expect(getCheckResponse.Check.WithSize).To(BeTrue())
				Expect(getCheckResponse.Check.Versioned).To(BeFalse())

				checkEntries, err := diffClient.GetReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
					Locations: locations,
					PageSize:  10,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(checkEntries.Entries).To(HaveLen(0))
			})

			It("Should succeed, check only sizes", func() {
				joint := testTreePicker.RandomJointValue()
				jointPath := joint.GetFullPath()

				objectPath := jointPath + "filewithdiffetag"

				err := storeClient.PutObject(ctx, bucket1Name, objectPath, bytes.NewReader([]byte{1, 2, 3, 4}), 4)
				Expect(err).NotTo(HaveOccurred())
				err = storeClient.PutObject(ctx, bucket2Name, objectPath, bytes.NewReader([]byte{5, 6, 7, 8}), 4)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func(g Gomega) {
					exists, err := storeClient.ObjectExists(ctx, bucket1Name, objectPath)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(exists).To(BeTrue())
					exists, err = storeClient.ObjectExists(ctx, bucket2Name, objectPath)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(exists).To(BeTrue())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				checkRequest := &pb.StartConsistencyCheckRequest{
					Locations:   locations,
					User:        CSyncUserKey,
					IgnoreEtags: true,
				}
				_, err = diffClient.Start(ctx, checkRequest)
				Expect(err).NotTo(HaveOccurred())

				var getCheckResponse *pb.GetConsistencyCheckReportResponse
				Eventually(func(g Gomega) {
					getCheckResponse, err = diffClient.GetReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(getCheckResponse).NotTo(BeNil())
					g.Expect(getCheckResponse.Check).NotTo(BeNil())
					g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				Expect(getCheckResponse.Check.Consistent).To(BeTrue())
				Expect(getCheckResponse.Check.WithEtag).To(BeFalse())
				Expect(getCheckResponse.Check.WithSize).To(BeTrue())
				Expect(getCheckResponse.Check.Versioned).To(BeFalse())

				checkEntries, err := diffClient.GetReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
					Locations: locations,
					PageSize:  10,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(checkEntries.Entries).To(HaveLen(0))
			})

			It("Should succeed, check only list", func() {
				joint := testTreePicker.RandomJointValue()
				jointPath := joint.GetFullPath()

				objectPath := jointPath + "filewithdiffetag"

				err := storeClient.PutObject(ctx, bucket1Name, objectPath, bytes.NewReader([]byte{1, 2, 3, 4}), 4)
				Expect(err).NotTo(HaveOccurred())
				err = storeClient.PutObject(ctx, bucket2Name, objectPath, bytes.NewReader([]byte{5, 6, 7, 8, 9}), 5)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func(g Gomega) {
					exists, err := storeClient.ObjectExists(ctx, bucket1Name, objectPath)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(exists).To(BeTrue())
					exists, err = storeClient.ObjectExists(ctx, bucket2Name, objectPath)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(exists).To(BeTrue())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				checkRequest := &pb.StartConsistencyCheckRequest{
					Locations:   locations,
					User:        CSyncUserKey,
					IgnoreSizes: true,
				}
				_, err = diffClient.Start(ctx, checkRequest)
				Expect(err).NotTo(HaveOccurred())

				var getCheckResponse *pb.GetConsistencyCheckReportResponse
				Eventually(func(g Gomega) {
					getCheckResponse, err = diffClient.GetReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(getCheckResponse).NotTo(BeNil())
					g.Expect(getCheckResponse.Check).NotTo(BeNil())
					g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				Expect(getCheckResponse.Check.Consistent).To(BeTrue())
				Expect(getCheckResponse.Check.WithEtag).To(BeFalse())
				Expect(getCheckResponse.Check.WithSize).To(BeFalse())
				Expect(getCheckResponse.Check.Versioned).To(BeFalse())

				checkEntries, err := diffClient.GetReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
					Locations: locations,
					PageSize:  10,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(checkEntries.Entries).To(HaveLen(0))
			})

			It("Should fail, no object", func() {
				leaf := testTreePicker.RandomLeafValue()
				leafPath := leaf.GetFullPath()

				Eventually(func(g Gomega) {
					exists, err := storeClient.ObjectExists(ctx, bucket2Name, leafPath)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(exists).To(BeTrue())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				err := storeClient.RemoveObject(ctx, bucket2Name, leafPath)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func(g Gomega) {
					exists, err := storeClient.ObjectExists(ctx, bucket2Name, leafPath)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(exists).To(BeFalse())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				checkRequest := &pb.StartConsistencyCheckRequest{
					Locations: locations,
					User:      CSyncUserKey,
				}
				_, err = diffClient.Start(ctx, checkRequest)
				Expect(err).NotTo(HaveOccurred())

				var getCheckResponse *pb.GetConsistencyCheckReportResponse
				Eventually(func(g Gomega) {
					getCheckResponse, err = diffClient.GetReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
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
					checkEntries, err := diffClient.GetReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
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
				jointSubtree := testTreePicker.RandomJointSubtree()
				subtreeObjects := slices.Collect(jointSubtree.WidthFirstValueIterator().Must())
				objectNamesToRemove := make([]string, 0, len(subtreeObjects))
				for _, subtreeObject := range subtreeObjects {
					objectNamesToRemove = append(objectNamesToRemove, subtreeObject.GetFullPath())
				}
				jointPath := objectNamesToRemove[0]
				slices.Reverse(objectNamesToRemove)

				Eventually(func(g Gomega) {
					exists, err := storeClient.ObjectExists(ctx, bucket2Name, jointPath)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(exists).To(BeTrue())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				err := storeClient.RemoveObjects(ctx, bucket2Name, objectNamesToRemove)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func(g Gomega) {
					exists, err := storeClient.ObjectExists(ctx, bucket2Name, jointPath)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(exists).To(BeFalse())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				checkRequest := &pb.StartConsistencyCheckRequest{
					Locations: locations,
					User:      CSyncUserKey,
				}
				_, err = diffClient.Start(ctx, checkRequest)
				Expect(err).NotTo(HaveOccurred())

				var getCheckResponse *pb.GetConsistencyCheckReportResponse
				Eventually(func(g Gomega) {
					getCheckResponse, err = diffClient.GetReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
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
					checkEntries, err := diffClient.GetReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
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
				joint := testTreePicker.RandomJointValue()
				jointPath := joint.GetFullPath()

				emptyDirPath := jointPath + "emptydirectory/"

				err := storeClient.PutObject(ctx, bucket1Name, emptyDirPath, bytes.NewReader([]byte{}), 0)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func(g Gomega) {
					exists, err := storeClient.ObjectExists(ctx, bucket1Name, emptyDirPath)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(exists).To(BeTrue())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				checkRequest := &pb.StartConsistencyCheckRequest{
					Locations: locations,
					User:      CSyncUserKey,
				}
				_, err = diffClient.Start(ctx, checkRequest)
				Expect(err).NotTo(HaveOccurred())

				var getCheckResponse *pb.GetConsistencyCheckReportResponse
				Eventually(func(g Gomega) {
					getCheckResponse, err = diffClient.GetReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
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
					checkEntries, err := diffClient.GetReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
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
					Expect(entry.Object).To(Equal(emptyDirPath))
					for _, storageEntry := range entry.StorageEntries {
						Expect(storageEntry.Storage).To(Equal(CStorage1Key))
					}
				}
			})

			It("Should fail, wrong etag", func() {
				joint := testTreePicker.RandomJointValue()
				jointPath := joint.GetFullPath()

				objectPath := jointPath + "filewithdiffetag"

				err := storeClient.PutObject(ctx, bucket1Name, objectPath, bytes.NewReader([]byte{1, 2, 3, 4}), 4)
				Expect(err).NotTo(HaveOccurred())
				err = storeClient.PutObject(ctx, bucket2Name, objectPath, bytes.NewReader([]byte{5, 6, 7, 8}), 4)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func(g Gomega) {
					exists, err := storeClient.ObjectExists(ctx, bucket1Name, objectPath)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(exists).To(BeTrue())
					exists, err = storeClient.ObjectExists(ctx, bucket2Name, objectPath)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(exists).To(BeTrue())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				checkRequest := &pb.StartConsistencyCheckRequest{
					Locations: locations,
					User:      CSyncUserKey,
				}
				_, err = diffClient.Start(ctx, checkRequest)
				Expect(err).NotTo(HaveOccurred())

				var getCheckResponse *pb.GetConsistencyCheckReportResponse
				Eventually(func(g Gomega) {
					getCheckResponse, err = diffClient.GetReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
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
					checkEntries, err := diffClient.GetReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
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
					Expect(entry.Object).To(Equal(objectPath))
					for _, storageEntry := range entry.StorageEntries {
						Expect(storageEntry.Storage).To(BeElementOf(CStorage1Key, CStorage2Key))
						Expect(storageEntry.VersionId).To(BeEmpty())
					}
				}
			})
		})

		Context(fmt.Sprintf("For %s versioned buckets", testStorageBackend), func() {
			var bucket1Name string
			var bucket2Name string
			var locations []*pb.MigrateLocation

			BeforeEach(func() {
				bucket1Name = testCredGen.GenerateBucketName()
				err := storeClient.CreateBucket(ctx, bucket1Name)
				Expect(err).NotTo(HaveOccurred())
				err = storeClient.EnableBucketVersioning(ctx, bucket1Name)
				Expect(err).NotTo(HaveOccurred())

				bucket2Name = testCredGen.GenerateBucketName()
				err = storeClient.CreateBucket(ctx, bucket2Name)
				Expect(err).NotTo(HaveOccurred())
				err = storeClient.EnableBucketVersioning(ctx, bucket2Name)
				Expect(err).NotTo(HaveOccurred())

				err = contentFiller.FillBucket(ctx, bucket1Name)
				Expect(err).NotTo(HaveOccurred())
				err = contentFiller.FillBucket(ctx, bucket2Name)
				Expect(err).NotTo(HaveOccurred())

				locations = []*pb.MigrateLocation{
					{
						Storage: CStorage1Key,
						Bucket:  bucket1Name,
					},
					{
						Storage: CStorage2Key,
						Bucket:  bucket2Name,
					},
				}
			})

			AfterEach(func() {
				_, err := diffClient.DeleteReport(ctx, &pb.ConsistencyCheckRequest{
					Locations: locations,
				})
				Expect(err).NotTo(HaveOccurred())
			})

			It("Should succeed", func() {
				checkRequest := &pb.StartConsistencyCheckRequest{
					Locations: locations,
					User:      CSyncUserKey,
				}
				_, err := diffClient.Start(ctx, checkRequest)
				Expect(err).NotTo(HaveOccurred())

				var getCheckResponse *pb.GetConsistencyCheckReportResponse
				Eventually(func(g Gomega) {
					getCheckResponse, err = diffClient.GetReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(getCheckResponse).NotTo(BeNil())
					g.Expect(getCheckResponse.Check).NotTo(BeNil())
					g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				Expect(getCheckResponse.Check.Consistent).To(BeTrue())
				Expect(getCheckResponse.Check.WithEtag).To(BeTrue())
				Expect(getCheckResponse.Check.WithSize).To(BeTrue())
				Expect(getCheckResponse.Check.Versioned).To(BeTrue())

				checkEntries, err := diffClient.GetReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
					Locations: locations,
					PageSize:  10,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(checkEntries.Entries).To(HaveLen(0))
			})

			It("Should succeed, check only sizes", func() {
				leaf := testTreePicker.RandomLeafValue()
				objectPath := leaf.GetFullPath()

				err := storeClient.PutObject(ctx, bucket1Name, objectPath, bytes.NewReader([]byte{1, 2, 3, 4}), 4)
				Expect(err).NotTo(HaveOccurred())
				err = storeClient.PutObject(ctx, bucket2Name, objectPath, bytes.NewReader([]byte{5, 6, 7, 8}), 4)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func(g Gomega) {
					stat1, err := storeClient.ObjectInfo(ctx, bucket1Name, objectPath)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(stat1.Size).To(BeNumerically("==", 4))
					stat2, err := storeClient.ObjectInfo(ctx, bucket2Name, objectPath)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(stat2.Size).To(BeNumerically("==", 4))
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				checkRequest := &pb.StartConsistencyCheckRequest{
					Locations:   locations,
					User:        CSyncUserKey,
					IgnoreEtags: true,
				}
				_, err = diffClient.Start(ctx, checkRequest)
				Expect(err).NotTo(HaveOccurred())

				var getCheckResponse *pb.GetConsistencyCheckReportResponse
				Eventually(func(g Gomega) {
					getCheckResponse, err = diffClient.GetReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(getCheckResponse).NotTo(BeNil())
					g.Expect(getCheckResponse.Check).NotTo(BeNil())
					g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				Expect(getCheckResponse.Check.Consistent).To(BeTrue())
				Expect(getCheckResponse.Check.WithEtag).To(BeFalse())
				Expect(getCheckResponse.Check.WithSize).To(BeTrue())
				Expect(getCheckResponse.Check.Versioned).To(BeTrue())

				checkEntries, err := diffClient.GetReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
					Locations: locations,
					PageSize:  10,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(checkEntries.Entries).To(HaveLen(0))
			})

			It("Should succeed, check only list", func() {
				leaf := testTreePicker.RandomLeafValue()
				objectPath := leaf.GetFullPath()

				err := storeClient.PutObject(ctx, bucket1Name, objectPath, bytes.NewReader([]byte{1, 2, 3, 4}), 4)
				Expect(err).NotTo(HaveOccurred())
				err = storeClient.PutObject(ctx, bucket2Name, objectPath, bytes.NewReader([]byte{5, 6, 7, 8, 9}), 5)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func(g Gomega) {
					stat1, err := storeClient.ObjectInfo(ctx, bucket1Name, objectPath)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(stat1.Size).To(BeNumerically("==", 4))
					stat2, err := storeClient.ObjectInfo(ctx, bucket2Name, objectPath)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(stat2.Size).To(BeNumerically("==", 5))
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				checkRequest := &pb.StartConsistencyCheckRequest{
					Locations:   locations,
					User:        CSyncUserKey,
					IgnoreSizes: true,
				}
				_, err = diffClient.Start(ctx, checkRequest)
				Expect(err).NotTo(HaveOccurred())

				var getCheckResponse *pb.GetConsistencyCheckReportResponse
				Eventually(func(g Gomega) {
					getCheckResponse, err = diffClient.GetReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(getCheckResponse).NotTo(BeNil())
					g.Expect(getCheckResponse.Check).NotTo(BeNil())
					g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				Expect(getCheckResponse.Check.Consistent).To(BeTrue())
				Expect(getCheckResponse.Check.WithEtag).To(BeFalse())
				Expect(getCheckResponse.Check.WithSize).To(BeFalse())
				Expect(getCheckResponse.Check.Versioned).To(BeTrue())

				checkEntries, err := diffClient.GetReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
					Locations: locations,
					PageSize:  10,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(checkEntries.Entries).To(HaveLen(0))
			})

			It("Should succeed, check only last version", func() {
				leaf := testTreePicker.RandomLeafValue()
				leafPath := leaf.GetFullPath()
				versionCount := leaf.GetVersionCount()
				Expect(versionCount).To(BeNumerically(">", 0))
				versionIdx := testRnd.IntInRange(1, int(versionCount)-1)

				objectIter := storeClient.ListObjects(ctx, bucket2Name, objstore.WithPrefix(leafPath), objstore.WithVersions())

				versionId := ""
				currentVersionIdx := 0
				for objectInfo, err := range objectIter {
					Expect(err).NotTo(HaveOccurred())

					if currentVersionIdx == versionIdx {
						versionId = objectInfo.VersionID
						break
					}

					currentVersionIdx++
				}
				Expect(versionId).NotTo(BeEmpty())

				err := storeClient.RemoveObject(ctx, bucket2Name, leafPath, objstore.WithVersionID(versionId))
				Expect(err).NotTo(HaveOccurred())

				Eventually(func(g Gomega) {
					exists, err := storeClient.ObjectExists(ctx, bucket2Name, leafPath, objstore.WithVersionID(versionId))
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(exists).To(BeFalse())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				checkRequest := &pb.StartConsistencyCheckRequest{
					Locations:             locations,
					User:                  CSyncUserKey,
					CheckOnlyLastVersions: true,
				}
				_, err = diffClient.Start(ctx, checkRequest)
				Expect(err).NotTo(HaveOccurred())

				var getCheckResponse *pb.GetConsistencyCheckReportResponse
				Eventually(func(g Gomega) {
					getCheckResponse, err = diffClient.GetReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(getCheckResponse).NotTo(BeNil())
					g.Expect(getCheckResponse.Check).NotTo(BeNil())
					g.Expect(getCheckResponse.Check.Ready).To(BeTrue())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				Expect(getCheckResponse.Check.Consistent).To(BeTrue())
				Expect(getCheckResponse.Check.WithEtag).To(BeTrue())
				Expect(getCheckResponse.Check.WithSize).To(BeTrue())
				Expect(getCheckResponse.Check.Versioned).To(BeFalse())

				checkEntries, err := diffClient.GetReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
					Locations: locations,
					PageSize:  10,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(checkEntries.Entries).To(HaveLen(0))
			})

			It("Should fail, no object", func() {
				leaf := testTreePicker.RandomLeafValue()
				leafPath := leaf.GetFullPath()

				err := storeClient.RemoveObject(ctx, bucket2Name, leafPath)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func(g Gomega) {
					exists, err := storeClient.ObjectExists(ctx, bucket2Name, leafPath)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(exists).To(BeFalse())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				checkRequest := &pb.StartConsistencyCheckRequest{
					Locations: locations,
					User:      CSyncUserKey,
				}
				_, err = diffClient.Start(ctx, checkRequest)
				Expect(err).NotTo(HaveOccurred())

				var getCheckResponse *pb.GetConsistencyCheckReportResponse
				Eventually(func(g Gomega) {
					getCheckResponse, err = diffClient.GetReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
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
					checkEntries, err := diffClient.GetReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
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

				var expectedDiffCount int
				if testStorageBackend == CSwiftTestStorageBackend {
					expectedDiffCount = int(leaf.GetVersionCount()) * 2
				} else {
					expectedDiffCount = int(leaf.GetVersionCount())
				}
				Expect(entries).To(HaveLen(expectedDiffCount))

				for _, entry := range entries {
					Expect(entry.Object).To(Equal(leafPath))
					for _, storageEntry := range entry.StorageEntries {
						Expect(storageEntry.Storage).To(Equal(CStorage1Key))
						Expect(storageEntry.VersionId).NotTo(BeEmpty())
					}
				}
			})

			It("Should fail, no directory", func() {
				jointSubtree := testTreePicker.RandomJointSubtree()
				subtreeObjects := slices.Collect(jointSubtree.WidthFirstValueIterator().Must())
				objectNamesToRemove := make([]string, 0, len(subtreeObjects))
				for _, subtreeObject := range subtreeObjects {
					objectNamesToRemove = append(objectNamesToRemove, subtreeObject.GetFullPath())
				}
				jointPath := objectNamesToRemove[0]
				slices.Reverse(objectNamesToRemove)

				err := storeClient.RemoveObjects(ctx, bucket2Name, objectNamesToRemove)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func(g Gomega) {
					exists, err := storeClient.ObjectExists(ctx, bucket2Name, jointPath)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(exists).To(BeFalse())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				checkRequest := &pb.StartConsistencyCheckRequest{
					Locations: locations,
					User:      CSyncUserKey,
				}
				_, err = diffClient.Start(ctx, checkRequest)
				Expect(err).NotTo(HaveOccurred())

				var getCheckResponse *pb.GetConsistencyCheckReportResponse
				Eventually(func(g Gomega) {
					getCheckResponse, err = diffClient.GetReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
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
					checkEntries, err := diffClient.GetReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
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

				var expectedDiffCount int
				for value := range jointSubtree.WidthFirstValueIterator().Must() {
					if testStorageBackend == CSwiftTestStorageBackend && value.GetNodeType() == gen.CLeafTreeNodeType {
						expectedDiffCount += int(value.GetVersionCount()) * 2
					} else {
						expectedDiffCount += int(value.GetVersionCount())
					}
				}

				Expect(entries).To(HaveLen(expectedDiffCount))

				for _, entry := range entries {
					Expect(entry.Object).To(HavePrefix(jointPath))
					for _, storageEntry := range entry.StorageEntries {
						Expect(storageEntry.Storage).To(Equal(CStorage1Key))
					}
				}
			})

			It("Should fail, no empty dir", func() {
				joint := testTreePicker.RandomJointValue()
				jointPath := joint.GetFullPath()

				emptyDirPath := jointPath + "emptydirectory/"

				err := storeClient.PutObject(ctx, bucket1Name, emptyDirPath, bytes.NewReader([]byte{}), 0)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func(g Gomega) {
					exists, err := storeClient.ObjectExists(ctx, bucket1Name, emptyDirPath)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(exists).To(BeTrue())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				checkRequest := &pb.StartConsistencyCheckRequest{
					Locations: locations,
					User:      CSyncUserKey,
				}
				_, err = diffClient.Start(ctx, checkRequest)
				Expect(err).NotTo(HaveOccurred())

				var getCheckResponse *pb.GetConsistencyCheckReportResponse
				Eventually(func(g Gomega) {
					getCheckResponse, err = diffClient.GetReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
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
					checkEntries, err := diffClient.GetReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
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
					Expect(entry.Object).To(Equal(emptyDirPath))
					for _, storageEntry := range entry.StorageEntries {
						Expect(storageEntry.Storage).To(Equal(CStorage1Key))
					}
				}
			})

			It("Should fail, wrong etag", func() {
				joint := testTreePicker.RandomJointValue()
				jointPath := joint.GetFullPath()

				objectPath := jointPath + "filewithdiffetag"

				err := storeClient.PutObject(ctx, bucket1Name, objectPath, bytes.NewReader([]byte{1, 2, 3, 4}), 4)
				Expect(err).NotTo(HaveOccurred())
				err = storeClient.PutObject(ctx, bucket2Name, objectPath, bytes.NewReader([]byte{5, 6, 7, 8}), 4)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func(g Gomega) {
					exists, err := storeClient.ObjectExists(ctx, bucket1Name, objectPath)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(exists).To(BeTrue())
					exists, err = storeClient.ObjectExists(ctx, bucket2Name, objectPath)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(exists).To(BeTrue())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				checkRequest := &pb.StartConsistencyCheckRequest{
					Locations: locations,
					User:      CSyncUserKey,
				}
				_, err = diffClient.Start(ctx, checkRequest)
				Expect(err).NotTo(HaveOccurred())

				var getCheckResponse *pb.GetConsistencyCheckReportResponse
				Eventually(func(g Gomega) {
					getCheckResponse, err = diffClient.GetReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
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
					checkEntries, err := diffClient.GetReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
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
					Expect(entry.Object).To(Equal(objectPath))
					for _, storageEntry := range entry.StorageEntries {
						Expect(storageEntry.Storage).To(BeElementOf(CStorage1Key, CStorage2Key))
						Expect(storageEntry.VersionId).NotTo(BeEmpty())
					}
				}
			})

			It("Should fail, wrong version count", func() {
				leaf := testTreePicker.RandomLeafValue()
				leafPath := leaf.GetFullPath()
				versionCount := leaf.GetVersionCount()
				Expect(versionCount).To(BeNumerically(">", 0))
				versionIdx := testRnd.IntInRange(0, int(versionCount)-1)

				objectIter := storeClient.ListObjects(ctx, bucket2Name, objstore.WithPrefix(leafPath), objstore.WithVersions())

				versionId := ""
				currentVersionIdx := 0
				for objectInfo, err := range objectIter {
					Expect(err).NotTo(HaveOccurred())

					if currentVersionIdx == versionIdx {
						versionId = objectInfo.VersionID
						break
					}

					currentVersionIdx++
				}
				Expect(versionId).NotTo(BeEmpty())

				err := storeClient.RemoveObject(ctx, bucket2Name, leafPath, objstore.WithVersionID(versionId))
				Expect(err).NotTo(HaveOccurred())

				Eventually(func(g Gomega) {
					exists, err := storeClient.ObjectExists(ctx, bucket2Name, leafPath, objstore.WithVersionID(versionId))
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(exists).To(BeFalse())
				}, 1*time.Minute, time.Millisecond*100).Should(Succeed())

				checkRequest := &pb.StartConsistencyCheckRequest{
					Locations: locations,
					User:      CSyncUserKey,
				}
				_, err = diffClient.Start(ctx, checkRequest)
				Expect(err).NotTo(HaveOccurred())

				var getCheckResponse *pb.GetConsistencyCheckReportResponse
				Eventually(func(g Gomega) {
					getCheckResponse, err = diffClient.GetReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
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
					checkEntries, err := diffClient.GetReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
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

				var expectedDiffCount int
				if testStorageBackend == CSwiftTestStorageBackend {
					// deleting object version marked as latest in swift means marking whole object version set as deleted.
					if versionIdx != 0 {
						expectedDiffCount = (int(versionCount)*2-versionIdx)*2 - 1
					} else {
						expectedDiffCount = int(versionCount) * 2
					}
				} else {
					expectedDiffCount = (int(versionCount)-versionIdx)*2 - 1
				}
				Expect(entries).To(HaveLen(expectedDiffCount))

				for _, entry := range entries {
					Expect(entry.Object).To(Equal(leafPath))
					for _, storageEntry := range entry.StorageEntries {
						Expect(storageEntry.Storage).To(BeElementOf(CStorage1Key, CStorage2Key))
						Expect(storageEntry.VersionId).NotTo(BeEmpty())
					}
				}
			})
		})
	}
})
