// Copyright 2025 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package versioned

import (
	"context"
	"encoding/json"
	"fmt"

	"net/http"
	"testing"
	"time"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack"
	kcredentials "github.com/gophercloud/gophercloud/v2/openstack/identity/v3/credentials"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/projects"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/roles"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/users"
	"github.com/minio/minio-go/v7"
	mcredentials "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rs/xid"
	"google.golang.org/grpc"

	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"

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
	"github.com/clyso/chorus/test/env"
	"github.com/clyso/chorus/test/gen"

	"github.com/minio/madmin-go/v4"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	CTestGenSeed    = 811509576612567777
	CUseTestGenSeed = true
)

var (
	testCtx           context.Context
	testCtxCancelFunc context.CancelFunc

	testTree *gen.Tree[*gen.GeneratedS3Object]
)

func TestEnv(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Versioned Migration Suite")
}

var _ = BeforeSuite(func() {
	ctx, cancelFunc := context.WithCancel(context.Background())

	objGen := gen.NewS3ObjectGenerator(
		gen.WithVersioned(),
	)
	genOpts := []gen.TreeGeneratorOption[*gen.GeneratedS3Object]{
		gen.WithObjectGenerator[*gen.GeneratedS3Object](objGen),
	}
	if CUseTestGenSeed {
		genOpts = append(genOpts, gen.WithRandomSeed[*gen.GeneratedS3Object](CTestGenSeed))
	} else {
		genOpts = append(genOpts, gen.WithRandomSeed[*gen.GeneratedS3Object](GinkgoRandomSeed()))
	}
	treeGen, err := gen.NewTreeGenerator(genOpts...)
	Expect(err).NotTo(HaveOccurred())

	tree, err := treeGen.Generate()
	Expect(err).NotTo(HaveOccurred())

	testCtx = ctx
	testCtxCancelFunc = cancelFunc
	testTree = tree
})

var _ = AfterSuite(func() {
	testCtxCancelFunc()
})

var _ = Describe("Minio versioned migration", func() {

	const (
		CMinioSrcInstance  = "minio1"
		CMinioDestInstance = "minio2"
		CRedisInstance     = "redis"

		CMinioProvider = "minio"

		CSyncUserKey = "test"

		CMinioSrcUser    = "user"
		CMinioSrcPass    = "userpass"
		CMinioSrcBucket  = "buck"
		CMinioDestUser   = "user"
		CMinioDestPass   = "userpass"
		CMinioDestBucket = "buck"
	)

	var (
		testEnv *env.TestEnvironment

		testMinioSrcUserClient  *minio.Client
		testMinioDestUserClient *minio.Client

		testApiClient pb.ChorusClient
	)

	BeforeEach(func() {
		ctx := context.WithoutCancel(testCtx)
		localTestEnv, err := env.NewTestEnvironment(ctx, map[string]env.ComponentCreationConfig{
			CMinioSrcInstance:  env.AsMinio(),
			CMinioDestInstance: env.AsMinio(),
			CRedisInstance:     env.AsRedis(),
		})
		Expect(err).NotTo(HaveOccurred())

		minioSrcAccessConfig, err := localTestEnv.GetMinioAccessConfig(CMinioSrcInstance)
		Expect(err).NotTo(HaveOccurred())

		minioSrcS3Endpoint := fmt.Sprintf("%s:%d", minioSrcAccessConfig.Host.Local, minioSrcAccessConfig.S3Port.Forwarded)
		minioSrcAdminClient, err := madmin.NewWithOptions(minioSrcS3Endpoint, &madmin.Options{
			Creds:  mcredentials.NewStaticV4(minioSrcAccessConfig.User, minioSrcAccessConfig.Password, ""),
			Secure: false,
		})
		Expect(err).NotTo(HaveOccurred())

		err = minioSrcAdminClient.AddUser(ctx, CMinioSrcUser, CMinioSrcPass)
		Expect(err).NotTo(HaveOccurred())

		_, err = minioSrcAdminClient.AttachPolicy(ctx, madmin.PolicyAssociationReq{
			Policies: []string{"readwrite"},
			User:     CMinioSrcUser,
		})

		minioSrcUserClient, err := minio.New(minioSrcS3Endpoint, &minio.Options{
			Creds:  mcredentials.NewStaticV4(CMinioSrcUser, CMinioSrcPass, ""),
			Secure: false,
		})

		minioDestAccessConfig, err := localTestEnv.GetMinioAccessConfig(CMinioDestInstance)
		Expect(err).NotTo(HaveOccurred())

		minioDestS3Endpoint := fmt.Sprintf("%s:%d", minioDestAccessConfig.Host.Local, minioDestAccessConfig.S3Port.Forwarded)
		minioDestAdminClient, err := madmin.NewWithOptions(minioDestS3Endpoint, &madmin.Options{
			Creds:  mcredentials.NewStaticV4(minioDestAccessConfig.User, minioDestAccessConfig.Password, ""),
			Secure: false,
		})
		Expect(err).NotTo(HaveOccurred())

		err = minioDestAdminClient.AddUser(ctx, CMinioDestUser, CMinioDestPass)
		Expect(err).NotTo(HaveOccurred())

		_, err = minioDestAdminClient.AttachPolicy(ctx, madmin.PolicyAssociationReq{
			Policies: []string{"readwrite"},
			User:     CMinioDestUser,
		})

		minioDestUserClient, err := minio.New(minioDestS3Endpoint, &minio.Options{
			Creds:  mcredentials.NewStaticV4(CMinioDestUser, CMinioDestPass, ""),
			Secure: false,
		})

		err = minioSrcUserClient.MakeBucket(ctx, CMinioSrcBucket, minio.MakeBucketOptions{})
		Expect(err).NotTo(HaveOccurred())
		err = minioSrcUserClient.EnableVersioning(ctx, CMinioSrcBucket)
		Expect(err).NotTo(HaveOccurred())

		filler := gen.NewS3Filler(testTree, minioSrcUserClient)
		err = filler.Fill(ctx, CMinioSrcBucket)
		Expect(err).NotTo(HaveOccurred())

		redisAccessConfig, err := localTestEnv.GetRedisAccessConfig(CRedisInstance)
		Expect(err).NotTo(HaveOccurred())

		grpcPort, err := env.RandomFreePort()
		Expect(err).NotTo(HaveOccurred())
		httpPort, err := env.RandomFreePort()
		Expect(err).NotTo(HaveOccurred())

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
			Storage: &s3.StorageConfig{
				CreateRouting: true,
				Storages: map[string]s3.Storage{
					CMinioSrcInstance: {
						Address: fmt.Sprintf("http://%s", minioSrcS3Endpoint),
						Credentials: map[string]s3.CredentialsV4{
							CSyncUserKey: {
								AccessKeyID:     CMinioSrcUser,
								SecretAccessKey: CMinioSrcPass,
							},
						},
						Provider: CMinioProvider,
						IsMain:   true,
					},
					CMinioDestInstance: {
						Address: fmt.Sprintf("http://%s", minioDestS3Endpoint),
						Credentials: map[string]s3.CredentialsV4{
							CSyncUserKey: {
								AccessKeyID:     CMinioDestUser,
								SecretAccessKey: CMinioDestPass,
							},
						},
						Provider: CMinioProvider,
						IsMain:   false,
					},
				},
			},
			Api: &api.Config{
				Enabled:  true,
				GrpcPort: grpcPort,
				HttpPort: httpPort,
			},
		}

		err = workerConf.Storage.Init()
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

		Eventually(func() bool {
			_, err := apiClient.GetAppVersion(ctx, &emptypb.Empty{})
			if err != nil {
				return false
			}
			return true
		}, 1*time.Minute, time.Second).Should(BeTrue())

		testEnv = localTestEnv
		testMinioSrcUserClient = minioSrcUserClient
		testMinioDestUserClient = minioDestUserClient
		testApiClient = apiClient
	})

	AfterEach(func() {
		testEnv.Terminate(context.WithoutCancel(testCtx))
	})

	It("Should migrate and preserve version ids", func() {
		ctx := context.Background()

		_, err := testApiClient.AddBucketReplication(ctx, &pb.AddBucketReplicationRequest{
			User:        CSyncUserKey,
			FromStorage: CMinioSrcInstance,
			FromBucket:  CMinioSrcBucket,
			ToStorage:   CMinioDestInstance,
			ToBucket:    CMinioDestBucket,
		})
		Expect(err).NotTo(HaveOccurred())

		resp, err := testApiClient.ListReplications(ctx, &emptypb.Empty{})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Replications).To(HaveLen(1))

		Eventually(func() bool {
			resp, err = testApiClient.ListReplications(ctx, &emptypb.Empty{})
			if err != nil {
				return false
			}
			if len(resp.Replications) != 1 {
				return false
			}
			return resp.Replications[0].IsInitDone
		}, 60*time.Second, time.Second).Should(BeTrue())

		for treeObject := range testTree.DepthFirstIterator().Must() {
			srcObjectList := testMinioSrcUserClient.ListObjects(ctx, CMinioSrcBucket, minio.ListObjectsOptions{
				WithVersions: true,
				Prefix:       treeObject.GetFullPath(),
			})
			destObjectList := testMinioDestUserClient.ListObjects(ctx, CMinioDestBucket, minio.ListObjectsOptions{
				WithVersions: true,
				Prefix:       treeObject.GetFullPath(),
			})

			for srcObject := range srcObjectList {
				destObject := <-destObjectList
				Expect(srcObject.ETag).To(Equal(destObject.ETag))
				Expect(srcObject.VersionID).To(Equal(destObject.VersionID))
				Expect(srcObject.Size).To(Equal(destObject.Size))
				Expect(srcObject.VersionID).To(Equal(destObject.Metadata[http.CanonicalHeaderKey(rclone.CChorusSourceVersionIDMetaHeader)]))
			}
		}
	})
})

var _ = Describe("Ceph versioned migration", func() {
	const (
		CKeystoneUrlTemplate = "http://%s:%d/v3"

		CKeystoneProject = "test"

		CCephSrcInstance     = "ceph1"
		CCephDestInstance    = "ceph2"
		CRedisInstance       = "redis"
		CKeystoneSrcInstance = "keystone1"

		CEC2CredsType = "ec2"

		CCephProvider = "ceph"

		CSyncUserKey = "test"

		CCephSrcUser       = "user"
		CCephSrcPass       = "userpass"
		CCephSrcAccessKey  = "AKIAIOSFODNN7EXAMPLE"
		CCephSrcSecretKey  = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
		CCephSrcBucket     = "buck1"
		CCephDestUser      = "user"
		CCephDestPass      = "userpass"
		CCephDestAccessKey = "AKIAIOSFODNN7EXAMPLE"
		CCephDestSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
		CCephDestBucket    = "buck2"
	)

	var (
		testEnv *env.TestEnvironment

		testMinioSrcUserClient  *minio.Client
		testMinioDestUserClient *minio.Client

		testApiClient pb.ChorusClient
	)

	type EC2Creds struct {
		Access string `json:"access"`
		Secret string `json:"secret"`
	}

	BeforeEach(func() {
		Skip("skip ceph test, since runner can't run ceph yet")

		ctx := context.WithoutCancel(testCtx)
		localTestEnv, err := env.NewTestEnvironment(ctx, map[string]env.ComponentCreationConfig{
			CKeystoneSrcInstance: env.AsKeystone(),
			CCephSrcInstance:     env.AsCeph(CKeystoneSrcInstance),
			CRedisInstance:       env.AsRedis(),
		})
		Expect(err).NotTo(HaveOccurred())

		// Create keystone user and allow it to interact with swift and keystone
		keystoneSrcAccessConfig, err := localTestEnv.GetKeystoneAccessConfig(CKeystoneSrcInstance)
		Expect(err).NotTo(HaveOccurred())

		cephSrcAccessConfig, err := localTestEnv.GetCephAccessConfig(CCephSrcInstance)
		Expect(err).NotTo(HaveOccurred())

		providerSrcClient, err := openstack.AuthenticatedClient(ctx, gophercloud.AuthOptions{
			IdentityEndpoint: fmt.Sprintf(CKeystoneUrlTemplate, keystoneSrcAccessConfig.Host.Local, keystoneSrcAccessConfig.ExternalPort.Forwarded),
			Username:         keystoneSrcAccessConfig.User,
			Password:         keystoneSrcAccessConfig.Password,
			DomainName:       keystoneSrcAccessConfig.DefaultDomain.Name,
			TenantName:       keystoneSrcAccessConfig.TenantName,
		})
		Expect(err).NotTo(HaveOccurred())

		identitySrcClient, err := openstack.NewIdentityV3(providerSrcClient, gophercloud.EndpointOpts{})
		Expect(err).NotTo(HaveOccurred())

		newSrcProject, err := projects.Create(ctx, identitySrcClient, projects.CreateOpts{
			DomainID: keystoneSrcAccessConfig.DefaultDomain.ID,
			Name:     CKeystoneProject,
		}).Extract()
		Expect(err).NotTo(HaveOccurred())

		newSrcUser, err := users.Create(ctx, identitySrcClient, users.CreateOpts{
			Name:     CCephSrcUser,
			Password: CCephSrcPass,
			DomainID: keystoneSrcAccessConfig.DefaultDomain.ID,
		}).Extract()
		Expect(err).NotTo(HaveOccurred())

		err = roles.Assign(ctx, identitySrcClient, cephSrcAccessConfig.Keystone.OperatorRole.ID, roles.AssignOpts{
			UserID:    newSrcUser.ID,
			ProjectID: newSrcProject.ID,
		}).ExtractErr()
		Expect(err).NotTo(HaveOccurred())

		err = roles.Assign(ctx, identitySrcClient, cephSrcAccessConfig.Keystone.OperatorRole.ID, roles.AssignOpts{
			UserID:    newSrcUser.ID,
			ProjectID: newSrcProject.ID,
		}).ExtractErr()
		Expect(err).NotTo(HaveOccurred())

		ec2SrcCreds := EC2Creds{
			Access: CCephSrcAccessKey,
			Secret: CCephSrcSecretKey,
		}

		serializedEC2SrcCreds, err := json.Marshal(ec2SrcCreds)
		Expect(err).NotTo(HaveOccurred())

		_, err = kcredentials.Create(ctx, identitySrcClient, kcredentials.CreateOpts{
			ProjectID: newSrcProject.ID,
			UserID:    newSrcUser.ID,
			Type:      CEC2CredsType,
			Blob:      string(serializedEC2SrcCreds),
		}).Extract()
		Expect(err).NotTo(HaveOccurred())

		minioSrcS3Endpoint := fmt.Sprintf("%s:%d", cephSrcAccessConfig.Host.Local, cephSrcAccessConfig.Port.Forwarded)
		minioSrcUserClient, err := minio.New(minioSrcS3Endpoint, &minio.Options{
			Creds:  mcredentials.NewStaticV4(CCephSrcAccessKey, CCephSrcSecretKey, ""),
			Secure: false,
		})

		minioDestS3Endpoint := fmt.Sprintf("%s:%d", cephSrcAccessConfig.Host.Local, cephSrcAccessConfig.Port.Forwarded)
		minioDestUserClient, err := minio.New(minioDestS3Endpoint, &minio.Options{
			Creds:  mcredentials.NewStaticV4(CCephDestAccessKey, CCephDestSecretKey, ""),
			Secure: false,
		})

		err = minioSrcUserClient.MakeBucket(ctx, CCephSrcBucket, minio.MakeBucketOptions{})
		Expect(err).NotTo(HaveOccurred())
		err = minioSrcUserClient.EnableVersioning(ctx, CCephSrcBucket)
		Expect(err).NotTo(HaveOccurred())

		filler := gen.NewS3Filler(testTree, minioSrcUserClient)
		err = filler.Fill(ctx, CCephSrcBucket)
		Expect(err).NotTo(HaveOccurred())

		redisAccessConfig, err := localTestEnv.GetRedisAccessConfig(CRedisInstance)
		Expect(err).NotTo(HaveOccurred())

		grpcPort, err := env.RandomFreePort()
		Expect(err).NotTo(HaveOccurred())
		httpPort, err := env.RandomFreePort()
		Expect(err).NotTo(HaveOccurred())

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
			Storage: &s3.StorageConfig{
				CreateRouting: true,
				Storages: map[string]s3.Storage{
					CCephSrcInstance: {
						Address: fmt.Sprintf("http://%s", minioSrcS3Endpoint),
						Credentials: map[string]s3.CredentialsV4{
							CSyncUserKey: {
								AccessKeyID:     CCephSrcAccessKey,
								SecretAccessKey: CCephSrcSecretKey,
							},
						},
						Provider: CCephProvider,
						IsMain:   true,
					},
					CCephDestInstance: {
						Address: fmt.Sprintf("http://%s", minioDestS3Endpoint),
						Credentials: map[string]s3.CredentialsV4{
							CSyncUserKey: {
								AccessKeyID:     CCephDestAccessKey,
								SecretAccessKey: CCephDestSecretKey,
							},
						},
						Provider: CCephProvider,
						IsMain:   false,
					},
				},
			},
			Api: &api.Config{
				Enabled:  true,
				GrpcPort: grpcPort,
				HttpPort: httpPort,
			},
		}

		err = workerConf.Storage.Init()
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

		Eventually(func() bool {
			_, err := apiClient.GetAppVersion(ctx, &emptypb.Empty{})
			if err != nil {
				return false
			}
			return true
		}, 1*time.Minute, time.Second).Should(BeTrue())

		testEnv = localTestEnv
		testMinioSrcUserClient = minioSrcUserClient
		testMinioDestUserClient = minioDestUserClient
		testApiClient = apiClient
	})

	AfterEach(func() {
		testEnv.Terminate(context.WithoutCancel(testCtx))
	})

	It("Should migrate and preserve version ids", func() {
		ctx := context.Background()

		_, err := testApiClient.AddBucketReplication(ctx, &pb.AddBucketReplicationRequest{
			User:        CSyncUserKey,
			FromStorage: CCephSrcInstance,
			FromBucket:  CCephSrcBucket,
			ToStorage:   CCephDestInstance,
			ToBucket:    CCephDestBucket,
		})
		Expect(err).NotTo(HaveOccurred())

		resp, err := testApiClient.ListReplications(ctx, &emptypb.Empty{})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Replications).To(HaveLen(1))

		Eventually(func() bool {
			resp, err = testApiClient.ListReplications(ctx, &emptypb.Empty{})
			if err != nil {
				return false
			}
			if len(resp.Replications) != 1 {
				return false
			}
			return resp.Replications[0].IsInitDone
		}, 60*time.Second, time.Second).Should(BeTrue())

		for treeObject := range testTree.DepthFirstIterator().Must() {
			if treeObject.GetVersionCount() == 0 || treeObject.GetVersionCount() == 1 && treeObject.GetContentReader().Len() == 0 {
				continue
			}
			srcObjectList := testMinioSrcUserClient.ListObjects(ctx, CCephSrcBucket, minio.ListObjectsOptions{
				WithVersions: true,
				Prefix:       treeObject.GetFullPath(),
			})
			destObjectList := testMinioDestUserClient.ListObjects(ctx, CCephDestBucket, minio.ListObjectsOptions{
				WithVersions: true,
				Prefix:       treeObject.GetFullPath(),
			})

			for srcObject := range srcObjectList {
				destObject := <-destObjectList

				destObjectInfo, err := testMinioDestUserClient.StatObject(ctx, CCephDestBucket, treeObject.GetFullPath(), minio.StatObjectOptions{
					VersionID: destObject.VersionID,
				})
				Expect(err).NotTo(HaveOccurred())

				Expect(srcObject.ETag).To(Equal(destObject.ETag))
				Expect(srcObject.Size).To(Equal(destObject.Size))
				Expect(destObjectInfo.Metadata[http.CanonicalHeaderKey(rclone.CChorusSourceVersionIDMetaHeader)]).To(HaveLen(1))
				Expect(srcObject.VersionID).To(Equal(destObjectInfo.Metadata[http.CanonicalHeaderKey(rclone.CChorusSourceVersionIDMetaHeader)][0]))
			}
		}
	})
})
