package env

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack"
	kcredentials "github.com/gophercloud/gophercloud/v2/openstack/identity/v3/credentials"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/projects"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/roles"
	"github.com/gophercloud/gophercloud/v2/openstack/identity/v3/users"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/containers"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
	"github.com/minio/madmin-go/v4"
	"github.com/minio/minio-go/v7"
	mcredentials "github.com/minio/minio-go/v7/pkg/credentials"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redis/go-redis/v9"
)

const (
	CMultipartContentType = "application/octet-stream"

	CKeystoneURLTemplate = "http://%s:%d/v3"

	CMinioReadWritePolicy = "readwrite"
	CMinioTestUsername    = "miniotest"
	CMinioTestPassword    = "miniotest"
	CMinioTestBucketName  = "test"
	CMinioTestFileName    = "test.txt"

	CKeystoneTestProjectName    = "demo"
	CKeystoneTestUsername       = "demo"
	CKeystoneTestPassword       = "demo"
	CKeystoneTestEC2AccessToken = "a7f1e798b7c2417cba4a02de97dc3cdc"
	CKeystoneTestEC2SecretToken = "18f4f6761ada4e3795fa5273c30349b9"
	CKeystoneEC2CredentialsType = "ec2"

	CSwiftTestContainerName = "test"
	CSwiftTestFileName      = "test.txt"

	CKeystoneTestComponentKey = "keystone"
	CRedisTestComponentKey    = "redis"
	CMinioTestComponentKey    = "minio"
	CSwiftTestComponentKey    = "swift"
	CCephTestComponentKey     = "ceph"

	CRedisTestKey   = "redistestkey"
	CRedisTestValue = "redistestval"
)

var (
	suiteCtx context.Context
	suiteEnv *TestEnvironment
)

type KeystoneEC2Credentials struct {
	Access string `json:"access,omitempty"`
	Secret string `json:"secret,omitempty"`
}

func TestEnv(t *testing.T) {
	RegisterFailHandler(Fail)
	// This test is provided as guide on testcontainers environment usage,
	// therefore its execution is skipped.
	t.Skip()
	RunSpecs(t, "Test Env Suite")
}

var _ = BeforeSuite(func() {
	suiteCtx = context.Background()

	componentConfig := map[string]ComponentCreationConfig{
		CMinioTestComponentKey:    AsMinio(WithDisabledSTDErrLog(), WithDisabledSTDOutLog()), // Example:Disable all logs for minio component
		CKeystoneTestComponentKey: AsKeystone(),
		CRedisTestComponentKey:    AsRedis(WithDisabledSTDOutLog()), // Example:Disable stdout logs for redis component
		CSwiftTestComponentKey:    AsSwift(CKeystoneTestComponentKey),
		CCephTestComponentKey:     AsCeph(CKeystoneTestComponentKey),
	}
	testEnv, err := NewTestEnvironment(suiteCtx, componentConfig)
	Expect(err).NotTo(HaveOccurred())
	suiteEnv = testEnv

	// Create minio user and allow read writes to it
	minioAccessConfig, err := suiteEnv.GetMinioAccessConfig(CMinioTestComponentKey)
	Expect(err).NotTo(HaveOccurred())

	minioS3Endpoint := fmt.Sprintf("%s:%d", minioAccessConfig.Host.Local, minioAccessConfig.S3Port.Forwarded)
	adminClient, err := madmin.NewWithOptions(minioS3Endpoint, &madmin.Options{
		Creds:  mcredentials.NewStaticV4(minioAccessConfig.User, minioAccessConfig.Password, ""),
		Secure: false,
	})
	Expect(err).NotTo(HaveOccurred())

	err = adminClient.AddUser(suiteCtx, CMinioTestUsername, CMinioTestPassword)
	Expect(err).NotTo(HaveOccurred())

	_, err = adminClient.AttachPolicy(suiteCtx, madmin.PolicyAssociationReq{
		Policies: []string{CMinioReadWritePolicy},
		User:     CMinioTestUsername,
	})
	Expect(err).NotTo(HaveOccurred())

	// Create keystone user and allow it to interact with swift and keystone
	keystoneAccessConfig, err := suiteEnv.GetKeystoneAccessConfig(CKeystoneTestComponentKey)
	Expect(err).NotTo(HaveOccurred())

	swiftAccessConfig, err := suiteEnv.GetSwiftAccessConfig(CSwiftTestComponentKey)
	Expect(err).NotTo(HaveOccurred())

	cephAccessConfig, err := suiteEnv.GetCephAccessConfig(CCephTestComponentKey)
	Expect(err).NotTo(HaveOccurred())

	providerClient, err := openstack.AuthenticatedClient(suiteCtx, gophercloud.AuthOptions{
		IdentityEndpoint: fmt.Sprintf(CKeystoneURLTemplate, keystoneAccessConfig.Host.Local, keystoneAccessConfig.ExternalPort.Forwarded),
		Username:         keystoneAccessConfig.User,
		Password:         keystoneAccessConfig.Password,
		DomainName:       keystoneAccessConfig.DefaultDomain.Name,
		TenantName:       keystoneAccessConfig.TenantName,
	})
	Expect(err).NotTo(HaveOccurred())

	identityClient, err := openstack.NewIdentityV3(providerClient, gophercloud.EndpointOpts{})
	Expect(err).NotTo(HaveOccurred())

	newProject, err := projects.Create(suiteCtx, identityClient, projects.CreateOpts{
		DomainID: keystoneAccessConfig.DefaultDomain.ID,
		Name:     CKeystoneTestProjectName,
	}).Extract()
	Expect(err).NotTo(HaveOccurred())

	newUser, err := users.Create(suiteCtx, identityClient, users.CreateOpts{
		Name:     CKeystoneTestUsername,
		Password: CKeystoneTestPassword,
		DomainID: keystoneAccessConfig.DefaultDomain.ID,
	}).Extract()
	Expect(err).NotTo(HaveOccurred())

	err = roles.Assign(suiteCtx, identityClient, swiftAccessConfig.Keystone.OperatorRole.ID, roles.AssignOpts{
		UserID:    newUser.ID,
		ProjectID: newProject.ID,
	}).ExtractErr()
	Expect(err).NotTo(HaveOccurred())

	err = roles.Assign(suiteCtx, identityClient, cephAccessConfig.Keystone.OperatorRole.ID, roles.AssignOpts{
		UserID:    newUser.ID,
		ProjectID: newProject.ID,
	}).ExtractErr()
	Expect(err).NotTo(HaveOccurred())

	ec2Creds := KeystoneEC2Credentials{
		Access: CKeystoneTestEC2AccessToken,
		Secret: CKeystoneTestEC2SecretToken,
	}

	serializedEC2Creds, err := json.Marshal(ec2Creds)
	Expect(err).NotTo(HaveOccurred())

	_, err = kcredentials.Create(suiteCtx, identityClient, kcredentials.CreateOpts{
		ProjectID: newProject.ID,
		UserID:    newUser.ID,
		Type:      CKeystoneEC2CredentialsType,
		Blob:      string(serializedEC2Creds),
	}).Extract()
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	if suiteEnv == nil {
		return
	}
	err := suiteEnv.Terminate(suiteCtx)
	Expect(err).NotTo(HaveOccurred())
})

var _ = Describe("Interacting with test components", func() {
	It("Should allow to upload to minio", func() {
		By("Creating minio client")
		minioAccessConfig, err := suiteEnv.GetMinioAccessConfig(CMinioTestComponentKey)
		Expect(err).NotTo(HaveOccurred())

		minioS3Endpoint := fmt.Sprintf("%s:%d", minioAccessConfig.Host.Local, minioAccessConfig.S3Port.Forwarded)
		s3Client, err := minio.New(minioS3Endpoint, &minio.Options{
			Creds:  mcredentials.NewStaticV4(CMinioTestUsername, CMinioTestPassword, ""),
			Secure: false,
		})
		Expect(err).NotTo(HaveOccurred())

		By("Creating bucket")
		err = s3Client.MakeBucket(suiteCtx, CMinioTestBucketName, minio.MakeBucketOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("Uploading file")
		file, err := os.Open("proxy-server.conf")
		Expect(err).NotTo(HaveOccurred())
		defer file.Close()

		fileStat, err := file.Stat()
		Expect(err).NotTo(HaveOccurred())

		_, err = s3Client.PutObject(suiteCtx, CMinioTestBucketName, CMinioTestFileName, file, fileStat.Size(), minio.PutObjectOptions{})
		Expect(err).NotTo(HaveOccurred())
	})

	It("Should allow to upload to ceph with keystone credentials", func() {
		By("Creating S3 client")
		cephAccessConfig, err := suiteEnv.GetCephAccessConfig(CCephTestComponentKey)
		Expect(err).NotTo(HaveOccurred())

		endpoint := fmt.Sprintf("%s:%d", cephAccessConfig.Host.Local, cephAccessConfig.Port.Forwarded)
		s3Client, err := minio.New(endpoint, &minio.Options{
			Creds:  mcredentials.NewStaticV4(CKeystoneTestEC2AccessToken, CKeystoneTestEC2SecretToken, ""),
			Secure: false,
		})
		Expect(err).NotTo(HaveOccurred())

		By("Creating bucket")
		err = s3Client.MakeBucket(suiteCtx, CMinioTestBucketName, minio.MakeBucketOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("Uploading file")
		file, err := os.Open("proxy-server.conf")
		Expect(err).NotTo(HaveOccurred())
		defer file.Close()

		fileStat, err := file.Stat()
		Expect(err).NotTo(HaveOccurred())

		_, err = s3Client.PutObject(suiteCtx, CMinioTestBucketName, CMinioTestFileName, file, fileStat.Size(), minio.PutObjectOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("Creating openstack clients")
		keystoneAccessConfig, err := suiteEnv.GetKeystoneAccessConfig(CKeystoneTestComponentKey)
		Expect(err).NotTo(HaveOccurred())

		providerClient, err := openstack.AuthenticatedClient(suiteCtx, gophercloud.AuthOptions{
			IdentityEndpoint: fmt.Sprintf(CKeystoneURLTemplate, keystoneAccessConfig.Host.Local, keystoneAccessConfig.ExternalPort.Forwarded),
			Username:         CKeystoneTestUsername,
			Password:         CKeystoneTestPassword,
			TenantName:       CKeystoneTestProjectName,
			DomainName:       keystoneAccessConfig.DefaultDomain.Name,
		})
		Expect(err).NotTo(HaveOccurred())

		swiftClient, err := openstack.NewObjectStorageV1(providerClient, gophercloud.EndpointOpts{
			Type:         CKeystoneObjectStoreServiceType,
			Name:         CKeystoneCephEndpointName,
			Availability: gophercloud.AvailabilityPublic,
		})
		Expect(err).NotTo(HaveOccurred())

		By("Creating object container")
		_, err = containers.Create(suiteCtx, swiftClient, CSwiftTestContainerName, containers.CreateOpts{}).Extract()
		Expect(err).NotTo(HaveOccurred())

		By("Uploading file")
		_, err = objects.Create(suiteCtx, swiftClient, CSwiftTestContainerName, CSwiftTestFileName, objects.CreateOpts{
			Content:       file,
			ContentLength: fileStat.Size(),
			ContentType:   CMultipartContentType,
		}).Extract()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Should allow to upload to swift with keystone credentials", func() {
		By("Creating openstack clients")
		keystoneAccessConfig, err := suiteEnv.GetKeystoneAccessConfig(CKeystoneTestComponentKey)
		Expect(err).NotTo(HaveOccurred())

		providerClient, err := openstack.AuthenticatedClient(suiteCtx, gophercloud.AuthOptions{
			IdentityEndpoint: fmt.Sprintf(CKeystoneURLTemplate, keystoneAccessConfig.Host.Local, keystoneAccessConfig.ExternalPort.Forwarded),
			Username:         CKeystoneTestUsername,
			Password:         CKeystoneTestPassword,
			TenantName:       CKeystoneTestProjectName,
			DomainName:       keystoneAccessConfig.DefaultDomain.Name,
		})
		Expect(err).NotTo(HaveOccurred())

		swiftClient, err := openstack.NewObjectStorageV1(providerClient, gophercloud.EndpointOpts{
			Type:         CKeystoneObjectStoreServiceType,
			Name:         CKeystoneSwiftEndpointName,
			Availability: gophercloud.AvailabilityPublic,
		})
		Expect(err).NotTo(HaveOccurred())

		By("Creating object container")
		_, err = containers.Create(suiteCtx, swiftClient, CSwiftTestContainerName, containers.CreateOpts{}).Extract()
		Expect(err).NotTo(HaveOccurred())

		By("Uploading file")
		file, err := os.Open("proxy-server.conf")
		Expect(err).NotTo(HaveOccurred())
		defer file.Close()

		fileStat, err := file.Stat()
		Expect(err).NotTo(HaveOccurred())

		_, err = objects.Create(suiteCtx, swiftClient, CSwiftTestContainerName, CSwiftTestFileName, objects.CreateOpts{
			Content:       file,
			ContentLength: fileStat.Size(),
			ContentType:   CMultipartContentType,
		}).Extract()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Should allow to put data to redis", func() {
		By("Creating client")
		redisAccessConfig, err := suiteEnv.GetRedisAccessConfig(CRedisTestComponentKey)
		Expect(err).NotTo(HaveOccurred())

		rdb := redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%d", redisAccessConfig.Host.Local, redisAccessConfig.Port.Forwarded),
			Password: redisAccessConfig.Password,
			DB:       0,
		})

		By("Putting key")
		err = rdb.Set(suiteCtx, CRedisTestKey, CRedisTestValue, 1*time.Minute).Err()
		Expect(err).NotTo(HaveOccurred())
	})
})
