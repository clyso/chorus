package agent

import (
	"context"
	"fmt"
	"os"
	"testing"

	cadmin "github.com/ceph/go-ceph/rgw/admin"
	"github.com/minio/minio-go/v7"
	mcredentials "github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/test/app"
	"github.com/clyso/chorus/test/env"
)

const (
	cephKey  = "ceph"
	minioKey = "minio"
	user     = "testuser"

	accessKey = "TESTUSER0ACCESS00KEY"
	secretKey = "testuser0secret00key0testuser0sec"
)

var (
	workerStorage  objstore.Config
	workerHttpPort int

	cephClient  *minio.Client
	minioClient *minio.Client
)

func TestMain(m *testing.M) {
	os.Exit(setup(m))
}

func setup(m *testing.M) int {
	ctx := context.Background()

	testEnv, err := env.NewTestEnvironment(ctx, map[string]env.ComponentCreationConfig{
		cephKey:  env.AsCeph(),
		minioKey: env.AsMinio(),
	})
	if err != nil {
		panic(fmt.Sprintf("unable to create test environment: %v", err))
	}
	defer testEnv.Terminate(ctx)

	cephAccess, err := testEnv.GetCephAccessConfig(cephKey)
	if err != nil {
		panic(fmt.Sprintf("unable to get ceph config: %v", err))
	}
	minioAccess, err := testEnv.GetMinioAccessConfig(minioKey)
	if err != nil {
		panic(fmt.Sprintf("unable to get minio config: %v", err))
	}

	// Create user on Ceph with hardcoded creds.
	cephEndpoint := fmt.Sprintf("http://%s:%d", cephAccess.Host.Local, cephAccess.Port.Forwarded)
	cephAdminClient, err := cadmin.New(cephEndpoint, cephAccess.SystemUser, cephAccess.SystemPassword, nil)
	if err != nil {
		panic(fmt.Sprintf("unable to create ceph admin client: %v", err))
	}
	_, err = cephAdminClient.CreateUser(ctx, cadmin.User{
		ID:          user,
		DisplayName: user,
		Keys: []cadmin.UserKeySpec{
			{AccessKey: accessKey, SecretKey: secretKey},
		},
	})
	if err != nil {
		panic(fmt.Sprintf("unable to create ceph user: %v", err))
	}

	// Create S3 clients for direct access.
	cephS3Endpoint := fmt.Sprintf("%s:%d", cephAccess.Host.Local, cephAccess.Port.Forwarded)
	cephClient, err = minio.New(cephS3Endpoint, &minio.Options{
		Creds:  mcredentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		panic(fmt.Sprintf("unable to create ceph client: %v", err))
	}

	minioS3Endpoint := fmt.Sprintf("%s:%d", minioAccess.Host.Local, minioAccess.S3Port.Forwarded)
	minioClient, err = minio.New(minioS3Endpoint, &minio.Options{
		Creds:  mcredentials.NewStaticV4(minioAccess.User, minioAccess.Password, ""),
		Secure: false,
	})
	if err != nil {
		panic(fmt.Sprintf("unable to create minio client: %v", err))
	}

	// Build storage config.
	cephURL := fmt.Sprintf("http://%s", cephS3Endpoint)
	minioURL := fmt.Sprintf("http://%s", minioS3Endpoint)

	s3Storages := map[string]s3.Storage{
		cephKey: {
			StorageAddress: s3.StorageAddress{Address: cephURL, Provider: s3.ProviderCeph},
			Credentials:    map[string]s3.CredentialsV4{user: {AccessKeyID: accessKey, SecretAccessKey: secretKey}},
		},
		minioKey: {
			StorageAddress: s3.StorageAddress{Address: minioURL, Provider: s3.ProviderMinIO},
			Credentials:    map[string]s3.CredentialsV4{user: {AccessKeyID: minioAccess.User, SecretAccessKey: minioAccess.Password}},
		},
	}
	workerStorage = app.WorkerS3Config(cephKey, s3Storages)

	// Pre-allocate worker HTTP port so tests can set webhook baseURL before SetupChorus.
	workerHttpPort, err = env.RandomFreePort()
	if err != nil {
		panic(fmt.Sprintf("unable to allocate worker http port: %v", err))
	}

	return m.Run()
}
