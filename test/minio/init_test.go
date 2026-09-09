/*
 * Copyright © 2026 Clyso GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package minio

import (
	"context"
	"fmt"
	"os"
	"testing"

	mclient "github.com/minio/minio-go/v7"
	mcredentials "github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/service/proxy"
	"github.com/clyso/chorus/service/worker"
	"github.com/clyso/chorus/test/app"
	"github.com/clyso/chorus/test/env"
)

const (
	minioKey    = "minio"
	followerKey = "minio-follower"
	user        = "testuser"
)

var (
	workerConf *worker.Config
	proxyConf  *proxy.Config

	storageCreds s3.CredentialsV4

	minioEndpoint string

	// minioClient and followerClient talk to the MinIO containers directly,
	// bypassing the proxy.
	minioClient    *mclient.Client
	followerClient *mclient.Client
)

func TestMain(m *testing.M) {
	os.Exit(setup(m))
}

func setup(m *testing.M) int {
	ctx := context.Background()

	testEnv, err := env.NewTestEnvironment(ctx, map[string]env.ComponentCreationConfig{
		minioKey:    env.AsMinio(env.WithDisabledSTDOutLog(), env.WithDisabledSTDErrLog()),
		followerKey: env.AsMinio(env.WithDisabledSTDOutLog(), env.WithDisabledSTDErrLog()),
	})
	if err != nil {
		panic(fmt.Sprintf("unable to create test environment: %v", err))
	}
	defer testEnv.Terminate(ctx)

	minioAccess, err := testEnv.GetMinioAccessConfig(minioKey)
	if err != nil {
		panic(fmt.Sprintf("unable to get minio config: %v", err))
	}
	minioEndpoint = fmt.Sprintf("%s:%d", minioAccess.Host.Local, minioAccess.S3Port.Forwarded)
	storageCreds = s3.CredentialsV4{
		AccessKeyID:     minioAccess.User,
		SecretAccessKey: minioAccess.Password,
	}

	followerAccess, err := testEnv.GetMinioAccessConfig(followerKey)
	if err != nil {
		panic(fmt.Sprintf("unable to get follower minio config: %v", err))
	}
	followerS3Endpoint := fmt.Sprintf("%s:%d", followerAccess.Host.Local, followerAccess.S3Port.Forwarded)

	minioClient, err = newS3Client(minioEndpoint, storageCreds)
	if err != nil {
		panic(fmt.Sprintf("unable to create minio client: %v", err))
	}
	followerCreds := s3.CredentialsV4{
		AccessKeyID:     followerAccess.User,
		SecretAccessKey: followerAccess.Password,
	}
	followerClient, err = newS3Client(followerS3Endpoint, followerCreds)
	if err != nil {
		panic(fmt.Sprintf("unable to create follower minio client: %v", err))
	}

	storages := map[string]s3.Storage{
		minioKey: {
			StorageAddress: s3.StorageAddress{
				Address:  "http://" + minioEndpoint,
				Provider: s3.ProviderMinIO,
			},
			Credentials: map[string]s3.CredentialsV4{user: storageCreds},
		},
		followerKey: {
			StorageAddress: s3.StorageAddress{
				Address:  "http://" + followerS3Endpoint,
				Provider: s3.ProviderMinIO,
			},
			Credentials: map[string]s3.CredentialsV4{user: followerCreds},
		},
	}

	workerConf, err = worker.GetConfig()
	if err != nil {
		panic(fmt.Sprintf("unable to get worker config: %v", err))
	}
	workerConf.Log.Level = "warn"
	workerConf.Storage = app.WorkerS3Config(minioKey, storages)

	proxyConf, err = proxy.GetConfig()
	if err != nil {
		panic(fmt.Sprintf("unable to get proxy config: %v", err))
	}
	proxyConf.Log.Level = "warn"
	proxyConf.Storage = app.ProxyS3Config(minioKey, storages)
	proxyConf.Auth.UseStorage = minioKey

	return m.Run()
}

func newS3Client(endpoint string, cred s3.CredentialsV4) (*mclient.Client, error) {
	return mclient.New(endpoint, &mclient.Options{
		Creds:  mcredentials.NewStaticV4(cred.AccessKeyID, cred.SecretAccessKey, ""),
		Secure: false,
	})
}
