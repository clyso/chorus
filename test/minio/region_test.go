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
	"strings"
	"testing"

	mclient "github.com/minio/minio-go/v7"
	mcredentials "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/test/app"
)

// Test_e2e_storage_region checks a storage that accepts only requests signed
// for its own region, configured with DefaultRegion: as a replication
// destination, as main behind the proxy and in the online check.
func Test_e2e_storage_region(t *testing.T) {
	regionStorage := s3.Storage{
		StorageAddress: s3.StorageAddress{
			Address:       "http://" + regionEndpoint,
			Provider:      s3.ProviderMinIO,
			DefaultRegion: storageRegion,
		},
		Credentials: map[string]s3.CredentialsV4{user: regionCreds},
	}
	regionStorages := map[string]s3.Storage{
		minioKey:  storages[minioKey],
		regionKey: regionStorage,
	}
	regionClient, err := mclient.New(regionEndpoint, &mclient.Options{
		Creds:  mcredentials.NewStaticV4(regionCreds.AccessKeyID, regionCreds.SecretAccessKey, ""),
		Region: storageRegion,
	})
	require.NoError(t, err)

	t.Run("replication", func(t *testing.T) {
		r := require.New(t)
		ctx := t.Context()
		wConf, err := app.DeepCopyStruct(workerConf)
		r.NoError(err)
		wConf.Storage = app.WorkerS3Config(minioKey, regionStorages)
		e := app.SetupChorus(t, wConf, nil)

		bucket := bucketName("region-replication")
		obj, content := "obj.txt", "replicated to a storage with region"
		r.NoError(minioClient.MakeBucket(ctx, bucket, mclient.MakeBucketOptions{}))
		_, err = minioClient.PutObject(ctx, bucket, obj, strings.NewReader(content), int64(len(content)), mclient.PutObjectOptions{})
		r.NoError(err)

		replID := &pb.ReplicationID{
			User:        user,
			FromStorage: minioKey,
			ToStorage:   regionKey,
			FromBucket:  &bucket,
			ToBucket:    &bucket,
		}
		_, err = e.PolicyClient.AddReplication(ctx, &pb.AddReplicationRequest{Id: replID})
		r.NoError(err)
		r.Eventually(func() bool {
			repl, err := e.PolicyClient.GetReplication(ctx, replID)
			return err == nil && repl.IsInitDone
		}, e.WaitLong, e.RetryLong, "initial replication to the storage with region did not finish")

		r.Equal(content, string(readObject(t, regionClient, bucket, obj)))
	})

	t.Run("proxy", func(t *testing.T) {
		r := require.New(t)
		ctx := t.Context()
		pConf, err := app.DeepCopyStruct(proxyConf)
		r.NoError(err)
		pConf.Storage = app.ProxyS3Config(regionKey, regionStorages)
		pConf.Auth.UseStorage = regionKey
		wConf, err := app.DeepCopyStruct(workerConf)
		r.NoError(err)
		wConf.Storage = app.WorkerS3Config(regionKey, regionStorages)
		e := app.SetupChorus(t, wConf, pConf)

		proxyClient, err := mclient.New(e.ProxyAddr, &mclient.Options{
			Creds: mcredentials.NewStaticV4(regionCreds.AccessKeyID, regionCreds.SecretAccessKey, ""),
		})
		r.NoError(err)

		bucket := bucketName("region-proxy")
		obj, content := "obj.txt", "written through the proxy"
		r.NoError(proxyClient.MakeBucket(ctx, bucket, mclient.MakeBucketOptions{}))
		_, err = proxyClient.PutObject(ctx, bucket, obj, strings.NewReader(content), int64(len(content)), mclient.PutObjectOptions{DisableContentSha256: true})
		r.NoError(err)
		r.Equal(content, string(readObject(t, proxyClient, bucket, obj)))
		r.Equal(content, string(readObject(t, regionClient, bucket, obj)))
	})

	t.Run("online_check", func(t *testing.T) {
		r := require.New(t)
		ctx := t.Context()
		_, err := s3client.NewClient(ctx, metrics.NewS3Service(false), regionStorage.StorageAddress, regionCreds, regionKey, user)
		r.NoError(err)

		wrongSecret := regionCreds
		wrongSecret.SecretAccessKey += "x"
		_, err = s3client.NewClient(ctx, metrics.NewS3Service(false), regionStorage.StorageAddress, wrongSecret, regionKey, user)
		r.Error(err, "storage must be offline with a wrong secret key")

		wrongKey := regionCreds
		wrongKey.AccessKeyID += "x"
		_, err = s3client.NewClient(ctx, metrics.NewS3Service(false), regionStorage.StorageAddress, wrongKey, regionKey, user)
		r.Error(err, "storage must be offline with an unknown access key")
	})
}
