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

package test

import (
	"bytes"
	"io"
	"net"
	"net/http/httptest"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"

	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/test/app"
)

const ipv6TestUser = "test"

// startFakeS3IPv6 boots an in-memory gofakes3 server on an IPv6 loopback
// listener and returns its URL (e.g. "http://[::1]:<port>").
func startFakeS3IPv6(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp6", "[::1]:0")
	require.NoError(t, err, "IPv6 loopback not available")
	srv := httptest.NewUnstartedServer(gofakes3.New(s3mem.New()).Server())
	srv.Listener = l
	srv.Start()
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestChorusReplicatesAndProxiesOverIPv6(t *testing.T) {
	r := require.New(t)
	ctx := t.Context()

	mainURL := startFakeS3IPv6(t)
	f1URL := startFakeS3IPv6(t)
	t.Log("ipv6 URL:", mainURL)

	cred := s3.CredentialsV4{
		AccessKeyID:     "AKIATESTTESTTESTTEST",
		SecretAccessKey: "SECRETTESTSECRETTESTSECRETTESTSECRETTEST",
	}
	storages := map[string]s3.Storage{
		"main": {
			StorageAddress: s3.StorageAddress{Address: mainURL, Provider: s3.ProviderOther},
			Credentials:    map[string]s3.CredentialsV4{ipv6TestUser: cred},
		},
		"f1": {
			StorageAddress: s3.StorageAddress{Address: f1URL, Provider: s3.ProviderOther},
			Credentials:    map[string]s3.CredentialsV4{ipv6TestUser: cred},
		},
	}

	wc, err := app.DeepCopyStruct(workerConf)
	r.NoError(err)
	wc.Storage = app.WorkerS3Config("main", storages)

	pc, err := app.DeepCopyStruct(proxyConf)
	r.NoError(err)
	pc.Storage = app.ProxyS3Config("main", storages)
	pc.Auth.UseStorage = "main"

	e := app.SetupChorus(t, wc, pc)

	mainClient, _ := app.CreateClient(t, storages["main"].StorageAddress, cred)
	f1Client, _ := app.CreateClient(t, storages["f1"].StorageAddress, cred)
	proxyClient, _ := app.CreateClient(t, s3.StorageAddress{Address: e.ProxyAddr}, cred)

	_, err = e.PolicyClient.AddReplication(ctx, &pb.AddReplicationRequest{
		Id: &pb.ReplicationID{
			User:        ipv6TestUser,
			FromStorage: "main",
			ToStorage:   "f1",
		},
	})
	r.NoError(err)

	bucket := "ipv6-bucket"
	r.NoError(proxyClient.MakeBucket(ctx, bucket, mclient.MakeBucketOptions{}))

	r.Eventually(func() bool {
		ok, err := mainClient.BucketExists(ctx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = f1Client.BucketExists(ctx, bucket)
		return err == nil && ok
	}, e.WaitShort, e.RetryShort, "bucket did not replicate to f1 over IPv6")

	objName := "hello.txt"
	source := bytes.Repeat([]byte("ipv6"), 1024)
	_, err = proxyClient.PutObject(ctx, bucket, objName,
		bytes.NewReader(source), int64(len(source)),
		mclient.PutObjectOptions{ContentType: "text/plain", DisableContentSha256: true})
	r.NoError(err)

	r.Eventually(func() bool {
		_, err := f1Client.StatObject(ctx, bucket, objName, mclient.StatObjectOptions{})
		return err == nil
	}, e.WaitShort, e.RetryShort, "object did not replicate to f1 over IPv6")

	obj, err := proxyClient.GetObject(ctx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)
	got, err := io.ReadAll(obj)
	r.NoError(err)
	r.Equal(source, got)
}
