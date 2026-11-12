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
	"bytes"
	"crypto/rand"
	"testing"

	mclient "github.com/minio/minio-go/v7"
	mcredentials "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/test/app"
)

// Test_e2e_proxy_streaming_signature uploads through the proxy with streaming
// SigV4 (aws-chunked body with per-chunk signatures), which minio-go uses by
// default over plain http. MinIO validates every chunk signature, so the proxy
// has to re-sign the chunks with the storage credentials.
func Test_e2e_proxy_streaming_signature(t *testing.T) {
	e := app.SetupChorus(t, workerConf, proxyConf)
	proxyClient, err := newS3Client(e.ProxyAddr, storageCreds)
	require.NoError(t, err)

	randBytes := func(t *testing.T, n int) []byte {
		b := make([]byte, n)
		_, err := rand.Read(b)
		require.NoError(t, err)
		return b
	}
	put := func(t *testing.T, c *mclient.Client, bucket, obj string, data []byte) {
		r := require.New(t)
		_, err := c.PutObject(t.Context(), bucket, obj, bytes.NewReader(data), int64(len(data)), mclient.PutObjectOptions{PartSize: 5 << 20})
		r.NoError(err)
		r.Equal(data, readObject(t, proxyClient, bucket, obj))
		r.Equal(data, readObject(t, minioClient, bucket, obj))
	}

	bucket := bucketName("streaming")
	require.NoError(t, proxyClient.MakeBucket(t.Context(), bucket, mclient.MakeBucketOptions{}))

	t.Run("put_object", func(t *testing.T) {
		put(t, proxyClient, bucket, "small", randBytes(t, 1000))
	})
	t.Run("put_object_multiple_chunks", func(t *testing.T) {
		put(t, proxyClient, bucket, "chunks", randBytes(t, 200<<10))
	})
	t.Run("put_object_empty", func(t *testing.T) {
		put(t, proxyClient, bucket, "empty", []byte{})
	})
	t.Run("upload_part", func(t *testing.T) {
		r := require.New(t)
		ctx := t.Context()
		core := mclient.Core{Client: proxyClient}
		obj := "multipart"
		part1, part2 := randBytes(t, 5<<20), randBytes(t, 1000)
		uploadID, err := core.NewMultipartUpload(ctx, bucket, obj, mclient.PutObjectOptions{})
		r.NoError(err)
		p1, err := core.PutObjectPart(ctx, bucket, obj, uploadID, 1, bytes.NewReader(part1), int64(len(part1)), mclient.PutObjectPartOptions{})
		r.NoError(err)
		p2, err := core.PutObjectPart(ctx, bucket, obj, uploadID, 2, bytes.NewReader(part2), int64(len(part2)), mclient.PutObjectPartOptions{})
		r.NoError(err)
		_, err = core.CompleteMultipartUpload(ctx, bucket, obj, uploadID, []mclient.CompletePart{
			{PartNumber: 1, ETag: p1.ETag}, {PartNumber: 2, ETag: p2.ETag},
		}, mclient.PutObjectOptions{})
		r.NoError(err)
		r.Equal(append(part1, part2...), readObject(t, minioClient, bucket, obj))
	})
	t.Run("multipart_signed_trailer", func(t *testing.T) {
		// with trailing headers minio-go sends each part's checksum in a signed
		// trailer: STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER
		trailerClient, err := mclient.New(e.ProxyAddr, &mclient.Options{
			Creds:           mcredentials.NewStaticV4(storageCreds.AccessKeyID, storageCreds.SecretAccessKey, ""),
			TrailingHeaders: true,
		})
		require.NoError(t, err)
		put(t, trailerClient, bucket, "trailer", randBytes(t, 5<<20+1000))
	})
}
