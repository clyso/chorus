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
	"crypto/sha256"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	mclient "github.com/minio/minio-go/v7"
	mcredentials "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/s3"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/test/app"
)

const (
	presignExpiry = 10 * time.Minute
	emptySHA256   = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// bucketName suffixes name, so that the suite can be re-run against the same
// storage container.
func bucketName(name string) string {
	return name + "-" + xid.New().String()
}

func doRequest(t *testing.T, method string, u *url.URL, body string) (int, http.Header, string) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, u.String(), reader)
	require.NoError(t, err)
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	resBody, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	return res.StatusCode, res.Header, string(resBody)
}

func Test_e2e_proxy_presigned_sigv4(t *testing.T) {
	e := app.SetupChorus(t, workerConf, proxyConf)

	proxyClient, err := mclient.New(e.ProxyAddr, &mclient.Options{
		Creds:  mcredentials.NewStaticV4(storageCreds.AccessKeyID, storageCreds.SecretAccessKey, ""),
		Secure: false,
	})
	require.NoError(t, err)

	ctx := t.Context()
	bucket := bucketName("presign-test")
	require.NoError(t, proxyClient.MakeBucket(ctx, bucket, mclient.MakeBucketOptions{}))

	t.Run("presigned_get", func(t *testing.T) {
		r := require.New(t)
		ctx := t.Context()

		obj, content := "get-obj.txt", "presigned get content"
		_, err := minioClient.PutObject(ctx, bucket, obj, strings.NewReader(content), int64(len(content)), mclient.PutObjectOptions{ContentType: "text/plain"})
		r.NoError(err)

		presignedURL, err := proxyClient.PresignedGetObject(ctx, bucket, obj, presignExpiry, nil)
		r.NoError(err)

		code, _, body := doRequest(t, http.MethodGet, presignedURL, "")
		r.Equal(http.StatusOK, code, body)
		r.Equal(content, body)
	})

	t.Run("presigned_put", func(t *testing.T) {
		r := require.New(t)
		ctx := t.Context()

		obj, content := "put-obj.txt", "presigned put content"
		presignedURL, err := proxyClient.PresignedPutObject(ctx, bucket, obj, presignExpiry)
		r.NoError(err)

		code, _, body := doRequest(t, http.MethodPut, presignedURL, content)
		r.Equal(http.StatusOK, code, body)

		r.Equal(content, string(readObject(t, minioClient, bucket, obj)))
	})

	t.Run("presigned_head", func(t *testing.T) {
		r := require.New(t)
		ctx := t.Context()

		obj, content := "head-obj.txt", "presigned head content"
		_, err := minioClient.PutObject(ctx, bucket, obj, strings.NewReader(content), int64(len(content)), mclient.PutObjectOptions{})
		r.NoError(err)

		presignedURL, err := proxyClient.PresignedHeadObject(ctx, bucket, obj, presignExpiry, nil)
		r.NoError(err)

		code, _, body := doRequest(t, http.MethodHead, presignedURL, "")
		r.Equal(http.StatusOK, code, body)
	})

	t.Run("presigned_get_with_response_params", func(t *testing.T) {
		r := require.New(t)
		ctx := t.Context()

		obj, content := "params-obj.txt", "presigned get with params"
		_, err := minioClient.PutObject(ctx, bucket, obj, strings.NewReader(content), int64(len(content)), mclient.PutObjectOptions{})
		r.NoError(err)

		// non-auth query params must survive the auth-param stripping
		reqParams := url.Values{}
		reqParams.Set("response-content-disposition", `attachment; filename="renamed.txt"`)
		presignedURL, err := proxyClient.PresignedGetObject(ctx, bucket, obj, presignExpiry, reqParams)
		r.NoError(err)

		code, _, body := doRequest(t, http.MethodGet, presignedURL, "")
		r.Equal(http.StatusOK, code, body)
		r.Equal(content, body)
	})

	t.Run("presigned_get_signed_payload_hash", func(t *testing.T) {
		r := require.New(t)
		ctx := t.Context()

		obj, content := "signed-hash-obj.txt", "presigned get with signed payload hash"
		_, err := minioClient.PutObject(ctx, bucket, obj, strings.NewReader(content), int64(len(content)), mclient.PutObjectOptions{})
		r.NoError(err)

		// PresignHeader covers the payload hash by the signature, as MinIO does
		// for a presigned request carrying the header.
		hdr := http.Header{}
		hdr.Set(s3.AmzContentSha256, emptySHA256)
		presignedURL, err := proxyClient.PresignHeader(ctx, http.MethodGet, bucket, obj, presignExpiry, nil, hdr)
		r.NoError(err)
		r.Contains(presignedURL.Query().Get("X-Amz-SignedHeaders"), "x-amz-content-sha256")

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, presignedURL.String(), nil)
		r.NoError(err)
		req.Header.Set(s3.AmzContentSha256, emptySHA256)
		res, err := http.DefaultClient.Do(req)
		r.NoError(err)
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		r.NoError(err)
		r.Equal(http.StatusOK, res.StatusCode, string(body))
		r.Equal(content, string(body))
	})

	t.Run("presigned_get_tampered_signature", func(t *testing.T) {
		r := require.New(t)
		ctx := t.Context()

		obj, content := "tampered-obj.txt", "must not be readable"
		_, err := minioClient.PutObject(ctx, bucket, obj, strings.NewReader(content), int64(len(content)), mclient.PutObjectOptions{})
		r.NoError(err)

		presignedURL, err := proxyClient.PresignedGetObject(ctx, bucket, obj, presignExpiry, nil)
		r.NoError(err)

		query := presignedURL.Query()
		sig := query.Get("X-Amz-Signature")
		r.NotEmpty(sig)
		last := "a"
		if strings.HasSuffix(sig, "a") {
			last = "b"
		}
		query.Set("X-Amz-Signature", sig[:len(sig)-1]+last)
		presignedURL.RawQuery = query.Encode()

		code, _, body := doRequest(t, http.MethodGet, presignedURL, "")
		r.Equal(http.StatusForbidden, code, body)
		r.Contains(body, "SignatureDoesNotMatch")
	})

	t.Run("presigned_get_tampered_object_key", func(t *testing.T) {
		r := require.New(t)
		ctx := t.Context()

		obj, content := "signed-obj.txt", "signed object"
		_, err := minioClient.PutObject(ctx, bucket, obj, strings.NewReader(content), int64(len(content)), mclient.PutObjectOptions{})
		r.NoError(err)

		other, otherContent := "other-obj.txt", "other object"
		_, err = minioClient.PutObject(ctx, bucket, other, strings.NewReader(otherContent), int64(len(otherContent)), mclient.PutObjectOptions{})
		r.NoError(err)

		presignedURL, err := proxyClient.PresignedGetObject(ctx, bucket, obj, presignExpiry, nil)
		r.NoError(err)
		presignedURL.Path = strings.Replace(presignedURL.Path, obj, other, 1)

		code, _, body := doRequest(t, http.MethodGet, presignedURL, "")
		r.Equal(http.StatusForbidden, code, body)
		r.Contains(body, "SignatureDoesNotMatch")
	})

	t.Run("presigned_get_expired", func(t *testing.T) {
		r := require.New(t)
		ctx := t.Context()

		obj, content := "expired-obj.txt", "expired url content"
		_, err := minioClient.PutObject(ctx, bucket, obj, strings.NewReader(content), int64(len(content)), mclient.PutObjectOptions{})
		r.NoError(err)

		presignedURL, err := proxyClient.PresignedGetObject(ctx, bucket, obj, time.Second, nil)
		r.NoError(err)

		time.Sleep(2 * time.Second)

		code, _, body := doRequest(t, http.MethodGet, presignedURL, "")
		r.Equal(http.StatusForbidden, code, body)
		r.Contains(body, "AccessDenied")
	})

	t.Run("presigned_get_unknown_access_key", func(t *testing.T) {
		r := require.New(t)
		ctx := t.Context()

		presignedURL, err := proxyClient.PresignedGetObject(ctx, bucket, "get-obj.txt", presignExpiry, nil)
		r.NoError(err)

		query := presignedURL.Query()
		cred := query.Get("X-Amz-Credential")
		r.NotEmpty(cred)
		_, scope, ok := strings.Cut(cred, "/")
		r.True(ok)
		query.Set("X-Amz-Credential", "UNKNOWNACCESSKEY0000/"+scope)
		presignedURL.RawQuery = query.Encode()

		code, _, body := doRequest(t, http.MethodGet, presignedURL, "")
		r.Equal(http.StatusForbidden, code, body)
	})

	t.Run("no_auth_still_rejected", func(t *testing.T) {
		r := require.New(t)

		u, err := url.Parse("http://" + e.ProxyAddr + "/" + bucket + "/get-obj.txt")
		r.NoError(err)

		code, _, body := doRequest(t, http.MethodGet, u, "")
		r.Equal(http.StatusBadRequest, code, body)
		r.Contains(body, "CredentialsNotSupported")
	})

	t.Run("header_auth_still_works", func(t *testing.T) {
		r := require.New(t)
		ctx := t.Context()

		obj, content := "header-auth-obj.txt", "header auth content"
		// DisableContentSha256 avoids a streaming upload, see
		// streaming_signature_is_not_supported below.
		_, err := proxyClient.PutObject(ctx, bucket, obj, strings.NewReader(content), int64(len(content)),
			mclient.PutObjectOptions{DisableContentSha256: true})
		r.NoError(err)

		r.Equal(content, string(readObject(t, proxyClient, bucket, obj)))
	})
}

// Test_e2e_proxy_streaming_upload pins the streaming upload limitation
// documented in service/proxy/README.md: minio-go signs a payload chunk chain
// against the request signature, which the proxy invalidates by re-signing the
// request. If this test starts failing, chunked uploads got supported and the
// DisableContentSha256 workarounds in this suite can be dropped.
func Test_e2e_proxy_streaming_signature_is_not_supported(t *testing.T) {
	r := require.New(t)
	e := app.SetupChorus(t, workerConf, proxyConf)
	ctx := t.Context()

	proxyClient, err := mclient.New(e.ProxyAddr, &mclient.Options{
		Creds:  mcredentials.NewStaticV4(storageCreds.AccessKeyID, storageCreds.SecretAccessKey, ""),
		Secure: false,
	})
	r.NoError(err)

	bucket := bucketName("streaming-upload")
	r.NoError(proxyClient.MakeBucket(ctx, bucket, mclient.MakeBucketOptions{}))

	content := "streaming upload content"
	_, err = proxyClient.PutObject(ctx, bucket, "obj.txt", strings.NewReader(content), int64(len(content)), mclient.PutObjectOptions{})
	r.Error(err)
	// MinIO reports the broken chunk chain as SignatureDoesNotMatch, Ceph RGW
	// as XAmzContentSHA256Mismatch.
	r.Contains([]string{"SignatureDoesNotMatch", "XAmzContentSHA256Mismatch"}, mclient.ToErrorResponse(err).Code)
}

// Test_e2e_proxy_sigv2 covers the V2 signature path, which - like a presigned
// request - carries no X-Amz-Content-Sha256 header.
func Test_e2e_proxy_sigv2(t *testing.T) {
	r := require.New(t)

	conf, err := app.DeepCopyStruct(proxyConf)
	r.NoError(err)
	conf.Auth.AllowV2Signature = true
	e := app.SetupChorus(t, workerConf, conf)

	ctx := t.Context()
	bucket := bucketName("sigv2-test")
	obj, content := "v2-auth-obj.txt", "v2 auth content"
	r.NoError(minioClient.MakeBucket(ctx, bucket, mclient.MakeBucketOptions{}))
	_, err = minioClient.PutObject(ctx, bucket, obj, strings.NewReader(content), int64(len(content)), mclient.PutObjectOptions{})
	r.NoError(err)

	v2Client, err := mclient.New(e.ProxyAddr, &mclient.Options{
		Creds:  mcredentials.NewStaticV2(storageCreds.AccessKeyID, storageCreds.SecretAccessKey, ""),
		Secure: false,
	})
	r.NoError(err)

	r.Equal(content, string(readObject(t, v2Client, bucket, obj)))
}

// uploadPresignedMultipart uploads obj as a multipart upload where every part
// is PUT through its own presigned URL, as a web front-end would do. The
// upload is created and completed with header-authenticated requests through
// the proxy.
func uploadPresignedMultipart(t *testing.T, core *mclient.Core, bucket, obj string, partSizes ...int) []byte {
	t.Helper()
	r := require.New(t)
	ctx := t.Context()

	uploadID, err := core.NewMultipartUpload(ctx, bucket, obj, mclient.PutObjectOptions{})
	r.NoError(err)

	var content []byte
	parts := make([]mclient.CompletePart, 0, len(partSizes))
	for i, size := range partSizes {
		partNumber := i + 1
		part := bytes.Repeat([]byte{byte('a' + i)}, size)
		content = append(content, part...)

		presignedURL, err := core.Presign(ctx, http.MethodPut, bucket, obj, presignExpiry, url.Values{
			"uploadId":   []string{uploadID},
			"partNumber": []string{strconv.Itoa(partNumber)},
		})
		r.NoError(err)

		code, header, body := doRequest(t, http.MethodPut, presignedURL, string(part))
		r.Equal(http.StatusOK, code, body)
		etag := header.Get("ETag")
		r.NotEmpty(etag, "part upload must return an ETag")

		parts = append(parts, mclient.CompletePart{PartNumber: partNumber, ETag: etag})
	}

	_, err = core.CompleteMultipartUpload(ctx, bucket, obj, uploadID, parts, mclient.PutObjectOptions{})
	r.NoError(err)

	return content
}

func readObject(t *testing.T, client *mclient.Client, bucket, obj string) []byte {
	t.Helper()
	o, err := client.GetObject(t.Context(), bucket, obj, mclient.GetObjectOptions{})
	require.NoError(t, err)
	defer o.Close()
	data, err := io.ReadAll(o)
	require.NoError(t, err)
	return data
}

// requireSameObject compares size and content of an object on two storages.
// ETags are not compared: chorus may split a replicated object into different
// parts than the client did, which changes the multipart ETag suffix.
func requireSameObject(t *testing.T, a, b *mclient.Client, bucket, obj string) {
	t.Helper()
	r := require.New(t)
	ctx := t.Context()

	statA, err := a.StatObject(ctx, bucket, obj, mclient.StatObjectOptions{})
	r.NoError(err)
	statB, err := b.StatObject(ctx, bucket, obj, mclient.StatObjectOptions{})
	r.NoError(err)
	r.Equal(statA.Size, statB.Size, "object size")
	r.Equal(statA.ContentType, statB.ContentType, "object content type")

	dataA, dataB := readObject(t, a, bucket, obj), readObject(t, b, bucket, obj)
	r.Equal(sha256.Sum256(dataA), sha256.Sum256(dataB), "object content")
}

func Test_e2e_proxy_presigned_multipart(t *testing.T) {
	r := require.New(t)
	e := app.SetupChorus(t, workerConf, proxyConf)

	core, err := mclient.NewCore(e.ProxyAddr, &mclient.Options{
		Creds:  mcredentials.NewStaticV4(storageCreds.AccessKeyID, storageCreds.SecretAccessKey, ""),
		Secure: false,
	})
	r.NoError(err)

	ctx := t.Context()
	bucket := bucketName("presign-multipart")
	r.NoError(core.MakeBucket(ctx, bucket, mclient.MakeBucketOptions{}))

	obj := "multipart-obj.bin"
	content := uploadPresignedMultipart(t, core, bucket, obj, 5*1024*1024, 1024)

	stat, err := minioClient.StatObject(ctx, bucket, obj, mclient.StatObjectOptions{})
	r.NoError(err)
	r.EqualValues(len(content), stat.Size)
	r.Contains(stat.ETag, "-2", "object must be stored as a 2 part multipart upload")
	r.Equal(sha256.Sum256(content), sha256.Sum256(readObject(t, minioClient, bucket, obj)))
}

// Test_e2e_proxy_presigned_replication checks that writes authenticated with a
// presigned URL are captured by the proxy and replicated to a follower storage.
func Test_e2e_proxy_presigned_replication(t *testing.T) {
	r := require.New(t)
	e := app.SetupChorus(t, workerConf, proxyConf)
	ctx := t.Context()

	proxyClient, err := mclient.NewCore(e.ProxyAddr, &mclient.Options{
		Creds:  mcredentials.NewStaticV4(storageCreds.AccessKeyID, storageCreds.SecretAccessKey, ""),
		Secure: false,
	})
	r.NoError(err)

	replID := &pb.ReplicationID{
		User:        user,
		FromStorage: minioKey,
		ToStorage:   followerKey,
	}
	_, err = e.PolicyClient.AddReplication(ctx, &pb.AddReplicationRequest{Id: replID})
	r.NoError(err)

	bucket := bucketName("presign-replication")
	r.NoError(proxyClient.MakeBucket(ctx, bucket, mclient.MakeBucketOptions{}))

	singleObj, singleContent := "presigned-put.txt", "replicated presigned put"
	presignedURL, err := proxyClient.PresignedPutObject(ctx, bucket, singleObj, presignExpiry)
	r.NoError(err)
	code, _, body := doRequest(t, http.MethodPut, presignedURL, singleContent)
	r.Equal(http.StatusOK, code, body)

	multipartObj := "presigned-multipart.bin"
	multipartContent := uploadPresignedMultipart(t, proxyClient, bucket, multipartObj, 5*1024*1024, 1024)

	r.Eventually(func() bool {
		repl, err := e.PolicyClient.GetReplication(ctx, replID)
		if err != nil {
			return false
		}
		return repl.IsInitDone && repl.Events != 0 && repl.EventsDone == repl.Events
	}, e.WaitLong, e.RetryLong, "replication did not drain the event queue")

	requireSameObject(t, minioClient, followerClient, bucket, singleObj)
	requireSameObject(t, minioClient, followerClient, bucket, multipartObj)

	r.EqualValues(len(singleContent), len(readObject(t, followerClient, bucket, singleObj)))
	r.EqualValues(len(multipartContent), len(readObject(t, followerClient, bucket, multipartObj)))
}
