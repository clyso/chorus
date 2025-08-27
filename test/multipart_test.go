package test

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/clyso/chorus/test/env"
	mclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/stretchr/testify/require"
)

func TestApi_Object_Multipart(t *testing.T) {
	e := env.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	bucket := "object-mp"
	r := require.New(t)

	err := e.ProxyClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)
	ok, err := e.ProxyClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(ok)

	r.Eventually(func() bool {
		ok, err = e.MainClient.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = e.F1Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = e.F2Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		return true
	}, time.Second*3, time.Millisecond*100)

	objName := "obj-mp"
	_, err = e.MainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = e.F1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = e.F2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = e.ProxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)

	uploadID, err := e.MpProxyClient.NewMultipartUpload(tstCtx, bucket, objName, mclient.PutObjectOptions{UserMetadata: map[string]string{"Content-Type": "binary/octet-stream"}, DisableContentSha256: true})
	r.NoError(err)

	buf := bytes.Repeat([]byte("a"), 32*1024*1024)
	br := bytes.NewReader(buf)
	partBuf := make([]byte, 100*1024*1024)
	parts := make([]mclient.CompletePart, 0, 5)
	partID := 0
	for {
		n, err := br.Read(partBuf)
		if err != nil && err != io.EOF {
			t.Fatal("Error:", err)
		}
		if err == io.EOF {
			break
		}
		if n > 0 {
			partID++
			data := bytes.NewReader(partBuf[:n])
			dataLen := int64(len(partBuf[:n]))
			objectPart, err := e.MpProxyClient.PutObjectPart(tstCtx, bucket, objName, uploadID, partID,
				data, dataLen,
				mclient.PutObjectPartOptions{SSE: encrypt.NewSSE()},
			)
			r.NoError(err)
			parts = append(parts, mclient.CompletePart{
				PartNumber: partID,
				ETag:       objectPart.ETag,
			})
		}
	}

	objectParts, err := e.MpProxyClient.ListObjectParts(tstCtx, bucket, objName, uploadID, 0, 0)
	r.NoError(err)
	r.EqualValues(len(parts), len(objectParts.ObjectParts))

	_, err = e.ProxyClient.StatObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.Error(err)

	_, err = e.MpProxyClient.CompleteMultipartUpload(tstCtx, bucket, objName, uploadID, parts, mclient.PutObjectOptions{})
	r.NoError(err)

	obj, err := e.ProxyClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)

	proxyBytes, err := io.ReadAll(obj)
	r.NoError(err)

	_, err = e.ProxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.NoError(err)

	r.Eventually(func() bool {
		_, err = e.MainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = e.F1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = e.F2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		return true
	}, time.Second*3, time.Millisecond*100)

	obj, err = e.MainClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)
	objBytes, err := io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(proxyBytes, objBytes)

	obj, err = e.F1Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)
	objBytes, err = io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(proxyBytes, objBytes)

	obj, err = e.F2Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)
	objBytes, err = io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(proxyBytes, objBytes)

}
