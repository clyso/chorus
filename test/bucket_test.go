package test

import (
	"bytes"
	"io"
	"math/rand"
	"testing"

	"github.com/clyso/chorus/test/env"
	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
)

func TestApi_MockBucket(t *testing.T) {
	e := env.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	bucket := "bucket-mock"
	r := require.New(t)
	ok, err := e.MainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)
	ok, err = e.F1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)
	ok, err = e.F2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)
	ok, err = e.ProxyClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)

	_, err = e.MainClient.GetBucketLocation(tstCtx, bucket)
	r.Error(err)
	_, err = e.F1Client.GetBucketLocation(tstCtx, bucket)
	r.Error(err)
	_, err = e.F2Client.GetBucketLocation(tstCtx, bucket)
	r.Error(err)
	_, err = e.ProxyClient.GetBucketLocation(tstCtx, bucket)
	r.Error(err)

	buckets, err := e.MainClient.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)
	buckets, err = e.F1Client.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)
	buckets, err = e.F2Client.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)
	buckets, err = e.ProxyClient.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)

	err = e.MainClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)
	ok, err = e.MainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(ok)

	_, err = e.MainClient.GetBucketLocation(tstCtx, bucket)
	r.NoError(err)

	buckets, err = e.MainClient.ListBuckets(tstCtx)
	r.NoError(err)
	r.Len(buckets, 1)

	r.Eventually(func() bool {
		ok, _ = e.F1Client.BucketExists(tstCtx, bucket)
		if ok {
			return false
		}
		ok, _ = e.F2Client.BucketExists(tstCtx, bucket)
		if ok {
			return false
		}
		return true
	}, e.WaitShort, e.RetryShort)

	_, err = e.F1Client.GetBucketLocation(tstCtx, bucket)
	r.Error(err)
	_, err = e.F2Client.GetBucketLocation(tstCtx, bucket)
	r.Error(err)

	buckets, err = e.F1Client.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)
	buckets, err = e.F2Client.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)
}

func TestApi_Bucket_CRUD(t *testing.T) {
	e := env.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	bucket := "bucket-crud"
	r := require.New(t)
	ok, err := e.MainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)
	ok, err = e.F1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)
	ok, err = e.F2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)
	ok, err = e.ProxyClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)

	_, err = e.MainClient.GetBucketLocation(tstCtx, bucket)
	r.Error(err)
	_, err = e.F1Client.GetBucketLocation(tstCtx, bucket)
	r.Error(err)
	_, err = e.F2Client.GetBucketLocation(tstCtx, bucket)
	r.Error(err)
	l, err := e.ProxyClient.GetBucketLocation(tstCtx, bucket)
	r.Error(err, l)

	buckets, err := e.MainClient.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)
	buckets, err = e.F1Client.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)
	buckets, err = e.F2Client.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)
	buckets, err = e.ProxyClient.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)

	err = e.ProxyClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)
	ok, err = e.ProxyClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(ok)

	_, err = e.ProxyClient.GetBucketLocation(tstCtx, bucket)
	r.NoError(err)

	buckets, err = e.ProxyClient.ListBuckets(tstCtx)
	r.NoError(err)
	r.Len(buckets, 1)

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
	}, e.WaitLong, e.RetryLong)

	_, err = e.MainClient.GetBucketLocation(tstCtx, bucket)
	r.NoError(err)
	_, err = e.F1Client.GetBucketLocation(tstCtx, bucket)
	r.NoError(err)
	_, err = e.F2Client.GetBucketLocation(tstCtx, bucket)
	r.NoError(err)

	buckets, err = e.MainClient.ListBuckets(tstCtx)
	r.NoError(err)
	r.Len(buckets, 1)
	buckets, err = e.F1Client.ListBuckets(tstCtx)
	r.NoError(err)
	r.Len(buckets, 1)
	buckets, err = e.F2Client.ListBuckets(tstCtx)
	r.NoError(err)
	r.Len(buckets, 1)

}

func TestApi_Bucket_List(t *testing.T) {
	e := env.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	b1, b2 := "bucket-list-1", "bucket-list-2"
	r := require.New(t)
	ok, err := e.MainClient.BucketExists(tstCtx, b1)
	r.NoError(err)
	r.False(ok)
	ok, err = e.MainClient.BucketExists(tstCtx, b2)
	r.NoError(err)
	r.False(ok)

	err = e.ProxyClient.MakeBucket(tstCtx, b1, mclient.MakeBucketOptions{})
	r.NoError(err)
	ok, err = e.ProxyClient.BucketExists(tstCtx, b1)
	r.NoError(err)
	r.True(ok)

	err = e.ProxyClient.MakeBucket(tstCtx, b2, mclient.MakeBucketOptions{})
	r.NoError(err)
	ok, err = e.ProxyClient.BucketExists(tstCtx, b2)
	r.NoError(err)
	r.True(ok)

	buckets, err := e.ProxyClient.ListBuckets(tstCtx)
	r.NoError(err)
	r.Len(buckets, 2)
	var bucketList []string
	for _, bucket := range buckets {
		bucketList = append(bucketList, bucket.Name)
	}
	r.Contains(bucketList, b1)
	r.Contains(bucketList, b2)

	r.Eventually(func() bool {
		buckets, err = e.MainClient.ListBuckets(tstCtx)
		if err != nil || len(buckets) != 2 {
			return false
		}

		buckets, err = e.F1Client.ListBuckets(tstCtx)
		if err != nil || len(buckets) != 2 {
			return false
		}

		buckets, err = e.F2Client.ListBuckets(tstCtx)
		if err != nil || len(buckets) != 2 {
			return false
		}
		return true
	}, e.WaitShort, e.RetryShort)

	err = e.ProxyClient.RemoveBucket(tstCtx, b1)
	r.NoError(err)

	buckets, err = e.ProxyClient.ListBuckets(tstCtx)
	r.NoError(err)
	r.Len(buckets, 1)
	r.EqualValues(b2, buckets[0].Name)

	r.Eventually(func() bool {
		buckets, err = e.MainClient.ListBuckets(tstCtx)
		if err != nil || len(buckets) != 1 {
			return false
		}

		buckets, err = e.F1Client.ListBuckets(tstCtx)
		if err != nil || len(buckets) != 1 {
			return false
		}

		buckets, err = e.F2Client.ListBuckets(tstCtx)
		if err != nil || len(buckets) != 1 {
			return false
		}
		return true
	}, e.WaitShort, e.RetryShort)

	body1 := bytes.Repeat([]byte("1"), rand.Intn(1<<20)+32*1024)
	body2 := bytes.Repeat([]byte("2"), rand.Intn(1<<20)+32*1024)
	body3 := bytes.Repeat([]byte("3"), rand.Intn(1<<20)+32*1024)

	oName1 := "obj1"
	oName2 := "obj2"
	oName3 := "obj3"

	objMap := map[string][]byte{
		oName1: body1,
		oName2: body2,
		oName3: body3,
	}

	_, err = e.ProxyClient.PutObject(tstCtx, b2, oName1, bytes.NewReader(body1), int64(len(body1)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)

	objRes, err := e.ProxyClient.GetObject(tstCtx, b2, oName1, mclient.GetObjectOptions{})
	r.NoError(err)

	objBytes, err := io.ReadAll(objRes)
	r.NoError(err)
	r.EqualValues(body1, objBytes)

	objCh := e.ProxyClient.ListObjects(tstCtx, b2, mclient.ListObjectsOptions{UseV1: true})
	objNum := 0
	for obj := range objCh {
		body := objMap[obj.Key]
		r.EqualValues(len(body), obj.Size)
		objNum++
	}
	r.EqualValues(1, objNum)

	_, err = e.ProxyClient.PutObject(tstCtx, b2, oName2, bytes.NewReader(body2), int64(len(body2)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)

	objRes, err = e.ProxyClient.GetObject(tstCtx, b2, oName2, mclient.GetObjectOptions{})
	r.NoError(err)

	objBytes, err = io.ReadAll(objRes)
	r.NoError(err)
	r.EqualValues(body2, objBytes)

	_, err = e.ProxyClient.PutObject(tstCtx, b2, oName3, bytes.NewReader(body3), int64(len(body3)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)

	objRes, err = e.ProxyClient.GetObject(tstCtx, b2, oName3, mclient.GetObjectOptions{})
	r.NoError(err)

	objBytes, err = io.ReadAll(objRes)
	r.NoError(err)
	r.EqualValues(body3, objBytes)

	objCh = e.ProxyClient.ListObjects(tstCtx, b2, mclient.ListObjectsOptions{WithMetadata: true})
	objNum = 0
	for obj := range objCh {
		body := objMap[obj.Key]
		r.EqualValues(len(body), obj.Size)
		objNum++
	}
	r.EqualValues(3, objNum)

	r.Eventually(func() bool {
		objCh = e.MainClient.ListObjects(tstCtx, b2, mclient.ListObjectsOptions{WithMetadata: true})
		objNum = 0
		for range objCh {
			objNum++
		}
		if objNum != 3 {
			return false
		}

		objCh = e.F1Client.ListObjects(tstCtx, b2, mclient.ListObjectsOptions{WithMetadata: true})
		objNum = 0
		for range objCh {
			objNum++
		}
		if objNum != 3 {
			return false
		}

		objCh = e.F2Client.ListObjects(tstCtx, b2, mclient.ListObjectsOptions{WithMetadata: true})
		objNum = 0
		for range objCh {
			objNum++
		}
		if objNum != 3 {
			return false
		}
		return true
	}, e.WaitShort, e.RetryShort)

	remCh := make(chan mclient.ObjectInfo, len(objMap))
	errCh := e.ProxyClient.RemoveObjects(tstCtx, b2, remCh, mclient.RemoveObjectsOptions{})
	for name := range objMap {
		remCh <- mclient.ObjectInfo{
			Key: name,
		}
	}
	close(remCh)
	for errRem := range errCh {
		r.NoError(errRem.Err)
	}

	objCh = e.ProxyClient.ListObjects(tstCtx, b2, mclient.ListObjectsOptions{WithMetadata: true})
	objNum = 0
	for obj := range objCh {
		body := objMap[obj.Key]
		r.EqualValues(len(body), obj.Size)
		objNum++
	}
	r.EqualValues(0, objNum)

	r.Eventually(func() bool {
		objCh = e.MainClient.ListObjects(tstCtx, b2, mclient.ListObjectsOptions{WithMetadata: true})
		objNum = 0
		for range objCh {
			objNum++
		}
		if objNum != 0 {
			return false
		}

		objCh = e.F1Client.ListObjects(tstCtx, b2, mclient.ListObjectsOptions{WithMetadata: true})
		objNum = 0
		for range objCh {
			objNum++
		}
		if objNum != 0 {
			return false
		}

		objCh = e.F2Client.ListObjects(tstCtx, b2, mclient.ListObjectsOptions{WithMetadata: true})
		objNum = 0
		for range objCh {
			objNum++
		}
		if objNum != 0 {
			return false
		}
		return true
	}, e.WaitLong, e.RetryLong)
}
