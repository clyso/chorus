package test

import (
	"bytes"
	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"io"
	"math/rand"
	"testing"
	"time"
)

func TestApi_MockBucket(t *testing.T) {
	bucket := "bucket-mock"
	r := require.New(t)
	ok, err := mainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)
	ok, err = f1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)
	ok, err = f2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)
	ok, err = proxyClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)

	_, err = mainClient.GetBucketLocation(tstCtx, bucket)
	r.Error(err)
	_, err = f1Client.GetBucketLocation(tstCtx, bucket)
	r.Error(err)
	_, err = f2Client.GetBucketLocation(tstCtx, bucket)
	r.Error(err)
	_, err = proxyClient.GetBucketLocation(tstCtx, bucket)
	r.Error(err)

	buckets, err := mainClient.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)
	buckets, err = f1Client.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)
	buckets, err = f2Client.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)
	buckets, err = proxyClient.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)

	err = mainClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)
	t.Cleanup(func() {
		cleanup(t, false, bucket)
	})
	ok, err = mainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(ok)

	_, err = mainClient.GetBucketLocation(tstCtx, bucket)
	r.NoError(err)

	buckets, err = mainClient.ListBuckets(tstCtx)
	r.NoError(err)
	r.Len(buckets, 1)

	r.Eventually(func() bool {
		ok, _ = f1Client.BucketExists(tstCtx, bucket)
		if ok {
			return false
		}
		ok, _ = f2Client.BucketExists(tstCtx, bucket)
		if ok {
			return false
		}
		return true
	}, time.Second*2, time.Millisecond*100)

	_, err = f1Client.GetBucketLocation(tstCtx, bucket)
	r.Error(err)
	_, err = f2Client.GetBucketLocation(tstCtx, bucket)
	r.Error(err)

	buckets, err = f1Client.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)
	buckets, err = f2Client.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)
}

func TestApi_Bucket_CRUD(t *testing.T) {
	bucket := "bucket-crud"
	r := require.New(t)
	ok, err := mainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)
	ok, err = f1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)
	ok, err = f2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)
	ok, err = proxyClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)

	_, err = mainClient.GetBucketLocation(tstCtx, bucket)
	r.Error(err)
	_, err = f1Client.GetBucketLocation(tstCtx, bucket)
	r.Error(err)
	_, err = f2Client.GetBucketLocation(tstCtx, bucket)
	r.Error(err)
	l, err := proxyClient.GetBucketLocation(tstCtx, bucket)
	r.Error(err, l)

	buckets, err := mainClient.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)
	buckets, err = f1Client.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)
	buckets, err = f2Client.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)
	buckets, err = proxyClient.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)

	err = proxyClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)
	t.Cleanup(func() {
		cleanup(t, false, bucket)
	})
	ok, err = proxyClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(ok)

	_, err = proxyClient.GetBucketLocation(tstCtx, bucket)
	r.NoError(err)

	buckets, err = proxyClient.ListBuckets(tstCtx)
	r.NoError(err)
	r.Len(buckets, 1)

	r.Eventually(func() bool {
		ok, err = mainClient.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = f1Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = f2Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		return true
	}, time.Second*5, time.Millisecond*100)

	_, err = mainClient.GetBucketLocation(tstCtx, bucket)
	r.NoError(err)
	_, err = f1Client.GetBucketLocation(tstCtx, bucket)
	r.NoError(err)
	_, err = f2Client.GetBucketLocation(tstCtx, bucket)
	r.NoError(err)

	buckets, err = mainClient.ListBuckets(tstCtx)
	r.NoError(err)
	r.Len(buckets, 1)
	buckets, err = f1Client.ListBuckets(tstCtx)
	r.NoError(err)
	r.Len(buckets, 1)
	buckets, err = f2Client.ListBuckets(tstCtx)
	r.NoError(err)
	r.Len(buckets, 1)

}

func TestApi_Bucket_List(t *testing.T) {
	b1, b2 := "bucket-list-1", "bucket-list-2"
	r := require.New(t)
	ok, err := mainClient.BucketExists(tstCtx, b1)
	r.NoError(err)
	r.False(ok)
	ok, err = mainClient.BucketExists(tstCtx, b2)
	r.NoError(err)
	r.False(ok)

	err = proxyClient.MakeBucket(tstCtx, b1, mclient.MakeBucketOptions{})
	r.NoError(err)
	t.Cleanup(func() {
		cleanup(t, false, b1)
	})
	ok, err = proxyClient.BucketExists(tstCtx, b1)
	r.NoError(err)
	r.True(ok)

	err = proxyClient.MakeBucket(tstCtx, b2, mclient.MakeBucketOptions{})
	r.NoError(err)
	t.Cleanup(func() {
		cleanup(t, false, b2)
	})
	ok, err = proxyClient.BucketExists(tstCtx, b2)
	r.NoError(err)
	r.True(ok)

	buckets, err := proxyClient.ListBuckets(tstCtx)
	r.NoError(err)
	r.Len(buckets, 2)
	var bucketList []string
	for _, bucket := range buckets {
		bucketList = append(bucketList, bucket.Name)
	}
	r.Contains(bucketList, b1)
	r.Contains(bucketList, b2)

	r.Eventually(func() bool {
		buckets, err = mainClient.ListBuckets(tstCtx)
		if err != nil || len(buckets) != 2 {
			return false
		}

		buckets, err = f1Client.ListBuckets(tstCtx)
		if err != nil || len(buckets) != 2 {
			return false
		}

		buckets, err = f2Client.ListBuckets(tstCtx)
		if err != nil || len(buckets) != 2 {
			return false
		}
		return true
	}, time.Second*3, time.Millisecond*100)

	err = proxyClient.RemoveBucket(tstCtx, b1)
	r.NoError(err)

	buckets, err = proxyClient.ListBuckets(tstCtx)
	r.NoError(err)
	r.Len(buckets, 1)
	r.EqualValues(b2, buckets[0].Name)

	r.Eventually(func() bool {
		buckets, err = mainClient.ListBuckets(tstCtx)
		if err != nil || len(buckets) != 1 {
			return false
		}

		buckets, err = f1Client.ListBuckets(tstCtx)
		if err != nil || len(buckets) != 1 {
			return false
		}

		buckets, err = f2Client.ListBuckets(tstCtx)
		if err != nil || len(buckets) != 1 {
			return false
		}
		return true
	}, time.Second*5, time.Millisecond*100)

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

	_, err = proxyClient.PutObject(tstCtx, b2, oName1, bytes.NewReader(body1), int64(len(body1)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)

	objRes, err := proxyClient.GetObject(tstCtx, b2, oName1, mclient.GetObjectOptions{})
	r.NoError(err)

	objBytes, err := io.ReadAll(objRes)
	r.NoError(err)
	r.EqualValues(body1, objBytes)

	objCh := proxyClient.ListObjects(tstCtx, b2, mclient.ListObjectsOptions{UseV1: true})
	objNum := 0
	for obj := range objCh {
		body := objMap[obj.Key]
		r.EqualValues(len(body), obj.Size)
		objNum++
	}
	r.EqualValues(1, objNum)

	_, err = proxyClient.PutObject(tstCtx, b2, oName2, bytes.NewReader(body2), int64(len(body2)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)

	objRes, err = proxyClient.GetObject(tstCtx, b2, oName2, mclient.GetObjectOptions{})
	r.NoError(err)

	objBytes, err = io.ReadAll(objRes)
	r.NoError(err)
	r.EqualValues(body2, objBytes)

	_, err = proxyClient.PutObject(tstCtx, b2, oName3, bytes.NewReader(body3), int64(len(body3)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)

	objRes, err = proxyClient.GetObject(tstCtx, b2, oName3, mclient.GetObjectOptions{})
	r.NoError(err)

	objBytes, err = io.ReadAll(objRes)
	r.NoError(err)
	r.EqualValues(body3, objBytes)

	objCh = proxyClient.ListObjects(tstCtx, b2, mclient.ListObjectsOptions{WithMetadata: true})
	objNum = 0
	for obj := range objCh {
		body := objMap[obj.Key]
		r.EqualValues(len(body), obj.Size)
		objNum++
	}
	r.EqualValues(3, objNum)

	r.Eventually(func() bool {
		objCh = mainClient.ListObjects(tstCtx, b2, mclient.ListObjectsOptions{WithMetadata: true})
		objNum = 0
		for range objCh {
			objNum++
		}
		if objNum != 3 {
			return false
		}

		objCh = f1Client.ListObjects(tstCtx, b2, mclient.ListObjectsOptions{WithMetadata: true})
		objNum = 0
		for range objCh {
			objNum++
		}
		if objNum != 3 {
			return false
		}

		objCh = f2Client.ListObjects(tstCtx, b2, mclient.ListObjectsOptions{WithMetadata: true})
		objNum = 0
		for range objCh {
			objNum++
		}
		if objNum != 3 {
			return false
		}
		return true
	}, time.Second*3, time.Millisecond*100)

	remCh := make(chan mclient.ObjectInfo, len(objMap))
	errCh := proxyClient.RemoveObjects(tstCtx, b2, remCh, mclient.RemoveObjectsOptions{})
	for name := range objMap {
		remCh <- mclient.ObjectInfo{
			Key: name,
		}
	}
	close(remCh)
	for errRem := range errCh {
		r.NoError(errRem.Err)
	}

	objCh = proxyClient.ListObjects(tstCtx, b2, mclient.ListObjectsOptions{WithMetadata: true})
	objNum = 0
	for obj := range objCh {
		body := objMap[obj.Key]
		r.EqualValues(len(body), obj.Size)
		objNum++
	}
	r.EqualValues(0, objNum)

	r.Eventually(func() bool {
		objCh = mainClient.ListObjects(tstCtx, b2, mclient.ListObjectsOptions{WithMetadata: true})
		objNum = 0
		for range objCh {
			objNum++
		}
		if objNum != 0 {
			return false
		}

		objCh = f1Client.ListObjects(tstCtx, b2, mclient.ListObjectsOptions{WithMetadata: true})
		objNum = 0
		for range objCh {
			objNum++
		}
		if objNum != 0 {
			return false
		}

		objCh = f2Client.ListObjects(tstCtx, b2, mclient.ListObjectsOptions{WithMetadata: true})
		objNum = 0
		for range objCh {
			objNum++
		}
		if objNum != 0 {
			return false
		}
		return true
	}, time.Second*10, time.Millisecond*100)
}
