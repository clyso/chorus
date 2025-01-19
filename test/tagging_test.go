package test

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	mclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/stretchr/testify/require"
)

func TestApi_Tagging_Bucket(t *testing.T) {
	t.Skip("fake s3 does not support bucket tagging")
	bucket := "bucket-tag"
	r := require.New(t)

	ok, err := proxyClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)

	err = proxyClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)
	t.Cleanup(func() {
		cleanup(t, false, bucket)
	})

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
	}, time.Second*3, time.Millisecond*100)

	tagging, err := proxyClient.GetBucketTagging(tstCtx, bucket)
	r.NoError(err)
	r.Empty(tagging.ToMap())

	tagging, err = mainClient.GetBucketTagging(tstCtx, bucket)
	r.NoError(err)
	r.Empty(tagging.ToMap())

	tagging, err = f1Client.GetBucketTagging(tstCtx, bucket)
	r.NoError(err)
	r.Empty(tagging.ToMap())

	tagging, err = f2Client.GetBucketTagging(tstCtx, bucket)
	r.NoError(err)
	r.Empty(tagging.ToMap())

	oldObjName := "obj-old"
	sourceOld := bytes.Repeat([]byte("3"), rand.Intn(1<<20)+32*1024)
	_, err = proxyClient.PutObject(tstCtx, bucket, oldObjName, bytes.NewReader(sourceOld), int64(len(sourceOld)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)
	r.Eventually(func() bool {
		_, err = mainClient.StatObject(tstCtx, bucket, oldObjName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = f1Client.StatObject(tstCtx, bucket, oldObjName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = f2Client.StatObject(tstCtx, bucket, oldObjName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		return true
	}, time.Second*3, time.Millisecond*100)

	objTag, err := proxyClient.GetObjectTagging(tstCtx, bucket, oldObjName, mclient.GetObjectTaggingOptions{})
	r.NoError(err)
	r.Empty(objTag.ToMap())
	objTag, err = mainClient.GetObjectTagging(tstCtx, bucket, oldObjName, mclient.GetObjectTaggingOptions{})
	r.NoError(err)
	r.Empty(objTag.ToMap())
	objTag, err = f1Client.GetObjectTagging(tstCtx, bucket, oldObjName, mclient.GetObjectTaggingOptions{})
	r.NoError(err)
	r.Empty(objTag.ToMap())
	objTag, err = f2Client.GetObjectTagging(tstCtx, bucket, oldObjName, mclient.GetObjectTaggingOptions{})
	r.NoError(err)
	r.Empty(objTag.ToMap())

	bTags, err := tags.NewTags(map[string]string{"mytag1": "foo", "mytag2": "bar"}, false)
	r.NoError(err)
	err = proxyClient.SetBucketTagging(tstCtx, bucket, bTags)
	r.NoError(err)

	bTagsRes, err := proxyClient.GetBucketTagging(tstCtx, bucket)
	r.NoError(err)
	r.Equal(bTags.String(), bTagsRes.String())

	r.Eventually(func() bool {
		bTagsRes, err = mainClient.GetBucketTagging(tstCtx, bucket)
		if err != nil || bTagsRes.String() != bTags.String() {
			return false
		}
		bTagsRes, err = f1Client.GetBucketTagging(tstCtx, bucket)
		if err != nil || bTagsRes.String() != bTags.String() {
			return false
		}
		bTagsRes, err = f2Client.GetBucketTagging(tstCtx, bucket)
		if err != nil || bTagsRes.String() != bTags.String() {
			return false
		}
		return true
	}, time.Second*3, time.Millisecond*100)

	objTag, err = proxyClient.GetObjectTagging(tstCtx, bucket, oldObjName, mclient.GetObjectTaggingOptions{})
	r.NoError(err)
	r.Equal(bTags.String(), objTag.String())
	objTag, err = mainClient.GetObjectTagging(tstCtx, bucket, oldObjName, mclient.GetObjectTaggingOptions{})
	r.NoError(err)
	r.Equal(bTags.String(), objTag.String())
	objTag, err = f1Client.GetObjectTagging(tstCtx, bucket, oldObjName, mclient.GetObjectTaggingOptions{})
	r.NoError(err)
	r.Equal(bTags.String(), objTag.String())
	objTag, err = f2Client.GetObjectTagging(tstCtx, bucket, oldObjName, mclient.GetObjectTaggingOptions{})
	r.NoError(err)
	r.Equal(bTags.String(), objTag.String())

}

func TestApi_Tagging_Object(t *testing.T) {
	t.Skip("fake s3 does not support bucket tagging")

	bucket := "bucket-tag-obj"
	r := require.New(t)

	ok, err := proxyClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)

	err = proxyClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)
	t.Cleanup(func() {
		cleanup(t, false, bucket)
	})

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
	}, time.Second*3, time.Millisecond*100)

	objName := "obj-old"
	source := bytes.Repeat([]byte("6"), rand.Intn(1<<20)+32*1024)
	_, err = proxyClient.PutObject(tstCtx, bucket, objName, bytes.NewReader(source), int64(len(source)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)
	r.Eventually(func() bool {
		_, err = mainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = f1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = f2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		return true
	}, time.Second*3, time.Millisecond*100)

	bTags, err := tags.NewTags(map[string]string{"mytag1": "foo", "mytag2": "bar"}, false)
	r.NoError(err)
	err = proxyClient.PutObjectTagging(tstCtx, bucket, objName, bTags, mclient.PutObjectTaggingOptions{})
	r.NoError(err)

	bTagsRes, err := proxyClient.GetObjectTagging(tstCtx, bucket, objName, mclient.GetObjectTaggingOptions{})
	r.NoError(err)
	r.Equal(bTags.String(), bTagsRes.String())

	r.Eventually(func() bool {
		bTagsRes, err = mainClient.GetObjectTagging(tstCtx, bucket, objName, mclient.GetObjectTaggingOptions{})
		if err != nil || bTagsRes.String() != bTags.String() {
			return false
		}
		bTagsRes, err = f1Client.GetObjectTagging(tstCtx, bucket, objName, mclient.GetObjectTaggingOptions{})
		if err != nil || bTagsRes.String() != bTags.String() {
			return false
		}
		bTagsRes, err = f2Client.GetObjectTagging(tstCtx, bucket, objName, mclient.GetObjectTaggingOptions{})
		if err != nil || bTagsRes.String() != bTags.String() {
			return false
		}
		return true
	}, time.Second*60, time.Millisecond*100)

	//todo: remove tagging not works
	//err = proxyClient.RemoveObjectTagging(tstCtx, bucket, objName, mclient.RemoveObjectTaggingOptions{})
	//r.NoError(err)
	//
	//bTagsRes, err = proxyClient.GetObjectTagging(tstCtx, bucket, objName, mclient.GetObjectTaggingOptions{})
	//r.Error(err)
	//
	//r.Eventually(func() bool {
	//	_, err = mainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	//	if err != nil {
	//		return false
	//	}
	//	_, err = f1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	//	if err != nil {
	//		return false
	//	}
	//	_, err = f2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	//	if err != nil {
	//		return false
	//	}
	//	return true
	//}, time.Second*3, time.Millisecond*100)
	//
	//r.Eventually(func() bool {
	//	_, err = mainClient.GetObjectTagging(tstCtx, bucket, objName, mclient.GetObjectTaggingOptions{})
	//	if err == nil {
	//		return false
	//	}
	//	_, err = f1Client.GetObjectTagging(tstCtx, bucket, objName, mclient.GetObjectTaggingOptions{})
	//	if err == nil {
	//		return false
	//	}
	//	_, err = f2Client.GetObjectTagging(tstCtx, bucket, objName, mclient.GetObjectTaggingOptions{})
	//	if err == nil {
	//		return false
	//	}
	//	return true
	//}, time.Second*3, time.Millisecond*100)
}
