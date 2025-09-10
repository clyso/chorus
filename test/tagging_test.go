package test

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/clyso/chorus/test/env"
	mclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/stretchr/testify/require"
)

func TestApi_Tagging_Bucket(t *testing.T) {
	t.Skip("fake s3 does not support bucket tagging")
	e, _, _ := env.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	bucket := "bucket-tag"
	r := require.New(t)

	ok, err := e.ProxyClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)

	err = e.ProxyClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)

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
	}, e.WaitShort, e.RetryShort)

	tagging, err := e.ProxyClient.GetBucketTagging(tstCtx, bucket)
	r.NoError(err)
	r.Empty(tagging.ToMap())

	tagging, err = e.MainClient.GetBucketTagging(tstCtx, bucket)
	r.NoError(err)
	r.Empty(tagging.ToMap())

	tagging, err = e.F1Client.GetBucketTagging(tstCtx, bucket)
	r.NoError(err)
	r.Empty(tagging.ToMap())

	tagging, err = e.F2Client.GetBucketTagging(tstCtx, bucket)
	r.NoError(err)
	r.Empty(tagging.ToMap())

	oldObjName := "obj-old"
	sourceOld := bytes.Repeat([]byte("3"), rand.Intn(1<<20)+32*1024)
	_, err = e.ProxyClient.PutObject(tstCtx, bucket, oldObjName, bytes.NewReader(sourceOld), int64(len(sourceOld)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)
	r.Eventually(func() bool {
		_, err = e.MainClient.StatObject(tstCtx, bucket, oldObjName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = e.F1Client.StatObject(tstCtx, bucket, oldObjName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = e.F2Client.StatObject(tstCtx, bucket, oldObjName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		return true
	}, e.WaitShort, e.RetryShort)

	objTag, err := e.ProxyClient.GetObjectTagging(tstCtx, bucket, oldObjName, mclient.GetObjectTaggingOptions{})
	r.NoError(err)
	r.Empty(objTag.ToMap())
	objTag, err = e.MainClient.GetObjectTagging(tstCtx, bucket, oldObjName, mclient.GetObjectTaggingOptions{})
	r.NoError(err)
	r.Empty(objTag.ToMap())
	objTag, err = e.F1Client.GetObjectTagging(tstCtx, bucket, oldObjName, mclient.GetObjectTaggingOptions{})
	r.NoError(err)
	r.Empty(objTag.ToMap())
	objTag, err = e.F2Client.GetObjectTagging(tstCtx, bucket, oldObjName, mclient.GetObjectTaggingOptions{})
	r.NoError(err)
	r.Empty(objTag.ToMap())

	bTags, err := tags.NewTags(map[string]string{"mytag1": "foo", "mytag2": "bar"}, false)
	r.NoError(err)
	err = e.ProxyClient.SetBucketTagging(tstCtx, bucket, bTags)
	r.NoError(err)

	bTagsRes, err := e.ProxyClient.GetBucketTagging(tstCtx, bucket)
	r.NoError(err)
	r.Equal(bTags.String(), bTagsRes.String())

	r.Eventually(func() bool {
		bTagsRes, err = e.MainClient.GetBucketTagging(tstCtx, bucket)
		if err != nil || bTagsRes.String() != bTags.String() {
			return false
		}
		bTagsRes, err = e.F1Client.GetBucketTagging(tstCtx, bucket)
		if err != nil || bTagsRes.String() != bTags.String() {
			return false
		}
		bTagsRes, err = e.F2Client.GetBucketTagging(tstCtx, bucket)
		if err != nil || bTagsRes.String() != bTags.String() {
			return false
		}
		return true
	}, e.WaitShort, e.RetryShort)

	objTag, err = e.ProxyClient.GetObjectTagging(tstCtx, bucket, oldObjName, mclient.GetObjectTaggingOptions{})
	r.NoError(err)
	r.Equal(bTags.String(), objTag.String())
	objTag, err = e.MainClient.GetObjectTagging(tstCtx, bucket, oldObjName, mclient.GetObjectTaggingOptions{})
	r.NoError(err)
	r.Equal(bTags.String(), objTag.String())
	objTag, err = e.F1Client.GetObjectTagging(tstCtx, bucket, oldObjName, mclient.GetObjectTaggingOptions{})
	r.NoError(err)
	r.Equal(bTags.String(), objTag.String())
	objTag, err = e.F2Client.GetObjectTagging(tstCtx, bucket, oldObjName, mclient.GetObjectTaggingOptions{})
	r.NoError(err)
	r.Equal(bTags.String(), objTag.String())

}

func TestApi_Tagging_Object(t *testing.T) {
	t.Skip("fake s3 does not support bucket tagging")
	e, _, _ := env.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()

	bucket := "bucket-tag-obj"
	r := require.New(t)

	ok, err := e.ProxyClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)

	err = e.ProxyClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)

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
	}, e.WaitShort, e.RetryShort)

	objName := "obj-old"
	source := bytes.Repeat([]byte("6"), rand.Intn(1<<20)+32*1024)
	_, err = e.ProxyClient.PutObject(tstCtx, bucket, objName, bytes.NewReader(source), int64(len(source)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
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
	}, e.WaitShort, e.RetryShort)

	bTags, err := tags.NewTags(map[string]string{"mytag1": "foo", "mytag2": "bar"}, false)
	r.NoError(err)
	err = e.ProxyClient.PutObjectTagging(tstCtx, bucket, objName, bTags, mclient.PutObjectTaggingOptions{})
	r.NoError(err)

	bTagsRes, err := e.ProxyClient.GetObjectTagging(tstCtx, bucket, objName, mclient.GetObjectTaggingOptions{})
	r.NoError(err)
	r.Equal(bTags.String(), bTagsRes.String())

	r.Eventually(func() bool {
		bTagsRes, err = e.MainClient.GetObjectTagging(tstCtx, bucket, objName, mclient.GetObjectTaggingOptions{})
		if err != nil || bTagsRes.String() != bTags.String() {
			return false
		}
		bTagsRes, err = e.F1Client.GetObjectTagging(tstCtx, bucket, objName, mclient.GetObjectTaggingOptions{})
		if err != nil || bTagsRes.String() != bTags.String() {
			return false
		}
		bTagsRes, err = e.F2Client.GetObjectTagging(tstCtx, bucket, objName, mclient.GetObjectTaggingOptions{})
		if err != nil || bTagsRes.String() != bTags.String() {
			return false
		}
		return true
	}, e.WaitLong, e.RetryLong)

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
