package migration

import (
	"bytes"
	"io"
	"testing"

	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/test/env"
)

func TestApi_Migrate_CustomBucket(t *testing.T) {
	e := env.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()

	bucketSrc := "src-cb-test"
	bucketDst := "dst-cb-test"

	r := require.New(t)

	// create source bucket in main
	err := e.MainClient.MakeBucket(tstCtx, bucketSrc, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)
	// check that only src bucket exists only in main:
	ok, err := e.MainClient.BucketExists(tstCtx, bucketSrc)
	r.NoError(err)
	r.True(ok)
	ok, err = e.F1Client.BucketExists(tstCtx, bucketSrc)
	r.NoError(err)
	r.False(ok)
	ok, err = e.F2Client.BucketExists(tstCtx, bucketSrc)
	r.NoError(err)
	r.False(ok)
	ok, err = e.MainClient.BucketExists(tstCtx, bucketDst)
	r.NoError(err)
	r.False(ok)
	ok, err = e.F1Client.BucketExists(tstCtx, bucketDst)
	r.NoError(err)
	r.False(ok)
	ok, err = e.F2Client.BucketExists(tstCtx, bucketDst)
	r.NoError(err)
	r.False(ok)

	// put objects in main src bucket
	obj1 := getTestObj("obj1", bucketSrc)
	_, err = e.MainClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj2 := getTestObj("photo/sept/obj2", bucketSrc)
	_, err = e.MainClient.PutObject(tstCtx, obj2.bucket, obj2.name, bytes.NewReader(obj2.data), int64(len(obj2.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj3 := getTestObj("photo/obj3", bucketSrc)
	_, err = e.MainClient.PutObject(tstCtx, obj3.bucket, obj3.name, bytes.NewReader(obj3.data), int64(len(obj3.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj4 := getTestObj("photo/obj4", bucketSrc)
	_, err = e.MainClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)

	// start replication to same storage and custom bucket
	repl, err := e.ApiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Empty(repl.Replications)

	_, err = e.ApiClient.AddBucketReplication(tstCtx, &pb.AddBucketReplicationRequest{
		User:        user,
		FromStorage: "main",
		FromBucket:  bucketSrc,
		ToStorage:   "main",
		ToBucket:    bucketDst,
	})
	r.NoError(err)

	reps, err := e.ApiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(reps.Replications, 1)
	r.EqualValues(user, reps.Replications[0].User)
	r.EqualValues("main", reps.Replications[0].From)
	r.EqualValues("main", reps.Replications[0].To)
	r.EqualValues(bucketSrc, reps.Replications[0].Bucket)
	r.NotNil(reps.Replications[0].ToBucket)
	r.EqualValues(bucketDst, reps.Replications[0].ToBucket)

	//w8 replication to start
	r.Eventually(func() bool {
		reps, err = e.ApiClient.ListReplications(tstCtx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		if len(reps.Replications) != 1 {
			return false
		}
		return reps.Replications[0].InitObjListed > 0
	}, e.WaitLong, e.RetryLong)

	// add live event
	obj5 := getTestObj("obj5", bucketSrc)
	_, err = e.ProxyClient.PutObject(tstCtx, obj5.bucket, obj5.name, bytes.NewReader(obj5.data), int64(len(obj5.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)

	// wait replication to be done
	r.Eventually(func() bool {
		reps, err = e.ApiClient.ListReplications(tstCtx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		if len(reps.Replications) != 1 {
			return false
		}
		return reps.Replications[0].IsInitDone && reps.Replications[0].Events > 0 && reps.Replications[0].Events == reps.Replications[0].EventsDone
	}, e.WaitLong, e.RetryLong)

	diff, err := e.ApiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Bucket:    bucketSrc,
		From:      "main",
		To:        "main",
		ShowMatch: true,
		User:      user,
		ToBucket:  bucketDst,
	})
	r.NoError(err)
	r.True(diff.IsMatch)
	r.Empty(diff.Error)
	r.Len(diff.Match, 5)
	r.Empty(diff.MissFrom)
	r.Empty(diff.MissTo)

	// get obj from proxy
	proxyObj1, err := e.ProxyClient.GetObject(tstCtx, obj1.bucket, obj1.name, mclient.GetObjectOptions{})
	r.NoError(err)
	proxyObj1Bytes, err := io.ReadAll(proxyObj1)
	r.NoError(err)
	// check that dest bucket is now blocked in proxy
	blocked, err := e.ProxyClient.GetObject(tstCtx, bucketDst, obj1.name, mclient.GetObjectOptions{})
	r.NoError(err)
	_, err = io.ReadAll(blocked)
	r.Error(err)

	//but can be accessed directly from main
	mainObj1Dest, err := e.MainClient.GetObject(tstCtx, bucketDst, obj1.name, mclient.GetObjectOptions{})
	r.NoError(err)
	mainObj1DestBytes, err := io.ReadAll(mainObj1Dest)
	r.NoError(err)
	r.True(bytes.Equal(proxyObj1Bytes, mainObj1DestBytes))

	// check that dest bucket is blocked in proxy but exists if accessed directly
	ok, err = e.ProxyClient.BucketExists(tstCtx, bucketDst)
	r.NoError(err)
	r.False(ok)
	ok, err = e.MainClient.BucketExists(tstCtx, bucketDst)
	r.NoError(err)
	r.True(ok)

	// create replication to different storage and different bucket
	_, err = e.ApiClient.AddBucketReplication(tstCtx, &pb.AddBucketReplicationRequest{
		User:        user,
		FromStorage: "main",
		FromBucket:  bucketSrc,
		ToStorage:   "f1",
		ToBucket:    bucketDst,
	})
	r.NoError(err)

	reps, err = e.ApiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(reps.Replications, 2)
	r.EqualValues(user, reps.Replications[0].User)
	r.EqualValues("main", reps.Replications[0].From)
	r.EqualValues("f1", reps.Replications[0].To)
	r.EqualValues(bucketSrc, reps.Replications[0].Bucket)
	r.NotNil(reps.Replications[0].ToBucket)
	r.EqualValues(bucketDst, reps.Replications[0].ToBucket)

	//w8 replication to finish
	r.Eventually(func() bool {
		reps, err = e.ApiClient.ListReplications(tstCtx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		if len(reps.Replications) != 2 {
			return false
		}
		rep := reps.Replications[0]
		if rep.To != "f1" {
			return false
		}
		return rep.IsInitDone && rep.InitObjDone > 4
	}, e.WaitLong, e.RetryLong)

	//check that all 3 buckets are the same
	diff, err = e.ApiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Bucket:    bucketSrc,
		From:      "main",
		To:        "main",
		ShowMatch: true,
		User:      user,
		ToBucket:  bucketDst,
	})
	r.NoError(err)
	r.True(diff.IsMatch)
	r.Empty(diff.Error)
	r.Len(diff.Match, 5)
	r.Empty(diff.MissFrom)
	r.Empty(diff.MissTo)

	diff, err = e.ApiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Bucket:    bucketSrc,
		From:      "main",
		To:        "f1",
		ShowMatch: true,
		User:      user,
		ToBucket:  bucketDst,
	})
	r.NoError(err)
	r.True(diff.IsMatch)
	r.Empty(diff.Error)
	r.Len(diff.Match, 5)
	r.Empty(diff.MissFrom)
	r.Empty(diff.MissTo)
}
