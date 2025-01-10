package migration

import (
	"bytes"
	"testing"
	"time"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

func Test_Restart_Replication(t *testing.T) {
	const waitInterval = 5 * time.Second
	const retryInterval = 50 * time.Millisecond

	bucket := "restart"
	r := require.New(t)
	err := mainClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)
	t.Cleanup(func() {
		cleanup(bucket)
		apiClient.DeleteReplication(tstCtx, &pb.ReplicationRequest{
			User:   user,
			Bucket: bucket,
			From:   "main",
			To:     "f1",
		})
	})
	ok, err := mainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(ok)
	ok, err = f1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)
	ok, err = f2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)

	// add initial objects
	obj1 := getTestObj("obj1", bucket)
	_, err = mainClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj2 := getTestObj("obj2", bucket)
	_, err = mainClient.PutObject(tstCtx, obj2.bucket, obj2.name, bytes.NewReader(obj2.data), int64(len(obj2.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)

	// start replication
	_, err = apiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		User:    user,
		From:    "main",
		To:      "f1",
		Buckets: []string{bucket},
	})
	r.NoError(err)
	t.Cleanup(func() {
		apiClient.DeleteReplication(tstCtx, &pb.ReplicationRequest{
			User:   user,
			Bucket: bucket,
			From:   "main",
			To:     "f1",
		})
	})

	reps, err := apiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(reps.Replications, 1)
	r.EqualValues(user, reps.Replications[0].User)
	r.EqualValues("main", reps.Replications[0].From)
	r.EqualValues("f1", reps.Replications[0].To)

	r.Eventually(func() bool {
		reps, err = apiClient.ListReplications(tstCtx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		if len(reps.Replications) != 1 {
			return false
		}
		return reps.Replications[0].InitObjDone > 0
	}, waitInterval, retryInterval)

	// add live event
	obj3 := getTestObj("obj3", bucket)
	_, err = proxyClient.PutObject(tstCtx, obj3.bucket, obj3.name, bytes.NewReader(obj3.data), int64(len(obj3.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)

	// wait replication to be done
	r.Eventually(func() bool {
		reps, err = apiClient.ListReplications(tstCtx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		if len(reps.Replications) != 1 {
			return false
		}
		return reps.Replications[0].IsInitDone && reps.Replications[0].Events > 0 && reps.Replications[0].Events == reps.Replications[0].EventsDone
	}, waitInterval, retryInterval)

	diff, err := apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Bucket:    bucket,
		From:      "main",
		To:        "f1",
		ShowMatch: true,
		User:      user,
	})
	r.NoError(err)
	r.True(diff.IsMatch)
	r.Empty(diff.Error)
	r.Len(diff.Match, 3)
	r.Empty(diff.MissFrom)
	r.Empty(diff.MissTo)

	// delete replication
	_, err = apiClient.DeleteReplication(tstCtx, &pb.ReplicationRequest{
		User:   user,
		Bucket: bucket,
		From:   "main",
		To:     "f1",
	})
	r.NoError(err)

	// delete dest bucket
	r.NoError(rmBucket(f1Client, bucket))

	diff, err = apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Bucket:    bucket,
		From:      "main",
		To:        "f1",
		ShowMatch: true,
		User:      user,
	})
	r.NoError(err)
	r.False(diff.IsMatch)

	reps, err = apiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Empty(reps.Replications)

	// restart replication
	_, err = apiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		User:    user,
		From:    "main",
		To:      "f1",
		Buckets: []string{bucket},
	})
	r.NoError(err)

	reps, err = apiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(reps.Replications, 1)

	// wait replication to be done
	r.Eventually(func() bool {
		reps, err = apiClient.ListReplications(tstCtx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		if len(reps.Replications) != 1 {
			return false
		}
		stat := reps.Replications[0]
		return stat.IsInitDone && stat.Events == stat.EventsDone
	}, waitInterval, retryInterval)

	// check that sync was correct
	diff, err = apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Bucket:    bucket,
		From:      "main",
		To:        "f1",
		ShowMatch: true,
		User:      user,
	})
	r.NoError(err)
	r.True(diff.IsMatch)
	r.Empty(diff.Error)
	r.Len(diff.Match, 3)
	r.Empty(diff.MissFrom)
	r.Empty(diff.MissTo)
}
