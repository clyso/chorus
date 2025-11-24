package migration

import (
	"bytes"
	"testing"

	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/test/app"
)

func Test_Restart_Replication(t *testing.T) {
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()

	bucket := "restart"
	r := require.New(t)
	err := e.MainClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)
	ok, err := e.MainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(ok)
	ok, err = e.F1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)
	ok, err = e.F2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)

	// add initial objects
	obj1 := getTestObj("obj1", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj2 := getTestObj("obj2", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj2.bucket, obj2.name, bytes.NewReader(obj2.data), int64(len(obj2.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)

	// start replication
	id := &pb.ReplicationID{
		User:        user,
		FromStorage: "main",
		ToStorage:   "f1",
		FromBucket:  &bucket,
		ToBucket:    &bucket,
	}
	_, err = e.PolicyClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		Id: id,
	})
	r.NoError(err)

	reps, err := e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	r.Len(reps.Replications, 1)
	r.True(proto.Equal(id, reps.Replications[0].Id))

	r.Eventually(func() bool {
		reps, err = e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
		if err != nil {
			return false
		}
		if len(reps.Replications) != 1 {
			return false
		}
		return reps.Replications[0].InitObjDone > 0
	}, e.WaitShort, e.RetryShort)

	// add live event
	obj3 := getTestObj("obj3", bucket)
	_, err = e.ProxyClient.PutObject(tstCtx, obj3.bucket, obj3.name, bytes.NewReader(obj3.data), int64(len(obj3.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)

	// wait replication to be done
	r.Eventually(func() bool {
		reps, err = e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
		if err != nil {
			return false
		}
		if len(reps.Replications) != 1 {
			return false
		}
		return reps.Replications[0].IsInitDone && reps.Replications[0].Events > 0 && reps.Replications[0].Events == reps.Replications[0].EventsDone
	}, e.WaitShort, e.RetryShort)

	diff, err := e.PolicyClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Target:    id,
		ShowMatch: true,
	})
	r.NoError(err)
	r.True(diff.IsMatch)
	r.Empty(diff.Error)
	r.Len(diff.Match, 3)
	r.Empty(diff.MissFrom)
	r.Empty(diff.MissTo)

	// delete replication
	_, err = e.PolicyClient.DeleteReplication(tstCtx, id)
	r.NoError(err)

	// delete dest bucket
	r.NoError(rmBucket(e.F1Client, bucket))

	diff, err = e.PolicyClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Target:    id,
		ShowMatch: true,
	})
	r.NoError(err)
	r.False(diff.IsMatch)

	reps, err = e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	r.Empty(reps.Replications)

	// restart replication
	_, err = e.PolicyClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		Id: id,
	})
	r.NoError(err)

	reps, err = e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	r.Len(reps.Replications, 1)

	// wait replication to be done
	r.Eventually(func() bool {
		reps, err = e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
		if err != nil {
			return false
		}
		if len(reps.Replications) != 1 {
			return false
		}
		stat := reps.Replications[0]
		return stat.IsInitDone && stat.Events == stat.EventsDone
	}, e.WaitShort, e.RetryShort)

	// check that sync was correct
	diff, err = e.PolicyClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Target:    id,
		ShowMatch: true,
	})
	r.NoError(err)
	r.True(diff.IsMatch)
	r.Empty(diff.Error)
	r.Len(diff.Match, 3)
	r.Empty(diff.MissFrom)
	r.Empty(diff.MissTo)
}
