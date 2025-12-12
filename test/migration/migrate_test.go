package migration

import (
	"bytes"
	"io"
	"testing"
	"time"

	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/test/app"
)

func TestApi_Migrate_test(t *testing.T) {
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()

	r := require.New(t)
	buckets, err := e.MainClient.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)

	buckets, err = e.F1Client.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)

	// create 2 buckets with files in main storage
	b1, b2 := "migrate-buck1", "migrate-buck2"
	err = e.MainClient.MakeBucket(tstCtx, b1, mclient.MakeBucketOptions{})
	r.NoError(err)
	err = e.MainClient.MakeBucket(tstCtx, b2, mclient.MakeBucketOptions{})
	r.NoError(err)

	id1 := &pb.ReplicationID{
		FromBucket:  &b1,
		ToBucket:    &b1,
		FromStorage: "main",
		ToStorage:   "f1",
		User:        user,
	}
	diff := replicationDiff(t, e, id1)
	r.True(diff.IsMatch)
	r.Empty(diff.Differ)
	r.Empty(diff.MissFrom)
	r.Empty(diff.MissTo)

	id2 := &pb.ReplicationID{
		FromBucket:  &b2,
		ToBucket:    &b2,
		FromStorage: "main",
		ToStorage:   "f1",
		User:        user,
	}
	diff = replicationDiff(t, e, id2)
	r.True(diff.IsMatch)
	r.Empty(diff.Differ)
	r.Empty(diff.MissFrom)
	r.Empty(diff.MissTo)

	obj1 := getTestObj("obj1", b1)
	_, err = e.MainClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj2 := getTestObj("photo/sept/obj2", b1)
	_, err = e.MainClient.PutObject(tstCtx, obj2.bucket, obj2.name, bytes.NewReader(obj2.data), int64(len(obj2.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj3 := getTestObj("photo/obj3", b1)
	_, err = e.MainClient.PutObject(tstCtx, obj3.bucket, obj3.name, bytes.NewReader(obj3.data), int64(len(obj3.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj4 := getTestObj("obj4", b2)
	_, err = e.MainClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj5 := getTestObj("obj5", b2)
	_, err = e.MainClient.PutObject(tstCtx, obj5.bucket, obj5.name, bytes.NewReader(obj5.data), int64(len(obj5.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj6 := getTestObj("obj6", b2)
	_, err = e.MainClient.PutObject(tstCtx, obj6.bucket, obj6.name, bytes.NewReader(obj6.data), int64(len(obj6.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)

	buckets, err = e.MainClient.ListBuckets(tstCtx)
	r.NoError(err)
	r.Len(buckets, 2)

	buckets, err = e.F1Client.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)

	diff = replicationDiff(t, e, id1)
	r.False(diff.IsMatch)
	r.Empty(diff.Differ)
	r.Empty(diff.MissFrom)
	r.Len(diff.MissTo, 3)
	r.ElementsMatch([]string{"obj1", "photo/sept/obj2", "photo/obj3"}, diff.MissTo)

	diff = replicationDiff(t, e, id2)
	r.False(diff.IsMatch)
	r.Empty(diff.Differ)
	r.Empty(diff.MissFrom)
	r.Len(diff.MissTo, 3)
	r.ElementsMatch([]string{"obj4", "obj5", "obj6"}, diff.MissTo)

	ur, err := e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	r.Empty(ur.Replications)

	bfr, err := e.PolicyClient.AvailableBuckets(tstCtx, &pb.AvailableBucketsRequest{
		User:           user,
		FromStorage:    "main",
		ToStorage:      "f1",
		ShowReplicated: true,
	})
	r.NoError(err)
	r.Empty(bfr.ReplicatedBuckets)
	r.Len(bfr.Buckets, 2)
	r.ElementsMatch([]string{b1, b2}, bfr.Buckets)

	repl, err := e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	r.Empty(repl.Replications)

	wrongID := proto.Clone(id1).(*pb.ReplicationID)
	wrongID.FromStorage = "f1"
	wrongID.ToStorage = "main"
	_, err = e.PolicyClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		Id: wrongID,
	})
	r.Error(err)

	_, err = e.PolicyClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		Id: id1,
	})
	r.NoError(err)
	_, err = e.PolicyClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		Id: id2,
	})
	r.NoError(err)

	bfr, err = e.PolicyClient.AvailableBuckets(tstCtx, &pb.AvailableBucketsRequest{
		User:           user,
		FromStorage:    "main",
		ToStorage:      "f1",
		ShowReplicated: true,
	})
	r.NoError(err)
	r.Empty(bfr.Buckets)
	r.Len(bfr.ReplicatedBuckets, 2)
	r.ElementsMatch([]string{b1, b2}, bfr.ReplicatedBuckets)

	repl, err = e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	r.Len(repl.Replications, 2)

	time.Sleep(time.Millisecond * 50)

	// perform updates after migration started

	obj7 := getTestObj("photo/sept/obj7", b1)
	_, err = e.ProxyClient.PutObject(tstCtx, obj7.bucket, obj7.name, bytes.NewReader(obj7.data), int64(len(obj7.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	obj8 := getTestObj("obj8", b2)
	_, err = e.ProxyClient.PutObject(tstCtx, obj8.bucket, obj8.name, bytes.NewReader(obj8.data), int64(len(obj8.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	obj1upd := getTestObj(obj1.name, obj1.bucket)
	r.False(bytes.Equal(obj1.data, obj1upd.data))
	_, err = e.ProxyClient.PutObject(tstCtx, obj1upd.bucket, obj1upd.name, bytes.NewReader(obj1upd.data), int64(len(obj1upd.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	obj4upd := getTestObj(obj4.name, obj4.bucket)
	r.False(bytes.Equal(obj4.data, obj4upd.data))
	_, err = e.ProxyClient.PutObject(tstCtx, obj4upd.bucket, obj4upd.name, bytes.NewReader(obj4upd.data), int64(len(obj4upd.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	// check that storages are in sync

	r.Eventually(func() bool {
		buckets, _ = e.F1Client.ListBuckets(tstCtx)
		return len(buckets) == 2
	}, e.WaitLong, e.RetryLong)

	objects, err := listObjects(e.MainClient, b1, "")
	r.NoError(err)
	r.Len(objects, 4)
	r.Contains(objects, obj1.name)
	r.Contains(objects, obj2.name)
	r.Contains(objects, obj3.name)
	r.Contains(objects, obj7.name)

	objects, err = listObjects(e.MainClient, b2, "")
	r.NoError(err)
	r.Len(objects, 4)
	r.Contains(objects, obj4.name)
	r.Contains(objects, obj5.name)
	r.Contains(objects, obj6.name)
	r.Contains(objects, obj8.name)

	resLen := 0
	r.Eventually(func() bool {
		objects, err = listObjects(e.F1Client, b1, "")
		if err != nil {
			return false
		}
		resLen = len(objects)
		return resLen == 4
	}, e.WaitLong, e.RetryLong, resLen)

	r.Eventually(func() bool {
		objects, err = listObjects(e.F1Client, b2, "")
		if err != nil {
			return false
		}
		resLen = len(objects)
		return resLen == 4
	}, e.WaitLong, e.RetryLong, resLen)

	proxyObj2, err := e.ProxyClient.GetObject(tstCtx, obj2.bucket, obj2.name, mclient.GetObjectOptions{})
	r.NoError(err)
	proxyObj2Bytes, err := io.ReadAll(proxyObj2)
	r.NoError(err)

	mainObj2, err := e.MainClient.GetObject(tstCtx, obj2.bucket, obj2.name, mclient.GetObjectOptions{})
	r.NoError(err)
	mainObj2Bytes, err := io.ReadAll(mainObj2)
	r.NoError(err)

	f1Obj2, err := e.F1Client.GetObject(tstCtx, obj2.bucket, obj2.name, mclient.GetObjectOptions{})
	r.NoError(err)
	f1Obj2Bytes, err := io.ReadAll(f1Obj2)
	r.NoError(err)

	r.True(bytes.Equal(obj2.data, proxyObj2Bytes))
	r.True(bytes.Equal(mainObj2Bytes, proxyObj2Bytes))
	r.True(bytes.Equal(mainObj2Bytes, f1Obj2Bytes))

	r.Eventually(func() bool {
		mainObj1Upd, err := e.MainClient.GetObject(tstCtx, obj1upd.bucket, obj1upd.name, mclient.GetObjectOptions{})
		if err != nil {
			return false
		}
		mainObj1UpdBytes, err := io.ReadAll(mainObj1Upd)
		if err != nil {
			return false
		}
		return bytes.Equal(obj1upd.data, mainObj1UpdBytes)
	}, e.WaitLong, e.RetryLong)

	r.Eventually(func() bool {
		f1Obj1Upd, err := e.F1Client.GetObject(tstCtx, obj1upd.bucket, obj1upd.name, mclient.GetObjectOptions{})
		if err != nil {
			return false
		}
		f1Obj1UpdBytes, err := io.ReadAll(f1Obj1Upd)
		if err != nil {
			return false
		}
		return bytes.Equal(obj1upd.data, f1Obj1UpdBytes)
	}, e.WaitLong, e.RetryLong)

	diff = replicationDiff(t, e, id1)
	r.True(diff.IsMatch)
	r.Empty(diff.Differ)
	r.Empty(diff.MissFrom)
	r.Empty(diff.MissTo)

	diff = replicationDiff(t, e, id2)
	r.True(diff.IsMatch)
	r.Empty(diff.Differ)
	r.Empty(diff.MissFrom)
	r.Empty(diff.MissTo)

	r.Eventually(func() bool {
		repl, err = e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
		if err != nil {
			return false
		}
		if len(repl.Replications) != 2 {
			return false
		}
		for _, replication := range repl.Replications {
			if !replication.IsInitDone {
				return false
			}
			if replication.Events > replication.EventsDone {
				return false
			}
		}
		return true
	}, e.WaitLong, e.RetryLong)

	repl, err = e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	r.Len(repl.Replications, 2)
	for i := 0; i < len(repl.Replications); i++ {
		r.EqualValues("main", repl.Replications[i].Id.FromStorage)
		r.EqualValues("f1", repl.Replications[i].Id.ToStorage)
		r.EqualValues(user, repl.Replications[i].Id.User)
		r.True(repl.Replications[i].IsInitDone)
		r.False(repl.Replications[i].IsPaused)
		r.EqualValues(repl.Replications[i].InitObjListed, repl.Replications[i].InitObjDone)
		r.EqualValues(repl.Replications[i].Events, repl.Replications[i].EventsDone)
		r.False(repl.Replications[i].CreatedAt.AsTime().IsZero())

		r.Equal(repl.Replications[i].InitObjListed, repl.Replications[i].InitObjDone)
		r.Equal(repl.Replications[i].Events, repl.Replications[i].EventsDone)
		r.EqualValues(2, repl.Replications[i].Events)
	}

	// start migration to f2
	wrongID = &pb.ReplicationID{
		FromStorage: "f1",
		ToStorage:   "f2",
		User:        user,
		FromBucket:  &b1,
		ToBucket:    &b1,
	}
	_, err = e.PolicyClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		Id: wrongID,
	})
	r.Error(err)
	id3 := &pb.ReplicationID{
		FromStorage: "main",
		ToStorage:   "f2",
		User:        user,
		FromBucket:  &b1,
		ToBucket:    &b1,
	}
	_, err = e.PolicyClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		Id: id3,
	})
	r.NoError(err)

	bfr, err = e.PolicyClient.AvailableBuckets(tstCtx, &pb.AvailableBucketsRequest{
		User:           user,
		FromStorage:    "main",
		ToStorage:      "f2",
		ShowReplicated: true,
	})
	r.NoError(err)
	r.Len(bfr.Buckets, 1)
	r.EqualValues(b2, bfr.Buckets[0])
	r.Len(bfr.ReplicatedBuckets, 1)
	r.EqualValues(b1, bfr.ReplicatedBuckets[0])

	repl, err = e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	r.Len(repl.Replications, 3)

	id4 := &pb.ReplicationID{
		FromStorage: "main",
		ToStorage:   "f2",
		User:        user,
		FromBucket:  &b2,
		ToBucket:    &b2,
	}
	_, err = e.PolicyClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		Id: id4,
	})
	r.NoError(err)

	bfr, err = e.PolicyClient.AvailableBuckets(tstCtx, &pb.AvailableBucketsRequest{
		User:           user,
		FromStorage:    "main",
		ToStorage:      "f2",
		ShowReplicated: true,
	})
	r.NoError(err)
	r.Empty(bfr.Buckets)
	r.Len(bfr.ReplicatedBuckets, 2)
	r.ElementsMatch([]string{b1, b2}, bfr.ReplicatedBuckets)

	repl, err = e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	r.Len(repl.Replications, 4)

	r.Eventually(func() bool {
		buckets, _ = e.F2Client.ListBuckets(tstCtx)
		return len(buckets) == 2
	}, e.WaitLong, e.RetryLong)

	r.Eventually(func() bool {
		objects, err = listObjects(e.F2Client, b1, "")
		if err != nil {
			return false
		}
		return len(objects) == 4
	}, e.WaitLong, e.RetryLong)

	r.Eventually(func() bool {
		objects, err = listObjects(e.F2Client, b2, "")
		if err != nil {
			return false
		}
		return len(objects) == 4
	}, e.WaitLong, e.RetryLong)

	r.Eventually(func() bool {
		f2Obj1Upd, err := e.F2Client.GetObject(tstCtx, obj1upd.bucket, obj1upd.name, mclient.GetObjectOptions{})
		if err != nil {
			return false
		}
		f2Obj1UpdBytes, err := io.ReadAll(f2Obj1Upd)
		if err != nil {
			return false
		}
		return bytes.Equal(obj1upd.data, f2Obj1UpdBytes)
	}, e.WaitLong, e.RetryLong)

	r.Eventually(func() bool {
		f2Obj4Upd, err := e.F2Client.GetObject(tstCtx, obj4upd.bucket, obj4upd.name, mclient.GetObjectOptions{})
		if err != nil {
			return false
		}
		f2Obj4UpdBytes, err := io.ReadAll(f2Obj4Upd)
		if err != nil {
			return false
		}
		return bytes.Equal(obj4upd.data, f2Obj4UpdBytes)
	}, e.WaitLong, e.RetryLong)

	repl, err = e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	r.Len(repl.Replications, 4)
	cnt := 0

	for i := 0; i < len(repl.Replications); i++ {
		r.EqualValues("main", repl.Replications[i].Id.FromStorage)
		if repl.Replications[i].Id.ToStorage != "f2" {
			continue
		}
		cnt++
		r.EqualValues("f2", repl.Replications[i].Id.ToStorage)
		r.EqualValues(user, repl.Replications[i].Id.User)
		r.True(repl.Replications[i].IsInitDone)
		r.False(repl.Replications[i].IsPaused)
		r.EqualValues(repl.Replications[i].InitObjListed, repl.Replications[i].InitObjDone)
		r.EqualValues(repl.Replications[i].Events, repl.Replications[i].EventsDone)
		r.False(repl.Replications[i].CreatedAt.AsTime().IsZero())

		r.Equal(repl.Replications[i].InitObjListed, repl.Replications[i].InitObjDone)
		r.EqualValues(0, repl.Replications[i].Events)
	}
	r.EqualValues(2, cnt)

	obj9 := getTestObj("obj9", b1)
	_, err = e.ProxyClient.PutObject(tstCtx, obj9.bucket, obj9.name, bytes.NewReader(obj9.data), int64(len(obj9.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	obj, err := e.ProxyClient.GetObject(tstCtx, obj9.bucket, obj9.name, mclient.GetObjectOptions{})
	r.NoError(err)
	objBytes, err := io.ReadAll(obj)
	r.NoError(err)
	r.True(bytes.Equal(obj9.data, objBytes))

	r.Eventually(func() bool {
		obj, err = e.MainClient.GetObject(tstCtx, obj9.bucket, obj9.name, mclient.GetObjectOptions{})
		if err != nil {
			return false
		}
		objBytes, err = io.ReadAll(obj)
		if err != nil {
			return false
		}
		return bytes.Equal(obj9.data, objBytes)
	}, e.WaitLong, e.RetryLong)

	r.Eventually(func() bool {
		obj, err = e.F1Client.GetObject(tstCtx, obj9.bucket, obj9.name, mclient.GetObjectOptions{})
		if err != nil {
			return false
		}
		objBytes, err = io.ReadAll(obj)
		if err != nil {
			return false
		}
		return bytes.Equal(obj9.data, objBytes)
	}, e.WaitLong, e.RetryLong)

	r.Eventually(func() bool {
		obj, err = e.F2Client.GetObject(tstCtx, obj9.bucket, obj9.name, mclient.GetObjectOptions{})
		if err != nil {
			return false
		}
		objBytes, err = io.ReadAll(obj)
		if err != nil {
			return false
		}
		return bytes.Equal(obj9.data, objBytes)
	}, e.WaitLong, e.RetryLong)

	repl, err = e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	r.Len(repl.Replications, 4)
	for i := 0; i < len(repl.Replications); i++ {
		r.EqualValues("main", repl.Replications[i].Id.FromStorage)

		r.EqualValues(user, repl.Replications[i].Id.User)
		r.True(repl.Replications[i].IsInitDone)
		r.False(repl.Replications[i].IsPaused)
		r.EqualValues(repl.Replications[i].InitObjListed, repl.Replications[i].InitObjDone)
		r.EqualValues(repl.Replications[i].Events, repl.Replications[i].EventsDone)
		r.False(repl.Replications[i].CreatedAt.AsTime().IsZero())

		r.EqualValues(repl.Replications[i].InitObjListed, repl.Replications[i].InitObjDone)
		if repl.Replications[i].Id.ToStorage == "f2" {
			if *repl.Replications[i].Id.FromBucket == b1 {
				r.EqualValues(1, repl.Replications[i].Events)
			} else {
				r.EqualValues(0, repl.Replications[i].Events)
			}
		}
	}
}

func Test_User_migration(t *testing.T) {
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	var (
		bucket1 = "user-migration-1"
		bucket2 = "user-migration-2"
		bucket3 = "user-migration-3"
	)

	r := require.New(t)
	exists, err := e.MainClient.BucketExists(tstCtx, bucket2)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F1Client.BucketExists(tstCtx, bucket1)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F2Client.BucketExists(tstCtx, bucket1)
	r.NoError(err)
	r.False(exists)
	exists, err = e.MainClient.BucketExists(tstCtx, bucket2)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F1Client.BucketExists(tstCtx, bucket2)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F2Client.BucketExists(tstCtx, bucket2)
	r.NoError(err)
	r.False(exists)

	id := &pb.ReplicationID{
		User:        user,
		FromStorage: "main",
		ToStorage:   "f1",
	}
	_, err = e.PolicyClient.GetReplication(tstCtx, id)
	r.Error(err)

	// fill main buckets with init data
	err = e.MainClient.MakeBucket(tstCtx, bucket1, mclient.MakeBucketOptions{})
	r.NoError(err)
	// bucket1: obj1, obj2, obj3
	obj1 := getTestObj("obj1", bucket1)
	_, err = e.MainClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj2 := getTestObj("photo/sept/obj2", bucket1)
	_, err = e.MainClient.PutObject(tstCtx, obj2.bucket, obj2.name, bytes.NewReader(obj2.data), int64(len(obj2.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj3 := getTestObj("photo/obj3", bucket1)
	_, err = e.MainClient.PutObject(tstCtx, obj3.bucket, obj3.name, bytes.NewReader(obj3.data), int64(len(obj3.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)

	err = e.MainClient.MakeBucket(tstCtx, bucket2, mclient.MakeBucketOptions{})
	r.NoError(err)
	// bucket2: obj1 (same as in bucket1),  obj4
	_, err = e.MainClient.PutObject(tstCtx, bucket2, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj4 := getTestObj("photo/obj3", bucket2)
	_, err = e.MainClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)

	// check buckets exists only in main
	exists, err = e.MainClient.BucketExists(tstCtx, bucket1)
	r.NoError(err)
	r.True(exists)
	exists, err = e.F1Client.BucketExists(tstCtx, bucket1)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F2Client.BucketExists(tstCtx, bucket1)
	r.NoError(err)
	r.False(exists)
	exists, err = e.MainClient.BucketExists(tstCtx, bucket2)
	r.NoError(err)
	r.True(exists)
	exists, err = e.F1Client.BucketExists(tstCtx, bucket2)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F2Client.BucketExists(tstCtx, bucket2)
	r.NoError(err)
	r.False(exists)

	// create replication for user
	_, err = e.PolicyClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		Id: id,
	})
	r.NoError(err)
	// get replication from list
	repls, err := e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	r.Len(repls.Replications, 1)
	userRepl := repls.Replications[0]
	r.False(userRepl.HasSwitch)
	r.False(userRepl.IsArchived)

	// get replication by id
	repl, err := e.PolicyClient.GetReplication(tstCtx, id)
	r.NoError(err)
	r.True(proto.Equal(repl.Id, id))
	r.False(repl.HasSwitch)
	r.False(repl.IsArchived)
	r.NotNil(repl.CreatedAt)

	// wait until repl started
	r.Eventually(func() bool {
		repl, err = e.PolicyClient.GetReplication(tstCtx, id)
		if err != nil {
			return false
		}
		return repl.InitObjListed > 0
	}, e.WaitLong, e.RetryLong)

	// perform updates after migration started

	// bucket1: obj1, obj2, obj3  + obj5 (new) + obj3 (updated) - obj2 (deleted)
	obj5 := getTestObj("photo/sept/obj5", bucket1)
	_, err = e.ProxyClient.PutObject(tstCtx, obj5.bucket, obj5.name, bytes.NewReader(obj5.data), int64(len(obj5.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)
	obj3 = getTestObj("photo/obj3", bucket1)
	_, err = e.ProxyClient.PutObject(tstCtx, obj3.bucket, obj3.name, bytes.NewReader(obj3.data), int64(len(obj3.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	err = e.ProxyClient.RemoveObject(tstCtx, bucket1, obj2.name, mclient.RemoveObjectOptions{})
	r.NoError(err)

	// bucket2: obj1 (same as in bucket1),  obj4  + obj6 (new)
	obj6 := getTestObj("photo/sept/obj6", bucket2)
	_, err = e.ProxyClient.PutObject(tstCtx, obj6.bucket, obj6.name, bytes.NewReader(obj6.data), int64(len(obj6.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)
	r.NoError(err)

	// wait init done
	r.Eventually(func() bool {
		repl, err = e.PolicyClient.GetReplication(tstCtx, id)
		if err != nil {
			return false
		}
		return repl.IsInitDone
	}, e.WaitLong*2, e.RetryLong)

	// create a new bucket after migration started
	err = e.ProxyClient.MakeBucket(tstCtx, bucket3, mclient.MakeBucketOptions{})
	r.NoError(err)
	// put an object to the new bucket
	objNew := getTestObj("new-obj", bucket3)
	_, err = e.ProxyClient.PutObject(tstCtx, objNew.bucket, objNew.name, bytes.NewReader(objNew.data), int64(len(objNew.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	// wait events done
	r.Eventually(func() bool {
		repl, err = e.PolicyClient.GetReplication(tstCtx, id)
		if err != nil {
			return false
		}
		return repl.IsInitDone && repl.Events == repl.EventsDone && repl.Events > 0
	}, e.WaitLong*2, e.RetryLong)

	// check that data is in sync
	diff := replicationDiff(t, e, &pb.ReplicationID{
		User:        user,
		FromStorage: "main",
		ToStorage:   "f1",
		FromBucket:  &bucket1,
		ToBucket:    &bucket1,
	})
	r.True(diff.IsMatch)
	diff = replicationDiff(t, e, &pb.ReplicationID{
		User:        user,
		FromStorage: "main",
		ToStorage:   "f1",
		FromBucket:  &bucket2,
		ToBucket:    &bucket2,
	})
	r.True(diff.IsMatch)
	diff = replicationDiff(t, e, &pb.ReplicationID{
		User:        user,
		FromStorage: "main",
		ToStorage:   "f1",
		FromBucket:  &bucket3,
		ToBucket:    &bucket3,
	})
	r.True(diff.IsMatch)

	// bucket1: obj1,  obj3  obj5
	var objects = []testObj{obj1, obj3, obj5}
	for _, object := range objects {
		// f1 equal to actual data
		objData, err := e.ProxyClient.GetObject(tstCtx, bucket1, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err := io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = e.F1Client.GetObject(tstCtx, bucket1, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = e.MainClient.GetObject(tstCtx, bucket1, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
	}
	b1ObjF1, err := listObjects(e.F1Client, bucket1, "")
	r.NoError(err)
	r.Len(b1ObjF1, 3)
	b1ObjM, err := listObjects(e.MainClient, bucket1, "")
	r.NoError(err)
	r.Len(b1ObjM, 3)
	b1ObjP, err := listObjects(e.ProxyClient, bucket1, "")
	r.NoError(err)
	r.Len(b1ObjP, 3)
	r.ElementsMatch(b1ObjF1, b1ObjM)

	// bucket2: obj1 (same as in bucket1),  obj4  + obj6 (new)
	objects = []testObj{obj1, obj4, obj6}
	for _, object := range objects {
		// f1 equal to actual data
		objData, err := e.ProxyClient.GetObject(tstCtx, bucket2, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err := io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = e.F1Client.GetObject(tstCtx, bucket2, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = e.MainClient.GetObject(tstCtx, bucket2, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
	}
	b2ObjF1, err := listObjects(e.F1Client, bucket2, "")
	r.NoError(err)
	r.Len(b2ObjF1, 3)
	b2ObjM, err := listObjects(e.MainClient, bucket2, "")
	r.NoError(err)
	r.Len(b2ObjM, 3)
	b2ObjP, err := listObjects(e.ProxyClient, bucket2, "")
	r.NoError(err)
	r.Len(b2ObjP, 3)
	r.ElementsMatch(b2ObjF1, b2ObjM)

	// bucket3: objNew
	objData, err := e.ProxyClient.GetObject(tstCtx, bucket3, objNew.name, mclient.GetObjectOptions{})
	r.NoError(err, objNew.name)
	objBytes, err := io.ReadAll(objData)
	r.NoError(err)
	r.True(bytes.Equal(objNew.data, objBytes), objNew.name)
	objData, err = e.F1Client.GetObject(tstCtx, bucket3, objNew.name, mclient.GetObjectOptions{})
	r.NoError(err, objNew.name)
	objBytes, err = io.ReadAll(objData)
	r.NoError(err)
	r.True(bytes.Equal(objNew.data, objBytes), objNew.name)
	objData, err = e.MainClient.GetObject(tstCtx, bucket3, objNew.name, mclient.GetObjectOptions{})
	r.NoError(err, objNew.name)
	objBytes, err = io.ReadAll(objData)
	r.NoError(err)
	r.True(bytes.Equal(objNew.data, objBytes), objNew.name)

	b3ObjF1, err := listObjects(e.F1Client, bucket3, "")
	r.NoError(err)
	r.Len(b3ObjF1, 1)
	b3ObjM, err := listObjects(e.MainClient, bucket3, "")
	r.NoError(err)
	r.Len(b3ObjM, 1)
	b3ObjP, err := listObjects(e.ProxyClient, bucket3, "")
	r.NoError(err)
	r.Len(b3ObjP, 1)
	r.ElementsMatch(b3ObjF1, b3ObjM)

}
