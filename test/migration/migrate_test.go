package migration

import (
	"bytes"
	"io"
	"testing"
	"time"

	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/test/env"
)

func TestApi_Migrate_test(t *testing.T) {
	e := env.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	const waitInterval = 15 * time.Second
	const retryInterval = 100 * time.Millisecond

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

	diff, err := e.ApiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Bucket:    b1,
		ToBucket:  b1,
		From:      "main",
		To:        "f1",
		ShowMatch: true,
		User:      user,
	})
	r.NoError(err)
	r.True(diff.IsMatch)
	r.Empty(diff.Error)
	r.Empty(diff.Match)
	r.Empty(diff.MissFrom)
	r.Empty(diff.MissTo)

	diff, err = e.ApiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Bucket:    b2,
		ToBucket:  b2,
		From:      "main",
		To:        "f1",
		ShowMatch: true,
		User:      user,
	})
	r.NoError(err)
	r.True(diff.IsMatch)
	r.Empty(diff.Error)
	r.Empty(diff.Match)
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

	diff, err = e.ApiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Bucket:    b1,
		ToBucket:  b1,
		From:      "main",
		To:        "f1",
		ShowMatch: true,
		User:      user,
	})
	r.NoError(err)
	r.False(diff.IsMatch)
	r.Empty(diff.Error)
	r.Empty(diff.Match)
	r.Empty(diff.MissFrom)
	r.Len(diff.MissTo, 3)
	r.ElementsMatch([]string{"obj1", "photo/sept/obj2", "photo/obj3"}, diff.MissTo)

	diff, err = e.ApiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Bucket:    b2,
		ToBucket:  b2,
		From:      "main",
		To:        "f1",
		ShowMatch: true,
		User:      user,
	})
	r.NoError(err)
	r.False(diff.IsMatch)
	r.Empty(diff.Error)
	r.Empty(diff.Match)
	r.Empty(diff.MissFrom)
	r.Len(diff.MissTo, 3)
	r.ElementsMatch([]string{"obj4", "obj5", "obj6"}, diff.MissTo)

	ur, err := e.ApiClient.ListUserReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Empty(ur.Replications)

	bfr, err := e.ApiClient.ListBucketsForReplication(tstCtx, &pb.ListBucketsForReplicationRequest{
		User:           user,
		From:           "main",
		To:             "f1",
		ShowReplicated: true,
	})
	r.NoError(err)
	r.Empty(bfr.ReplicatedBuckets)
	r.Len(bfr.Buckets, 2)
	r.ElementsMatch([]string{b1, b2}, bfr.Buckets)

	repl, err := e.ApiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Empty(repl.Replications)

	_, err = e.ApiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		User:    user,
		From:    "f1",
		To:      "main",
		Buckets: []string{b1, b2},
	})
	r.Error(err)

	_, err = e.ApiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		User:    user,
		From:    "main",
		To:      "f1",
		Buckets: []string{b1, b2},
	})
	r.NoError(err)

	bfr, err = e.ApiClient.ListBucketsForReplication(tstCtx, &pb.ListBucketsForReplicationRequest{
		User:           user,
		From:           "main",
		To:             "f1",
		ShowReplicated: true,
	})
	r.NoError(err)
	r.Empty(bfr.Buckets)
	r.Len(bfr.ReplicatedBuckets, 2)
	r.ElementsMatch([]string{b1, b2}, bfr.ReplicatedBuckets)

	repl, err = e.ApiClient.ListReplications(tstCtx, &emptypb.Empty{})
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
	}, waitInterval, retryInterval)

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
	}, waitInterval, retryInterval, resLen)

	r.Eventually(func() bool {
		objects, err = listObjects(e.F1Client, b2, "")
		if err != nil {
			return false
		}
		resLen = len(objects)
		return resLen == 4
	}, waitInterval, retryInterval, resLen)

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
	}, waitInterval, retryInterval)

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
	}, waitInterval, retryInterval)

	diff, err = e.ApiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Bucket:    b1,
		ToBucket:  b1,
		From:      "main",
		To:        "f1",
		ShowMatch: true,
		User:      user,
	})
	r.NoError(err)
	r.True(diff.IsMatch)
	r.Empty(diff.Error)
	r.Len(diff.Match, 4)
	r.Empty(diff.MissFrom)
	r.Empty(diff.MissTo)
	r.ElementsMatch([]string{"obj1", "photo/sept/obj2", "photo/obj3", "photo/sept/obj7"}, diff.Match)

	diff, err = e.ApiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Bucket:    b2,
		ToBucket:  b2,
		From:      "main",
		To:        "f1",
		ShowMatch: false,
		User:      user,
	})
	r.NoError(err)
	r.True(diff.IsMatch)
	r.Empty(diff.Error)
	r.Empty(diff.Match)
	r.Empty(diff.MissFrom)
	r.Empty(diff.MissTo)

	r.Eventually(func() bool {
		repl, err = e.ApiClient.ListReplications(tstCtx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		if len(repl.Replications) != 2 {
			return false
		}
		for _, replication := range repl.Replications {
			if replication.InitObjDone != replication.InitObjListed {
				return false
			}
			if replication.Events != replication.EventsDone {
				return false
			}
		}
		return true
	}, waitInterval, retryInterval)

	repl, err = e.ApiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(repl.Replications, 2)
	for i := 0; i < len(repl.Replications); i++ {
		r.EqualValues("main", repl.Replications[i].From)
		r.EqualValues("f1", repl.Replications[i].To)
		r.EqualValues(user, repl.Replications[i].User)
		r.True(repl.Replications[i].IsInitDone)
		r.False(repl.Replications[i].IsPaused)
		r.EqualValues(repl.Replications[i].InitObjListed, repl.Replications[i].InitObjDone)
		r.EqualValues(repl.Replications[i].InitBytesListed, repl.Replications[i].InitBytesDone)
		r.EqualValues(repl.Replications[i].Events, repl.Replications[i].EventsDone)
		r.NotNil(repl.Replications[i].LastProcessedAt)
		r.NotNil(repl.Replications[i].LastEmittedAt)
		r.False(repl.Replications[i].InitDoneAt.AsTime().IsZero())
		r.True(
			repl.Replications[i].InitDoneAt.AsTime().After(repl.Replications[i].CreatedAt.AsTime()) ||
				repl.Replications[i].InitDoneAt.AsTime().Equal(repl.Replications[i].CreatedAt.AsTime()),
		)
		//		r.True(repl.Replications[i].LastProcessedAt.AsTime().Equal(repl.Replications[i].LastEmittedAt.AsTime()))
		r.False(repl.Replications[i].LastEmittedAt.AsTime().IsZero())
		r.False(repl.Replications[i].LastProcessedAt.AsTime().IsZero())
		r.False(repl.Replications[i].CreatedAt.AsTime().IsZero())

		r.EqualValues(4, repl.Replications[i].InitObjListed)
		r.EqualValues(2, repl.Replications[i].Events)
	}

	// start migration to f2
	_, err = e.ApiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		User:            user,
		From:            "f1",
		To:              "f2",
		Buckets:         []string{b1},
		IsForAllBuckets: false,
	})
	r.Error(err)
	_, err = e.ApiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		User:            user,
		From:            "main",
		To:              "f2",
		Buckets:         []string{b1},
		IsForAllBuckets: false,
	})
	r.NoError(err)

	bfr, err = e.ApiClient.ListBucketsForReplication(tstCtx, &pb.ListBucketsForReplicationRequest{
		User:           user,
		From:           "main",
		To:             "f2",
		ShowReplicated: true,
	})
	r.NoError(err)
	r.Len(bfr.Buckets, 1)
	r.EqualValues(b2, bfr.Buckets[0])
	r.Len(bfr.ReplicatedBuckets, 1)
	r.EqualValues(b1, bfr.ReplicatedBuckets[0])

	repl, err = e.ApiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(repl.Replications, 3)

	_, err = e.ApiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		User:            user,
		From:            "main",
		To:              "f2",
		Buckets:         []string{b2},
		IsForAllBuckets: false,
	})
	r.NoError(err)

	bfr, err = e.ApiClient.ListBucketsForReplication(tstCtx, &pb.ListBucketsForReplicationRequest{
		User:           user,
		From:           "main",
		To:             "f2",
		ShowReplicated: true,
	})
	r.NoError(err)
	r.Empty(bfr.Buckets)
	r.Len(bfr.ReplicatedBuckets, 2)
	r.ElementsMatch([]string{b1, b2}, bfr.ReplicatedBuckets)

	repl, err = e.ApiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(repl.Replications, 4)

	r.Eventually(func() bool {
		buckets, _ = e.F2Client.ListBuckets(tstCtx)
		return len(buckets) == 2
	}, waitInterval, retryInterval)

	r.Eventually(func() bool {
		objects, err = listObjects(e.F2Client, b1, "")
		if err != nil {
			return false
		}
		return len(objects) == 4
	}, waitInterval, retryInterval)

	r.Eventually(func() bool {
		objects, err = listObjects(e.F2Client, b2, "")
		if err != nil {
			return false
		}
		return len(objects) == 4
	}, waitInterval, retryInterval)

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
	}, waitInterval, retryInterval)

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
	}, waitInterval, retryInterval)

	repl, err = e.ApiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(repl.Replications, 4)
	cnt := 0

	for i := 0; i < len(repl.Replications); i++ {
		r.EqualValues("main", repl.Replications[i].From)
		if repl.Replications[i].To != "f2" {
			continue
		}
		cnt++
		r.EqualValues("f2", repl.Replications[i].To)
		r.EqualValues(user, repl.Replications[i].User)
		r.True(repl.Replications[i].IsInitDone)
		r.False(repl.Replications[i].IsPaused)
		r.EqualValues(repl.Replications[i].InitObjListed, repl.Replications[i].InitObjDone)
		r.EqualValues(repl.Replications[i].InitBytesListed, repl.Replications[i].InitBytesDone)
		r.EqualValues(repl.Replications[i].Events, repl.Replications[i].EventsDone)
		r.NotNil(repl.Replications[i].LastProcessedAt)
		r.NotNil(repl.Replications[i].LastEmittedAt)

		r.False(repl.Replications[i].InitDoneAt.AsTime().IsZero())
		r.True(
			repl.Replications[i].InitDoneAt.AsTime().After(repl.Replications[i].CreatedAt.AsTime()) ||
				repl.Replications[i].InitDoneAt.AsTime().Equal(repl.Replications[i].CreatedAt.AsTime()),
		)

		//		r.True(repl.Replications[i].LastProcessedAt.AsTime().Equal(repl.Replications[i].LastEmittedAt.AsTime()))
		r.False(repl.Replications[i].LastEmittedAt.AsTime().IsZero())
		r.False(repl.Replications[i].LastProcessedAt.AsTime().IsZero())
		r.False(repl.Replications[i].CreatedAt.AsTime().IsZero())

		r.EqualValues(4, repl.Replications[i].InitObjListed)
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
	}, waitInterval, retryInterval)

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
	}, waitInterval, retryInterval)

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
	}, waitInterval, retryInterval)

	repl, err = e.ApiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(repl.Replications, 4)
	for i := 0; i < len(repl.Replications); i++ {
		r.EqualValues("main", repl.Replications[i].From)

		r.EqualValues(user, repl.Replications[i].User)
		r.True(repl.Replications[i].IsInitDone)
		r.False(repl.Replications[i].IsPaused)
		r.EqualValues(repl.Replications[i].InitObjListed, repl.Replications[i].InitObjDone)
		r.EqualValues(repl.Replications[i].InitBytesListed, repl.Replications[i].InitBytesDone)
		r.EqualValues(repl.Replications[i].Events, repl.Replications[i].EventsDone)
		r.NotNil(repl.Replications[i].LastProcessedAt)
		r.NotNil(repl.Replications[i].LastEmittedAt)

		r.False(repl.Replications[i].InitDoneAt.AsTime().IsZero())
		r.True(repl.Replications[i].InitDoneAt.AsTime().After(repl.Replications[i].CreatedAt.AsTime()) || repl.Replications[i].InitDoneAt.AsTime().Equal(repl.Replications[i].CreatedAt.AsTime()))

		//		r.True(repl.Replications[i].LastProcessedAt.AsTime().Equal(repl.Replications[i].LastEmittedAt.AsTime()))
		r.False(repl.Replications[i].LastEmittedAt.AsTime().IsZero())
		r.False(repl.Replications[i].LastProcessedAt.AsTime().IsZero())
		r.False(repl.Replications[i].CreatedAt.AsTime().IsZero())

		r.EqualValues(4, repl.Replications[i].InitObjListed)
		if repl.Replications[i].To == "f2" {
			if repl.Replications[i].Bucket == b1 {
				r.EqualValues(1, repl.Replications[i].Events)
			} else {
				r.EqualValues(0, repl.Replications[i].Events)
			}
		}
	}
}
