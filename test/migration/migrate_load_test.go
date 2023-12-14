package migration

import (
	"bytes"
	"context"
	"fmt"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/lock"
	"github.com/clyso/chorus/pkg/log"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	mclient "github.com/minio/minio-go/v7"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
	"math/rand"
	"testing"
	"time"
)

func TestApi_Migrate_Load_test(t *testing.T) {
	t.Skip()

	const waitInterval = 180 * time.Second
	const retryInterval = 3 * time.Second

	const objPerBucket = 150
	const bucketsNum = 10
	r := require.New(t)

	objData := bytes.Repeat([]byte("A"), rand.Intn(1<<20)+32*1024)
	for b := 0; b < bucketsNum; b++ {
		bucketName := fmt.Sprintf("bucket-%d", b)
		err := mainClient.MakeBucket(tstCtx, bucketName, mclient.MakeBucketOptions{})
		r.NoError(err)
		for i := 0; i < objPerBucket; i++ {
			objName := fmt.Sprintf("obj-%d", i)
			_, err = mainClient.PutObject(tstCtx, bucketName, objName, bytes.NewReader(objData), int64(len(objData)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
			r.NoError(err)
		}
		t.Log(bucketName, "created")
	}

	_, err := apiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		User:            user,
		From:            "main",
		To:              "f1",
		Buckets:         nil,
		IsForAllBuckets: true,
	})
	r.NoError(err)
	time.Sleep(time.Millisecond * 50)

	// check that storages are in sync
	r.Eventually(func() bool {
		buckets, _ := f1Client.ListBuckets(tstCtx)
		t.Log("f1 buckets", len(buckets))
		return len(buckets) == bucketsNum
	}, waitInterval, retryInterval)
	t.Log("f1 buckets created")

	for b := 0; b < bucketsNum; b++ {
		bucketName := fmt.Sprintf("bucket-%d", b)
		r.Eventually(func() bool {
			objects, err := listObjects(f1Client, bucketName, "")
			if err != nil {
				return false
			}
			t.Log(bucketName, len(objects))
			return len(objects) == objPerBucket
		}, waitInterval, retryInterval)
	}

	m, err := apiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)

	r.Len(m.Replications, bucketsNum)
	for _, buck := range m.Replications {
		r.True(buck.IsInitDone)
		r.EqualValues(buck.InitObjListed, buck.InitObjDone)
		r.EqualValues(buck.InitBytesListed, buck.InitBytesDone)
		r.EqualValues(buck.InitBytesListed, buck.InitBytesDone)
		r.EqualValues(objPerBucket, buck.InitObjDone)
	}
}

func TestApi_Migrate_Lock_test(t *testing.T) {
	t.Skip()
	r := require.New(t)
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	obj := dom.Object{
		Bucket:  "test",
		Name:    "obj",
		Version: "",
	}
	locker := lock.New(client)
	logger := log.GetLogger(&log.Config{Level: "info"}, "lock", "")
	ctx := logger.WithContext(context.TODO())

	release, refresh, err := locker.Lock(ctx, lock.ObjKey("stor", obj), lock.WithDuration(time.Millisecond*500))
	r.NoError(err)
	time.Sleep(time.Millisecond * 400)
	err = lock.WithRefresh(ctx, func() error {
		time.Sleep(time.Second * 30)
		return nil
	}, refresh, time.Second)
	r.NoError(err)
	release()
}
