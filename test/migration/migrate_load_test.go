package migration

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	mclient "github.com/minio/minio-go/v7"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/store"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/test/app"
)

func TestApi_Migrate_Load_test(t *testing.T) {
	t.Skip()
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()

	const objPerBucket = 150
	const bucketsNum = 10
	r := require.New(t)

	objData := bytes.Repeat([]byte("A"), rand.Intn(1<<20)+32*1024)
	for b := 0; b < bucketsNum; b++ {
		bucketName := fmt.Sprintf("bucket-%d", b)
		err := e.MainClient.MakeBucket(tstCtx, bucketName, mclient.MakeBucketOptions{})
		r.NoError(err)
		for i := 0; i < objPerBucket; i++ {
			objName := fmt.Sprintf("obj-%d", i)
			_, err = e.MainClient.PutObject(tstCtx, bucketName, objName, bytes.NewReader(objData), int64(len(objData)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
			r.NoError(err)
		}
		t.Log(bucketName, "created")
	}

	_, err := e.ApiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
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
		buckets, _ := e.F1Client.ListBuckets(tstCtx)
		t.Log("f1 buckets", len(buckets))
		return len(buckets) == bucketsNum
	}, e.WaitLong, e.RetryLong)
	t.Log("f1 buckets created")

	for b := 0; b < bucketsNum; b++ {
		bucketName := fmt.Sprintf("bucket-%d", b)
		r.Eventually(func() bool {
			objects, err := listObjects(e.F1Client, bucketName, "")
			if err != nil {
				return false
			}
			t.Log(bucketName, len(objects))
			return len(objects) == objPerBucket
		}, e.WaitLong, e.RetryLong)
	}

	m, err := e.ApiClient.ListReplications(tstCtx, &emptypb.Empty{})
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
	objectLocker := store.NewObjectLocker(client, 0)
	logger := log.GetLogger(&log.Config{Level: "info"}, "lock", "")
	ctx := logger.WithContext(context.TODO())

	objectLockID := entity.NewVersionedObjectLockID("stor", "test", "obj", "")
	lock, err := objectLocker.Lock(ctx, objectLockID, store.WithDuration(time.Millisecond*500))
	r.NoError(err)
	time.Sleep(time.Millisecond * 400)
	err = lock.Do(ctx, time.Second, func() error {
		time.Sleep(time.Second * 30)
		return nil
	})
	r.NoError(err)
	lock.Release(ctx)
}
