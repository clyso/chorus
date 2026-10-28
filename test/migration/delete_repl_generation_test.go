package migration

import (
	"bytes"
	"testing"

	mclient "github.com/minio/minio-go/v7"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/test/app"
)

// A proxy request that read the policy before delete can create a task after it; that task must not run.
func TestDeleteReplication_DropsTaskOfDeletedReplication(t *testing.T) {
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	insp := newInspector(t)
	ctx := t.Context()
	r := require.New(t)

	bucket := "del-stale-gen"
	fillBucket(t, e.MainClient, bucket, 10, 64)
	uid := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User: user, FromStorage: "main", FromBucket: bucket, ToStorage: "f1", ToBucket: bucket,
	})
	id := &pb.ReplicationID{User: user, FromStorage: "main", ToStorage: "f1", FromBucket: &bucket, ToBucket: &bucket}

	configDB := redis.NewClient(&redis.Options{Addr: workerConf.Redis.Address, DB: workerConf.Redis.ConfigDB})
	t.Cleanup(func() { _ = configDB.Close() })
	metaDB := redis.NewClient(&redis.Options{Addr: workerConf.Redis.Address, DB: workerConf.Redis.MetaDB})
	t.Cleanup(func() { _ = metaDB.Close() })
	generations := store.NewReplicationGenerationStore(configDB)
	versionSvc := meta.NewVersionService(metaDB)
	queueSvc := tasks.NewQueueService(e.TaskClient, insp)

	putWithProxyTask := func(name string, generation int64) {
		t.Helper()
		data := []byte(name)
		_, err := e.MainClient.PutObject(ctx, bucket, name, bytes.NewReader(data), int64(len(data)), mclient.PutObjectOptions{})
		r.NoError(err)
		obj := dom.Object{Bucket: bucket, Name: name}
		_, err = versionSvc.IncrementObj(ctx, uid, obj, meta.Destination{Storage: "main", Bucket: bucket})
		r.NoError(err)
		task := tasks.ObjectSyncPayload{Object: obj, ObjSize: int64(len(data))}
		task.SetReplicationID(uid, generation)
		r.NoError(queueSvc.EnqueueTask(ctx, task))
	}
	eventQueueDrained := func() bool {
		s := queueState(t, insp, tasks.EventMigrationQueue(uid))
		return s.exists && s.pending == 0 && s.active == 0 && s.retry == 0
	}

	_, err := e.PolicyClient.AddReplication(ctx, &pb.AddReplicationRequest{Id: id})
	r.NoError(err)
	r.Eventually(func() bool {
		rep, err := e.PolicyClient.GetReplication(ctx, id)
		return err == nil && rep.IsInitDone
	}, e.WaitLong, e.RetryShort)
	generation, err := generations.Get(ctx, uid)
	r.NoError(err)

	putWithProxyTask("before-delete", generation)
	r.Eventually(func() bool { return countObjs(e.F1Client, bucket) == 11 }, e.WaitLong, e.RetryShort)

	_, err = e.PolicyClient.DeleteReplication(ctx, id)
	r.NoError(err)

	putWithProxyTask("after-delete", generation)
	r.Eventually(eventQueueDrained, e.WaitLong, e.RetryShort)
	r.Equal(11, countObjs(e.F1Client, bucket), "task of deleted replication must not be executed")

	_, err = e.PolicyClient.AddReplication(ctx, &pb.AddReplicationRequest{Id: id})
	r.NoError(err)
	r.Eventually(func() bool {
		rep, err := e.PolicyClient.GetReplication(ctx, id)
		return err == nil && rep.IsInitDone
	}, e.WaitLong, e.RetryShort)
	r.Equal(12, countObjs(e.F1Client, bucket), "re-added replication must copy all objects")
}

func TestDeleteReplication_MissingRemovesLeftoverQueues(t *testing.T) {
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	insp := newInspector(t)
	queueSvc := tasks.NewQueueService(e.TaskClient, insp)
	bucket := "del-missing"
	tests := []struct {
		name string
		id   *pb.ReplicationID
		uid  entity.UniversalReplicationID
	}{
		{
			name: "user",
			id:   &pb.ReplicationID{User: user, FromStorage: "main", ToStorage: "f1"},
			uid:  entity.UniversalFromUserReplication(entity.UserReplicationPolicy{User: user, FromStorage: "main", ToStorage: "f1"}),
		},
		{
			name: "bucket",
			id:   &pb.ReplicationID{User: user, FromStorage: "main", ToStorage: "f1", FromBucket: &bucket, ToBucket: &bucket},
			uid: entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
				User: user, FromStorage: "main", FromBucket: bucket, ToStorage: "f1", ToBucket: bucket,
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			ctx := t.Context()
			queue := tasks.EventMigrationQueue(tt.uid)
			r.NoError(queueSvc.Stop(ctx, queue))
			task := tasks.ObjectSyncPayload{Object: dom.Object{Bucket: bucket, Name: "obj"}}
			task.SetReplicationID(tt.uid, 0)
			r.NoError(queueSvc.EnqueueTask(ctx, task))
			r.True(queueState(t, insp, queue).exists)

			_, err := e.PolicyClient.DeleteReplication(ctx, tt.id)
			r.NoError(err)
			r.False(queueState(t, insp, queue).exists)
		})
	}
}
