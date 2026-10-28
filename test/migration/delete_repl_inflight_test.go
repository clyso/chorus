package migration

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	mclient "github.com/minio/minio-go/v7"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/test/app"
)

func fillBucket(t *testing.T, c *mclient.Client, bucket string, n, size int) {
	t.Helper()
	r := require.New(t)
	ctx := t.Context()
	r.NoError(c.MakeBucket(ctx, bucket, mclient.MakeBucketOptions{Region: "us-east"}))
	data := bytes.Repeat([]byte("x"), size)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(32)
	for i := range n {
		g.Go(func() error {
			_, err := c.PutObject(gctx, bucket, fmt.Sprintf("obj-%06d", i), bytes.NewReader(data), int64(len(data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
			return err
		})
	}
	r.NoError(g.Wait())
}

func countObjs(c *mclient.Client, bucket string) int {
	objs, err := listObjects(c, bucket, "")
	if err != nil {
		return 0
	}
	return len(objs)
}

type qState struct {
	exists  bool
	active  int
	pending int
	retry   int
	paused  bool
}

func (s qState) String() string {
	if !s.exists {
		return "<absent>"
	}
	return fmt.Sprintf("active=%d pending=%d retry=%d paused=%v", s.active, s.pending, s.retry, s.paused)
}

func queueState(t *testing.T, insp *asynq.Inspector, q string) qState {
	t.Helper()
	all, err := insp.Queues()
	require.NoError(t, err)
	if !slices.Contains(all, q) {
		return qState{}
	}
	info, err := insp.GetQueueInfo(q)
	if err != nil {
		return qState{}
	}
	return qState{exists: true, active: info.Active, pending: info.Pending, retry: info.Retry, paused: info.Paused}
}

func logQueues(t *testing.T, insp *asynq.Inspector, stage string, queues []string) {
	t.Helper()
	for _, q := range queues {
		t.Logf("[%s] queue %-60s %s", stage, q, queueState(t, insp, q))
	}
}

func newInspector(t *testing.T) *asynq.Inspector {
	t.Helper()
	// SetupEmbedded writes the redis address into the shared workerConf
	insp := asynq.NewInspector(asynq.RedisClientOpt{Addr: workerConf.Redis.Address, DB: workerConf.Redis.QueueDB})
	t.Cleanup(func() { _ = insp.Close() })
	return insp
}

func TestDeleteReplication_InFlightMigration_ActiveTasks(t *testing.T) {
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	insp := newInspector(t)
	ctx := t.Context()
	r := require.New(t)

	const n = 3000
	bucket := "del-inflight-a"
	fillBucket(t, e.MainClient, bucket, n, 4*1024)

	id := &pb.ReplicationID{User: user, FromStorage: "main", ToStorage: "f1", FromBucket: &bucket, ToBucket: &bucket}
	queues := tasks.AllReplicationQueues(entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User: user, FromStorage: "main", FromBucket: bucket, ToStorage: "f1", ToBucket: bucket,
	}))
	_, err := e.PolicyClient.AddReplication(ctx, &pb.AddReplicationRequest{Id: id})
	r.NoError(err)

	r.Eventually(func() bool { return countObjs(e.F1Client, bucket) >= 50 }, e.WaitLong, 5*time.Millisecond)
	logQueues(t, insp, "before delete", queues)

	_, err = e.PolicyClient.DeleteReplication(ctx, id)
	r.NoError(err)
	atDelete := countObjs(e.F1Client, bucket)
	logQueues(t, insp, "after delete", queues)

	all, err := insp.Queues()
	r.NoError(err)
	for _, q := range queues {
		r.NotContains(all, q, "replication queue must be removed after delete")
	}
	assertNoCopiesAfterDelete(t, e, bucket, atDelete)
}

func TestDeleteReplication_InFlightMigration_ActiveListTask(t *testing.T) {
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	insp := newInspector(t)
	ctx := t.Context()
	r := require.New(t)

	const n = 8000
	bucket := "del-inflight-b"
	fillBucket(t, e.MainClient, bucket, n, 512)

	id := &pb.ReplicationID{User: user, FromStorage: "main", ToStorage: "f1", FromBucket: &bucket, ToBucket: &bucket}
	uid := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User: user, FromStorage: "main", FromBucket: bucket, ToStorage: "f1", ToBucket: bucket,
	})
	queues := tasks.AllReplicationQueues(uid)
	listQ, copyQ := tasks.InitMigrationListQueue(uid), tasks.InitMigrationCopyQueue(uid)

	_, err := e.PolicyClient.AddReplication(ctx, &pb.AddReplicationRequest{Id: id})
	r.NoError(err)
	r.Eventually(func() bool { return queueState(t, insp, listQ).active == 1 }, e.WaitLong, time.Millisecond)
	listed := queueState(t, insp, copyQ)
	r.Less(listed.pending+listed.active+countObjs(e.F1Client, bucket), n/2, "list task must be far from completion at delete")

	_, err = e.PolicyClient.DeleteReplication(ctx, id)
	r.NoError(err)
	atDelete := countObjs(e.F1Client, bucket)
	logQueues(t, insp, "after delete", queues)
	assertNoCopiesAfterDelete(t, e, bucket, atDelete)

	_, err = e.PolicyClient.AddReplication(ctx, &pb.AddReplicationRequest{Id: id})
	r.NoError(err)
	rep, err := e.PolicyClient.GetReplication(ctx, id)
	r.NoError(err)
	r.False(rep.IsPaused, "re-added replication must not inherit paused state of deleted one")
	r.Eventually(func() bool { return countObjs(e.F1Client, bucket) == n }, e.WaitLong, e.RetryShort, "re-added replication must copy all objects")
}

// asynq releases a canceled task without waiting for its handler, so up to Concurrency
// already sent copies can land after delete returned.
func assertNoCopiesAfterDelete(t *testing.T, e app.EmbeddedEnv, bucket string, atDelete int) {
	t.Helper()
	time.Sleep(5 * workerConf.Worker.QueueUpdateInterval)
	final := countObjs(e.F1Client, bucket)
	t.Logf("dest objects: at delete=%d final=%d", atDelete, final)
	require.LessOrEqual(t, final-atDelete, workerConf.Concurrency, "objects must not be copied after replication delete")
}

// Leftover metadata (late write of a canceled copy task, interrupted delete) must not make a re-added replication skip objects.
func TestAddReplication_IgnoresLeftoverMetadata(t *testing.T) {
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	ctx := t.Context()
	r := require.New(t)

	const n = 200
	bucket := "readd-leftover"
	fillBucket(t, e.MainClient, bucket, n, 64)

	uid := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User: user, FromStorage: "main", FromBucket: bucket, ToStorage: "f1", ToBucket: bucket,
	})
	metaDB := redis.NewClient(&redis.Options{Addr: workerConf.Redis.Address, DB: workerConf.Redis.MetaDB})
	t.Cleanup(func() { _ = metaDB.Close() })

	// equal source and destination versions make copy skip the object
	versionSvc := meta.NewVersionService(metaDB)
	for i := range n / 2 {
		obj := dom.Object{Bucket: bucket, Name: fmt.Sprintf("obj-%06d", i)}
		ver, err := versionSvc.IncrementObj(ctx, uid, obj, meta.Destination{Storage: "main", Bucket: bucket})
		r.NoError(err)
		r.NoError(versionSvc.UpdateIfGreater(ctx, uid, obj, meta.Destination{Storage: "f1", Bucket: bucket}, ver))
	}
	listState := store.NewMigrationObjectListStateStore(metaDB)
	r.NoError(listState.Set(ctx, entity.NewMigrationObjectIDFromUniversalReplicationID(uid, bucket, ""), fmt.Sprintf("obj-%06d", n-50)))

	id := &pb.ReplicationID{User: user, FromStorage: "main", ToStorage: "f1", FromBucket: &bucket, ToBucket: &bucket}
	_, err := e.PolicyClient.AddReplication(ctx, &pb.AddReplicationRequest{Id: id})
	r.NoError(err)
	r.Eventually(func() bool {
		rep, err := e.PolicyClient.GetReplication(ctx, id)
		return err == nil && rep.IsInitDone
	}, e.WaitLong, e.RetryShort)
	r.Equal(n, countObjs(e.F1Client, bucket), "re-added replication must copy all objects")
}
