package tasks

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/testutil"
)

func Test_queueService_UnprocessedCount(t *testing.T) {
	ctx := t.Context()
	c := testutil.SetupRedis(t)
	inspector := asynq.NewInspectorFromRedisClient(c)
	client := asynq.NewClientFromRedisClient(c)
	t.Cleanup(func() {
		client.Close()
		inspector.Close()
	})
	qs := NewQueueService(client, inspector)

	r := require.New(t)
	queueName := "test-queue-is-empty"

	// validate arguments
	_, err := qs.UnprocessedCount(ctx, false)
	r.ErrorIs(err, dom.ErrInvalidArg, "expected error for empty queue name")

	// check non-existing queue
	_, err = qs.UnprocessedCount(ctx, false, queueName)
	r.ErrorIs(err, dom.ErrNotFound, "expected error for non-existing queue")

	// check non-existing queue with ignoreNotFound
	count, err := qs.UnprocessedCount(ctx, true, queueName)
	r.NoError(err, "expected no error for non-existing queue with ignoreNotFound")
	r.Zero(count, "expected count to be zero for non-existing queue with ignoreNotFound")

	// create queue
	_, err = client.EnqueueContext(ctx, asynq.NewTask("test-task", nil), asynq.Queue(queueName))
	r.NoError(err, "failed to enqueue task to create queue")

	// check count of unprocessed tasks
	count, err = qs.UnprocessedCount(ctx, false, queueName)
	r.NoError(err, "failed to check if queue is empty")
	r.Equal(1, count, "expected queue to have 1 unprocessed task")
	count, err = qs.UnprocessedCount(ctx, true, queueName)
	r.NoError(err, "failed to check if queue is empty")
	r.Equal(1, count, "expected queue to have 1 unprocessed task")

	// dequeue all tasks
	archieved, err := inspector.ArchiveAllPendingTasks(queueName)
	r.NoError(err, "failed to archive all pending tasks")
	r.Equal(1, archieved, "expected to archive 1 task")

	// check if queue is empty again
	count, err = qs.UnprocessedCount(ctx, false, queueName)
	r.NoError(err, "failed to check if queue is empty")
	r.Zero(count, "expected queue to be empty after archiving tasks")
	count, err = qs.UnprocessedCount(ctx, true, queueName)
	r.NoError(err, "failed to check if queue is empty")
	r.Zero(count, "expected queue to be empty after archiving tasks")

	t.Run("multiple queues", func(t *testing.T) {
		c.FlushAll(ctx)
		r := require.New(t)
		q1, q2 := "test-queue-1", "test-queue-2"
		queues := []string{q1, q2}

		// both queues do not exist
		_, err := qs.UnprocessedCount(ctx, false, queues...)
		r.ErrorIs(err, dom.ErrNotFound, "expected error for non-existing queues")
		count, err := qs.UnprocessedCount(ctx, true, queues...)
		r.NoError(err, "expected no error for non-existing queues with ignoreNotFound")
		r.Zero(count, "expected count to be zero for non-existing queues with ignoreNotFound")

		// create first queue
		_, err = client.EnqueueContext(ctx, asynq.NewTask("test-task-1", nil), asynq.Queue(q1))
		r.NoError(err, "failed to enqueue task to create first queue")

		// check count of unprocessed tasks in first queue
		count, err = qs.UnprocessedCount(ctx, false, q1)
		r.NoError(err, "failed to check if first queue is empty")
		r.Equal(1, count, "expected first queue to have 1 unprocessed task")

		// check count of unprocessed tasks in second queue
		count, err = qs.UnprocessedCount(ctx, false, q2)
		r.ErrorIs(err, dom.ErrNotFound, "expected error for non-existing second queue")

		// check count of unprocessed tasks in both queues
		count, err = qs.UnprocessedCount(ctx, false, queues...)
		r.ErrorIs(err, dom.ErrNotFound, "expected error for non-existing second queue in multiple queues check")

		// check count of unprocessed tasks in both queues with ignoreNotFound
		count, err = qs.UnprocessedCount(ctx, true, queues...)
		r.NoError(err, "expected no error for multiple queues with ignoreNotFound")
		r.Equal(1, count, "expected count to be 1 for first queue and 0 for second queue with ignoreNotFound")

		// create second queue
		_, err = client.EnqueueContext(ctx, asynq.NewTask("test-task-2", nil), asynq.Queue(q2))
		r.NoError(err, "failed to enqueue task to create second queue")

		// check count of unprocessed tasks in both queues again
		count, err = qs.UnprocessedCount(ctx, false, queues...)
		r.NoError(err, "failed to check if both queues are empty")
		r.Equal(2, count, "expected both queues to have 2 unprocessed tasks")
		count, err = qs.UnprocessedCount(ctx, true, queues...)
		r.NoError(err, "failed to check if both queues are empty with ignoreNotFound")
		r.Equal(2, count, "expected both queues to have 2 unprocessed tasks with ignoreNotFound")
	})
}

func Test_queueService_RetriedTasksCountAsUnprocessed(t *testing.T) {
	ctx := t.Context()
	c := testutil.SetupRedis(t)
	inspector := asynq.NewInspectorFromRedisClient(c)
	defer inspector.Close()
	client := asynq.NewClientFromRedisClient(c)
	defer client.Close()
	qs := NewQueueService(client, inspector)

	r := require.New(t)
	queueName := "test-queue-is-empty-with-retry"

	// create queue
	_, err := client.EnqueueContext(ctx, asynq.NewTask("test-task", nil), asynq.Queue(queueName))
	r.NoError(err, "failed to enqueue task to create queue")

	//register a handler to return a retryable error
	stop := registerHandler(t, ctx, c, queueName, func(ctx context.Context, t *asynq.Task) error {
		return dom.ErrNotFound
	})

	r.Eventually(func() bool {
		info, err := inspector.GetQueueInfo(queueName)
		if err != nil {
			return false
		}
		return info.Retry > 0
	}, time.Second, 100*time.Millisecond, "expected task to be retried")
	stop()

	// check if queue is empty
	count, err := qs.UnprocessedCount(ctx, false, queueName)
	r.NoError(err, "failed to check if queue is empty")
	r.Equal(1, count, "expected queue to have 1 unprocessed task")

	// dequeue all retry tasks
	archieved, err := inspector.ArchiveAllRetryTasks(queueName)
	r.NoError(err, "failed to archive all pending tasks")
	r.Equal(1, archieved, "expected to archive 1 task")

	// check if queue is empty again
	count, err = qs.UnprocessedCount(ctx, false, queueName)
	r.NoError(err, "failed to check if queue is empty after archiving tasks")
	r.Zero(count, "expected queue to be empty after archiving tasks")
}

func registerHandler(t *testing.T, ctx context.Context, redisClient redis.UniversalClient, queue string, handler asynq.HandlerFunc) func() {
	t.Helper()
	srv := asynq.NewServerFromRedisClient(redisClient, asynq.Config{
		Concurrency: 1,
		BaseContext: func() context.Context {
			return ctx
		},
		IsFailure: func(err error) bool {
			return !errors.Is(err, dom.ErrNotFound)
		},
		Queues: map[string]int{
			queue: 1,
		},
		ShutdownTimeout: time.Microsecond,
	})
	err := srv.Start(handler)
	if err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	return srv.Shutdown
}

func Test_queueService_PauseResume(t *testing.T) {
	ctx := t.Context()
	c := testutil.SetupRedis(t)
	inspector := asynq.NewInspectorFromRedisClient(c)
	defer inspector.Close()
	client := asynq.NewClientFromRedisClient(c)
	defer client.Close()
	qs := NewQueueService(client, inspector)

	r := require.New(t)
	queueName := "test-queue-pause-resume"

	// queue not found
	_, err := qs.IsPaused(ctx, queueName)
	r.ErrorIs(err, dom.ErrNotFound, "expected error for non-existing queue")

	// create queue
	_, err = client.EnqueueContext(ctx, asynq.NewTask("test-task", nil), asynq.Queue(queueName))
	r.NoError(err, "failed to enqueue task to create queue")

	// check if queue is paused
	paused, err := qs.IsPaused(ctx, queueName)
	r.NoError(err, "failed to check if queue is paused")
	r.False(paused, "expected queue to be not paused")

	// resume non-paused queue
	err = qs.Resume(ctx, queueName)
	r.NoError(err, "expected no error when resuming non-paused queue")
	// still not paused
	paused, err = qs.IsPaused(ctx, queueName)
	r.NoError(err, "failed to check if queue is paused after resuming")
	r.False(paused, "expected queue to be not paused after resuming")

	// pause queue
	err = qs.Pause(ctx, queueName)
	r.NoError(err, "failed to pause queue")
	// check if queue is paused
	paused, err = qs.IsPaused(ctx, queueName)
	r.NoError(err, "failed to check if queue is paused after pausing")
	r.True(paused, "expected queue to be paused after pausing")

	// pause already paused queue
	err = qs.Pause(ctx, queueName)
	r.NoError(err, "expected no error when pausing already paused queue")
	// check if queue is still paused
	paused, err = qs.IsPaused(ctx, queueName)
	r.NoError(err, "failed to check if queue is paused after pausing again")
	r.True(paused, "expected queue to be still paused after pausing again")

	// resume paused queue
	err = qs.Resume(ctx, queueName)
	r.NoError(err, "failed to resume queue")
	// check if queue is resumed
	paused, err = qs.IsPaused(ctx, queueName)
	r.NoError(err, "failed to check if queue is paused after resuming")
	r.False(paused, "expected queue to be not paused after resuming")
}

func Test_queueService_Stats(t *testing.T) {
	ctx := t.Context()
	c := testutil.SetupRedis(t)
	inspector := asynq.NewInspectorFromRedisClient(c)
	defer inspector.Close()
	client := asynq.NewClientFromRedisClient(c)
	defer client.Close()
	qs := NewQueueService(client, inspector)

	r := require.New(t)
	queueName := "test-queue-stats"

	// queue not found
	_, err := qs.Stats(ctx, queueName)
	r.ErrorIs(err, dom.ErrNotFound, "expected error for non-existing queue")

	// create queue
	_, err = client.EnqueueContext(ctx, asynq.NewTask("test-task", nil), asynq.Queue(queueName))
	r.NoError(err, "failed to enqueue task to create queue")

	// check stats of unprocessed tasks
	stats, err := qs.Stats(ctx, queueName)
	r.NoError(err, "failed to get stats for queue")

	count, err := qs.UnprocessedCount(ctx, false, queueName)
	r.NoError(err, "failed to check unprocessed count for queue")
	r.Equal(1, count, "expected queue to have 1 unprocessed task")

	paused, err := qs.IsPaused(ctx, queueName)
	r.NoError(err, "failed to check if queue is paused")
	r.False(paused, "expected queue to be not paused")

	// check stats unproccessed and paused match
	r.Equal(count, stats.Unprocessed, "expected stats unprocessed count to match")
	r.Equal(paused, stats.Paused, "expected stats paused to match")
	r.NotZero(stats.Latency, "expected stats latency to be non-zero")

	// pause queue
	err = qs.Pause(ctx, queueName)
	r.NoError(err, "failed to pause queue")

	// check stats after pausing
	stats, err = qs.Stats(ctx, queueName)
	r.NoError(err, "failed to get stats for queue after pausing")
	r.True(stats.Paused, "expected queue to be paused after pausing")
	r.Equal(1, stats.Unprocessed, "expected stats unprocessed count to match after pausing")
	r.NotZero(stats.Latency, "expected stats latency to be non-zero after pausing")

	// compare with inspector info
	info, err := inspector.GetQueueInfo(queueName)
	r.NoError(err, "failed to get queue info from inspector")
	r.Equal(info.FailedTotal, stats.FailedTotal, "expected stats failed total to match inspector info")
	r.Equal(info.ProcessedTotal, stats.ProcessedTotal, "expected stats processed total to match inspector info")
	r.Equal(info.MemoryUsage, stats.MemoryUsage, "expected stats memory usage to match inspector info")
}

func Test_queueService_Enqueue(t *testing.T) {
	ctx := t.Context()
	c := testutil.SetupRedis(t)
	inspector := asynq.NewInspectorFromRedisClient(c)
	defer inspector.Close()
	client := asynq.NewClientFromRedisClient(c)
	defer client.Close()
	qs := NewQueueService(client, inspector)

	r := require.New(t)

	err := qs.EnqueueTask(ctx, entity.UniversalReplicationID{})
	r.ErrorIs(err, dom.ErrNotImplemented, "expected error for unsupported payload type")

	payload := BucketCreatePayload{
		replicationID: replicationID{},
		Bucket:        "",
		Location:      "",
	}
	err = qs.EnqueueTask(ctx, payload)
	r.ErrorIs(err, dom.ErrInternal, "expected error when replication ID is not set")
	err = qs.EnqueueTask(ctx, &payload)
	r.ErrorIs(err, dom.ErrInternal, "expected error when replication ID is not set")

	payload.SetReplicationID(entity.IDFromBucketReplication(entity.ReplicationStatusID{
		User:        "u",
		FromStorage: "fs",
		FromBucket:  "fb",
		ToStorage:   "ts",
		ToBucket:    "tb",
	}))
	err = qs.EnqueueTask(ctx, payload)
	r.NoError(err, "expected no error when replication ID is set")
	err = qs.EnqueueTask(ctx, &payload)
	r.NoError(err, "expected no error when replication ID is set")
}
