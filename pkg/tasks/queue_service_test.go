package tasks

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func Test_queueService_Delete(t *testing.T) {
	ctx := t.Context()
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	inspector := asynq.NewInspectorFromRedisClient(c)
	defer inspector.Close()
	client := asynq.NewClientFromRedisClient(c)
	defer client.Close()
	qs := NewQueueService(inspector)

	r := require.New(t)
	queueName := "test-queue"

	// delete non-existing queue
	err := qs.Delete(ctx, queueName, false)
	r.ErrorIs(err, dom.ErrNotFound, "expected error for non-existing queue")
	err = qs.Delete(ctx, queueName, true)
	r.ErrorIs(err, dom.ErrNotFound, "expected error for non-existing queue with force")

	// create queue
	_, err = client.EnqueueContext(ctx, asynq.NewTask("test-task", nil), asynq.Queue(queueName))
	r.NoError(err, "failed to enqueue task to create queue")

	// delete non-empty queue without force
	empty, err := qs.IsEmpty(ctx, queueName)
	r.NoError(err, "failed to check if queue is empty")
	r.False(empty, "expected queue to not be empty before deletion")
	err = qs.Delete(ctx, queueName, false)
	r.ErrorIs(err, dom.ErrInvalidArg, "expected error for non-empty queue without force")
	// delete non-empty queue with force
	err = qs.Delete(ctx, queueName, true)
	r.NoError(err, "failed to delete non-empty queue with force")
	// check if queue exists
	_, err = inspector.GetQueueInfo(queueName)
	r.Error(err, "expected queue to be deleted")
}

func Test_queueService_IsEmpty(t *testing.T) {
	ctx := t.Context()
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	inspector := asynq.NewInspectorFromRedisClient(c)
	defer inspector.Close()
	client := asynq.NewClientFromRedisClient(c)
	defer client.Close()
	qs := NewQueueService(inspector)

	r := require.New(t)
	queueName := "test-queue-is-empty"

	// check non-existing queue
	_, err := qs.IsEmpty(ctx, queueName)
	r.ErrorIs(err, dom.ErrNotFound, "expected error for non-existing queue")

	// create queue
	_, err = client.EnqueueContext(ctx, asynq.NewTask("test-task", nil), asynq.Queue(queueName))
	r.NoError(err, "failed to enqueue task to create queue")

	// check if queue is empty
	empty, err := qs.IsEmpty(ctx, queueName)
	r.NoError(err, "failed to check if queue is empty")
	r.False(empty, "expected queue to not be empty after enqueueing task")

	// dequeue all tasks
	archieved, err := inspector.ArchiveAllPendingTasks(queueName)
	r.NoError(err, "failed to archive all pending tasks")
	r.Equal(1, archieved, "expected to archive 1 task")

	// check if queue is empty again
	empty, err = qs.IsEmpty(ctx, queueName)
	r.NoError(err, "failed to check if queue is empty after archiving tasks")
	r.True(empty, "expected queue to be empty after archiving tasks")
}

func Test_queueService_IsEmptyWithRetry(t *testing.T) {
	ctx := t.Context()
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	inspector := asynq.NewInspectorFromRedisClient(c)
	defer inspector.Close()
	client := asynq.NewClientFromRedisClient(c)
	defer client.Close()
	qs := NewQueueService(inspector)

	r := require.New(t)
	queueName := "test-queue-is-empty-with-retry"

	// create queue
	_, err := client.EnqueueContext(ctx, asynq.NewTask("test-task", nil), asynq.Queue(queueName))
	r.NoError(err, "failed to enqueue task to create queue")

	//register a handler to return a retryable error
	registerHandler(t, ctx, c, queueName, func(ctx context.Context, t *asynq.Task) error {
		return dom.ErrNotFound
	})

	r.Eventually(func() bool {
		info, err := inspector.GetQueueInfo(queueName)
		if err != nil {
			return false
		}
		return info.Retry > 0
	}, time.Second, 100*time.Millisecond, "expected task to be retried")

	// check if queue is empty
	empty, err := qs.IsEmpty(ctx, queueName)
	r.NoError(err, "failed to check if queue is empty")
	r.False(empty, "expected queue to not be empty after enqueueing task")

	// dequeue all retry tasks
	archieved, err := inspector.ArchiveAllRetryTasks(queueName)
	r.NoError(err, "failed to archive all pending tasks")
	r.Equal(1, archieved, "expected to archive 1 task")

	// check if queue is empty again
	empty, err = qs.IsEmpty(ctx, queueName)
	r.NoError(err, "failed to check if queue is empty after archiving tasks")
	r.True(empty, "expected queue to be empty after archiving tasks")
}

func registerHandler(t *testing.T, ctx context.Context, redisClient redis.UniversalClient, queue string, handler asynq.HandlerFunc) {
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
	go func() {
		<-ctx.Done()
		srv.Shutdown()
	}()
}
