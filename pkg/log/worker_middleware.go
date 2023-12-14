package log

import (
	"context"
	"errors"
	"github.com/buger/jsonparser"
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/hibiken/asynq"
)

func WorkerMiddleware(cfg *Config, app, appID string) asynq.MiddlewareFunc {
	return func(next asynq.Handler) asynq.Handler {
		return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			l := CreateLogger(cfg, app, appID)
			builder := l.With()
			builder = builder.Str("task_type", t.Type()).RawJSON("task_payload", t.Payload())
			f := xctx.Migration
			if queue, ok := asynq.GetQueueName(ctx); ok {
				builder = builder.Str("task_queue", queue)
				switch queue {
				case tasks.QueueEventsDefault1,
					tasks.QueueEvents2,
					tasks.QueueEvents3,
					tasks.QueueEvents4,
					tasks.QueueEventsHighest5:
					f = xctx.Event
				}
			}
			builder = builder.Str("flow", string(f))
			if taskID, ok := asynq.GetTaskID(ctx); ok {
				builder = builder.Str("task_id", taskID)
			}
			if maxRetry, ok := asynq.GetMaxRetry(ctx); ok {
				builder = builder.Int("task_max_retry", maxRetry)
			}
			if retryCnt, ok := asynq.GetRetryCount(ctx); ok {
				builder = builder.Int("task_retry_count", retryCnt)
			}

			u, err := jsonparser.GetString(t.Payload(), "User")
			if err != nil && !errors.Is(err, jsonparser.KeyPathNotFoundError) {
				l.Err(err).Msg("unable to get user meta from task payload")
			}
			if u != "" {
				builder = builder.Str(user, u)
				ctx = xctx.SetUser(ctx, u)
			}

			newLogger := builder.Logger()
			taskCtx := newLogger.WithContext(ctx)
			taskCtx = xctx.SetFlow(taskCtx, f)

			return next.ProcessTask(taskCtx, t)
		})
	}
}
