package trace

import (
	"context"
	"github.com/clyso/chorus/pkg/log"
	"github.com/hibiken/asynq"
	"go.opentelemetry.io/otel/trace"
)

func WorkerMiddleware(tp trace.TracerProvider) asynq.MiddlewareFunc {
	return func(next asynq.Handler) asynq.Handler {
		return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
			tr := tp.Tracer("worker_handler")
			taskCtx, span := tr.Start(ctx, t.Type())
			defer span.End()
			traceID := trace.SpanFromContext(taskCtx).
				SpanContext().
				TraceID()
			taskCtx = log.WithTraceID(taskCtx, traceID.String())
			return next.ProcessTask(taskCtx, t)
		})
	}
}
