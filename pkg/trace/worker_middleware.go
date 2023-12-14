/*
 * Copyright © 2023 Clyso GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
