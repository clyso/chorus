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

package log

import (
	"context"
	"errors"
	"strings"

	"github.com/buger/jsonparser"
	"github.com/hibiken/asynq"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/tasks"
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
				if strings.HasPrefix(queue, string(tasks.QueueEventsPrefix)) {
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
