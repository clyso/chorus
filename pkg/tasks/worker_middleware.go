// Copyright 2025 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tasks

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/entity"
)

// DropStaleReplicationTasks recognizes replication tasks by queue, not by task type:
// diff fix reuses replication task types with IDs that are not replication policies.
func DropStaleReplicationTasks(currentGeneration func(ctx context.Context, id entity.UniversalReplicationID) (int64, error)) asynq.MiddlewareFunc {
	return func(next asynq.Handler) asynq.Handler {
		return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
			queue, _ := asynq.GetQueueName(ctx)
			if !isReplicationQueue(queue) {
				return next.ProcessTask(ctx, t)
			}
			var task replicationID
			if err := json.Unmarshal(t.Payload(), &task); err != nil {
				return fmt.Errorf("unable to unmarshal replication task payload: %w: %w", err, asynq.SkipRetry)
			}
			generation, err := currentGeneration(ctx, task.ID)
			if err != nil {
				return fmt.Errorf("unable to get replication generation: %w", err)
			}
			if task.Generation != generation {
				zerolog.Ctx(ctx).Debug().
					Int64("task_generation", task.Generation).
					Int64("replication_generation", generation).
					Msg("drop task of deleted replication")
				return nil
			}
			return next.ProcessTask(ctx, t)
		})
	}
}
