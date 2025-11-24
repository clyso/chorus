/*
 * Copyright © 2024 Clyso GmbH
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

package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/tasks"
)

func (s *svc) HandleMigrationS3User(ctx context.Context, t *asynq.Task) error {
	var p tasks.MigrateS3UserPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("MigrateS3UserPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	logger := zerolog.Ctx(ctx)

	if err := s.limit.StorReq(ctx, p.ID.FromStorage()); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.FromStorage()).Msg("rate limit error")
		return err
	}

	fromClient, err := s.clients.AsS3(ctx, p.ID.FromStorage(), p.ID.User())
	if err != nil {
		return fmt.Errorf("migration bucket list obj: unable to get %q s3 client: %w: %w", p.ID.FromStorage(), err, asynq.SkipRetry)
	}

	// iterate through account containers
	buckets, err := fromClient.S3().ListBuckets(ctx)
	if err != nil {
		return fmt.Errorf("error listing buckets: %w", err)
	}
	for _, bucket := range buckets {
		// start migration for each bucket:
		task := tasks.BucketCreatePayload{
			Bucket: bucket.Name,
		}
		task.SetReplicationID(p.ID)
		err = s.queueSvc.EnqueueTask(ctx, task)
		if err != nil {
			return err
		}
	}
	return nil
}
