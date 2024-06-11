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
	"errors"
	"fmt"
	"strings"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/hibiken/asynq"
	mclient "github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"
)

func (s *svc) HandleMigrationBucketListObj(ctx context.Context, t *asynq.Task) error {
	// todo: aggregate task to not list multiple times
	var p tasks.MigrateBucketListObjectsPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("HandleMigrationBucketListObj Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Bucket)
	logger := zerolog.Ctx(ctx)

	paused, err := s.policySvc.IsReplicationPolicyPaused(ctx, xctx.GetUser(ctx), p.Bucket, p.FromStorage, p.ToStorage)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			zerolog.Ctx(ctx).Err(err).Msg("drop replication task: replication policy not found")
			return nil
		}
		return err
	}
	if paused {
		return &dom.ErrRateLimitExceeded{RetryIn: s.conf.PauseRetryInterval}
	}

	if err = s.limit.StorReq(ctx, p.FromStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.FromStorage).Msg("rate limit error")
		return err
	}

	fromClient, err := s.clients.GetByName(ctx, p.FromStorage)
	if err != nil {
		return fmt.Errorf("migration bucket list obj: unable to get %q s3 client: %v: %w", p.FromStorage, err, asynq.SkipRetry)
	}

	lastObjName, err := s.storageSvc.GetLastListedObj(ctx, p)
	if err != nil {
		return err
	}

	objects := fromClient.S3().ListObjects(ctx, p.Bucket, mclient.ListObjectsOptions{StartAfter: lastObjName, Prefix: p.Prefix})
	objectsNum := 0
	for object := range objects {
		if object.Err != nil {
			return fmt.Errorf("migration bucket list obj: list objects error %w", object.Err)
		}
		objectsNum++
		isDir := object.Size == 0 && strings.HasSuffix(object.Key, "/")
		logger.Debug().Str(log.Object, object.Key).Str("obj_version_id", object.VersionID).Bool("is_dir", isDir).Msg("migration bucket list obj: start processing object from the list")
		if isDir {
			subP := p
			subP.Prefix = object.Key
			subTask, err := tasks.NewTask(ctx, subP)
			if err != nil {
				return fmt.Errorf("migration bucket list obj: unable to create list obj sub task: %w", err)
			}
			_, err = s.taskClient.EnqueueContext(ctx, subTask)
			if err != nil && !errors.Is(err, asynq.ErrDuplicateTask) && !errors.Is(err, asynq.ErrTaskIDConflict) {
				return fmt.Errorf("migration bucket list obj: unable to enqueue list obj sub task: %w", err)
			} else if err != nil {
				logger.Info().Interface("enqueue_task_payload", subP).Msg("cannot enqueue task with duplicate id")
			}
			err = s.storageSvc.SetLastListedObj(ctx, p, object.Key)
			if err != nil {
				return fmt.Errorf("migration bucket list obj: unable to update last obj meta: %w", err)
			}
			continue
		}
		p.Sync.InitDate()
		task, err := tasks.NewTask(ctx, tasks.MigrateObjCopyPayload{
			Sync:   p.Sync,
			Bucket: p.Bucket,
			Obj: tasks.ObjPayload{
				Name:        object.Key,
				VersionID:   object.VersionID,
				ETag:        object.ETag,
				Size:        object.Size,
				ContentType: object.ContentType,
			},
		})
		if err != nil {
			return fmt.Errorf("migration bucket list obj: unable to create copy obj task: %w", err)
		}
		_, err = s.taskClient.EnqueueContext(ctx, task)
		if err != nil {
			if errors.Is(err, asynq.ErrDuplicateTask) || errors.Is(err, asynq.ErrTaskIDConflict) {
				logger.Info().RawJSON("enqueue_task_payload", task.Payload()).Msg("cannot enqueue task with duplicate id")
				continue
			}
			return fmt.Errorf("migration bucket list obj: unable to enqueue copy obj task: %w", err)
		}
		err = s.policySvc.IncReplInitObjListed(ctx, xctx.GetUser(ctx), p.Bucket, p.FromStorage, p.ToStorage, object.Size, p.GetDate())
		if err != nil {
			return fmt.Errorf("migration bucket list obj: unable to inc obj listed meta: %w", err)
		}

		err = s.storageSvc.SetLastListedObj(ctx, p, object.Key)
		if err != nil {
			return fmt.Errorf("migration bucket list obj: unable to update last obj meta: %w", err)
		}
	}

	if lastObjName == "" && objectsNum == 0 && p.Prefix != "" {
		p.Sync.InitDate()
		// copy empty dir object
		task, err := tasks.NewTask(ctx, tasks.MigrateObjCopyPayload{
			Sync:   p.Sync,
			Bucket: p.Bucket,
			Obj: tasks.ObjPayload{
				Name: p.Prefix,
			},
		})
		if err != nil {
			return fmt.Errorf("migration bucket list obj: unable to create copy obj task: %w", err)
		}
		_, err = s.taskClient.EnqueueContext(ctx, task)
		if errors.Is(err, asynq.ErrDuplicateTask) || errors.Is(err, asynq.ErrTaskIDConflict) {
			logger.Info().RawJSON("enqueue_task_payload", task.Payload()).Msg("cannot enqueue task with duplicate id")
		} else if err != nil {
			return fmt.Errorf("migration bucket list obj: unable to enqueue copy obj task: %w", err)
		} else {
			err = s.policySvc.IncReplInitObjListed(ctx, xctx.GetUser(ctx), p.Bucket, p.FromStorage, p.ToStorage, 0, p.GetDate())
			if err != nil {
				return fmt.Errorf("migration bucket list obj: unable to inc obj listed meta: %w", err)
			}
		}
	}
	_ = s.storageSvc.DelLastListedObj(ctx, p)

	if p.Prefix == "" {
		err = s.policySvc.ObjListStarted(ctx, xctx.GetUser(ctx), p.Bucket, p.FromStorage, p.ToStorage)
		if err != nil {
			logger.Err(err).Msg("migration bucket list obj: unable to set ObjListStarted")
		}
	}

	logger.Info().Msg("migration bucket list obj: done")
	return nil
}
