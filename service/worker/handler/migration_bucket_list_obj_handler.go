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

	"github.com/hibiken/asynq"
	mclient "github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/tasks"
)

func (s *svc) HandleMigrationBucketListObj(ctx context.Context, t *asynq.Task) error {
	// todo: aggregate task to not list multiple times
	var p tasks.MigrateBucketListObjectsPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("HandleMigrationBucketListObj Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Bucket)
	logger := zerolog.Ctx(ctx)

	replicationID := p.GetReplicationID()

	if err := s.limit.StorReq(ctx, p.ID.FromStorage()); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.FromStorage()).Msg("rate limit error")
		return err
	}

	fromClient, err := s.clients.AsS3(ctx, p.ID.FromStorage(), p.ID.User())
	if err != nil {
		return fmt.Errorf("migration bucket list obj: unable to get %q s3 client: %w: %w", p.ID.FromStorage(), err, asynq.SkipRetry)
	}

	migrationID := entity.NewMigrationObjectIDFromUniversalReplicationID(p.ID, p.Bucket, p.Prefix)
	lastObjName, err := s.listStateStore.Get(ctx, migrationID)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("unable to get last listed object: %w", err)
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
			if err = s.queueSvc.EnqueueTask(ctx, subP); err != nil {
				return fmt.Errorf("migration bucket list obj: unable to enqueue list obj sub task: %w", err)
			}
			err = s.listStateStore.Set(ctx, migrationID, object.Key)
			if err != nil {
				return fmt.Errorf("migration bucket list obj: unable to update last obj meta: %w", err)
			}
			continue
		}

		if p.Versioned {
			task := tasks.ListObjectVersionsPayload{
				Bucket: p.Bucket,
				Prefix: object.Key,
			}
			task.SetReplicationID(replicationID)
			err = s.queueSvc.EnqueueTask(ctx, task)
			if err != nil {
				return fmt.Errorf("unable to create list object versions task: %w", err)
			}
		} else {
			task := tasks.MigrateObjCopyPayload{
				Bucket: p.Bucket,
				Obj: tasks.ObjPayload{
					Name:        object.Key,
					VersionID:   object.VersionID,
					ETag:        object.ETag,
					Size:        object.Size,
					ContentType: object.ContentType,
				},
			}
			task.SetReplicationID(replicationID)
			err = s.queueSvc.EnqueueTask(ctx, task)
			if err != nil {
				return fmt.Errorf("migration bucket list obj: unable to create copy obj task: %w", err)
			}
		}
		if err = s.listStateStore.Set(ctx, migrationID, object.Key); err != nil {
			return fmt.Errorf("migration bucket list obj: unable to update last obj meta: %w", err)
		}
	}

	if lastObjName == "" && objectsNum == 0 && p.Prefix != "" {
		// copy empty dir object
		task := tasks.MigrateObjCopyPayload{
			Bucket: p.Bucket,
			Obj: tasks.ObjPayload{
				Name: p.Prefix,
			},
		}
		task.SetReplicationID(replicationID)
		err = s.queueSvc.EnqueueTask(ctx, task)
		if err != nil {
			return fmt.Errorf("migration bucket list obj: unable to enqueue copy obj task: %w", err)
		}
	}
	_, _ = s.listStateStore.Drop(ctx, migrationID)

	logger.Info().Msg("migration bucket list obj: done")
	return nil
}
