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
	"strings"

	"github.com/hibiken/asynq"
	mclient "github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"

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

	if err := s.limit.StorReq(ctx, p.Replication.FromStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.Replication.FromStorage).Msg("rate limit error")
		return err
	}

	fromClient, err := s.clients.GetByName(ctx, p.Replication.FromStorage)
	if err != nil {
		return fmt.Errorf("migration bucket list obj: unable to get %q s3 client: %w: %w", p.Replication.FromStorage, err, asynq.SkipRetry)
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
			if err = s.queueSvc.EnqueueTask(ctx, subP); err != nil {
				return fmt.Errorf("migration bucket list obj: unable to enqueue list obj sub task: %w", err)
			}
			err = s.storageSvc.SetLastListedObj(ctx, p, object.Key)
			if err != nil {
				return fmt.Errorf("migration bucket list obj: unable to update last obj meta: %w", err)
			}
			continue
		}

		if p.Versioned {
			err = s.queueSvc.EnqueueTask(ctx, tasks.ListObjectVersionsPayload{
				ReplicationID: p.ReplicationID,
				Bucket:        p.Bucket,
				Prefix:        object.Key,
			})
			if err != nil {
				return fmt.Errorf("unable to create list object versions task: %w", err)
			}
		} else {
			err = s.queueSvc.EnqueueTask(ctx, tasks.MigrateObjCopyPayload{
				ReplicationID: p.ReplicationID,
				Bucket:        p.Bucket,
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
		}
		err = s.storageSvc.SetLastListedObj(ctx, p, object.Key)
		if err != nil {
			return fmt.Errorf("migration bucket list obj: unable to update last obj meta: %w", err)
		}
	}

	if lastObjName == "" && objectsNum == 0 && p.Prefix != "" {
		// copy empty dir object
		err = s.queueSvc.EnqueueTask(ctx, tasks.MigrateObjCopyPayload{
			ReplicationID: p.ReplicationID,
			Bucket:        p.Bucket,
			Obj: tasks.ObjPayload{
				Name: p.Prefix,
			},
		})
		if err != nil {
			return fmt.Errorf("migration bucket list obj: unable to enqueue copy obj task: %w", err)
		}
	}
	_ = s.storageSvc.DelLastListedObj(ctx, p)

	if p.Prefix == "" {
		err = s.policySvc.ObjListStarted(ctx, replicationID)
		if err != nil {
			logger.Err(err).Msg("migration bucket list obj: unable to set ObjListStarted")
		}
	}

	logger.Info().Msg("migration bucket list obj: done")
	return nil
}
