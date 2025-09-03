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
	"time"

	"github.com/hibiken/asynq"
	mclient "github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/rclone"
	"github.com/clyso/chorus/pkg/tasks"
)

func (s *svc) HandleObjectSync(ctx context.Context, t *asynq.Task) (err error) {
	var p tasks.ObjectSyncPayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("ObjectSyncPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Object.Bucket)
	ctx = log.WithObjName(ctx, p.Object.Name)
	logger := zerolog.Ctx(ctx)
	fromBucket, toBucket := p.ID.FromToBuckets(p.Object.Bucket)

	if err = s.limit.StorReq(ctx, p.ID.FromStorage()); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.FromStorage()).Msg("rate limit error")
		return err
	}
	if err = s.limit.StorReq(ctx, p.ID.ToStorage()); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.ToStorage()).Msg("rate limit error")
		return err
	}

	objectLockID := entity.NewVersionedObjectLockID(p.ID.ToStorage(), toBucket, p.Object.Name, p.Object.Version)
	lock, err := s.objectLocker.Lock(ctx, objectLockID)
	if err != nil {
		return err
	}
	defer lock.Release(ctx)
	objMeta, err := s.versionSvc.GetObj(ctx, p.Object)
	if err != nil {
		return err
	}
	isObjDeleted := len(objMeta) == 0
	if isObjDeleted {
		return s.objectDelete(ctx, p)
	}

	destVersionKey := meta.ToDest(p.ID.ToStorage(), toBucket)
	fromVer, toVer := objMeta[meta.ToDest(p.ID.FromStorage(), "")], objMeta[destVersionKey]
	if fromVer <= toVer {
		logger.Info().Int64("from_ver", fromVer).Int64("to_ver", toVer).Msg("object sync: identical from/to obj version: skip copy")
		return nil
	}

	err = lock.Do(ctx, time.Second*2, func() error {
		return s.rc.CopyTo(ctx, p.ID.User(), rclone.File{
			Storage: p.ID.FromStorage(),
			Bucket:  fromBucket,
			Name:    p.Object.Name,
		}, rclone.File{
			Storage: p.ID.ToStorage(),
			Bucket:  toBucket,
			Name:    p.Object.Name,
		}, p.ObjSize)
	})
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			logger.Warn().Msg("object sync: skip object sync: object missing in source")
			return nil
		}
		return err
	}
	logger.Info().Msg("object sync: done")

	if fromVer != 0 {
		return s.versionSvc.UpdateIfGreater(ctx, p.Object, destVersionKey, fromVer)
	}

	return nil
}

func (s *svc) objectDelete(ctx context.Context, p tasks.ObjectSyncPayload) (err error) {
	fromClient, toClient, err := s.getClients(ctx, p.ID.User(), p.ID.FromStorage(), p.ID.ToStorage())
	if err != nil {
		return err
	}
	_, err = fromClient.S3().StatObject(ctx, p.Object.Bucket, p.Object.Name, mclient.StatObjectOptions{})
	if err == nil {
		zerolog.Ctx(ctx).Warn().Msg("skip obj delete: obj still exists in source storage")
		return nil
	}

	_, toBucket := p.ID.FromToBuckets(p.Object.Bucket)
	err = toClient.S3().RemoveObject(ctx, toBucket, p.Object.Name, mclient.RemoveObjectOptions{VersionID: p.Object.Version})
	return
}
