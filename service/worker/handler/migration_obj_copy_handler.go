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
	"github.com/rs/zerolog"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/rclone"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
)

func (s *svc) HandleMigrationObjCopy(ctx context.Context, t *asynq.Task) (err error) {
	var p tasks.MigrateObjCopyPayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("MigrateObjCopyPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Bucket)
	ctx = log.WithObjName(ctx, p.Obj.Name)
	logger := zerolog.Ctx(ctx)

	replicationID := entity.ReplicationStatusID{
		User:        xctx.GetUser(ctx),
		FromStorage: p.FromStorage,
		FromBucket:  p.Bucket,
		ToStorage:   p.ToStorage,
		ToBucket:    p.ToBucket,
	}
	paused, err := s.policySvc.IsReplicationPolicyPaused(ctx, replicationID)
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
	if err = s.limit.StorReq(ctx, p.ToStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ToStorage).Msg("rate limit error")
		return err
	}

	domObj := dom.Object{
		Bucket:  p.Bucket,
		Name:    p.Obj.Name,
		Version: p.Obj.VersionID,
	}
	defer func() {
		// complete obj migration meta if not err:
		if err != nil {
			return
		}
		metaErr := s.policySvc.IncReplInitObjDone(ctx, replicationID, uint64(p.Obj.Size), p.CreatedAt)
		if metaErr != nil {
			logger.Err(metaErr).Msg("migration obj copy: unable to inc obj done meta")
		}
	}()

	objectLockID := entity.NewObjectLockID(p.ToStorage, p.ToBucket, p.Obj.Name, p.Obj.VersionID)
	lock, err := s.objectLocker.Lock(ctx, objectLockID)
	if err != nil {
		return err
	}
	defer lock.Release(ctx)
	objMeta, err := s.versionSvc.GetObj(ctx, domObj)
	if err != nil {
		return fmt.Errorf("migration obj copy: unable to get obj meta: %w", err)
	}
	destVersionKey := meta.ToDest(p.ToStorage, p.ToBucket)
	fromVer, toVer := objMeta[meta.ToDest(p.FromStorage, "")], objMeta[destVersionKey]

	if fromVer != 0 && fromVer <= toVer {
		logger.Info().Int64("from_ver", fromVer).Int64("to_ver", toVer).Msg("migration obj copy: identical from/to obj version: skip copy")
		return nil
	}
	fromBucket, toBucket := p.Bucket, p.Bucket
	if p.ToBucket != "" {
		toBucket = p.ToBucket
	}
	// 1. sync obj meta and content
	err = lock.Do(ctx, time.Second*2, func() error {
		return s.rc.CopyTo(ctx, rclone.File{
			Storage: p.FromStorage,
			Bucket:  fromBucket,
			Name:    p.Obj.Name,
		}, rclone.File{
			Storage: p.ToStorage,
			Bucket:  toBucket,
			Name:    p.Obj.Name,
		}, p.Obj.Size)
	})
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			logger.Warn().Msg("migration obj copy: skip object sync: object missing in source")
			return nil
		}
		return fmt.Errorf("migration obj copy: unable to copy with rclone: %w", err)
	}

	fromClient, toClient, err := s.getClients(ctx, p.FromStorage, p.ToStorage)
	if err != nil {
		return fmt.Errorf("migration obj copy: unable to get %q s3 client: %w: %w", p.FromStorage, err, asynq.SkipRetry)
	}

	// 2. sync obj ACL
	err = s.syncObjectACL(ctx, fromClient, toClient, p.Bucket, p.Obj.Name, p.ToBucket)
	if err != nil {
		return err
	}

	// 3. sync obj tags
	err = s.syncObjectTagging(ctx, fromClient, toClient, p.Bucket, p.Obj.Name, p.ToBucket)
	if err != nil {
		return err
	}

	if fromVer != 0 {
		err = s.versionSvc.UpdateIfGreater(ctx, domObj, destVersionKey, fromVer)
		if err != nil {
			return fmt.Errorf("migration obj copy: unable to update obj meta: %w", err)
		}
	}
	logger.Info().Msg("migration obj copy: done")

	return nil
}

func (s *svc) HandleMigrationObjCopyHead(ctx context.Context, task *asynq.Task) error {
	var payload tasks.MigrateObjCopyPayload
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		return fmt.Errorf("MigrateObjCopyPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}

	ctx = log.WithBucket(ctx, payload.Bucket)
	ctx = log.WithObjName(ctx, payload.Obj.Name)
	logger := zerolog.Ctx(ctx)

	replicationID := entity.ReplicationStatusID{
		User:        xctx.GetUser(ctx),
		FromStorage: payload.FromStorage,
		FromBucket:  payload.Bucket,
		ToStorage:   payload.ToStorage,
		ToBucket:    payload.ToBucket,
	}
	paused, err := s.policySvc.IsReplicationPolicyPaused(ctx, replicationID)
	if errors.Is(err, dom.ErrNotFound) {
		logger.Err(err).Msg("drop replication task: replication policy not found")
		return nil
	}
	if err != nil {
		return fmt.Errorf("unable to get replication policy state: %w", err)
	}
	if paused {
		return &dom.ErrRateLimitExceeded{RetryIn: s.conf.PauseRetryInterval}
	}

	if err := s.limit.StorReq(ctx, payload.FromStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, payload.FromStorage).Msg("rate limit error")
		return err
	}
	if err := s.limit.StorReq(ctx, payload.ToStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, payload.ToStorage).Msg("rate limit error")
		return err
	}

	objectLockID := entity.NewObjectLockID(payload.ToStorage, payload.ToBucket, payload.Obj.Name, payload.Obj.VersionID)
	lock, err := s.objectLocker.Lock(ctx, objectLockID)
	if err != nil {
		return fmt.Errorf("unable to make object lock: %w", err)
	}
	defer lock.Release(ctx)

	if err := s.process(ctx, logger, payload, lock); err != nil {
		return fmt.Errorf("unable to copy domain object: %w", err)
	}

	if err := s.policySvc.IncReplInitObjDone(ctx, replicationID, uint64(payload.Obj.Size), payload.CreatedAt); err != nil {
		logger.Err(err).Msg("migration obj copy: unable to inc obj done meta")
	}

	return nil
}

func (s *svc) process(ctx context.Context, logger *zerolog.Logger, payload tasks.MigrateObjCopyPayload, lock *store.Lock) error {
	domObj := dom.Object{
		Bucket:  payload.Bucket,
		Name:    payload.Obj.Name,
		Version: payload.Obj.VersionID,
	}

	objMeta, err := s.versionSvc.GetObj(ctx, domObj)
	if err != nil {
		return fmt.Errorf("migration obj copy: unable to get obj meta: %w", err)
	}
	destVersionKey := meta.ToDest(payload.ToStorage, payload.ToBucket)
	fromVer, toVer := objMeta[meta.ToDest(payload.FromStorage, "")], objMeta[destVersionKey]

	if fromVer != 0 && fromVer <= toVer {
		logger.Info().Int64("from_ver", fromVer).Int64("to_ver", toVer).Msg("migration obj copy: identical from/to obj version: skip copy")
		return nil
	}
	fromBucket, toBucket := payload.Bucket, payload.Bucket
	if payload.ToBucket != "" {
		toBucket = payload.ToBucket
	}
	// 1. sync obj meta and content
	err = lock.Do(ctx, time.Second*2, func() error {
		return s.rc.CopyTo(ctx, rclone.File{
			Storage: payload.FromStorage,
			Bucket:  fromBucket,
			Name:    payload.Obj.Name,
		}, rclone.File{
			Storage: payload.ToStorage,
			Bucket:  toBucket,
			Name:    payload.Obj.Name,
		}, payload.Obj.Size)
	})

	if errors.Is(err, dom.ErrNotFound) {
		logger.Warn().Msg("migration obj copy: skip object sync: object missing in source")
		return nil
	}
	if err != nil {
		return fmt.Errorf("migration obj copy: unable to copy with rclone: %w", err)
	}

	fromClient, toClient, err := s.getClients(ctx, payload.FromStorage, payload.ToStorage)
	if err != nil {
		return fmt.Errorf("migration obj copy: unable to get %q s3 client: %w: %w", payload.FromStorage, err, asynq.SkipRetry)
	}

	// 2. sync obj ACL
	err = s.syncObjectACL(ctx, fromClient, toClient, payload.Bucket, payload.Obj.Name, payload.ToBucket)
	if err != nil {
		return err
	}

	// 3. sync obj tags
	err = s.syncObjectTagging(ctx, fromClient, toClient, payload.Bucket, payload.Obj.Name, payload.ToBucket)
	if err != nil {
		return err
	}

	if fromVer != 0 {
		err = s.versionSvc.UpdateIfGreater(ctx, domObj, destVersionKey, fromVer)
		if err != nil {
			return fmt.Errorf("migration obj copy: unable to update obj meta: %w", err)
		}
	}
	logger.Info().Msg("migration obj copy: done")

	return nil
}
