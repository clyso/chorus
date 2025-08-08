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

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/tasks"
)

func (s *svc) HandleBucketTags(ctx context.Context, t *asynq.Task) error {
	var p tasks.BucketSyncTagsPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("BucketSyncTagsPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Bucket)

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

	fromClient, toClient, err := s.getClients(ctx, p.FromStorage, p.ToStorage)
	if err != nil {
		return err
	}

	lock, err := s.bucketLocker.Lock(ctx, entity.NewBucketLockID(p.ToStorage, p.ToBucket))
	if err != nil {
		return err
	}
	defer lock.Release(ctx)
	err = s.syncBucketTagging(ctx, fromClient, toClient, p.Bucket, p.ToBucket)
	if err != nil {
		return err
	}
	incErr := s.policySvc.IncReplEventsDone(ctx, replicationID, p.CreatedAt)
	if incErr != nil {
		zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc processed events")
	}
	return nil
}

func (s *svc) HandleObjectTags(ctx context.Context, t *asynq.Task) error {
	var p tasks.ObjSyncTagsPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("ObjSyncTagsPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Object.Bucket)
	ctx = log.WithObjName(ctx, p.Object.Name)

	replicationID := entity.ReplicationStatusID{
		User:        xctx.GetUser(ctx),
		FromStorage: p.FromStorage,
		FromBucket:  p.Object.Bucket,
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

	fromClient, toClient, err := s.getClients(ctx, p.FromStorage, p.ToStorage)
	if err != nil {
		return err
	}

	objectLockID := entity.NewObjectLockID(p.ToStorage, p.ToBucket, p.Object.Name, p.Object.Version)
	lock, err := s.objectLocker.Lock(ctx, objectLockID)
	if err != nil {
		return err
	}
	defer lock.Release(ctx)

	err = s.syncObjectTagging(ctx, fromClient, toClient, p.Object.Bucket, p.Object.Name, p.ToBucket)
	if err != nil {
		return err
	}
	incErr := s.policySvc.IncReplEventsDone(ctx, replicationID, p.CreatedAt)
	if incErr != nil {
		zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc processed events")
	}
	return nil
}

func (s *svc) syncBucketTagging(ctx context.Context, fromClient, toClient s3client.Client, fromBucket string, toBucket string) error {
	if !features.Tagging(ctx) {
		zerolog.Ctx(ctx).Info().Msg("Tagging feature is disabled: skip bucket tags sync")
		return nil
	}
	versions, err := s.versionSvc.GetBucketTags(ctx, fromBucket)
	if err != nil {
		return err
	}
	fromVer := versions[meta.ToDest(fromClient.Name(), "")]
	destVersionKey := meta.ToDest(toClient.Name(), toBucket)
	toVer := versions[destVersionKey]
	if fromVer == toVer && fromVer != 0 {
		zerolog.Ctx(ctx).Info().Msg("skip bucket tagging sync: already synced")
		return nil
	}

	fromTags, err := fromClient.S3().GetBucketTagging(ctx, fromBucket)

	// destination bucket name is equal to source bucke name unless toBucket param is set
	toBucketName := fromBucket
	if toBucket != "" {
		toBucketName = toBucket
	}
	var mcErr mclient.ErrorResponse
	if errors.As(err, &mcErr) && strings.Contains(mcErr.Code, "NoSuchTagSetError") {
		err = toClient.S3().RemoveBucketTagging(ctx, toBucketName)
		if err != nil {
			if mclient.IsNetworkOrHostDown(err, true) {
				return fmt.Errorf("sync bucket tags: remove tags err: %w", err)
			}
			zerolog.Ctx(ctx).Err(err).Msg("skip bucket tags sync due to remove tags err")
			return nil
		}
	}
	if err != nil {
		if mclient.IsNetworkOrHostDown(err, true) {
			return fmt.Errorf("sync bucket tags: get tags err: %w", err)
		}
		zerolog.Ctx(ctx).Err(err).Msg("skip bucket tags sync due to get tags err")
		return nil
	}

	if fromTags != nil && len(fromTags.ToMap()) != 0 {
		err = toClient.S3().SetBucketTagging(ctx, toBucketName, fromTags)
	} else {
		err = toClient.S3().RemoveBucketTagging(ctx, toBucketName)
	}
	if err != nil {
		if mclient.IsNetworkOrHostDown(err, true) {
			return fmt.Errorf("sync bucket tags: put tags err: %w", err)
		}
		zerolog.Ctx(ctx).Err(err).Msg("skip bucket tags sync due to put tags err")
		return nil
	}
	if fromVer != 0 {
		return s.versionSvc.UpdateBucketTagsIfGreater(ctx, fromBucket, destVersionKey, fromVer)
	}
	return nil
}

func (s *svc) syncObjectTagging(ctx context.Context, fromClient, toClient s3client.Client, fromBucket, object string, toBucket string) error {
	if !features.Tagging(ctx) {
		zerolog.Ctx(ctx).Info().Msg("Tagging feature is disabled: skip object tags sync")
		return nil
	}
	versions, err := s.versionSvc.GetTags(ctx, dom.Object{Bucket: fromBucket, Name: object})
	if err != nil {
		return err
	}
	fromVer := versions[meta.ToDest(fromClient.Name(), "")]
	destVersionKey := meta.ToDest(toClient.Name(), toBucket)
	toVer := versions[destVersionKey]
	if fromVer == toVer && fromVer != 0 {
		zerolog.Ctx(ctx).Info().Msg("skip object Tagging sync: already synced")
		return nil
	}

	fromTags, err := fromClient.S3().GetObjectTagging(ctx, fromBucket, object, mclient.GetObjectTaggingOptions{VersionID: ""}) //todo: versioning

	// destination bucket name is equal to source bucke name unless toBucket param is set
	toBucketName := fromBucket
	if toBucket != "" {
		toBucketName = toBucket
	}
	var mcErr mclient.ErrorResponse
	if errors.As(err, &mcErr) && strings.Contains(mcErr.Code, "NoSuchTagSetError") {
		err = toClient.S3().RemoveObjectTagging(ctx, toBucketName, object, mclient.RemoveObjectTaggingOptions{VersionID: ""}) //todo: versioning
		if err != nil {
			if mclient.IsNetworkOrHostDown(err, true) {
				return fmt.Errorf("sync object tags: remove tags err: %w", err)
			}
			zerolog.Ctx(ctx).Err(err).Msg("skip object tags sync due to remove tags err")
			return nil
		}
	}
	if err != nil {
		if mclient.IsNetworkOrHostDown(err, true) {
			return fmt.Errorf("sync object tags: get tags err: %w", err)
		}
		zerolog.Ctx(ctx).Err(err).Msg("skip object tags sync due to get tags err")
		return nil
	}

	if fromTags != nil && len(fromTags.ToMap()) != 0 {
		err = toClient.S3().PutObjectTagging(ctx, toBucketName, object, fromTags, mclient.PutObjectTaggingOptions{VersionID: ""}) //todo: versioning
	} else {
		err = toClient.S3().RemoveObjectTagging(ctx, toBucketName, object, mclient.RemoveObjectTaggingOptions{VersionID: ""}) //todo: versioning
	}
	if err != nil {
		if mclient.IsNetworkOrHostDown(err, true) {
			return fmt.Errorf("sync object tags: put tags err: %w", err)
		}
		zerolog.Ctx(ctx).Err(err).Msg("skip object tags sync due to put tags err")
		return nil
	}
	if fromVer != 0 {
		return s.versionSvc.UpdateTagsIfGreater(ctx, dom.Object{Bucket: fromBucket, Name: object}, destVersionKey, fromVer)
	}
	return nil
}
