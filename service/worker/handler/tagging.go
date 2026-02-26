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
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/tasks"
)

func (s *svc) HandleBucketTags(ctx context.Context, t *asynq.Task) error {
	var p tasks.BucketSyncTagsPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("BucketSyncTagsPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Bucket)
	logger := zerolog.Ctx(ctx)
	// acquire rate limits for source and destination storage before proceeding
	if err := s.rateLimit(ctx, p.ID.FromStorage(), s3.GetBucketTagging); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.FromStorage()).Msg("rate limit error")
		return err
	}
	if err := s.rateLimit(ctx, p.ID.ToStorage(), s3.GetBucketTagging, s3.PutBucketTagging); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.ToStorage()).Msg("rate limit error")
		return err
	}

	_, toBucket := p.ID.FromToBuckets(p.Bucket)

	fromClient, toClient, err := s.getClients(ctx, p.ID.User(), p.ID.FromStorage(), p.ID.ToStorage())
	if err != nil {
		return err
	}

	lock, err := s.bucketLocker.Lock(ctx, entity.NewBucketLockID(p.ID.ToStorage(), toBucket))
	if err != nil {
		return err
	}
	defer lock.Release(ctx)
	err = s.syncBucketTagging(ctx, fromClient, toClient, p.ID, p.Bucket)
	if err != nil {
		return err
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
	logger := zerolog.Ctx(ctx)
	// acquire rate limits for source and destination storage before proceeding
	if err := s.rateLimit(ctx, p.ID.FromStorage(), s3.GetObjectTagging); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.FromStorage()).Msg("rate limit error")
		return err
	}
	if err := s.rateLimit(ctx, p.ID.ToStorage(), s3.GetObjectTagging, s3.PutObjectTagging); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.ToStorage()).Msg("rate limit error")
		return err
	}
	_, toBucket := p.ID.FromToBuckets(p.Object.Bucket)

	fromClient, toClient, err := s.getClients(ctx, p.ID.User(), p.ID.FromStorage(), p.ID.ToStorage())
	if err != nil {
		return err
	}

	objectLockID := entity.NewVersionedObjectLockID(p.ID.ToStorage(), toBucket, p.Object.Name, p.Object.Version)
	lock, err := s.objectLocker.Lock(ctx, objectLockID)
	if err != nil {
		return err
	}
	defer lock.Release(ctx)

	err = s.syncObjectTagging(ctx, fromClient, toClient, p.ID, p.Object)
	if err != nil {
		return err
	}
	return nil
}

func (s *svc) syncBucketTagging(ctx context.Context, fromClient, toClient s3client.Client, replID entity.UniversalReplicationID, bucket string) error {
	if !features.Tagging(ctx) {
		zerolog.Ctx(ctx).Info().Msg("Tagging feature is disabled: skip bucket tags sync")
		return nil
	}
	fromBucket, toBucket := replID.FromToBuckets(bucket)
	versions, err := s.versionSvc.GetBucketTags(ctx, replID, fromBucket)
	if err != nil {
		return err
	}
	fromVer, toVer := versions.From, versions.To
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
		destination := meta.Destination{Storage: replID.ToStorage(), Bucket: toBucketName}
		return s.versionSvc.UpdateBucketTagsIfGreater(ctx, replID, fromBucket, destination, fromVer)
	}
	return nil
}

func (s *svc) syncObjectTagging(ctx context.Context, fromClient, toClient s3client.Client, replID entity.UniversalReplicationID, object dom.Object) error {
	if !features.Tagging(ctx) {
		zerolog.Ctx(ctx).Info().Msg("Tagging feature is disabled: skip object tags sync")
		return nil
	}
	fromBucket, toBucket := replID.FromToBuckets(object.Bucket)
	versions, err := s.versionSvc.GetTags(ctx, replID, object)
	if err != nil {
		return err
	}
	fromVer, toVer := versions.From, versions.To
	if fromVer == toVer && fromVer != 0 {
		zerolog.Ctx(ctx).Info().Msg("skip object Tagging sync: already synced")
		return nil
	}

	fromTags, err := fromClient.S3().GetObjectTagging(ctx, fromBucket, object.Name, mclient.GetObjectTaggingOptions{VersionID: object.Version})

	var mcErr mclient.ErrorResponse
	if errors.As(err, &mcErr) && strings.Contains(mcErr.Code, "NoSuchTagSetError") {
		err = toClient.S3().RemoveObjectTagging(ctx, toBucket, object.Name, mclient.RemoveObjectTaggingOptions{VersionID: object.Version})
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
		err = toClient.S3().PutObjectTagging(ctx, toBucket, object.Name, fromTags, mclient.PutObjectTaggingOptions{VersionID: object.Version})
	} else {
		err = toClient.S3().RemoveObjectTagging(ctx, toBucket, object.Name, mclient.RemoveObjectTaggingOptions{VersionID: object.Version})
	}
	if err != nil {
		if mclient.IsNetworkOrHostDown(err, true) {
			return fmt.Errorf("sync object tags: put tags err: %w", err)
		}
		zerolog.Ctx(ctx).Err(err).Msg("skip object tags sync due to put tags err")
		return nil
	}
	if fromVer != 0 {
		destination := meta.Destination{Storage: replID.ToStorage(), Bucket: toBucket}
		return s.versionSvc.UpdateTagsIfGreater(ctx, replID, object, destination, fromVer)

	}
	return nil
}
