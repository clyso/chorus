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

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/service/worker/copy"
)

func (s *svc) HandleBucketACL(ctx context.Context, t *asynq.Task) error {
	var p tasks.BucketSyncACLPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("BucketSyncACLPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Bucket)
	logger := zerolog.Ctx(ctx)
	// acquire rate limits for source and destination storage before proceeding
	if err := s.rateLimit(ctx, p.ID.FromStorage(), s3.GetBucketAcl); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.FromStorage()).Msg("rate limit error")
		return err
	}
	if err := s.rateLimit(ctx, p.ID.ToStorage(), s3.GetBucketAcl, s3.PutBucketAcl); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.ToStorage()).Msg("rate limit error")
		return err
	}

	fromClient, toClient, err := s.getClients(ctx, p.ID.User(), p.ID.FromStorage(), p.ID.ToStorage())
	if err != nil {
		return err
	}

	lock, err := s.bucketLocker.Lock(ctx, entity.NewBucketLockID(p.ID.ToStorage(), p.Bucket))
	if err != nil {
		return err
	}
	defer lock.Release(ctx)
	err = s.syncBucketACL(ctx, fromClient, toClient, p.ID, p.Bucket)
	if err != nil {
		return err
	}

	return nil
}

func (s *svc) HandleObjectACL(ctx context.Context, t *asynq.Task) error {
	var p tasks.ObjSyncACLPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("ObjSyncACLPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Object.Bucket)
	ctx = log.WithObjName(ctx, p.Object.Name)
	_, toBucket := p.ID.FromToBuckets(p.Object.Bucket)
	logger := zerolog.Ctx(ctx)
	// acquire rate limits for source and destination storage before proceeding
	if err := s.rateLimit(ctx, p.ID.FromStorage(), s3.GetObjectAcl); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.FromStorage()).Msg("rate limit error")
		return err
	}
	if err := s.rateLimit(ctx, p.ID.ToStorage(), s3.GetObjectAcl, s3.PutObjectAcl); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.ToStorage()).Msg("rate limit error")
		return err
	}

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

	err = s.syncObjectACL(ctx, fromClient, toClient, p.ID, p.Object)
	if err != nil {
		return err
	}

	return nil
}

func (s *svc) syncBucketACL(ctx context.Context, fromClient, toClient s3client.Client, replID entity.UniversalReplicationID, bucket string) error {
	if !features.ACL(ctx) {
		zerolog.Ctx(ctx).Info().Msg("ACL feature is disabled: skip bucket ACL sync")
		return nil
	}
	fromBucket, toBucket := replID.FromToBuckets(bucket)
	versions, err := s.versionSvc.GetBucketACL(ctx, replID, fromBucket)
	if err != nil {
		return err
	}
	fromVer, toVer := versions.From, versions.To
	if fromVer == toVer && fromVer != 0 {
		zerolog.Ctx(ctx).Info().Msg("skip bucket ACL sync: already synced")
		return nil
	}

	fromACL, err := fromClient.AWS().GetBucketAclWithContext(ctx, &aws_s3.GetBucketAclInput{Bucket: &fromBucket})
	if err != nil {
		if s3client.AwsErrRetry(err) {
			return err
		}
		zerolog.Ctx(ctx).Err(err).Msg("skip bucket ACL sync due to get ACL err")
		return nil
	}

	toACL, err := toClient.AWS().GetBucketAclWithContext(ctx, &aws_s3.GetBucketAclInput{Bucket: &toBucket})
	if err != nil {
		if s3client.AwsErrRetry(err) {
			return err
		}
		zerolog.Ctx(ctx).Err(err).Msg("skip bucket ACL sync due to get dest ACL err")
		return nil
	}

	var toOwnerID *string
	if toACL != nil && toACL.Owner != nil {
		toOwnerID = toACL.Owner.ID
	}

	_, err = toClient.AWS().PutBucketAclWithContext(ctx, &aws_s3.PutBucketAclInput{
		AccessControlPolicy: copy.MapOwnersACL(fromACL.Owner, fromACL.Grants, toOwnerID, features.PreserveACLGrants(ctx)),
		Bucket:              &toBucket,
	})
	if err != nil {
		if s3client.AwsErrRetry(err) {
			return err
		}
		zerolog.Ctx(ctx).Err(err).Msg("skip bucket ACL sync due to put ACL err")
		return nil
	}
	if fromVer != 0 {
		dest := meta.Destination{Storage: replID.ToStorage(), Bucket: toBucket}
		return s.versionSvc.UpdateBucketACLIfGreater(ctx, replID, fromBucket, dest, fromVer)
	}
	return nil
}

func (s *svc) syncObjectACL(ctx context.Context, fromClient, toClient s3client.Client, replID entity.UniversalReplicationID, object dom.Object) error {
	if !features.ACL(ctx) {
		zerolog.Ctx(ctx).Info().Msg("ACL feature is disabled: skip object ACL sync")
		return nil
	}
	fromBucket, toBucket := replID.FromToBuckets(object.Bucket)
	versions, err := s.versionSvc.GetACL(ctx, replID, object)
	if err != nil {
		return err
	}
	fromVer, toVer := versions.From, versions.To
	if fromVer == toVer && fromVer != 0 {
		zerolog.Ctx(ctx).Info().Msg("skip object ACL sync: already synced")
		return nil
	}

	err = s.copySvc.CopyACLs(ctx, replID.User(), copy.NewVersionedFile(replID.FromStorage(), fromBucket, object.Name, object.Version), copy.NewVersionedFile(replID.ToStorage(), toBucket, object.Name, object.Version))
	if err != nil {
		return fmt.Errorf("unable to copy object ACLs: %w", err)
	}

	if fromVer != 0 {
		destination := meta.Destination{Storage: replID.ToStorage(), Bucket: toBucket}
		return s.versionSvc.UpdateACLIfGreater(ctx, replID, object, destination, fromVer)
	}
	return nil
}
