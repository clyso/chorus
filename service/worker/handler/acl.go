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

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/lock"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"
)

func (s *svc) HandleBucketACL(ctx context.Context, t *asynq.Task) error {
	var p tasks.BucketSyncACLPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("BucketSyncACLPayload Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Bucket)

	paused, err := s.policySvc.IsReplicationPolicyPaused(ctx, xctx.GetUser(ctx), p.Bucket, p.FromStorage, p.ToStorage, p.ToBucket)
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

	bucketRelease, _, err := s.locker.Lock(ctx, lock.BucketKey(p.ToStorage, p.Bucket))
	if err != nil {
		return err
	}
	defer bucketRelease()
	err = s.syncBucketACL(ctx, fromClient, toClient, p.Bucket, p.ToBucket)
	if err != nil {
		return err
	}

	incErr := s.policySvc.IncReplEventsDone(ctx, xctx.GetUser(ctx), p.Bucket, p.FromStorage, p.ToStorage, p.ToBucket, p.CreatedAt)
	if incErr != nil {
		zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
	}
	return nil
}

func (s *svc) HandleObjectACL(ctx context.Context, t *asynq.Task) error {
	var p tasks.ObjSyncACLPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("ObjSyncACLPayload Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Object.Bucket)
	ctx = log.WithObjName(ctx, p.Object.Name)

	paused, err := s.policySvc.IsReplicationPolicyPaused(ctx, xctx.GetUser(ctx), p.Object.Bucket, p.FromStorage, p.ToStorage, p.ToBucket)
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

	release, _, err := s.locker.Lock(ctx, lock.ObjKey(p.ToStorage, p.Object))
	if err != nil {
		return err
	}
	defer release()

	err = s.syncObjectACL(ctx, fromClient, toClient, p.Object.Bucket, p.Object.Name, p.ToBucket)
	if err != nil {
		return err
	}

	incErr := s.policySvc.IncReplEventsDone(ctx, xctx.GetUser(ctx), p.Object.Bucket, p.FromStorage, p.ToStorage, p.ToBucket, p.CreatedAt)
	if incErr != nil {
		zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
	}
	return nil
}

func (s *svc) syncBucketACL(ctx context.Context, fromClient, toClient s3client.Client, fromBucket string, toBucket *string) error {
	if !features.ACL(ctx) {
		zerolog.Ctx(ctx).Info().Msg("ACL feature is disabled: skip bucket ACL sync")
		return nil
	}
	versions, err := s.versionSvc.GetBucketACL(ctx, fromBucket)
	if err != nil {
		return err
	}
	destVersionKey := toClient.Name()
	if toBucket != nil {
		destVersionKey += ":" + *toBucket
	}
	fromVer := versions[fromClient.Name()]
	toVer := versions[destVersionKey]
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
	toBucketName := fromBucket
	if toBucket != nil {
		toBucketName = *toBucket
	}
	toACL, err := toClient.AWS().GetBucketAclWithContext(ctx, &aws_s3.GetBucketAclInput{Bucket: &toBucketName})
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
		AccessControlPolicy: mappedOwnersACL(fromACL.Owner, fromACL.Grants, toOwnerID, features.PreserveACLGrants(ctx)),
		Bucket:              &toBucketName,
	})
	if err != nil {
		if s3client.AwsErrRetry(err) {
			return err
		}
		zerolog.Ctx(ctx).Err(err).Msg("skip bucket ACL sync due to put ACL err")
		return nil
	}
	if fromVer != 0 {
		return s.versionSvc.UpdateBucketACLIfGreater(ctx, fromBucket, destVersionKey, fromVer)
	}
	return nil
}

func (s *svc) syncObjectACL(ctx context.Context, fromClient, toClient s3client.Client, fromBucket, object string, toBucket *string) error {
	if !features.ACL(ctx) {
		zerolog.Ctx(ctx).Info().Msg("ACL feature is disabled: skip object ACL sync")
		return nil
	}
	versions, err := s.versionSvc.GetACL(ctx, dom.Object{Bucket: fromBucket, Name: object})
	if err != nil {
		return err
	}
	fromVer := versions[fromClient.Name()]
	destVersionKey := toClient.Name()
	if toBucket != nil {
		destVersionKey += ":" + *toBucket
	}
	toVer := versions[destVersionKey]
	if fromVer == toVer && fromVer != 0 {
		zerolog.Ctx(ctx).Info().Msg("skip object ACL sync: already synced")
		return nil
	}

	fromACL, err := fromClient.AWS().GetObjectAclWithContext(ctx, &aws_s3.GetObjectAclInput{
		Bucket:    &fromBucket,
		Key:       &object,
		VersionId: nil, //todo: versioning
	})
	if err != nil {
		if s3client.AwsErrRetry(err) {
			return err
		}
		zerolog.Ctx(ctx).Err(err).Msg("skip object ACL sync due to get ACL err")
		return nil
	}
	toBucketName := fromBucket
	if toBucket != nil {
		toBucketName = *toBucket
	}
	toACL, err := toClient.AWS().GetObjectAclWithContext(ctx, &aws_s3.GetObjectAclInput{
		Bucket:    &toBucketName,
		Key:       &object,
		VersionId: nil, //todo: versioning
	})
	if err != nil {
		if s3client.AwsErrRetry(err) {
			return err
		}
		zerolog.Ctx(ctx).Err(err).Msg("skip object ACL sync due to get dest ACL err")
		return nil
	}
	var toOwnerID *string
	if toACL != nil && toACL.Owner != nil {
		toOwnerID = toACL.Owner.ID
	}

	_, err = toClient.AWS().PutObjectAclWithContext(ctx, &aws_s3.PutObjectAclInput{
		AccessControlPolicy: mappedOwnersACL(fromACL.Owner, fromACL.Grants, toOwnerID, features.PreserveACLGrants(ctx)),
		Bucket:              &toBucketName,
		Key:                 &object,
		VersionId:           nil, //todo: versioning
	})
	if err != nil {
		if s3client.AwsErrRetry(err) {
			return err
		}
		zerolog.Ctx(ctx).Err(err).Msg("skip object ACL sync due to put ACL err")
		return nil
	}
	if fromVer != 0 {
		return s.versionSvc.UpdateACLIfGreater(ctx, dom.Object{Bucket: fromBucket, Name: object}, destVersionKey, fromVer)
	}
	return nil
}

func srcOwnerToDstOwner(owner, srcBucketOwner, dstBucketOwner *string) *string {
	if owner == nil || *owner != *srcBucketOwner {
		return owner
	}
	return dstBucketOwner
}

func mappedOwnersACL(srcOwner *aws_s3.Owner, srcGrants []*aws_s3.Grant, dstOwner *string, preserveACLGrants bool) *aws_s3.AccessControlPolicy {
	grants := make([]*aws_s3.Grant, len(srcGrants))
	for i, grant := range srcGrants {
		var dstID *string
		if preserveACLGrants {
			dstID = grant.Grantee.ID
		} else {
			dstID = srcOwnerToDstOwner(grant.Grantee.ID, srcOwner.ID, dstOwner)
		}
		grants[i] = &aws_s3.Grant{
			Grantee: &aws_s3.Grantee{
				ID:           dstID,
				EmailAddress: grant.Grantee.EmailAddress,
				Type:         grant.Grantee.Type,
				URI:          grant.Grantee.URI,
				DisplayName:  grant.Grantee.DisplayName,
			},
			Permission: grant.Permission,
		}
	}
	res := &aws_s3.AccessControlPolicy{
		Grants: grants,
	}
	if srcOwner != nil {
		res.Owner = &aws_s3.Owner{
			ID:          srcOwnerToDstOwner(srcOwner.ID, srcOwner.ID, dstOwner),
			DisplayName: srcOwner.DisplayName,
		}
	}

	return res
}
