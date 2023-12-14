package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/lock"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/hibiken/asynq"
	mclient "github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"
	"strings"
)

func (s *svc) HandleBucketTags(ctx context.Context, t *asynq.Task) error {
	var p tasks.BucketSyncTagsPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("BucketSyncTagsPayload Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Bucket)

	paused, err := s.policySvc.IsReplicationPolicyPaused(ctx, xctx.GetUser(ctx), p.Bucket, p.FromStorage, p.ToStorage)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			zerolog.Ctx(ctx).Err(err).Msg("drop replication task: replication policy not found")
			return nil
		}
		return err
	}
	if paused {
		return &dom.ErrRateLimitExceeded{RetryIn: replicationPauseRetryInterval}
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
	err = s.syncBucketTagging(ctx, fromClient, toClient, p.Bucket)
	if err != nil {
		return err
	}
	incErr := s.policySvc.IncReplEventsDone(ctx, xctx.GetUser(ctx), p.Bucket, p.FromStorage, p.ToStorage, p.CreatedAt)
	if incErr != nil {
		zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc processed events")
	}
	return nil
}

func (s *svc) HandleObjectTags(ctx context.Context, t *asynq.Task) error {
	var p tasks.ObjSyncTagsPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("ObjSyncTagsPayload Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Object.Bucket)
	ctx = log.WithObjName(ctx, p.Object.Name)

	paused, err := s.policySvc.IsReplicationPolicyPaused(ctx, xctx.GetUser(ctx), p.Object.Bucket, p.FromStorage, p.ToStorage)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			zerolog.Ctx(ctx).Err(err).Msg("drop replication task: replication policy not found")
			return nil
		}
		return err
	}
	if paused {
		return &dom.ErrRateLimitExceeded{RetryIn: replicationPauseRetryInterval}
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

	err = s.syncObjectTagging(ctx, fromClient, toClient, p.Object.Bucket, p.Object.Name)
	if err != nil {
		return err
	}
	incErr := s.policySvc.IncReplEventsDone(ctx, xctx.GetUser(ctx), p.Object.Bucket, p.FromStorage, p.ToStorage, p.CreatedAt)
	if incErr != nil {
		zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc processed events")
	}
	return nil
}

func (s *svc) syncBucketTagging(ctx context.Context, fromClient, toClient s3client.Client, bucket string) error {
	if !features.Tagging(ctx) {
		zerolog.Ctx(ctx).Info().Msg("Tagging feature is disabled: skip bucket tags sync")
		return nil
	}
	versions, err := s.versionSvc.GetBucketTags(ctx, bucket)
	if err != nil {
		return err
	}
	fromVer := versions[fromClient.Name()]
	toVer := versions[toClient.Name()]
	if fromVer == toVer && fromVer != 0 {
		zerolog.Ctx(ctx).Info().Msg("skip bucket tagging sync: already synced")
		return nil
	}

	fromTags, err := fromClient.S3().GetBucketTagging(ctx, bucket)
	var mcErr mclient.ErrorResponse
	if errors.As(err, &mcErr) && strings.Contains(mcErr.Code, "NoSuchTagSetError") {
		err = toClient.S3().RemoveBucketTagging(ctx, bucket)
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
		err = toClient.S3().SetBucketTagging(ctx, bucket, fromTags)
	} else {
		err = toClient.S3().RemoveBucketTagging(ctx, bucket)
	}
	if err != nil {
		if mclient.IsNetworkOrHostDown(err, true) {
			return fmt.Errorf("sync bucket tags: put tags err: %w", err)
		}
		zerolog.Ctx(ctx).Err(err).Msg("skip bucket tags sync due to put tags err")
		return nil
	}
	if fromVer != 0 {
		return s.versionSvc.UpdateBucketTagsIfGreater(ctx, bucket, toClient.Name(), fromVer)
	}
	return nil
}

func (s *svc) syncObjectTagging(ctx context.Context, fromClient, toClient s3client.Client, bucket, object string) error {
	if !features.Tagging(ctx) {
		zerolog.Ctx(ctx).Info().Msg("Tagging feature is disabled: skip object tags sync")
		return nil
	}
	versions, err := s.versionSvc.GetTags(ctx, dom.Object{Bucket: bucket, Name: object})
	if err != nil {
		return err
	}
	fromVer := versions[fromClient.Name()]
	toVer := versions[toClient.Name()]
	if fromVer == toVer && fromVer != 0 {
		zerolog.Ctx(ctx).Info().Msg("skip object Tagging sync: already synced")
		return nil
	}

	fromTags, err := fromClient.S3().GetObjectTagging(ctx, bucket, object, mclient.GetObjectTaggingOptions{VersionID: ""}) //todo: versioning
	var mcErr mclient.ErrorResponse
	if errors.As(err, &mcErr) && strings.Contains(mcErr.Code, "NoSuchTagSetError") {
		err = toClient.S3().RemoveObjectTagging(ctx, bucket, object, mclient.RemoveObjectTaggingOptions{VersionID: ""}) //todo: versioning
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
		err = toClient.S3().PutObjectTagging(ctx, bucket, object, fromTags, mclient.PutObjectTaggingOptions{VersionID: ""}) //todo: versioning
	} else {
		err = toClient.S3().RemoveObjectTagging(ctx, bucket, object, mclient.RemoveObjectTaggingOptions{VersionID: ""}) //todo: versioning
	}
	if err != nil {
		if mclient.IsNetworkOrHostDown(err, true) {
			return fmt.Errorf("sync object tags: put tags err: %w", err)
		}
		zerolog.Ctx(ctx).Err(err).Msg("skip object tags sync due to put tags err")
		return nil
	}
	if fromVer != 0 {
		return s.versionSvc.UpdateTagsIfGreater(ctx, dom.Object{Bucket: bucket, Name: object}, toClient.Name(), fromVer)
	}
	return nil
}
