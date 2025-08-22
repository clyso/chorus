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

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/hibiken/asynq"
	mclient "github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/tasks"
)

func (s *svc) HandleBucketCreate(ctx context.Context, t *asynq.Task) (err error) {
	var p tasks.BucketCreatePayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("BucketCreatePayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Bucket)
	logger := zerolog.Ctx(ctx)

	replicationID := entity.ReplicationStatusID{
		User:        xctx.GetUser(ctx),
		FromStorage: p.FromStorage,
		FromBucket:  p.Bucket,
		ToStorage:   p.ToStorage,
		ToBucket:    p.ToBucket,
	}

	fromClient, toClient, err := s.getClients(ctx, p.FromStorage, p.ToStorage)
	if err != nil {
		return err
	}

	srcExists := true
	versioningConfig, err := fromClient.S3().GetBucketVersioning(ctx, p.Bucket)
	if err != nil && mclient.ToErrorResponse(err).Code != "NoSuchBucket" {
		return fmt.Errorf("unable to get bucket versioning config: %w", err)
	}

	if !srcExists {
		zerolog.Ctx(ctx).Warn().Msg("skip bucket create: bucket not exists in source storage")
		return nil
	}

	shouldListVersions := versioningConfig.Enabled() && features.Versioning(ctx)

	// 1. create bucket
	err = s.createBucketIfNotExists(ctx, toClient, p)
	if err != nil {
		return err
	}

	// 2. copy tags
	err = s.syncBucketTagging(ctx, fromClient, toClient, p.Bucket, p.ToBucket)
	if err != nil {
		return err
	}
	// 3. copy ACL
	err = s.syncBucketACL(ctx, fromClient, toClient, p.Bucket, p.ToBucket)
	if err != nil {
		return err
	}
	// 4. copy lifecycle
	err = s.bucketCopyLC(ctx, fromClient, toClient, p)
	if err != nil {
		return err
	}
	// 5. copy policy
	err = s.bucketCopyPolicy(ctx, fromClient, toClient, p)
	if err != nil {
		return err
	}
	// 6. copy versioning
	err = s.bucketCopyVersioning(ctx, fromClient, toClient, p)
	if err != nil {
		return err
	}
	// 7. create list obj task
	task, err := tasks.NewReplicationTask(ctx, replicationID, tasks.MigrateBucketListObjectsPayload{
		Sync: tasks.Sync{
			FromStorage: p.FromStorage,
			ToStorage:   p.ToStorage,
			ToBucket:    p.ToBucket,
		},
		Bucket:    p.Bucket,
		Prefix:    "",
		Versioned: shouldListVersions,
	})
	if err != nil {
		return fmt.Errorf("create bucket: unable to create list obj task: %w", err)
	}
	_, err = s.taskClient.EnqueueContext(ctx, task)
	if err != nil && !errors.Is(err, asynq.ErrDuplicateTask) && !errors.Is(err, asynq.ErrTaskIDConflict) {
		return fmt.Errorf("create bucket: unable to enqueue list obj task: %w", err)
	} else if err != nil {
		logger.Info().Msg("cannot enqueue task with duplicate id")
	}
	logger.Info().Msg("create bucket: created migration list obj task")

	logger.Info().Msg("create bucket: done")

	return nil
}

func (s *svc) createBucketIfNotExists(ctx context.Context, toClient s3client.Client, p tasks.BucketCreatePayload) error {
	ctx = log.WithBucket(ctx, p.Bucket)
	logger := zerolog.Ctx(ctx)
	toBucketName := p.Bucket
	if p.ToBucket != "" {
		toBucketName = p.ToBucket
	}
	// check if bucket already exists:
	_, err := toClient.AWS().HeadBucketWithContext(ctx, &s3.HeadBucketInput{
		Bucket: &toBucketName,
	})
	if err == nil {
		// already exists
		return nil
	}

	// create bucket
	_, err = toClient.AWS().CreateBucketWithContext(ctx, &s3.CreateBucketInput{
		Bucket: &toBucketName,
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: &p.Location,
		},
	})
	if err != nil && (dom.ErrContains(err, "region", "location")) {
		defaultRegion := toClient.Config().DefaultRegion
		if defaultRegion == "" {
			defaultRegion = s.clients.DefaultRegion()
		}
		logger.Warn().Msgf("unable to create bucket: invalid region %q: retry with default region %q", p.Location, defaultRegion)
		_, err = toClient.AWS().CreateBucketWithContext(ctx, &s3.CreateBucketInput{
			Bucket: &toBucketName,
			CreateBucketConfiguration: &s3.CreateBucketConfiguration{
				LocationConstraint: &defaultRegion,
			},
		})
	}
	if err != nil {
		if !dom.ErrContains(err, "BucketAlreadyExists") {
			return err
		}
		// else BucketAlreadyExists - ok
	}
	return nil
}

func (s *svc) bucketCopyLC(ctx context.Context, fromClient, toClient s3client.Client, p tasks.BucketCreatePayload) error {
	logger := zerolog.Ctx(ctx)
	if !features.Lifecycle(ctx) {
		logger.Info().Msg("create bucket: lifecycle sync is disabled")
		return nil
	}
	fromBucket, toBucket := p.Bucket, p.Bucket
	if p.ToBucket != "" {
		toBucket = p.ToBucket
	}

	fromLC, err := fromClient.S3().GetBucketLifecycle(ctx, fromBucket)
	if err != nil {
		if mclient.IsNetworkOrHostDown(err, true) {
			return fmt.Errorf("create bucket: unable to get lifecycle: %w", err)
		}
		logger.Err(err).Msg("create bucket: skip lifecycle copy due to get lifecycle err")
		return nil
	}
	err = toClient.S3().SetBucketLifecycle(ctx, toBucket, fromLC)
	if err != nil {
		if mclient.IsNetworkOrHostDown(err, true) {
			return fmt.Errorf("create bucket: unable to set lifecycle: %w", err)
		}
		logger.Err(err).Msg("create bucket: skip lifecycle copy due to set lifecycle err")
		return nil
	}
	return nil
}

func (s *svc) bucketCopyPolicy(ctx context.Context, fromClient, toClient s3client.Client, p tasks.BucketCreatePayload) error {
	logger := zerolog.Ctx(ctx)
	if !features.Policy(ctx) {
		logger.Info().Msg("create bucket: policy sync is disabled")
		return nil
	}
	fromBucket, toBucket := p.Bucket, p.Bucket
	if p.ToBucket != "" {
		toBucket = p.ToBucket
	}

	fromPolicy, err := fromClient.S3().GetBucketPolicy(ctx, fromBucket)
	if err != nil {
		if mclient.IsNetworkOrHostDown(err, true) {
			return fmt.Errorf("migration bucket copy: unable to get policy: %w", err)
		}
		logger.Err(err).Msg("create bucket: skip policy copy due to get policy err")
		return nil
	}
	err = toClient.S3().SetBucketPolicy(ctx, toBucket, fromPolicy)
	if err != nil {
		if mclient.IsNetworkOrHostDown(err, true) {
			return fmt.Errorf("migration bucket copy: unable to set policy: %w", err)
		}
		logger.Err(err).Msg("create bucket: skip policy copy due to set policy err")
		return nil
	}
	return nil
}

func (s *svc) bucketCopyVersioning(ctx context.Context, fromClient, toClient s3client.Client, p tasks.BucketCreatePayload) error {
	logger := zerolog.Ctx(ctx)
	if !features.Versioning(ctx) {
		logger.Info().Msg("create bucket: versioning sync is disabled")
		return nil
	}
	fromBucket, toBucket := p.Bucket, p.Bucket
	if p.ToBucket != "" {
		toBucket = p.ToBucket
	}

	fromVer, err := fromClient.S3().GetBucketVersioning(ctx, fromBucket)
	if err != nil {
		if mclient.IsNetworkOrHostDown(err, true) {
			return fmt.Errorf("migration bucket copy: unable to get versioning: %w", err)
		}
		logger.Err(err).Msg("create bucket: skip versioning copy due to get versioning err")
		return nil
	}
	err = toClient.S3().SetBucketVersioning(ctx, toBucket, fromVer)
	if err != nil {
		if mclient.IsNetworkOrHostDown(err, true) {
			return fmt.Errorf("migration bucket copy: unable to set versioning: %w", err)
		}
		logger.Err(err).Msg("create bucket: skip versioning copy due to set versioning err")
		return nil
	}
	return nil
}

func (s *svc) HandleBucketDelete(ctx context.Context, t *asynq.Task) (err error) {
	var p tasks.BucketDeletePayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("BucketDeletePayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Bucket)

	fromClient, toClient, err := s.getClients(ctx, p.FromStorage, p.ToStorage)
	if err != nil {
		return err
	}

	fromBucket, toBucket := p.Bucket, p.Bucket
	if p.ToBucket != "" {
		toBucket = p.ToBucket
	}
	srcExists, err := fromClient.S3().BucketExists(ctx, fromBucket)
	if err != nil {
		return err
	}
	if srcExists {
		zerolog.Ctx(ctx).Warn().Msg("skip bucket delete: bucket still exists in source storage")
		return nil
	}

	return toClient.S3().RemoveBucket(ctx, toBucket)
}
