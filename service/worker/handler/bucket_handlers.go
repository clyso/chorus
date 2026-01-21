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

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/hibiken/asynq"
	mclient "github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/log"
	s3api "github.com/clyso/chorus/pkg/s3"
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
	// acquire rate limits for source and destination storage before proceeding
	if err := s.rateLimit(ctx, p.ID.FromStorage(), s3api.HeadBucket, s3api.GetBucketAcl, s3api.GetBucketVersioning, s3api.GetBucketLifecycle, s3api.GetBucketPolicy); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.FromStorage()).Msg("rate limit error")
		return err
	}
	if err := s.rateLimit(ctx, p.ID.ToStorage(), s3api.HeadBucket, s3api.CreateBucket, s3api.PutBucketAcl, s3api.PutBucketVersioning, s3api.PutBucketLifecycle, s3api.PutBucketPolicy); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.ToStorage()).Msg("rate limit error")
		return err
	}
	fromBucket, _ := p.ID.FromToBuckets(p.Bucket)

	replicationID := p.ID

	fromClient, toClient, err := s.getClients(ctx, p.ID.User(), p.ID.FromStorage(), p.ID.ToStorage())
	if err != nil {
		return err
	}

	versioningConfig, err := fromClient.S3().GetBucketVersioning(ctx, fromBucket)
	if err != nil {
		if mclient.ToErrorResponse(err).Code == mclient.NoSuchBucket {
			zerolog.Ctx(ctx).Warn().Msg("skip bucket create: bucket not exists in source storage")
			return nil
		}
		return fmt.Errorf("unable to get source bucket versioning config: %w", err)
	}
	shouldListVersions := versioningConfig.Enabled() && features.Versioning(ctx)

	// 1. create bucket
	err = s.createBucketIfNotExists(ctx, toClient, p)
	if err != nil {
		return err
	}

	// 2. copy tags
	err = s.syncBucketTagging(ctx, fromClient, toClient, p.ID, p.Bucket)
	if err != nil {
		return err
	}
	// 3. copy ACL
	err = s.syncBucketACL(ctx, fromClient, toClient, p.ID, p.Bucket)
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
	task := tasks.MigrateBucketListObjectsPayload{
		Bucket:    p.Bucket,
		Prefix:    "",
		Versioned: shouldListVersions,
	}
	task.SetReplicationID(replicationID)
	err = s.queueSvc.EnqueueTask(ctx, task)
	if err != nil {
		return fmt.Errorf("create bucket: unable to create list obj task: %w", err)
	}
	logger.Info().Msg("create bucket: created migration list obj task")

	logger.Info().Msg("create bucket: done")

	return nil
}

func (s *svc) createBucketIfNotExists(ctx context.Context, toClient s3client.Client, p tasks.BucketCreatePayload) error {
	ctx = log.WithBucket(ctx, p.Bucket)
	logger := zerolog.Ctx(ctx)
	_, toBucket := p.ID.FromToBuckets(p.Bucket)

	// check if bucket already exists:
	_, err := toClient.AWS().HeadBucketWithContext(ctx, &s3.HeadBucketInput{
		Bucket: &toBucket,
	})
	if err == nil {
		// already exists
		return nil
	}

	// create bucket
	_, err = toClient.AWS().CreateBucketWithContext(ctx, &s3.CreateBucketInput{
		Bucket: &toBucket,
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: &p.Location,
		},
	})
	if err != nil && (dom.ErrContains(err, "region", "location")) {
		toStorageConfig, errConf := s.credsSvc.GetS3Address(p.ID.ToStorage())
		if errConf != nil {
			return fmt.Errorf("unable to get storage config for storage %q: %w", p.ID.ToStorage(), errConf)
		}
		defaultRegion := toStorageConfig.DefaultRegion
		logger.Warn().Msgf("unable to create bucket: invalid region %q: retry with default region %q", p.Location, defaultRegion)
		_, err = toClient.AWS().CreateBucketWithContext(ctx, &s3.CreateBucketInput{
			Bucket: &toBucket,
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
	fromBucket, toBucket := p.ID.FromToBuckets(p.Bucket)

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
	fromBucket, toBucket := p.ID.FromToBuckets(p.Bucket)

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
	fromBucket, toBucket := p.ID.FromToBuckets(p.Bucket)

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
	logger := zerolog.Ctx(ctx)
	// acquire rate limits for source and destination storage before proceeding
	if err := s.rateLimit(ctx, p.ID.FromStorage(), s3api.HeadBucket); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.FromStorage()).Msg("rate limit error")
		return err
	}
	if err := s.rateLimit(ctx, p.ID.ToStorage(), s3api.DeleteBucket); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.ToStorage()).Msg("rate limit error")
		return err
	}

	fromClient, toClient, err := s.getClients(ctx, p.ID.User(), p.ID.FromStorage(), p.ID.ToStorage())
	if err != nil {
		return err
	}

	fromBucket, toBucket := p.ID.FromToBuckets(p.Bucket)
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
