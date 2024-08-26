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
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/hibiken/asynq"
	mclient "github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"
)

var defaultRegion = "us-east-1"

func (s *svc) HandleBucketCreate(ctx context.Context, t *asynq.Task) (err error) {
	var p tasks.BucketCreatePayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("BucketCreatePayload Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Bucket)
	logger := zerolog.Ctx(ctx)

	paused, err := s.policySvc.IsReplicationPolicyPaused(ctx, xctx.GetUser(ctx), p.Bucket, p.FromStorage, p.ToStorage)
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

	srcExists, err := fromClient.S3().BucketExists(ctx, p.Bucket)
	if err != nil {
		return err
	}
	if !srcExists {
		zerolog.Ctx(ctx).Warn().Msg("skip bucket create: bucket not exists in source storage")
		return nil
	}

	// 1. create bucket

	_, err = toClient.AWS().CreateBucketWithContext(ctx, &s3.CreateBucketInput{
		Bucket: &p.Bucket,
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: &p.Location,
		},
	})
	if err != nil && (dom.ErrContains(err, "region", "location")) {
		logger.Warn().Msgf("unable to create bucket: invalid region %q: retry with default region", p.Location)
		_, err = toClient.AWS().CreateBucketWithContext(ctx, &s3.CreateBucketInput{
			Bucket: &p.Bucket,
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

	// 2. copy tags
	err = s.syncBucketTagging(ctx, fromClient, toClient, p.Bucket)
	if err != nil {
		return err
	}
	// 3. copy ACL
	err = s.syncBucketACL(ctx, fromClient, toClient, p.Bucket)
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
	// 7. crete list obj task
	task, err := tasks.NewTask(ctx, tasks.MigrateBucketListObjectsPayload{
		Sync: tasks.Sync{
			FromStorage: p.FromStorage,
			ToStorage:   p.ToStorage,
		},
		Bucket: p.Bucket,
		Prefix: "",
	})
	if err != nil {
		return fmt.Errorf("create bucket: unable to create list obj task: %w", err)
	}
	_, err = s.taskClient.EnqueueContext(ctx, task)
	if err != nil && !errors.Is(err, asynq.ErrDuplicateTask) && !errors.Is(err, asynq.ErrTaskIDConflict) {
		return fmt.Errorf("create bucket: unable to enqueue list obj task: %w", err)
	} else if err != nil {
		logger.Info().RawJSON("enqueue_task_payload", task.Payload()).Msg("cannot enqueue task with duplicate id")
	}
	logger.Info().Msg("create bucket: created migration list obj task")

	logger.Info().Msg("create bucket: done")

	return nil
}

func (s *svc) bucketCopyLC(ctx context.Context, fromClient, toClient s3client.Client, p tasks.BucketCreatePayload) error {
	logger := zerolog.Ctx(ctx)
	if !features.Lifecycle(ctx) {
		logger.Info().Msg("create bucket: lifecycle sync is disabled")
		return nil
	}

	fromLC, err := fromClient.S3().GetBucketLifecycle(ctx, p.Bucket)
	if err != nil {
		if mclient.IsNetworkOrHostDown(err, true) {
			return fmt.Errorf("create bucket: unable to get lifecycle: %w", err)
		}
		logger.Err(err).Msg("create bucket: skip lifecycle copy due to get lifecycle err")
		return nil
	}
	err = toClient.S3().SetBucketLifecycle(ctx, p.Bucket, fromLC)
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

	fromPolicy, err := fromClient.S3().GetBucketPolicy(ctx, p.Bucket)
	if err != nil {
		if mclient.IsNetworkOrHostDown(err, true) {
			return fmt.Errorf("migration bucket copy: unable to get policy: %w", err)
		}
		logger.Err(err).Msg("create bucket: skip policy copy due to get policy err")
		return nil
	}
	err = toClient.S3().SetBucketPolicy(ctx, p.Bucket, fromPolicy)
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

	fromVer, err := fromClient.S3().GetBucketVersioning(ctx, p.Bucket)
	if err != nil {
		if mclient.IsNetworkOrHostDown(err, true) {
			return fmt.Errorf("migration bucket copy: unable to get versioning: %w", err)
		}
		logger.Err(err).Msg("create bucket: skip versioning copy due to get versioning err")
		return nil
	}
	err = toClient.S3().SetBucketVersioning(ctx, p.Bucket, fromVer)
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
		return fmt.Errorf("BucketDeletePayload Unmarshal failed: %v: %w", err, asynq.SkipRetry)
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
		return &dom.ErrRateLimitExceeded{RetryIn: s.conf.PauseRetryInterval}
	}

	fromClient, toClient, err := s.getClients(ctx, p.FromStorage, p.ToStorage)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			return
		}
		verErr := s.policySvc.IncReplEventsDone(ctx, xctx.GetUser(ctx), p.Bucket, p.FromStorage, p.ToStorage, p.CreatedAt)
		if verErr != nil {
			zerolog.Ctx(ctx).Err(verErr).Msg("unable to inc processed events")
		}
	}()

	srcExists, err := fromClient.S3().BucketExists(ctx, p.Bucket)
	if err != nil {
		return err
	}
	if srcExists {
		zerolog.Ctx(ctx).Warn().Msg("skip bucket delete: bucket still exists in source storage")
		return nil
	}

	return toClient.S3().RemoveBucket(ctx, p.Bucket)
}
