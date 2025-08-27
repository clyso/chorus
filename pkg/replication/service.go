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

package replication

import (
	"context"
	"errors"
	"fmt"

	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/tasks"
)

type Service interface {
	Replicate(ctx context.Context, task tasks.SyncTask) error
}

func New(taskClient *asynq.Client, versionSvc meta.VersionService, policySvc policy.Service) Service {
	return &svc{
		taskClient: taskClient,
		versionSvc: versionSvc,
		policySvc:  policySvc,
	}
}

type svc struct {
	taskClient *asynq.Client
	versionSvc meta.VersionService
	policySvc  policy.Service
}

func (s *svc) Replicate(ctx context.Context, task tasks.SyncTask) error {
	if task == nil {
		zerolog.Ctx(ctx).Info().Msg("replication task is nil")
		return nil
	}
	zerolog.Ctx(ctx).Debug().Msg("creating replication task")
	task.InitDate()
	if task.GetFrom() == "" {
		storage := xctx.GetStorage(ctx)
		zerolog.Ctx(ctx).Warn().Msgf("replication task from storage not set, using storage from context: %s", storage)
		task.SetFrom(storage)
	}

	// 1. find repl rule(-s)
	replTo, err := s.getDestinations(ctx, task)
	if err != nil {
		if errors.Is(err, dom.ErrPolicy) {
			zerolog.Ctx(ctx).Info().Err(err).Msg("skip Replicate: replication is not configured")
			return nil
		}
		return err
	}
	if len(replTo) == 0 {
		// no followers and no error means that zero-downtime switch is in progress
		// update obj version metadata without creating replication tasks
		zerolog.Ctx(ctx).Debug().Msg("zero-downtime switch in progress, skipping replication tasks")
	}
	user, bucket := xctx.GetUser(ctx), xctx.GetBucket(ctx)
	replicationID := entity.ReplicationStatusID{
		User:        user,
		FromStorage: xctx.GetStorage(ctx),
		FromBucket:  bucket,
	}

	switch t := task.(type) {
	case *tasks.BucketCreatePayload:
		for _, to := range replTo {
			t.SetTo(to.Storage, to.Bucket)
			replicationID.ToStorage, replicationID.ToBucket = to.Storage, to.Bucket
			payload, err := tasks.NewReplicationTask(ctx, replicationID, *t)
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil && !errors.Is(err, asynq.ErrDuplicateTask) && !errors.Is(err, asynq.ErrTaskIDConflict) {
				return err
			}
		}
	case *tasks.BucketDeletePayload:
		err = s.versionSvc.DeleteBucketAll(ctx, t.Bucket)
		if err != nil {
			return err
		}

		for _, to := range replTo {
			t.SetTo(to.Storage, to.Bucket)
			replicationID.ToStorage, replicationID.ToBucket = to.Storage, to.Bucket
			payload, err := tasks.NewReplicationTask(ctx, replicationID, *t)
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil {
				return err
			}
			replicationID := entity.ReplicationStatusID{
				User:        user,
				FromStorage: t.FromStorage,
				ToStorage:   t.ToStorage,
				FromBucket:  bucket,
				ToBucket:    t.ToBucket,
			}
			incErr := s.policySvc.IncReplEvents(ctx, replicationID, t.GetDate())
			if incErr != nil {
				zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
			}
		}
	case *tasks.BucketSyncACLPayload:
		_, err = s.versionSvc.IncrementBucketACL(ctx, t.Bucket, meta.ToDest(t.FromStorage, ""))
		if err != nil {
			return err
		}
		for _, to := range replTo {
			t.SetTo(to.Storage, to.Bucket)
			replicationID.ToStorage, replicationID.ToBucket = to.Storage, to.Bucket
			payload, err := tasks.NewReplicationTask(ctx, replicationID, *t)
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil {
				return err
			}
			replicationID := entity.ReplicationStatusID{
				User:        user,
				FromStorage: t.FromStorage,
				ToStorage:   t.ToStorage,
				FromBucket:  bucket,
				ToBucket:    t.ToBucket,
			}
			incErr := s.policySvc.IncReplEvents(ctx, replicationID, t.GetDate())
			if incErr != nil {
				zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
			}
		}
	case *tasks.BucketSyncTagsPayload:
		_, err = s.versionSvc.IncrementBucketTags(ctx, t.Bucket, meta.ToDest(t.FromStorage, ""))
		if err != nil {
			return err
		}
		for _, to := range replTo {
			t.SetTo(to.Storage, to.Bucket)
			replicationID.ToStorage, replicationID.ToBucket = to.Storage, to.Bucket
			payload, err := tasks.NewReplicationTask(ctx, replicationID, *t)
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil {
				return err
			}
			replicationID := entity.ReplicationStatusID{
				User:        user,
				FromStorage: t.FromStorage,
				ToStorage:   t.ToStorage,
				FromBucket:  bucket,
				ToBucket:    t.ToBucket,
			}
			incErr := s.policySvc.IncReplEvents(ctx, replicationID, t.GetDate())
			if incErr != nil {
				zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
			}
		}
	case *tasks.ObjectSyncPayload:
		if t.Deleted {
			err = s.versionSvc.DeleteObjAll(ctx, t.Object)
		} else {
			_, err = s.versionSvc.IncrementObj(ctx, t.Object, meta.ToDest(t.FromStorage, ""))
		}
		if err != nil {
			return err
		}
		for _, to := range replTo {
			t.SetTo(to.Storage, to.Bucket)
			replicationID.ToStorage, replicationID.ToBucket = to.Storage, to.Bucket
			payload, err := tasks.NewReplicationTask(ctx, replicationID, *t)
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil {
				return err
			}
			replicationID := entity.ReplicationStatusID{
				User:        user,
				FromStorage: t.FromStorage,
				ToStorage:   t.ToStorage,
				FromBucket:  bucket,
				ToBucket:    t.ToBucket,
			}
			incErr := s.policySvc.IncReplEvents(ctx, replicationID, t.GetDate())
			if incErr != nil {
				zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
			}
		}
	case *tasks.ObjSyncACLPayload:
		_, err = s.versionSvc.IncrementACL(ctx, t.Object, meta.ToDest(t.FromStorage, ""))
		if err != nil {
			return err
		}
		for _, to := range replTo {
			t.SetTo(to.Storage, to.Bucket)
			replicationID.ToStorage, replicationID.ToBucket = to.Storage, to.Bucket
			payload, err := tasks.NewReplicationTask(ctx, replicationID, *t)
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil {
				return err
			}
			replicationID := entity.ReplicationStatusID{
				User:        user,
				FromStorage: t.FromStorage,
				ToStorage:   t.ToStorage,
				FromBucket:  bucket,
				ToBucket:    t.ToBucket,
			}
			incErr := s.policySvc.IncReplEvents(ctx, replicationID, t.GetDate())
			if incErr != nil {
				zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
			}
		}
	case *tasks.ObjSyncTagsPayload:
		_, err = s.versionSvc.IncrementTags(ctx, t.Object, meta.ToDest(t.FromStorage, ""))
		if err != nil {
			return err
		}
		for _, to := range replTo {
			t.SetTo(to.Storage, to.Bucket)
			replicationID.ToStorage, replicationID.ToBucket = to.Storage, to.Bucket
			payload, err := tasks.NewReplicationTask(ctx, replicationID, *t)
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil {
				return err
			}
			replicationID := entity.ReplicationStatusID{
				User:        user,
				FromStorage: t.FromStorage,
				ToStorage:   t.ToStorage,
				FromBucket:  bucket,
				ToBucket:    t.ToBucket,
			}
			incErr := s.policySvc.IncReplEvents(ctx, replicationID, t.GetDate())
			if incErr != nil {
				zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
			}
		}
	default:
		return fmt.Errorf("%w: unsupported replication task type %T", dom.ErrInternal, task)
	}
	return nil
}

func (s *svc) getDestinations(ctx context.Context, task tasks.SyncTask) ([]entity.ReplicationPolicyDestination, error) {
	replPolicy, err := s.getReplicationPolicy(ctx, task)
	if err != nil {
		return nil, err
	}
	if replPolicy.FromStorage == task.GetFrom() {
		return replPolicy.Destinations, nil
	}
	// Policy source storage is different from task source storage.
	// This only possible if switch is in progress, and we got CompleteMultipartUpload request.
	if _, ok := task.(*tasks.ObjectSyncPayload); !ok || (xctx.GetMethod(ctx) != s3.UndefinedMethod && xctx.GetMethod(ctx) != s3.CompleteMultipartUpload) {
		zerolog.Ctx(ctx).Warn().Msgf("routing policy from %q is different from task %q", replPolicy.FromStorage, task.GetFrom())
	}

	user, bucket := xctx.GetUser(ctx), xctx.GetBucket(ctx)

	// construct previous replication destination based on switch info:
	replicationSwitchInfoID := entity.NewReplicationSwitchInfoID(user, bucket)
	replSwitch, err := s.policySvc.GetInProgressZeroDowntimeSwitchInfo(ctx, replicationSwitchInfoID)
	if err != nil {
		return nil, fmt.Errorf("%w: no in-progress zero-downtime switch for replication task with invalid from storage", err)
	}
	switchReplicationID := replSwitch.ReplicationID()
	if switchReplicationID.FromStorage != task.GetFrom() {
		return nil, fmt.Errorf("%w: replication switch OldMain %s not match with task from storage %s", dom.ErrInternal, switchReplicationID.FromStorage, task.GetFrom())
	}
	return []entity.ReplicationPolicyDestination{
		entity.NewUserReplicationPolicyDestination(switchReplicationID.ToStorage),
	}, nil
}

func (s *svc) getReplicationPolicy(ctx context.Context, task tasks.SyncTask) (*entity.StorageReplicationPolicies, error) {
	user, bucket := xctx.GetUser(ctx), xctx.GetBucket(ctx)
	// bucketPolicy, err := s.policySvc.GetBucketReplicationPolicies(ctx, user, bucket)
	bucketReplicationPolicyID := entity.NewBucketReplicationPolicyID(user, bucket)
	bucketPolicies, err := s.policySvc.GetBucketReplicationPolicies(ctx, bucketReplicationPolicyID)
	if err == nil {
		return bucketPolicies, nil
	}
	if !errors.Is(err, dom.ErrNotFound) {
		return nil, err
	}
	// if zero-downtime switch is in progress return empty replication policy
	// to update obj version metadata and avoid creating replication tasks
	replicationSwitchInfoID := entity.NewReplicationSwitchInfoID(user, bucket)
	switchInfo, err := s.policySvc.GetInProgressZeroDowntimeSwitchInfo(ctx, replicationSwitchInfoID)
	if err == nil {
		// BUGFIX:
		// 1. A is main, B is follower
		// 2. User initiates multipart upload to A
		// 3. User uploads some parts to A
		// 4. User initiates zero-downtime switch from A to B
		//     - reads goes to B or to A if object is not yet replicated
		//     - non-multipart writes now go to B
		//     - replication policy is archived: no copy from A to B
		//
		// 5. All writes except multipart now correctly routed to B
		// 5. We dont create replication tasks for new writes
		// 7. We handle UploadPart correctly and route to A
		//    For UploadPart chorus never creates replication tasks because user can call AbortMultipartUpload
		// 8. User calls CompleteMultipartUpload
		//    -- we are here on this line of code --
		// PROBLEM:
		// - because replication policy is archived we dont create replication tasks
		// - but we need to finish old multipart upload on A and replicate completed object to B
		// SOLUTION:
		//  lookup archived replication policy and create replication task from A to B
		if xctx.GetMethod(ctx) == s3.CompleteMultipartUpload {
			archivedPolicy, err := s.policySvc.GetReplicationPolicyInfo(ctx, switchInfo.ReplID)
			if err != nil {
				// should never happen
				return nil, fmt.Errorf("%w: unable to get archived replication policy during CompleteMultipartUpload with zero-downtime switch in progress: %w", dom.ErrInternal, err)
			}
			if !archivedPolicy.IsArchived {
				// should never happen
				return nil, fmt.Errorf("%w: replication policy is not archived during CompleteMultipartUpload with zero-downtime switch in progress", dom.ErrInternal)
			}
			return &entity.StorageReplicationPolicies{
				FromStorage: task.GetFrom(),
				Destinations: []entity.ReplicationPolicyDestination{
					{
						Storage: switchInfo.ReplID.ToStorage,
						Bucket:  switchInfo.ReplID.ToBucket,
					},
				},
			}, nil
		}

		// no-error means that zero-downtime switch in progress.
		// return replication policy without destinations
		return &entity.StorageReplicationPolicies{
			FromStorage: task.GetFrom(),
		}, nil
	}

	// policy not found. Create new bucket policy from user policy only for CreateBucket method
	if _, ok := task.(*tasks.BucketCreatePayload); !ok {
		return nil, fmt.Errorf("%w: replication policy not configured: %w", dom.ErrPolicy, err)
	}

	userPolicy, err := s.policySvc.GetUserReplicationPolicies(ctx, user)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			return nil, fmt.Errorf("%w: user replication policy not configured: %w", dom.ErrPolicy, err)
		}
		return nil, err
	}
	for _, to := range userPolicy.Destinations {
		replicationID := entity.ReplicationStatusID{
			User:        user,
			FromStorage: userPolicy.FromStorage,
			ToStorage:   to.Storage,
			FromBucket:  bucket,
			ToBucket:    bucket,
		}
		err = s.policySvc.AddBucketReplicationPolicy(ctx, replicationID, nil)
		if err != nil {
			if errors.Is(err, dom.ErrAlreadyExists) {
				continue
			}
			return nil, err
		}
	}

	bucketPolicies, err = s.policySvc.GetBucketReplicationPolicies(ctx, bucketReplicationPolicyID)
	if err != nil {
		return nil, fmt.Errorf("unable to get bucket replication policies: %w", err)
	}

	return bucketPolicies, nil
}
