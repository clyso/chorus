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

	switch t := task.(type) {
	case *tasks.BucketCreatePayload:
		for to, priority := range replTo {
			t.SetTo(to.Parse())
			payload, err := tasks.NewTask(ctx, *t, tasks.WithPriority(priority))
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

		for to, priority := range replTo {
			t.SetTo(to.Parse())
			payload, err := tasks.NewTask(ctx, *t, tasks.WithPriority(priority))
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil {
				return err
			}
			incErr := s.policySvc.IncReplEvents(ctx, user, bucket, t.GetFrom(), t.GetToStorage(), t.GetToBucket(), t.GetDate())
			if incErr != nil {
				zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
			}
		}
	case *tasks.BucketSyncACLPayload:
		_, err = s.versionSvc.IncrementBucketACL(ctx, t.Bucket, meta.ToDest(t.FromStorage, nil))
		if err != nil {
			return err
		}
		for to, priority := range replTo {
			t.SetTo(to.Parse())
			payload, err := tasks.NewTask(ctx, *t, tasks.WithPriority(priority))
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil {
				return err
			}
			incErr := s.policySvc.IncReplEvents(ctx, user, bucket, t.GetFrom(), t.GetToStorage(), t.GetToBucket(), t.GetDate())
			if incErr != nil {
				zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
			}
		}
	case *tasks.BucketSyncTagsPayload:
		_, err = s.versionSvc.IncrementBucketTags(ctx, t.Bucket, meta.ToDest(t.FromStorage, nil))
		if err != nil {
			return err
		}
		for to, priority := range replTo {
			t.SetTo(to.Parse())
			payload, err := tasks.NewTask(ctx, *t, tasks.WithPriority(priority))
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil {
				return err
			}
			incErr := s.policySvc.IncReplEvents(ctx, user, bucket, t.GetFrom(), t.GetToStorage(), t.GetToBucket(), t.GetDate())
			if incErr != nil {
				zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
			}
		}
	case *tasks.ObjectSyncPayload:
		if t.Deleted {
			err = s.versionSvc.DeleteObjAll(ctx, t.Object)
		} else {
			_, err = s.versionSvc.IncrementObj(ctx, t.Object, meta.ToDest(t.FromStorage, nil))
		}
		if err != nil {
			return err
		}
		for to, priority := range replTo {
			t.SetTo(to.Parse())
			payload, err := tasks.NewTask(ctx, *t, tasks.WithPriority(priority))
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil {
				return err
			}
			incErr := s.policySvc.IncReplEvents(ctx, user, bucket, t.GetFrom(), t.GetToStorage(), t.GetToBucket(), t.GetDate())
			if incErr != nil {
				zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
			}
		}
	case *tasks.ObjSyncACLPayload:
		_, err = s.versionSvc.IncrementACL(ctx, t.Object, meta.ToDest(t.FromStorage, nil))
		if err != nil {
			return err
		}
		for to, priority := range replTo {
			t.SetTo(to.Parse())
			payload, err := tasks.NewTask(ctx, *t, tasks.WithPriority(priority))
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil {
				return err
			}
			incErr := s.policySvc.IncReplEvents(ctx, user, bucket, t.GetFrom(), t.GetToStorage(), t.GetToBucket(), t.GetDate())
			if incErr != nil {
				zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
			}
		}
	case *tasks.ObjSyncTagsPayload:
		_, err = s.versionSvc.IncrementTags(ctx, t.Object, meta.ToDest(t.FromStorage, nil))
		if err != nil {
			return err
		}
		for to, priority := range replTo {
			t.SetTo(to.Parse())
			payload, err := tasks.NewTask(ctx, *t, tasks.WithPriority(priority))
			if err != nil {
				return err
			}
			_, err = s.taskClient.EnqueueContext(ctx, payload)
			if err != nil {
				return err
			}
			incErr := s.policySvc.IncReplEvents(ctx, user, bucket, t.GetFrom(), t.GetToStorage(), t.GetToBucket(), t.GetDate())
			if incErr != nil {
				zerolog.Ctx(ctx).Err(incErr).Msg("unable to inc repl event counter")
			}
		}
	default:
		return fmt.Errorf("%w: unsupported replication task type %T", dom.ErrInternal, task)
	}
	return nil
}

func (s *svc) getDestinations(ctx context.Context, task tasks.SyncTask) (map[policy.ReplicationPolicyDest]tasks.Priority, error) {
	replPolicy, err := s.getReplicationPolicy(ctx, task)
	if err != nil {
		return nil, err
	}
	if replPolicy.From == task.GetFrom() {
		return replPolicy.To, nil
	}
	// Policy source storage is different from task source storage.
	// This only possible if switch is in progress, and we got CompleteMultipartUpload request.
	if _, ok := task.(*tasks.ObjectSyncPayload); !ok || (xctx.GetMethod(ctx) != s3.UndefinedMethod && xctx.GetMethod(ctx) != s3.CompleteMultipartUpload) {
		zerolog.Ctx(ctx).Warn().Msgf("routing policy from %q is different from task %q", replPolicy.From, task.GetFrom())
	}

	user, bucket := xctx.GetUser(ctx), xctx.GetBucket(ctx)

	// construct previous replication destination based on switch info:
	replSwitch, err := s.policySvc.GetInProgressZeroDowntimeSwitchInfo(ctx, user, bucket)
	if err != nil {
		return nil, fmt.Errorf("%w: no in-progress zero-downtime switch for replication task with invalid from storage", err)
	}
	switchReplicationID, err := replSwitch.ReplicationID()
	if err != nil {
		return nil, fmt.Errorf("%w: unable to get replication ID from switch: %w", dom.ErrInternal, err)
	}
	if switchReplicationID.From != task.GetFrom() {
		return nil, fmt.Errorf("%w: replication switch OldMain %s not match with task from storage %s", dom.ErrInternal, switchReplicationID.From, task.GetFrom())
	}
	return map[policy.ReplicationPolicyDest]tasks.Priority{
		policy.ReplicationPolicyDest(switchReplicationID.To): tasks.Priority(replSwitch.ReplicationPriority),
	}, nil
}

func (s *svc) getReplicationPolicy(ctx context.Context, task tasks.SyncTask) (policy.ReplicationPolicies, error) {
	user, bucket := xctx.GetUser(ctx), xctx.GetBucket(ctx)
	bucketPolicy, err := s.policySvc.GetBucketReplicationPolicies(ctx, user, bucket)
	if err == nil {
		return bucketPolicy, nil
	}
	if !errors.Is(err, dom.ErrNotFound) {
		return policy.ReplicationPolicies{}, err
	}
	// if zero-downtime switch is in progress return empty replication policy
	// to update obj version metadata and avoid creating replication tasks
	_, err = s.policySvc.GetInProgressZeroDowntimeSwitchInfo(ctx, user, bucket)
	if err == nil {
		return policy.ReplicationPolicies{
			From: task.GetFrom(),
		}, nil
	}

	// policy not found. Create new bucket policy from user policy only for CreateBucket method
	if _, ok := task.(*tasks.BucketCreatePayload); !ok {
		return policy.ReplicationPolicies{}, fmt.Errorf("%w: replication policy not configured: %w", dom.ErrPolicy, err)
	}

	userPolicy, err := s.policySvc.GetUserReplicationPolicies(ctx, user)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			return policy.ReplicationPolicies{}, fmt.Errorf("%w: user replication policy not configured: %w", dom.ErrPolicy, err)
		}
		return policy.ReplicationPolicies{}, err
	}
	for to, priority := range userPolicy.To {
		toStorage, toBucket := to.Parse()
		err = s.policySvc.AddBucketReplicationPolicy(ctx, user, bucket, userPolicy.From, toStorage, toBucket, priority, nil)
		if err != nil {
			if errors.Is(err, dom.ErrAlreadyExists) {
				continue
			}
			return policy.ReplicationPolicies{}, err
		}
	}

	return userPolicy, nil
}
