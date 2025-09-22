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
	"github.com/clyso/chorus/pkg/tasks"
)

func NewSwift(taskClient *asynq.Client, versionSvc meta.VersionService, policySvc policy.Service) Service {
	return &s3SVC{
		taskClient: taskClient,
		versionSvc: versionSvc,
		policySvc:  policySvc,
	}
}

type swiftSVC struct {
	taskClient *asynq.Client
	versionSvc meta.VersionService
	policySvc  policy.Service
}

func (s *swiftSVC) Replicate(ctx context.Context, task tasks.SyncTask) error {
	if task == nil {
		zerolog.Ctx(ctx).Info().Msg("replication task is nil")
		return nil
	}
	zerolog.Ctx(ctx).Debug().Msg("creating replication task")
	task.InitDate()
	if task.GetFromStorage() == "" {
		// should not be possible. Indicates error in business logic.
		// return error to catch it in e2e tests
		zerolog.Ctx(ctx).Error().Msgf("swift replication task from storage not set: %+v", task)
		return fmt.Errorf("%w: invalid swift replication task: from storage is not set")
	}

	// 1. find repl rule(-s)
	replTo, err := s.getDestinations(ctx, task)
	if err != nil {
		if errors.Is(err, dom.ErrPolicy) {
			zerolog.Ctx(ctx).Debug().Err(err).Msg("skip Replicate: replication is not configured")
			return nil
		}
		return err
	}
	if len(replTo) == 0 {
		// no followers and no error means that zero-downtime switch is in progress
		// update obj version metadata without creating replication tasks
		zerolog.Ctx(ctx).Debug().Msg("zero-downtime switch in progress, skipping replication tasks")
	}

	// Handle all swift tasks here
	// There is no task-specific logic for SWIFT because chorus does not track obj/bucket versions metadata in Redis (unlike S3).
	// Versions are needed only for zero-downtime migration which is currently not supported for SWIFT.
	// Swift is using multiple buckets to implement obj versioning and multipart upload
	// and allows cross-account and cross-bucket references in API for symlinks and COPY.
	// This and eventual consistency makes zero-downtime implementation very tricky.
	switch t := task.(type) {
	// all case statements are the same but we cannot move it to a function
	// or add all types to a single case because of Go generics.
	// tasks.NewTask is generic func and we will get compile error otherwise
	case *tasks.SwiftAccountUpdatePayload:
		// Fan-out task to replication destinations:
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
	case *tasks.SwiftContainerUpdatePayload:
		// Fan-out task to replication destinations:
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
	case *tasks.SwiftObjectMetaUpdatePayload:
		// Fan-out task to replication destinations:
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
	case *tasks.SwiftObjectUpdatePayload:
		// Fan-out task to replication destinations:
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
	case *tasks.SwiftObjectDeletePayload:
		// Fan-out task to replication destinations:
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
	default:
		return fmt.Errorf("%w: unsupported replication task type %T", dom.ErrInternal, task)
	}
	return nil
}

func (s *swiftSVC) getDestinations(ctx context.Context, task tasks.SyncTask) (map[policy.ReplicationPolicyDest]tasks.Priority, error) {
	replPolicy, err := s.getReplicationPolicy(ctx, task)
	if err != nil {
		return nil, err
	}
	if replPolicy.From == task.GetFromStorage() {
		// TODO:swift check if replPolicy.From equals to storage or to storage:account for swift repl policy
		return replPolicy.To, nil
	}
	return nil, fmt.Errorf("%w: replication policy source %q is different from task %q", replPolicy.From, task.GetFromStorage())
}

func (s *swiftSVC) getReplicationPolicy(ctx context.Context, task tasks.SyncTask) (policy.ReplicationPolicies, error) {
	account, bucket := task.GetFromAccount(), xctx.GetBucket(ctx)
	bucketPolicy, err := s.policySvc.GetBucketReplicationPolicies(ctx, account, bucket)
	if err == nil {
		return bucketPolicy, nil
	}
	if !errors.Is(err, dom.ErrNotFound) {
		return policy.ReplicationPolicies{}, err
	}
	// policy not found. Create new bucket policy from account policy if needed:

	accPolicy, err := s.policySvc.GetUserReplicationPolicies(ctx, account)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			return policy.ReplicationPolicies{}, fmt.Errorf("%w: account replication policy not configured: %w", dom.ErrPolicy, err)
		}
		return policy.ReplicationPolicies{}, err
	}
	if bucket == "" {
		// account method - return account policy
		return accPolicy, nil
	}
	for to, priority := range accPolicy.To {
		toStorage, toAccount, toBucket := to.Parse()
		//TODO:swift support custom acc dest for replication policy in service
		err = s.policySvc.AddBucketReplicationPolicy(ctx, account, bucket, accPolicy.From, toStorage, toBucket, priority, nil)
		if err != nil {
			if errors.Is(err, dom.ErrAlreadyExists) {
				continue
			}
			return policy.ReplicationPolicies{}, err
		}
	}

	return accPolicy, nil
}
