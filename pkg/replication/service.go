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
	Replicate(ctx context.Context, routedTo string, task tasks.ReplicationTask) error
}

func New(queueSvc tasks.QueueService, versionSvc meta.VersionService, policySvc policy.Service) Service {
	return &svc{
		queueSvc:   queueSvc,
		versionSvc: versionSvc,
		policySvc:  policySvc,
	}
}

type svc struct {
	queueSvc   tasks.QueueService
	versionSvc meta.VersionService
	policySvc  policy.Service
}

func (s *svc) Replicate(ctx context.Context, routedTo string, task tasks.ReplicationTask) error {
	if task == nil {
		zerolog.Ctx(ctx).Info().Msg("replication task is nil")
		return nil
	}
	zerolog.Ctx(ctx).Debug().Msg("creating replication task")

	// 1. find repl rule(-s)
	incVersions, replications, err := s.getDestinations(ctx, routedTo)
	if err != nil {
		if errors.Is(err, dom.ErrPolicy) {
			zerolog.Ctx(ctx).Info().Err(err).Msg("skip Replicate: replication is not configured")
			return nil
		}
		return err
	}
	if !incVersions {
		if len(replications) == 0 {
			zerolog.Ctx(ctx).Info().Err(err).Msg("skip Replicate: replication is not configured")
			return nil
		} else {
			// if we replicate, we have to increment source versions
			// should never happen: sanity check for logic error
			return fmt.Errorf("%w: replication destinations found but not incrementing source versions", dom.ErrInternal)
		}

	}

	// 2. increment versions in source storage
	switch t := task.(type) {
	case *tasks.BucketCreatePayload:
	// no version increment needed
	case *tasks.BucketDeletePayload:
		err = s.versionSvc.DeleteBucketAll(ctx, t.Bucket)
		if err != nil {
			return err
		}
	case *tasks.BucketSyncACLPayload:
		_, err = s.versionSvc.IncrementBucketACL(ctx, t.Bucket, meta.ToDest(routedTo, ""))
		if err != nil {
			return err
		}
	case *tasks.BucketSyncTagsPayload:
		_, err = s.versionSvc.IncrementBucketTags(ctx, t.Bucket, meta.ToDest(routedTo, ""))
		if err != nil {
			return err
		}
	case *tasks.ObjectSyncPayload:
		if t.Deleted {
			err = s.versionSvc.DeleteObjAll(ctx, t.Object)
		} else {
			_, err = s.versionSvc.IncrementObj(ctx, t.Object, meta.ToDest(routedTo, ""))
		}
		if err != nil {
			return err
		}
	case *tasks.ObjSyncACLPayload:
		_, err = s.versionSvc.IncrementACL(ctx, t.Object, meta.ToDest(routedTo, ""))
		if err != nil {
			return err
		}
	case *tasks.ObjSyncTagsPayload:
		_, err = s.versionSvc.IncrementTags(ctx, t.Object, meta.ToDest(routedTo, ""))
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: unsupported replication task type %T", dom.ErrInternal, task)
	}
	// 3. fan out tasks for each destination
	for _, replID := range replications {
		task.SetReplicationID(replID)
		err := s.queueSvc.EnqueueTask(ctx, task)
		if err != nil {
			return fmt.Errorf("unable to fan out replication task to %+v: %w", replID, err)
		}
	}

	return nil
}

func (s *svc) getDestinations(ctx context.Context, routedTo string) (incSouceVersions bool, replicateTo []entity.ReplicationStatusID, err error) {
	destinations := xctx.GetReplications(ctx)
	if len(destinations) != 0 {
		// normal flow increment source version and replicate to all followers
		return true, destinations, nil
	}
	// no destinations means only 3 things:
	// 1. bucket replication is not configured and user replication is not configured - no need to do anything
	// 2. bucket replication is not configured BUT user replication is here - create bucket replication for CreateBucket request - Remove in next PR
	// 3. bucket replication is archived because zero-downtime switch is in progress
	//    - increment source version only if request routed to main storage
	//    - replicate only for dangling multipart uploads to old main storage

	inProgressZeroDowntimeSwitch := xctx.GetInProgressZeroDowntime(ctx)
	if inProgressZeroDowntimeSwitch == nil {
		// TODO: remove this function in the next PR after implementing new user replication policies
		return s.createBucketReplicationFromUserReplication(ctx, routedTo)
		// do nothing - replication was not configured
		// return false, nil, nil
	}

	// zero-downtime switch is in progress
	// check if we need to replicate dangling multipart upload to old main storage
	wasRoutedToOldStorage := routedTo == inProgressZeroDowntimeSwitch.ReplID.FromStorage
	isCompleteMutlipart := xctx.GetMethod(ctx) == s3.CompleteMultipartUpload
	if wasRoutedToOldStorage && isCompleteMutlipart {
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
		return true, []entity.ReplicationStatusID{
			inProgressZeroDowntimeSwitch.ReplID,
		}, nil
	}

	// no-error means that zero-downtime switch in progress.
	// increment version in routed storage and skip creating replication tasks
	return true, nil, nil
}

func (s *svc) createBucketReplicationFromUserReplication(ctx context.Context, routedTo string) (incSouceVersions bool, replicateTo []entity.ReplicationStatusID, err error) {
	if xctx.GetMethod(ctx) != s3.CreateBucket {
		return false, nil, nil
	}
	user := xctx.GetUser(ctx)
	bucket := xctx.GetBucket(ctx)
	// bucket replication not found. Create new bucket policy from user policy only for CreateBucket method

	userPolicy, err := s.policySvc.GetUserReplicationPolicies(ctx, user)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			// user replication not configured.
			return false, nil, nil
		}
		return false, nil, err
	}
	if userPolicy.FromStorage != routedTo {
		// should never happen
		return false, nil, fmt.Errorf("%w: user replication policy source storage %s does not match routed to storage %s", dom.ErrInternal, userPolicy.FromStorage, routedTo)
	}
	destinations := make([]entity.ReplicationStatusID, 0, len(userPolicy.Destinations))
	for _, to := range userPolicy.Destinations {
		replicationID := entity.ReplicationStatusID{
			User:        user,
			FromStorage: userPolicy.FromStorage,
			ToStorage:   to.Storage,
			FromBucket:  bucket,
			ToBucket:    bucket,
		}
		destinations = append(destinations, replicationID)
		err = s.policySvc.AddBucketReplicationPolicy(ctx, replicationID, nil)
		if err != nil {
			if errors.Is(err, dom.ErrAlreadyExists) {
				continue
			}
			return false, nil, fmt.Errorf("unable to add bucket replication policy from user policy: %w", err)
		}
	}
	return true, destinations, nil
}
