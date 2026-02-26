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

func NewS3(queueSvc tasks.QueueService, versionSvc meta.VersionService, policySvc policy.Service) Service {
	return &s3Svc{
		queueSvc:   queueSvc,
		versionSvc: versionSvc,
		policySvc:  policySvc,
	}
}

type s3Svc struct {
	queueSvc   tasks.QueueService
	versionSvc meta.VersionService
	policySvc  policy.Service
}

func (s *s3Svc) Replicate(ctx context.Context, routedTo string, task tasks.ReplicationTask) error {
	if task == nil {
		zerolog.Ctx(ctx).Info().Msg("replication task is nil")
		return nil
	}
	zerolog.Ctx(ctx).Debug().Msg("creating replication task")

	// 1. find repl rule(-s)
	replications, skipTasks, err := s.getDestinations(ctx, routedTo)
	if err != nil {
		if errors.Is(err, dom.ErrPolicy) {
			zerolog.Ctx(ctx).Info().Err(err).Msg("skip Replicate: replication is not configured")
			return nil
		}
		return err
	}
	if len(replications) == 0 {
		zerolog.Ctx(ctx).Info().Msg("skip Replicate: replication is not configured")
		return nil
	}
	destination := meta.Destination{
		Storage: routedTo,
		Bucket:  xctx.GetBucket(ctx),
	}
	for _, replID := range replications {
		// 2. increment versions in source storage
		switch t := task.(type) {
		case *tasks.BucketCreatePayload:
		// no version increment needed
		case *tasks.BucketDeletePayload:
			// During zero-downtime switch use Increment instead of DeleteAll
			// to keep version non-empty so old events see From <= To and skip.
			if skipTasks {
				_, err = s.versionSvc.IncrementBucket(ctx, replID, t.Bucket, destination)
			} else {
				err = s.versionSvc.DeleteBucketAll(ctx, replID, t.Bucket)
			}
			if err != nil {
				return err
			}
		case *tasks.BucketSyncACLPayload:
			_, err = s.versionSvc.IncrementBucketACL(ctx, replID, t.Bucket, destination)
			if err != nil {
				return err
			}
		case *tasks.BucketSyncTagsPayload:
			_, err = s.versionSvc.IncrementBucketTags(ctx, replID, t.Bucket, destination)
			if err != nil {
				return err
			}
		case *tasks.ObjectSyncPayload:
			// During zero-downtime switch use Increment even for deletes
			// to keep version non-empty so old events see From <= To and skip.
			if t.Deleted && !skipTasks {
				err = s.versionSvc.DeleteObjAll(ctx, replID, t.Object)
			} else {
				_, err = s.versionSvc.IncrementObj(ctx, replID, t.Object, destination)
			}
			if err != nil {
				return err
			}
		case *tasks.ObjSyncACLPayload:
			_, err = s.versionSvc.IncrementACL(ctx, replID, t.Object, destination)
			if err != nil {
				return err
			}
		case *tasks.ObjSyncTagsPayload:
			_, err = s.versionSvc.IncrementTags(ctx, replID, t.Object, destination)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: unsupported replication task type %T", dom.ErrInternal, task)
		}
		// 3. fan out tasks for each destination
		if !skipTasks {
			task.SetReplicationID(replID)
			err := s.queueSvc.EnqueueTask(ctx, task)
			if err != nil {
				return fmt.Errorf("unable to fan out replication task to %+v: %w", replID, err)
			}
		}
	}

	return nil
}

// getDestinations returns replication targets for the current request.
// skipTasks=true means: increment versions but do not enqueue replication tasks.
// This is used during zero-downtime switch to prevent old pre-switch events from
// overwriting post-switch data.
func (s *s3Svc) getDestinations(ctx context.Context, routedTo string) (replicateTo []entity.UniversalReplicationID, skipTasks bool, err error) {
	destinations := xctx.GetReplications(ctx)
	if len(destinations) != 0 {
		return destinations, false, nil
	}

	inProgressZeroDowntimeSwitch := xctx.GetInProgressZeroDowntime(ctx)
	if inProgressZeroDowntimeSwitch == nil {
		return nil, false, nil
	}
	originalReplID := inProgressZeroDowntimeSwitch.ReplicationID()

	// zero-downtime switch is in progress
	// check if we need to replicate dangling multipart upload to old main storage
	wasRoutedToOldStorage := routedTo == originalReplID.FromStorage()

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
		return []entity.UniversalReplicationID{originalReplID}, false, nil
	}

	// zero-downtime switch in progress.
	// increment version in routed storage and skip creating replication tasks
	return []entity.UniversalReplicationID{originalReplID}, true, nil
}
