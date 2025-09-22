// Copyright 2025 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package swift

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/containers"
	"github.com/gophercloud/gophercloud/v2/pagination"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"
)

const (
	swiftContainerListLimit = 1000
)

func (s *svc) HandleSwiftAccountMigration(ctx context.Context, t *asynq.Task) (err error) {
	var p tasks.SwiftAccountMigrationPayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("SwiftAccountMigrationPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	logger := zerolog.Ctx(ctx)

	// validation and setup:
	replProlicy, err := s.policySvc.GetUserReplicationPolicies(ctx, p.FromAccount)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			// policy was removed, delete checkpoint:
			_ = s.storageSvc.DelLastListedContainer(ctx, p)

			zerolog.Ctx(ctx).Err(err).Msg("drop replication task: replication policy not found")
			return nil
		}
		return err
	}
	if replProlicy.From != p.FromStorage {
		zerolog.Ctx(ctx).Err(err).Msg("drop replication task: replication policy does not match storage")
		return nil
	}
	if _, ok := replProlicy.To[policy.ReplicationPolicyDest(p.ToStorage)]; !ok {
		zerolog.Ctx(ctx).Err(err).Msg("drop replication task: replication policy does not match storage")
		return nil
	}

	// check rate limit:
	if err = s.limit.StorReq(ctx, p.FromStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.FromStorage).Msg("rate limit error")
		return err
	}
	fromClient, err := s.swiftClients.For(ctx, p.FromStorage, p.FromAccount)
	if err != nil {
		return err
	}

	// resume from last container
	lastContainerName, err := s.storageSvc.GetLastListedContainer(ctx, p)
	if err != nil {
		return err
	}

	// iterate through account containers
	pager := containers.List(fromClient, containers.ListOpts{
		Limit:  swiftContainerListLimit,
		Marker: lastContainerName,
	})
	err = pager.EachPage(ctx, func(ctx context.Context, page pagination.Page) (bool, error) {
		containerList, err := containers.ExtractNames(page)
		if err != nil {
			return false, err
		}
		for _, container := range containerList {
			// start migration for each container:
			err = s.policySvc.AddBucketReplicationPolicy(ctx, p.FromAccount, container, p.FromStorage, p.ToStorage, nil, replProlicy.To[policy.ReplicationPolicyDest(p.ToStorage)], nil)
			if err != nil && !errors.Is(err, dom.ErrAlreadyExists) {
				if errors.Is(err, dom.ErrInvalidArg) {
					// non recoverable error, skip this container
					return false, fmt.Errorf("%w: error adding bucket replication policy: %w", asynq.SkipRetry, err)
				}
				return false, fmt.Errorf("error adding bucket replication policy: %w", err)
			}
			task, err := tasks.NewTask(ctx, tasks.SwiftContainerMigrationPayload{
				FromStorage:  p.FromStorage,
				FromAccount:  p.FromAccount,
				FromContaier: container,
				ToStorage:    p.ToStorage,
				ToAccount:    p.ToAccount,
				ToContaier:   container,
			})
			if err != nil {
				return false, err
			}
			_, err = s.taskClient.EnqueueContext(ctx, task)
			if err != nil && !errors.Is(err, asynq.ErrDuplicateTask) && !errors.Is(err, asynq.ErrTaskIDConflict) {
				return false, err
			}
			// checkpoint last container
			if err = s.storageSvc.SetLastListedContainer(ctx, p, container); err != nil {
				return false, fmt.Errorf("error setting last listed container: %w", err)
			}
			//TODO: maintain account replication policy status:
			// - increment listed/migrated containers
		}

		// Continue to the next page
		return len(containerList) == swiftContainerListLimit, nil
	})

	if err != nil {
		return fmt.Errorf("error listing containers: %w", err)
	}

	// cleanup last listed container
	if err = s.storageSvc.DelLastListedContainer(ctx, p); err != nil {
		return fmt.Errorf("error deleting last listed container: %w", err)
	}
	return err
}
