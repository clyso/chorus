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
	"fmt"

	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/containers"
	"github.com/gophercloud/gophercloud/v2/pagination"
	"github.com/hibiken/asynq"

	"github.com/clyso/chorus/pkg/tasks"
)

const (
	swiftContainerListLimit = 1000
)

func (s *svc) HandleSwiftAccountMigration(ctx context.Context, t *asynq.Task) (err error) {
	var p tasks.SwiftAccountMigrationPayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("SwiftAccountMigrationPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}

	fromClient, err := s.clients.AsSwift(ctx, p.ID.FromStorage(), p.ID.User())
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
			task := tasks.SwiftContainerMigrationPayload{
				Bucket: container,
			}
			task.SetReplicationID(p.ID)
			err = s.queueSvc.EnqueueTask(ctx, task)
			if err != nil {
				return false, err
			}
			// checkpoint last container
			if err = s.storageSvc.SetLastListedContainer(ctx, p, container); err != nil {
				return false, fmt.Errorf("error setting last listed container: %w", err)
			}
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
