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
	"time"

	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
	"github.com/gophercloud/gophercloud/v2/pagination"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/tasks"
)

const (
	swiftObjectListLimit = 1000
)

func (s *svc) HandleSwiftContainerMigration(ctx context.Context, t *asynq.Task) (err error) {
	// setup:
	var p tasks.SwiftContainerMigrationPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("SwiftContainerMigrationPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	fromClient, err := s.clients.AsSwift(ctx, p.ID.FromStorage(), p.ID.User())
	if err != nil {
		return fmt.Errorf("get swift client: %w", err)
	}
	lastListedKey := tasks.MigrateBucketListObjectsPayload{
		Bucket: p.Bucket,
	}
	lastListedKey.SetReplicationID(p.ID)

	// check rate limits:
	if err = s.limit.StorReq(ctx, p.ID.FromStorage()); err != nil {
		zerolog.Ctx(ctx).Debug().Err(err).Str(log.Storage, p.ID.FromStorage()).Msg("rate limit error")
		return err
	}

	// migrate container metadata:
	containerUpdTask := tasks.SwiftContainerUpdatePayload{
		Bucket: p.Bucket,
	}
	containerUpdTask.SetReplicationID(p.ID)
	err = s.ContainerUpdate(ctx, containerUpdTask)
	if err != nil {
		return fmt.Errorf("handle container update: %w", err)
	}

	// list objects in the container:
	// resume from last listed object:
	lastObjectName, err := s.storageSvc.GetLastListedObj(ctx, lastListedKey)
	if err != nil {
		return fmt.Errorf("get last listed object: %w", err)
	}
	listOpts := objects.ListOpts{
		Limit:    swiftObjectListLimit,
		Marker:   lastObjectName,
		Versions: false, // TODO: support versions
	}
	pager := objects.List(fromClient, p.Bucket, listOpts)
	err = pager.EachPage(ctx, func(ctx context.Context, page pagination.Page) (bool, error) {
		objectList, err := objects.ExtractInfo(page)
		if err != nil {
			return false, err
		}
		for _, object := range objectList {
			// fan out sync object task:
			objTask := tasks.SwiftObjectMigrationPayload{
				Bucket:          p.Bucket,
				ObjName:         object.Name,
				ObjVersion:      object.VersionID,
				ObjEtag:         object.Hash,
				ObjSize:         object.Bytes,
				ObjLastModified: object.LastModified.Format(time.RFC3339),
			}
			objTask.SetReplicationID(p.ID)
			err = s.queueSvc.EnqueueTask(ctx, objTask)
			if err != nil {
				return false, fmt.Errorf("migration bucket list obj: unable to enqueue copy obj task: %w", err)
			}

			// checkpoint last listed object:
			err = s.storageSvc.SetLastListedObj(ctx, lastListedKey, object.Name)
			if err != nil {
				return false, fmt.Errorf("migration bucket list obj: unable to set last listed object: %w", err)
			}
		}
		return len(objectList) == swiftObjectListLimit, nil
	})
	if err != nil {
		return err
	}

	// cleanup listing checkpoint:
	_ = s.storageSvc.DelLastListedObj(ctx, lastListedKey)

	return nil
}
