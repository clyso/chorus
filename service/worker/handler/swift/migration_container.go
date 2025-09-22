package swift

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
	"github.com/gophercloud/gophercloud/v2/pagination"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"
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
	fromClient, err := s.swiftClients.For(ctx, p.FromStorage, p.FromAccount)
	if err != nil {
		return fmt.Errorf("get swift client: %w", err)
	}
	lastListedKey := tasks.MigrateBucketListObjectsPayload{
		Sync: tasks.Sync{
			FromStorage: p.FromStorage,
			FromAccount: p.FromAccount,
			ToStorage:   p.ToStorage,
			ToAccount:   p.ToAccount,
			ToBucket:    &p.ToContaier,
		},
		Bucket: p.FromContaier,
	}
	ctx = log.WithBucket(ctx, p.FromContaier)
	logger := zerolog.Ctx(ctx)

	// check if replication policy is paused or removed:
	paused, err := s.policySvc.IsReplicationPolicyPaused(ctx, p.FromAccount, p.FromContaier, p.FromStorage, p.ToStorage, &p.ToContaier)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			// policy was removed, clean up data:
			_ = s.storageSvc.DelLastListedObj(ctx, lastListedKey)

			zerolog.Ctx(ctx).Err(err).Msg("drop replication task: replication policy not found")
			return nil
		}
		return err
	}
	if paused {
		return &dom.ErrRateLimitExceeded{RetryIn: s.conf.PauseRetryInterval}
	}

	// check rate limits:
	if err = s.limit.StorReq(ctx, p.FromStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.FromStorage).Msg("rate limit error")
		return err
	}

	// migrate container metadata:
	err = s.handleContainerUpdate(ctx, tasks.SwiftContainerUpdatePayload{
		Sync: tasks.Sync{
			FromStorage: p.FromStorage,
			FromAccount: p.FromAccount,
			ToStorage:   p.ToStorage,
			ToAccount:   p.ToAccount,
			ToBucket:    &p.ToContaier,
			CreatedAt:   time.Now(),
		},
		Bucket: p.FromContaier,
	})
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
	pager := objects.List(fromClient, p.FromContaier, listOpts)
	err = pager.EachPage(ctx, func(ctx context.Context, page pagination.Page) (bool, error) {
		objectList, err := objects.ExtractInfo(page)
		if err != nil {
			return false, err
		}
		for _, object := range objectList {
			// fan out sync object task:
			task, err := tasks.NewTask(ctx, tasks.SwiftObjectMigrationPayload{
				FromStorage:     p.FromStorage,
				FromAccount:     p.FromAccount,
				FromContaier:    p.FromContaier,
				ToStorage:       p.ToStorage,
				ToAccount:       p.ToAccount,
				ToContaier:      p.ToContaier,
				ObjName:         object.Name,
				ObjVersion:      object.VersionID,
				ObjEtag:         object.Hash,
				ObjSize:         object.Bytes,
				ObjLastModified: object.LastModified.Format(time.RFC3339),
			})
			if err != nil {
				return false, fmt.Errorf("migration bucket list obj: unable to create copy obj task: %w", err)
			}
			_, err = s.taskClient.EnqueueContext(ctx, task)
			if err != nil {
				if errors.Is(err, asynq.ErrDuplicateTask) || errors.Is(err, asynq.ErrTaskIDConflict) {
					// ignore duplicate task and not increment listed object count
					logger.Info().RawJSON("enqueue_task_payload", task.Payload()).Msg("cannot enqueue task with duplicate id")
					continue
				}
				return false, fmt.Errorf("migration bucket list obj: unable to enqueue copy obj task: %w", err)
			}
			// increment listed object count:
			err = s.policySvc.IncReplInitObjListed(ctx, p.FromAccount, p.FromContaier, p.FromStorage, p.ToStorage, &p.ToContaier, object.Bytes, time.Now())
			if err != nil {
				return false, fmt.Errorf("migration bucket list obj: unable to inc listed obj meta: %w", err)
			}

			// checkpoint last listed object:
			err = s.storageSvc.SetLastListedObj(ctx, lastListedKey, object.Name)
			if err != nil {
				return false, fmt.Errorf("migration bucket list obj: unable to set last listed object: %w", err)
			}
		}
		return len(objectList) == swiftObjectListLimit, nil
	})

	// cleanup listing checkpoint:
	_ = s.storageSvc.DelLastListedObj(ctx, lastListedKey)
	// update replication progress: - set listing started is ok - we use it in this way because s3 listing is recursive
	err = s.policySvc.ObjListStarted(ctx, p.FromAccount, p.FromContaier, p.FromStorage, p.ToStorage, &p.ToContaier)
	if err != nil {
		logger.Err(err).Msg("migration bucket list obj: unable to set ObjListStarted")
	}

	return nil
}
