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

package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	"github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/storage"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/util"
)

const (
	CEmptyDirETagPlaceholder = "d"
)

func (s *svc) HandleConsistencyCheck(ctx context.Context, t *asynq.Task) (err error) {
	var payload tasks.ConsistencyCheckPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("unable to unmarshal paylaod: %w", err)
	}

	locationCount := len(payload.Locations)
	storages := make([]string, 0, locationCount)

	if locationCount == 0 {
		return fmt.Errorf("migration location list is empty: %w", asynq.SkipRetry)
	}

	if err := s.storageSvc.StoreConsistencyCheckID(ctx, payload.ID); err != nil {
		return fmt.Errorf("unable to record consistency check id: %w", err)
	}

	for _, location := range payload.Locations {
		listTask := tasks.ConsistencyCheckListPayload{
			MigrateLocation: location,
			StorageCount:    uint8(locationCount),
			ID:              payload.ID,
		}

		if err := s.storageSvc.IncrementConsistencyCheckScheduledCounter(ctx, payload.ID, 1); err != nil {
			return fmt.Errorf("unable to increment consistency check scheduled counter: %w", err)
		}

		if err := s.queueSvc.EnqueueTask(ctx, listTask); err != nil {
			return fmt.Errorf("unable to enqueue consistency check list task: %w", err)
		}

		storages = append(storages, fmt.Sprintf("%s:%s", location.Storage, location.Bucket))
	}

	readinessTask := tasks.ConsistencyCheckReadinessPayload{
		ID: payload.ID,
	}

	if err := s.storageSvc.SetConsistencyCheckStorages(ctx, payload.ID, storages); err != nil {
		return fmt.Errorf("unable to record consistency check storages: %w", err)
	}
	if err := s.storageSvc.SetConsistencyCheckReadiness(ctx, payload.ID, false); err != nil {
		return fmt.Errorf("unable to record consistency check readiness: %w", err)
	}

	if err := s.queueSvc.EnqueueTask(ctx, readinessTask); err != nil {
		return fmt.Errorf("unable to enqueue consistency check readiness task: %w", err)
	}

	return nil
}

func (s *svc) HandleConsistencyCheckList(ctx context.Context, t *asynq.Task) (err error) {
	var payload tasks.ConsistencyCheckListPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("unable to unmarshal paylaod: %w", err)
	}

	ctx = log.WithStorage(ctx, payload.Storage)
	ctx = log.WithBucket(ctx, payload.Bucket)
	logger := zerolog.Ctx(ctx)

	if err = s.limit.StorReq(ctx, payload.Storage); err != nil {
		logger.Debug().Err(err).Msg("unable to get rate limit for storage")
		return fmt.Errorf("unable to get rate limit for storage: %w", err)
	}

	storageClient, err := s.clients.GetByName(ctx, payload.User, payload.Storage)
	if err != nil {
		return fmt.Errorf("unable to get %q s3 client: %w", payload.Storage, err)
	}

	obj := &storage.ConsistencyCheckObject{
		ConsistencyCheckID: payload.ID,
		Storage:            payload.Storage,
		Prefix:             payload.Prefix,
	}
	lastObject, err := s.storageSvc.GetLastListedConsistencyCheckObj(ctx, obj)
	if err != nil {
		return fmt.Errorf("unable to get last listed object: %w", err)
	}

	objectCount := uint64(0)

	listOpts := minio.ListObjectsOptions{
		StartAfter: lastObject,
		Prefix:     payload.Prefix,
	}
	objects := storageClient.S3().ListObjects(ctx, payload.Bucket, listOpts)
	for object := range objects {
		if err := s.checkConsistencyForListedObject(ctx, &payload, &object); err != nil {
			return fmt.Errorf("unable to check consistency for listed object: %w", err)
		}

		if err := s.storageSvc.SetLastListedConsistencyCheckObj(ctx, obj, object.Key); err != nil {
			return fmt.Errorf("unable to set last listed object: %w", err)
		}
	}

	isEmptyDir := lastObject == "" && objectCount == 0 && payload.Prefix != ""

	if isEmptyDir {
		record := &storage.ConsistencyCheckRecord{
			ConsistencyCheckID: payload.ID,
			Storage:            payload.Storage,
			Object:             payload.Prefix,
			StorageCount:       payload.StorageCount,
			ETag:               CEmptyDirETagPlaceholder,
		}
		if err := s.checkConsistencyForObject(ctx, record); err != nil {
			return fmt.Errorf("unable to perform consistency check for object: %w", err)
		}
	} else {
		if err := s.storageSvc.DeleteLastListedConsistencyCheckObj(ctx, obj); err != nil {
			return fmt.Errorf("unable to delete last listed object: %w", err)
		}
	}

	if err := s.storageSvc.IncrementConsistencyCheckCompletedCounter(ctx, payload.ID, 1); err != nil {
		return fmt.Errorf("unable to increment consistency check scheduled counter: %w", err)
	}

	return nil
}

func (s *svc) checkConsistencyForListedObject(ctx context.Context, payload *tasks.ConsistencyCheckListPayload, object *minio.ObjectInfo) error {
	if object.Err != nil {
		return fmt.Errorf("object has error: %w", object.Err)
	}

	isDir := object.Size == 0 && strings.HasSuffix(object.Key, "/")

	if isDir {
		discoveredDirPayload := &tasks.ConsistencyCheckListPayload{
			MigrateLocation: payload.MigrateLocation,
			Prefix:          object.Key,
			ID:              payload.ID,
			StorageCount:    payload.StorageCount,
		}
		if err := s.checkConsistencyForDirectory(ctx, discoveredDirPayload); err != nil {
			return fmt.Errorf("unable to schedule sibdirectory consistency check list task %w", err)
		}
		return nil
	}

	record := &storage.ConsistencyCheckRecord{
		ConsistencyCheckID: payload.ID,
		Storage:            payload.Storage,
		Object:             object.Key,
		StorageCount:       payload.StorageCount,
		ETag:               object.ETag,
	}
	if err := s.checkConsistencyForObject(ctx, record); err != nil {
		return fmt.Errorf("unable to list objects: %w", err)
	}

	return nil
}

func (s *svc) checkConsistencyForDirectory(ctx context.Context, payload *tasks.ConsistencyCheckListPayload) error {
	if err := s.storageSvc.IncrementConsistencyCheckScheduledCounter(ctx, payload.ID, 1); err != nil {
		return fmt.Errorf("unable to increment consistency check scheduled counter: %w", err)
	}

	if err := s.queueSvc.EnqueueTask(ctx, *payload); err != nil {
		return fmt.Errorf("unable to enqueue consistency check list task: %w", err)
	}

	return nil
}

func (s *svc) checkConsistencyForObject(ctx context.Context, record *storage.ConsistencyCheckRecord) error {
	if err := s.storageSvc.AddToConsistencyCheckSet(ctx, record); err != nil {
		return fmt.Errorf("unable to add storage to consistency check set %w", err)
	}

	return nil
}

func (s *svc) HandleConsistencyCheckReadiness(ctx context.Context, t *asynq.Task) (err error) {
	var readinessPayload tasks.ConsistencyCheckReadinessPayload
	if err := json.Unmarshal(t.Payload(), &readinessPayload); err != nil {
		return fmt.Errorf("unable to unmarshal paylaod: %w", err)
	}

	scheduledCounter, err := s.storageSvc.GetConsistencyCheckScheduledCounter(ctx, readinessPayload.ID)
	if err != nil {
		return fmt.Errorf("unable to get scheduled counter: %w", err)
	}

	completedCounter, err := s.storageSvc.GetConsistencyCheckCompletedCounter(ctx, readinessPayload.ID)
	if err != nil {
		return fmt.Errorf("unable to get completed counter: %w", err)
	}

	if scheduledCounter != completedCounter {
		return &dom.ErrRateLimitExceeded{RetryIn: util.DurationJitter(time.Second, time.Second*2)}
	}

	if err := s.storageSvc.SetConsistencyCheckReadiness(ctx, readinessPayload.ID, true); err != nil {
		return fmt.Errorf("unable to set readiness to true: %w", err)
	}

	return nil
}

func (s *svc) HandleConsistencyCheckDelete(ctx context.Context, t *asynq.Task) (err error) {
	var payload tasks.ConsistencyCheckDeletePayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("unable to unmarshal paylaod: %w", err)
	}

	if err := s.storageSvc.DeleteAllConsistencyCheckSets(ctx, payload.ID); err != nil {
		return fmt.Errorf("unable to delete consistency check sets: %w", err)
	}
	if err := s.storageSvc.DeleteAllLastListedConsistencyCheckObj(ctx, payload.ID); err != nil {
		return fmt.Errorf("unable to delete last listed objects: %w", err)
	}
	if err := s.storageSvc.DeleteConsistencyCheckScheduledCounter(ctx, payload.ID); err != nil {
		return fmt.Errorf("unable to delete scheduled counter: %w", err)
	}
	if err := s.storageSvc.DeleteConsistencyCheckCompletedCounter(ctx, payload.ID); err != nil {
		return fmt.Errorf("unable to delete completed counter: %w", err)
	}
	if err := s.storageSvc.DeleteConsistencyCheckID(ctx, payload.ID); err != nil {
		return fmt.Errorf("unable to delete consistency check id: %w", err)
	}
	if err := s.storageSvc.DeleteConsistencyCheckStorages(ctx, payload.ID); err != nil {
		return fmt.Errorf("unable to delete consistency check storages: %w", err)
	}
	if err := s.storageSvc.DeleteConsistencyCheckReadiness(ctx, payload.ID); err != nil {
		return fmt.Errorf("unable to delete consistency check readiness flag: %w", err)
	}

	return nil
}
