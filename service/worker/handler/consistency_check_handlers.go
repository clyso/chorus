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
	"errors"
	"fmt"
	"iter"
	"strings"

	"github.com/hibiken/asynq"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/rclone"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
)

const (
	CEmptyDirETagPlaceholder = "d"
)

type ConsistencyCheckCtrl struct {
	svc      *ConsistencyCheckSvc
	queueSvc tasks.QueueService
}

func NewConsistencyCheckCtrl(svc *ConsistencyCheckSvc, queueSvc tasks.QueueService) *ConsistencyCheckCtrl {
	return &ConsistencyCheckCtrl{
		svc:      svc,
		queueSvc: queueSvc,
	}
}

func (r *ConsistencyCheckCtrl) HandleConsistencyCheck(ctx context.Context, t *asynq.Task) error {
	var payload tasks.ConsistencyCheckPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("unable to unmarshal paylaod: %w", err)
	}

	for idx := range payload.Locations {
		if err := r.queueSvc.EnqueueTask(ctx, tasks.ConsistencyCheckListObjectsPayload{
			Locations:       payload.Locations,
			User:            payload.User,
			Index:           idx,
			Versioned:       payload.Versioned,
			DoNotCheckEtags: payload.DoNotCheckEtags,
			DoNotCheckSizes: payload.DoNotCheckSizes,
		}); err != nil {
			return fmt.Errorf("unable to enqueue consistency check list task: %w", err)
		}
	}

	return nil
}

func (r *ConsistencyCheckCtrl) HandleConsistencyCheckList(ctx context.Context, t *asynq.Task) error {
	var payload tasks.ConsistencyCheckListObjectsPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("unable to unmarshal paylaod: %w", err)
	}

	locationCount := len(payload.Locations)
	locations := make([]entity.ConsistencyCheckLocation, 0, locationCount)
	for _, payloadLocation := range payload.Locations {
		locations = append(locations, entity.NewConsistencyCheckLocation(payloadLocation.Storage, payloadLocation.Bucket))
	}

	checkID := entity.NewConsistencyCheckID(locations...)

	storage := payload.Locations[payload.Index].Storage
	bucket := payload.Locations[payload.Index].Bucket

	ctx = log.WithStorage(ctx, storage)
	ctx = log.WithBucket(ctx, bucket)

	objectTasks := r.svc.ObjectTasks(ctx, checkID, payload.User,
		entity.NewConsistencyCheckLocation(storage, bucket), payload.Prefix,
		payload.Versioned, payload.DoNotCheckEtags, payload.DoNotCheckSizes)
	for objectTask, err := range objectTasks {
		if err != nil {
			return fmt.Errorf("unable to list object tasks: %w", err)
		}

		if objectTask.Dir {
			if err := r.queueSvc.EnqueueTask(ctx, tasks.ConsistencyCheckListObjectsPayload{
				Locations:       payload.Locations,
				User:            payload.User,
				Index:           payload.Index,
				Prefix:          objectTask.Key,
				Versioned:       payload.Versioned,
				DoNotCheckEtags: payload.DoNotCheckEtags,
				DoNotCheckSizes: payload.DoNotCheckSizes,
			}); err != nil {
				return fmt.Errorf("unable to enqueue consistency check list task: %w", err)
			}
		} else {
			if err := r.queueSvc.EnqueueTask(ctx, tasks.ConsistencyCheckListVersionsPayload{
				Locations:       payload.Locations,
				User:            payload.User,
				Index:           payload.Index,
				Prefix:          objectTask.Key,
				DoNotCheckEtags: payload.DoNotCheckEtags,
				DoNotCheckSizes: payload.DoNotCheckSizes,
			}); err != nil {
				return fmt.Errorf("unable to enqueue consistency check list task: %w", err)
			}
		}
	}

	return nil
}

func (r *ConsistencyCheckCtrl) HandleConsistencyCheckListVersions(ctx context.Context, t *asynq.Task) (err error) {
	var payload tasks.ConsistencyCheckListVersionsPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("unable to unmarshal paylaod: %w", err)
	}

	locationCount := len(payload.Locations)
	locations := make([]entity.ConsistencyCheckLocation, 0, locationCount)
	for _, payloadLocation := range payload.Locations {
		locations = append(locations, entity.NewConsistencyCheckLocation(payloadLocation.Storage, payloadLocation.Bucket))
	}

	checkID := entity.NewConsistencyCheckID(locations...)

	storage := payload.Locations[payload.Index].Storage
	bucket := payload.Locations[payload.Index].Bucket

	if err := r.svc.AccountObjectVersions(ctx, checkID, payload.User,
		entity.NewConsistencyCheckLocation(storage, bucket), payload.Prefix, payload.DoNotCheckEtags, payload.DoNotCheckSizes); err != nil {
		return fmt.Errorf("unable to account object versions: %w", err)
	}

	return nil
}

type ConsistencyCheckSvc struct {
	idStore        *store.ConsistencyCheckIDStore
	settingsStore  *store.ConsistencyCheckSettingsStore
	listStateStore *store.ConsistencyCheckListStateStore
	setStore       *store.ConsistencyCheckSetStore
	copySvc        rclone.CopySvc
	queueSvc       tasks.QueueService
}

func NewConsistencyCheckSvc(idStore *store.ConsistencyCheckIDStore, settingsStore *store.ConsistencyCheckSettingsStore, listStateStore *store.ConsistencyCheckListStateStore, setStore *store.ConsistencyCheckSetStore, copySvc rclone.CopySvc, queueSvc tasks.QueueService) *ConsistencyCheckSvc {
	return &ConsistencyCheckSvc{
		idStore:        idStore,
		settingsStore:  settingsStore,
		listStateStore: listStateStore,
		setStore:       setStore,
		copySvc:        copySvc,
		queueSvc:       queueSvc,
	}
}

func (r *ConsistencyCheckSvc) ShouldCheckVersions(ctx context.Context, user string, locations []entity.ConsistencyCheckLocation) (bool, error) {
	versionedLocations := 0

	for _, location := range locations {
		versioned, err := r.copySvc.IsBucketVersioned(ctx, user, rclone.NewBucket(location.Storage, location.Bucket))
		if err != nil {
			return false, fmt.Errorf("unable to check if bucket is versioned: %w", err)
		}
		if versioned {
			versionedLocations++
		}
	}

	return versionedLocations == len(locations), nil
}

type ObjectTask struct {
	Key string
	Dir bool
}

func (r *ConsistencyCheckSvc) ObjectTasks(ctx context.Context, checkID entity.ConsistencyCheckID,
	user string, location entity.ConsistencyCheckLocation, prefix string, versioned bool, doNotCheckEtags bool, doNotCheckSizes bool) iter.Seq2[ObjectTask, error] {
	locationCount := len(checkID.Locations)
	objectID := entity.NewConsistencyCheckObjectID(checkID, location.Storage, prefix)

	lastObject, err := r.listStateStore.Get(ctx, objectID)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return func(yield func(ObjectTask, error) bool) {
			yield(ObjectTask{}, fmt.Errorf("unable to get last listed object: %w", err))
		}
	}

	objectCount := uint64(0)
	listBucket := rclone.Bucket{
		Storage: location.Storage,
		Bucket:  location.Bucket,
	}

	objects := r.copySvc.BucketObjects(ctx, user, listBucket, rclone.WithAfter(lastObject), rclone.WithPrefix(prefix))
	return func(yield func(ObjectTask, error) bool) {
		for object, err := range objects {
			if err != nil {
				yield(ObjectTask{}, fmt.Errorf("unable to list objects: %w", err))
				return
			}

			isDir := object.Size == 0 && strings.HasSuffix(object.Key, "/")
			if isDir {
				yield(ObjectTask{Key: object.Key, Dir: true}, nil)
				continue
			}

			if versioned {
				yield(ObjectTask{Key: object.Key}, nil)
				continue
			}

			var id entity.ConsistencyCheckSetID
			switch {
			case doNotCheckSizes:
				id = entity.NewNameConsistencyCheckSetID(checkID, object.Key)
			case doNotCheckEtags:
				id = entity.NewSizeConsistencyCheckSetID(checkID, object.Key, object.Size)
			default:
				id = entity.NewEtagConsistencyCheckSetID(checkID, object.Key, object.Size, object.Etag)
			}

			entry := entity.NewConsistencyCheckSetEntry(location)
			if err := r.setStore.Add(ctx, id, entry, uint8(locationCount)); err != nil {
				yield(ObjectTask{}, fmt.Errorf("unable to add storage to consistency check set: %w", err))
				return
			}

			if err := r.listStateStore.Set(ctx, objectID, object.Key); err != nil {
				yield(ObjectTask{}, fmt.Errorf("unable to set last listed object: %w", err))
				return
			}
			objectCount++
		}

		isEmptyDir := lastObject == "" && objectCount == 0 && prefix != ""

		id := entity.NewNameConsistencyCheckSetID(checkID, prefix)
		entry := entity.NewConsistencyCheckSetEntry(location)
		if err := r.setStore.Add(ctx, id, entry, uint8(locationCount)); err != nil {
			yield(ObjectTask{}, fmt.Errorf("unable to add storage to consistency check set: %w", err))
			return
		}

		if !isEmptyDir {
			if _, err := r.listStateStore.Drop(ctx, objectID); err != nil {
				yield(ObjectTask{}, fmt.Errorf("unable to delete last listed object: %w", err))
			}
		}
	}
}

func (r *ConsistencyCheckSvc) AccountObjectVersions(ctx context.Context, checkID entity.ConsistencyCheckID,
	user string, location entity.ConsistencyCheckLocation, prefix string, doNotCheckEtags bool, doNotCheckSizes bool) error {
	locationCount := len(checkID.Locations)
	listBucket := rclone.Bucket{
		Storage: location.Storage,
		Bucket:  location.Bucket,
	}

	versionIdx := uint64(0)
	objects := r.copySvc.BucketObjects(ctx, user, listBucket, rclone.WithPrefix(prefix), rclone.WithVersions())
	for object, err := range objects {
		if err != nil {
			return fmt.Errorf("unable to list object versions: %w", err)
		}

		var id entity.ConsistencyCheckSetID
		switch {
		case doNotCheckSizes:
			id = entity.NewVersionedNameConsistencyCheckSetID(checkID, object.Key, versionIdx)
		case doNotCheckEtags:
			id = entity.NewVersionedSizeConsistencyCheckSetID(checkID, object.Key, versionIdx, object.Size)
		default:
			id = entity.NewVersionedEtagConsistencyCheckSetID(checkID, object.Key, versionIdx, object.Size, object.Etag)
		}

		entry := entity.NewVersionedConsistencyCheckSetEntry(location, object.VersionID)
		if err := r.setStore.Add(ctx, id, entry, uint8(locationCount)); err != nil {
			return fmt.Errorf("unable to add to check set: %w", err)
		}
		versionIdx++
	}

	return nil
}

func (r *ConsistencyCheckSvc) DeleteConsistencyCheck(ctx context.Context, id entity.ConsistencyCheckID) error {
	tokens, err := store.ConsistencyCheckIDToTokensConverter(id)
	if err != nil {
		return fmt.Errorf("unable to make tokens out of consistency check id: %w", err)
	}

	queue := tasks.ConsistencyCheckQueue(id)
	if err := r.queueSvc.Delete(ctx, queue, true); err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("unable to delete queue: %w", err)
	}

	exec := r.idStore.GroupExecutor()
	_ = r.idStore.WithExecutor(exec).RemoveOp(ctx, struct{}{}, id)
	_ = r.settingsStore.WithExecutor(exec).DropOp(ctx, id)
	_ = r.setStore.WithExecutor(exec).DropIDsOp(ctx, tokens...)
	_ = r.listStateStore.WithExecutor(exec).DropIDsOp(ctx, tokens...)

	if err := exec.Exec(ctx); err != nil {
		return fmt.Errorf("unable to clean up consistency check data: %w", err)
	}

	return nil
}

func (r *ConsistencyCheckSvc) RegisterConsistencyCheck(ctx context.Context, id entity.ConsistencyCheckID, settings entity.ConsistencyCheckSettings) error {
	affected, err := r.idStore.Add(ctx, struct{}{}, id)
	if err != nil {
		return fmt.Errorf("unable to check if consistency check id exists: %w", err)
	}
	if affected != 1 {
		return errors.New("consistency check for this set of storage locations already exists")
	}
	if err := r.settingsStore.Set(ctx, id, settings); err != nil {
		return fmt.Errorf("unable to save consistency check settings: %w", err)
	}
	return nil
}

func (r *ConsistencyCheckSvc) GetConsistencyCheckList(ctx context.Context) ([]entity.ConsistencyCheckStatus, error) {
	ids, err := r.idStore.Get(ctx, struct{}{})
	if errors.Is(err, dom.ErrNotFound) {
		return []entity.ConsistencyCheckStatus{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("unable to get consistency check ids: %w", err)
	}

	statuses := make([]entity.ConsistencyCheckStatus, 0, len(ids))
	for _, id := range ids {
		status, err := r.GetConsistencyCheckStatus(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("unable to get consistency check status: %w", err)
		}
		statuses = append(statuses, status)
	}

	return statuses, nil
}

func (r *ConsistencyCheckSvc) GetConsistencyCheckStatus(ctx context.Context, id entity.ConsistencyCheckID) (entity.ConsistencyCheckStatus, error) {
	queue := tasks.ConsistencyCheckQueue(id)

	settings, err := r.settingsStore.Get(ctx, id)
	if err != nil {
		return entity.ConsistencyCheckStatus{}, fmt.Errorf("unable to get consistency check settings: %w", err)
	}

	queueStats, err := r.queueSvc.Stats(ctx, queue)
	if err != nil {
		return entity.ConsistencyCheckStatus{}, fmt.Errorf("unable to get queue stats: %w", err)
	}

	ready := queueStats.Unprocessed == 0

	status := entity.ConsistencyCheckStatus{
		Locations:                id.Locations,
		Queued:                   uint64(queueStats.Unprocessed + queueStats.ProcessedTotal),
		Completed:                uint64(queueStats.ProcessedTotal),
		Ready:                    ready,
		ConsistencyCheckSettings: settings,
	}

	if !ready {
		return status, nil
	}

	tokens, err := store.ConsistencyCheckIDToTokensConverter(id)
	if err != nil {
		return entity.ConsistencyCheckStatus{}, fmt.Errorf("unable to make tokens out of consistency check id: %w", err)
	}

	notCosistent, err := r.setStore.HasIDs(ctx, tokens...)
	if err != nil {
		return entity.ConsistencyCheckStatus{}, fmt.Errorf("unable to check if there are inconsistencies: %w", err)
	}

	status.Consistent = !notCosistent

	return status, nil
}

func (r *ConsistencyCheckSvc) GetConsistencyCheckReportEntries(ctx context.Context, id entity.ConsistencyCheckID, cursor uint64, pageSize uint64) (entity.ConsistencyCheckReportEntryPage, error) {
	tokens, err := store.ConsistencyCheckIDToTokensConverter(id)
	if err != nil {
		return entity.ConsistencyCheckReportEntryPage{}, fmt.Errorf("unable to make tokens out of consistency check id: %w", err)
	}

	pager := store.NewPager(cursor, pageSize)
	idPage, err := r.setStore.GetIDs(ctx, pager, tokens...)
	if err != nil {
		return entity.ConsistencyCheckReportEntryPage{}, fmt.Errorf("unable to get id page: %w", err)
	}

	exec := r.setStore.GroupExecutor()
	execSetStore := r.setStore.WithExecutor(exec)

	ops := make([]store.OperationResult[[]entity.ConsistencyCheckSetEntry], 0, len(idPage.Entries))
	for _, id := range idPage.Entries {
		op := execSetStore.GetOp(ctx, id)
		ops = append(ops, op)
	}

	if err := exec.Exec(ctx); err != nil {
		return entity.ConsistencyCheckReportEntryPage{}, fmt.Errorf("unable to get consistency sets: %w", err)
	}

	entries := make([]entity.ConsistencyCheckReportEntry, 0, len(idPage.Entries))
	for idx, op := range ops {
		storageEntries, _ := op.Get()
		id := idPage.Entries[idx]
		entry := entity.NewConsistencyCheckReportEntry(id.Object, id.VersionIndex, id.Size, id.Etag, storageEntries)
		entries = append(entries, entry)
	}

	return entity.NewConsistencyCheckReportEntryPage(entries, idPage.Next), nil
}
