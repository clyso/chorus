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
	"strings"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
)

type DiffCtrl struct {
	diffSvc  *DiffSvc
	queueSvc tasks.QueueService
}

func NewDiffCtrl(svc *DiffSvc, queueSvc tasks.QueueService) *DiffCtrl {
	return &DiffCtrl{
		diffSvc:  svc,
		queueSvc: queueSvc,
	}
}

func (r *DiffCtrl) HandleDiff(ctx context.Context, t *asynq.Task) error {
	var payload tasks.DiffPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("unable to unmarshal payload: %w", err)
	}

	for idx := range payload.Locations {
		if err := r.queueSvc.EnqueueTask(ctx, tasks.DiffListObjectsPayload{
			Locations:   payload.Locations,
			User:        payload.User,
			Index:       idx,
			Versioned:   payload.Versioned,
			IgnoreEtags: payload.IgnoreEtags,
			IgnoreSizes: payload.IgnoreSizes,
		}); err != nil {
			return fmt.Errorf("unable to enqueue diff list task: %w", err)
		}
	}

	return nil
}

func (r *DiffCtrl) HandleDiffList(ctx context.Context, t *asynq.Task) error {
	var payload tasks.DiffListObjectsPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("unable to unmarshal payload: %w", err)
	}

	diffID := r.diffIDFromMigrateLocations(payload.Locations)
	settings := entity.NewDiffSettings(payload.User, payload.Versioned, payload.IgnoreSizes, payload.IgnoreEtags)
	location := entity.NewDiffLocation(payload.Locations[payload.Index].Storage, payload.Locations[payload.Index].Bucket)

	ctx = log.WithStorage(ctx, location.Storage)
	ctx = log.WithBucket(ctx, location.Bucket)

	payloadMaker := func(prefix string) tasks.DiffListVersionsPayload {
		return tasks.DiffListVersionsPayload{
			Locations:   payload.Locations,
			User:        payload.User,
			Index:       payload.Index,
			Prefix:      prefix,
			IgnoreEtags: payload.IgnoreEtags,
			IgnoreSizes: payload.IgnoreSizes,
		}
	}

	if err := r.diffSvc.ProduceObjectTasks(ctx, diffID, settings, location, payload.Prefix, payloadMaker); err != nil {
		return fmt.Errorf("unable to produce object tasks: %w", err)
	}

	return nil
}

func (r *DiffCtrl) HandleDiffListVersions(ctx context.Context, t *asynq.Task) error {
	var payload tasks.DiffListVersionsPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("unable to unmarshal payload: %w", err)
	}

	diffID := r.diffIDFromMigrateLocations(payload.Locations)
	storage := payload.Locations[payload.Index].Storage
	bucket := payload.Locations[payload.Index].Bucket

	if err := r.diffSvc.AccountObjectVersions(ctx, diffID, payload.User,
		entity.NewDiffLocation(storage, bucket), payload.Prefix, payload.IgnoreEtags, payload.IgnoreSizes); err != nil {
		return fmt.Errorf("unable to account object versions: %w", err)
	}

	return nil
}

func (r *DiffCtrl) HandleDiffCollectObjects(ctx context.Context, t *asynq.Task) error {
	var payload tasks.DiffFixCollectObjectsPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("unable to unmarshal payload: %w", err)
	}

	diffID := r.diffIDFromMigrateLocations(payload.Locations)
	sourceStorage := payload.Locations[payload.SourceIndex].Storage
	for idx := range payload.Locations {
		if idx == payload.SourceIndex {
			continue
		}

		destinationStorage := payload.Locations[idx].Storage
		diffFixID := entity.NewDiffFixID(diffID, destinationStorage)
		if err := r.diffSvc.CollectObjectsToFix(ctx, diffFixID, sourceStorage, payload.Versioned); err != nil {
			return fmt.Errorf("unable to collect objects to fix: %w", err)
		}

		if err := r.queueSvc.EnqueueTask(ctx, tasks.DiffFixRemoveObjectsPayload{
			Locations:   payload.Locations,
			SourceIndex: payload.SourceIndex,
			RemoveIndex: idx,
			StorageType: payload.StorageType,
			Versioned:   payload.Versioned,
			User:        payload.User,
		}); err != nil {
			return fmt.Errorf("unable to enqueue remove diff objects task: %w", err)
		}
	}

	return nil
}

func (r *DiffCtrl) HandleDiffRemoveObjects(ctx context.Context, t *asynq.Task) error {
	var payload tasks.DiffFixRemoveObjectsPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("unable to unmarshal payload: %w", err)
	}

	diffID := r.diffIDFromMigrateLocations(payload.Locations)
	toLocation := entity.NewDiffLocation(payload.Locations[payload.RemoveIndex].Storage, payload.Locations[payload.RemoveIndex].Bucket)
	diffFixID := entity.NewDiffFixID(diffID, toLocation.Storage)
	if err := r.diffSvc.RemoveDiffObjects(ctx, diffFixID, toLocation, payload.User, payload.Versioned); err != nil {
		return fmt.Errorf("unable to remove diff objects: %w", err)
	}

	if err := r.queueSvc.EnqueueTask(ctx, tasks.DiffFixEnsureObjectsRemovedPayload(payload)); err != nil {
		return fmt.Errorf("unable to enqueue remove diff objects task: %w", err)
	}

	return nil
}

func (r *DiffCtrl) HandleDiffEnsureObjectsRemoved(ctx context.Context, t *asynq.Task) error {
	var payload tasks.DiffFixEnsureObjectsRemovedPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("unable to unmarshal payload: %w", err)
	}

	diffID := r.diffIDFromMigrateLocations(payload.Locations)
	fromLocation := entity.NewDiffLocation(payload.Locations[payload.SourceIndex].Storage, payload.Locations[payload.SourceIndex].Bucket)
	toLocation := entity.NewDiffLocation(payload.Locations[payload.RemoveIndex].Storage, payload.Locations[payload.RemoveIndex].Bucket)
	bucketReplID := entity.NewBucketReplicationPolicy(payload.User, fromLocation.Storage, fromLocation.Bucket, toLocation.Storage, toLocation.Bucket)
	universalReplID := entity.UniversalFromBucketReplication(bucketReplID)
	diffFixID := entity.NewDiffFixID(diffID, toLocation.Storage)

	payloadMaker := func(object string, isDir bool) (any, error) {
		switch payload.StorageType {
		case dom.Swift:
			if payload.Versioned {
				return nil, errors.New("swift versioned objects are not supported")
			}
			task := tasks.DiffFixSwiftCopyPayload{
				Locations: payload.Locations,
				SwiftObjectMigrationPayload: tasks.SwiftObjectMigrationPayload{
					Bucket:  fromLocation.Bucket,
					ObjName: object,
				},
			}
			task.SetReplicationID(universalReplID)
			return task, nil
		case dom.S3:
			if payload.Versioned && !isDir {
				task := tasks.DiffFixS3ListVersionsPayload{
					Locations: payload.Locations,
					ListObjectVersionsPayload: tasks.ListObjectVersionsPayload{
						Bucket: fromLocation.Bucket,
						Prefix: object,
					},
				}
				task.SetReplicationID(universalReplID)
				return task, nil
			}
			task := tasks.DiffFixS3CopyPayload{
				Locations: payload.Locations,
				MigrateObjCopyPayload: tasks.MigrateObjCopyPayload{
					Bucket: fromLocation.Bucket,
					Obj: tasks.ObjPayload{
						Name: object,
					},
				},
			}
			task.SetReplicationID(universalReplID)
			return task, nil
		default:
			return nil, fmt.Errorf("unknown storage type %s", payload.StorageType)
		}
	}

	if err := r.diffSvc.EnsureObjectsDeleted(ctx, diffFixID, toLocation, payload.User, payload.Versioned, payloadMaker); err != nil {
		return fmt.Errorf("unable to ensure that objects are deleted: %w", err)
	}

	return nil
}

func (r *DiffCtrl) diffIDFromMigrateLocations(migrateLocations []tasks.MigrateLocation) entity.DiffID {
	locationCount := len(migrateLocations)
	locations := make([]entity.DiffLocation, 0, locationCount)
	for _, payloadLocation := range migrateLocations {
		locations = append(locations, entity.NewDiffLocation(payloadLocation.Storage, payloadLocation.Bucket))
	}

	return entity.NewDiffID(locations...)
}

type DiffSvc struct {
	idStore           *store.DiffIDStore
	settingsStore     *store.DiffSettingsStore
	listStateStore    *store.DiffListStateStore
	setStore          *store.DiffSetStore
	fixCopySetStore   *store.DiffFixCopySetStore
	fixRemoveSetStore *store.DiffFixRemoveSetStore
	clients           objstore.Clients
	queueSvc          tasks.QueueService
}

func NewDiffSvc(redisClient redis.Cmdable, clients objstore.Clients, queueSvc tasks.QueueService) *DiffSvc {
	return &DiffSvc{
		idStore:           store.NewDiffIDStore(redisClient),
		settingsStore:     store.NewDiffSettingsStore(redisClient),
		listStateStore:    store.NewDiffListStateStore(redisClient),
		setStore:          store.NewDiffSetStore(redisClient),
		fixCopySetStore:   store.NewDiffFixCopySetStore(redisClient),
		fixRemoveSetStore: store.NewDiffFixRemoveSetStore(redisClient),
		clients:           clients,
		queueSvc:          queueSvc,
	}
}

func (r *DiffSvc) ShouldCheckVersions(ctx context.Context, user string, locations []entity.DiffLocation) (bool, error) {
	versionedLocations := 0

	for _, location := range locations {
		client, err := r.clients.AsCommon(ctx, location.Storage, user)
		if err != nil {
			return false, fmt.Errorf("unable to obtain client: %w", err)
		}
		versioned, err := client.IsBucketVersioned(ctx, location.Bucket)
		if err != nil {
			return false, fmt.Errorf("unable to check if bucket is versioned: %w", err)
		}
		if versioned {
			versionedLocations++
		}
	}

	return versionedLocations == len(locations), nil
}

func (r *DiffSvc) ProduceObjectTasks(ctx context.Context, diffID entity.DiffID, settings entity.DiffSettings, location entity.DiffLocation, prefix string, makePayload func(prefix string) tasks.DiffListVersionsPayload) error {
	locationCount := len(diffID.Locations)
	objectID := entity.NewDiffObjectID(diffID, location.Storage, prefix)

	lastObject, err := r.listStateStore.Get(ctx, objectID)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("unable to get last listed object: %w", err)
	}

	client, err := r.clients.AsCommon(ctx, location.Storage, settings.User)
	if err != nil {
		return fmt.Errorf("unable to obtain client: %w", err)
	}

	objects := client.ListObjects(ctx, location.Bucket, objstore.WithPrefix(prefix), objstore.WithAfter(lastObject))
	for object, err := range objects {
		if err != nil {
			if errors.Is(err, objstore.ErrBucketDoesntExist) {
				return nil
			}

			return fmt.Errorf("unable to list objects: %w", err)
		}

		isDir := object.Size == 0 && object.VersionID == "" && strings.HasSuffix(object.Key, "/")

		if settings.Versioned && !isDir {
			listVersionsTask := makePayload(object.Key)
			if err := r.queueSvc.EnqueueTask(ctx, listVersionsTask); err != nil {
				return fmt.Errorf("unable to enqueue diff list task: %w", err)
			}

			continue
		}

		var id entity.DiffSetID
		switch {
		case settings.IgnoreSizes:
			id = entity.NewNameDiffSetID(diffID, object.Key)
		case settings.IgnoreEtags:
			id = entity.NewSizeDiffSetID(diffID, object.Key, object.Size)
		default:
			id = entity.NewEtagDiffSetID(diffID, object.Key, object.Size, object.Etag)
		}

		entry := entity.NewDiffSetEntry(location, isDir)
		if err := r.setStore.Add(ctx, id, entry, uint8(locationCount)); err != nil {
			return fmt.Errorf("unable to add storage to diff set: %w", err)
		}

		if err := r.listStateStore.Set(ctx, objectID, object.Key); err != nil {
			return fmt.Errorf("unable to set last listed object: %w", err)
		}
	}

	if _, err := r.listStateStore.Drop(ctx, objectID); err != nil {
		return fmt.Errorf("unable to delete last listed object: %w", err)
	}

	return nil
}

func (r *DiffSvc) AccountObjectVersions(ctx context.Context, diffID entity.DiffID,
	user string, location entity.DiffLocation, prefix string, ignoreEtags bool, ignoreSizes bool) error {
	locationCount := len(diffID.Locations)

	client, err := r.clients.AsCommon(ctx, location.Storage, user)
	if err != nil {
		return fmt.Errorf("unable to obtain client: %w", err)
	}

	versionIdx := uint64(0)
	objects := client.ListObjects(ctx, location.Bucket, objstore.WithPrefix(prefix), objstore.WithVersions())
	for object, err := range objects {
		if err != nil {
			return fmt.Errorf("unable to list object versions: %w", err)
		}

		var id entity.DiffSetID
		switch {
		case ignoreSizes:
			id = entity.NewVersionedNameDiffSetID(diffID, object.Key, versionIdx)
		case ignoreEtags:
			id = entity.NewVersionedSizeDiffSetID(diffID, object.Key, versionIdx, object.Size)
		default:
			id = entity.NewVersionedEtagDiffSetID(diffID, object.Key, versionIdx, object.Size, object.Etag)
		}

		entry := entity.NewVersionedDiffSetEntry(location, object.VersionID)
		if err := r.setStore.Add(ctx, id, entry, uint8(locationCount)); err != nil {
			return fmt.Errorf("unable to add to check set: %w", err)
		}
		versionIdx++
	}

	return nil
}

func (r *DiffSvc) DeleteDiff(ctx context.Context, id entity.DiffID) error {
	tokens, err := store.DiffIDToTokensConverter(id)
	if err != nil {
		return fmt.Errorf("unable to make tokens out of diff id: %w", err)
	}

	diffQueue := tasks.DiffQueue(id)
	if err := r.queueSvc.Delete(ctx, diffQueue, true); err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("unable to delete %s queue: %w", diffQueue, err)
	}

	fixQueue := tasks.DiffFixQueue(id)
	if err := r.queueSvc.Delete(ctx, fixQueue, true); err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("unable to delete %s queue: %w", fixQueue, err)
	}

	exec := r.idStore.GroupExecutor()
	_ = r.idStore.WithExecutor(exec).RemoveOp(ctx, struct{}{}, id)
	_ = r.settingsStore.WithExecutor(exec).DropOp(ctx, id)
	_ = r.setStore.WithExecutor(exec).DropIDsOp(ctx, tokens...)
	_ = r.listStateStore.WithExecutor(exec).DropIDsOp(ctx, tokens...)
	_ = r.fixCopySetStore.WithExecutor(exec).DropIDsOp(ctx, tokens...)
	_ = r.fixRemoveSetStore.WithExecutor(exec).DropIDsOp(ctx, tokens...)

	if err := exec.Exec(ctx); err != nil {
		return fmt.Errorf("unable to clean up diff data: %w", err)
	}

	return nil
}

func (r *DiffSvc) RegisterDiff(ctx context.Context, id entity.DiffID, settings entity.DiffSettings) error {
	affected, err := r.idStore.Add(ctx, struct{}{}, id)
	if err != nil {
		return fmt.Errorf("unable to check if diff id exists: %w", err)
	}
	if affected != 1 {
		return errors.New("diff for this set of storage locations already exists")
	}
	if err := r.settingsStore.Set(ctx, id, settings); err != nil {
		return fmt.Errorf("unable to save diff settings: %w", err)
	}
	return nil
}

func (r *DiffSvc) GetDiffList(ctx context.Context) ([]entity.DiffStatus, error) {
	ids, err := r.idStore.Get(ctx, struct{}{})
	if errors.Is(err, dom.ErrNotFound) {
		return []entity.DiffStatus{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("unable to get diff ids: %w", err)
	}

	statuses := make([]entity.DiffStatus, 0, len(ids))
	for _, id := range ids {
		status, err := r.GetDiffStatus(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("unable to get diff status: %w", err)
		}
		statuses = append(statuses, status)
	}

	return statuses, nil
}

func (r *DiffSvc) GetDiffStatus(ctx context.Context, id entity.DiffID) (entity.DiffStatus, error) {
	diffQueue := tasks.DiffQueue(id)

	settings, err := r.settingsStore.Get(ctx, id)
	if err != nil {
		return entity.DiffStatus{}, fmt.Errorf("unable to get diff settings: %w", err)
	}

	diffQueueStats, err := r.queueSvc.Stats(ctx, diffQueue)
	if err != nil {
		return entity.DiffStatus{}, fmt.Errorf("unable to get diff queue stats: %w", err)
	}

	ready := diffQueueStats.Unprocessed == 0

	status := entity.DiffStatus{
		Locations: id.Locations,
		Check: entity.DiffCheckStatus{
			Queue: entity.DiffQueueStatus{
				Queued:    uint64(diffQueueStats.Unprocessed + diffQueueStats.ProcessedTotal),
				Completed: uint64(diffQueueStats.ProcessedTotal),
				Ready:     ready,
			},
			Settings: settings,
		},
	}

	if !ready {
		return status, nil
	}

	tokens, err := store.DiffIDToTokensConverter(id)
	if err != nil {
		return entity.DiffStatus{}, fmt.Errorf("unable to make tokens out of diff id: %w", err)
	}

	notConsistent, err := r.setStore.HasIDs(ctx, tokens...)
	if err != nil {
		return entity.DiffStatus{}, fmt.Errorf("unable to check if there are inconsistencies: %w", err)
	}

	status.Check.Consistent = !notConsistent

	fixQueue := tasks.DiffFixQueue(id)
	fixQueueStats, err := r.queueSvc.Stats(ctx, fixQueue)
	if errors.Is(err, dom.ErrNotFound) {
		// if fix queue is not found, then returning collected diff stats
		return status, nil
	}
	if err != nil {
		return entity.DiffStatus{}, fmt.Errorf("unable to get diff fix queue stats: %w", err)
	}

	status.FixQueue = &entity.DiffQueueStatus{
		Queued:    uint64(fixQueueStats.Unprocessed + fixQueueStats.ProcessedTotal),
		Completed: uint64(fixQueueStats.ProcessedTotal),
		Ready:     fixQueueStats.Unprocessed == 0,
	}

	return status, nil
}

func (r *DiffSvc) GetDiffReportEntries(ctx context.Context, id entity.DiffID, cursor uint64, pageSize uint64) (entity.DiffReportEntryPage, error) {
	tokens, err := store.DiffIDToTokensConverter(id)
	if err != nil {
		return entity.DiffReportEntryPage{}, fmt.Errorf("unable to make tokens out of diff id: %w", err)
	}

	pager := store.NewPager(cursor, pageSize)
	idPage, err := r.setStore.GetIDs(ctx, pager, tokens...)
	if err != nil {
		return entity.DiffReportEntryPage{}, fmt.Errorf("unable to get id page: %w", err)
	}

	exec := r.setStore.GroupExecutor()
	execSetStore := r.setStore.WithExecutor(exec)

	ops := make([]store.OperationResult[[]entity.DiffSetEntry], 0, len(idPage.Entries))
	for _, id := range idPage.Entries {
		op := execSetStore.GetOp(ctx, id)
		ops = append(ops, op)
	}

	if err := exec.Exec(ctx); err != nil {
		return entity.DiffReportEntryPage{}, fmt.Errorf("unable to get diff sets: %w", err)
	}

	entries := make([]entity.DiffReportEntry, 0, len(idPage.Entries))
	for idx, op := range ops {
		storageEntries, _ := op.Get()
		id := idPage.Entries[idx]
		entry := entity.NewDiffReportEntry(id.Object, id.VersionIndex, id.Size, id.Etag, storageEntries)
		entries = append(entries, entry)
	}

	return entity.NewDiffReportEntryPage(entries, idPage.Next), nil
}

func (r *DiffSvc) StartDiff(ctx context.Context, id entity.DiffID, settings entity.DiffSettings) error {
	if err := r.RegisterDiff(ctx, id, settings); err != nil {
		return fmt.Errorf("unable to register diff: %w", err)
	}

	taskLocations := make([]tasks.MigrateLocation, 0, len(id.Locations))
	for _, location := range id.Locations {
		taskLocations = append(taskLocations, tasks.MigrateLocation{
			Storage: location.Storage,
			Bucket:  location.Bucket,
		})
	}

	diffTask := tasks.DiffPayload{
		Locations:   taskLocations,
		User:        settings.User,
		Versioned:   settings.Versioned,
		IgnoreEtags: settings.IgnoreEtags,
		IgnoreSizes: settings.IgnoreSizes,
	}
	if err := r.queueSvc.EnqueueTask(ctx, diffTask); err != nil {
		return fmt.Errorf("unable to enqueue diff check task: %w", err)
	}

	return nil
}

func (r *DiffSvc) RestartDiff(ctx context.Context, id entity.DiffID) error {
	status, err := r.GetDiffStatus(ctx, id)
	if err != nil {
		return fmt.Errorf("unable to get diff status: %w", err)
	}

	if err := r.DeleteDiff(ctx, id); err != nil {
		return fmt.Errorf("unable to delete diff: %w", err)
	}

	if err := r.StartDiff(ctx, id, status.Check.Settings); err != nil {
		return fmt.Errorf("unable to start diff: %w", err)
	}

	return nil
}

func (r *DiffSvc) FixDiff(ctx context.Context, id entity.DiffID, source entity.DiffLocation, storageType dom.StorageType) error {
	status, err := r.GetDiffStatus(ctx, id)
	if err != nil {
		return fmt.Errorf("unable to get diff status: %w", err)
	}
	if !status.Check.Queue.Ready {
		return errors.New("diff is not ready")
	}
	if status.Check.Consistent {
		return errors.New("storages are consistent, nothing to do")
	}
	if status.FixQueue != nil {
		return errors.New("diff fix already running")
	}

	sourceIdx := -1
	taskLocations := make([]tasks.MigrateLocation, 0, len(id.Locations))
	for idx, location := range id.Locations {
		if location == source {
			sourceIdx = idx
		}
		taskLocations = append(taskLocations, tasks.MigrateLocation{
			Storage: location.Storage,
			Bucket:  location.Bucket,
		})
	}

	if sourceIdx < 0 {
		return errors.New("unable to determine source index")
	}

	task := tasks.DiffFixCollectObjectsPayload{
		Locations:   taskLocations,
		SourceIndex: sourceIdx,
		StorageType: storageType,
		Versioned:   status.Check.Settings.Versioned,
		User:        status.Check.Settings.User,
	}

	if err := r.queueSvc.EnqueueTask(ctx, task); err != nil {
		return fmt.Errorf("unable to enqueue diff fix purge task: %w", err)
	}

	return nil
}

func (r *DiffSvc) CollectObjectsToFix(ctx context.Context, diffFixID entity.DiffFixID, source string, versioned bool) error {
	tokens, err := store.DiffIDToTokensConverter(diffFixID.DiffID)
	if err != nil {
		return fmt.Errorf("unable to make tokens out of diff id: %w", err)
	}

	destination := diffFixID.Destination
	// TODO use redis map to persist presence?
	type presence struct {
		inSource bool
		inDest   bool
		isDir    bool
	}
	objectPresence := map[string]*presence{}
	pager := store.NewPager(0, 1000)
	for {
		idPage, err := r.setStore.GetIDs(ctx, pager, tokens...)
		if err != nil {
			return fmt.Errorf("unable to get id page: %w", err)
		}

		exec := r.setStore.GroupExecutor()
		execSetStore := r.setStore.WithExecutor(exec)

		opResults := make([]store.OperationResult[[]entity.DiffSetEntry], 0, len(idPage.Entries))
		for _, id := range idPage.Entries {
			opResults = append(opResults, execSetStore.GetOp(ctx, id))
		}

		if err := exec.Exec(ctx); err != nil {
			return fmt.Errorf("unable to get diff sets: %w", err)
		}

		for idx, id := range idPage.Entries {
			isNew := false
			isHit := false
			present := objectPresence[id.Object]
			if present == nil {
				isNew = true
				present = &presence{}
			}
			entrySet, _ := opResults[idx].Get()
			for _, entry := range entrySet {
				switch entry.Location.Storage {
				case source:
					isHit = true
					present.inSource = true
					present.isDir = entry.IsDir
				case destination:
					isHit = true
					present.inDest = true
				}
			}
			if isNew && isHit {
				objectPresence[id.Object] = present
			}
		}

		if idPage.Next == 0 {
			break
		}

		pager.From = idPage.Next
	}

	if len(objectPresence) == 0 {
		return nil
	}

	copyObjectSet := []entity.DiffFixCopyObject{}
	removeObjectSet := []string{}
	for object, present := range objectPresence {
		switch {
		case present.inSource && !present.inDest:
			// Only in source - copy
			copyObjectSet = append(copyObjectSet, entity.NewDiffFixCopyObject(object, present.isDir))
		case !present.inSource && present.inDest:
			// Only in destination - remove
			removeObjectSet = append(removeObjectSet, object)
		case present.inSource && present.inDest:
			if !versioned {
				// In both, not versioned - copy
				copyObjectSet = append(copyObjectSet, entity.NewDiffFixCopyObject(object, present.isDir))
			} else {
				// In both, versioned - delete and copy
				copyObjectSet = append(copyObjectSet, entity.NewDiffFixCopyObject(object, present.isDir))
				removeObjectSet = append(removeObjectSet, object)
			}
		}
	}

	exec := r.fixCopySetStore.GroupExecutor()
	if len(copyObjectSet) > 0 {
		r.fixCopySetStore.WithExecutor(exec).AddOp(ctx, diffFixID, copyObjectSet...)
	}
	if len(removeObjectSet) > 0 {
		r.fixRemoveSetStore.WithExecutor(exec).AddOp(ctx, diffFixID, removeObjectSet...)
	}

	if err := exec.Exec(ctx); err != nil {
		return fmt.Errorf("unable to add objects to fix sets: %w", err)
	}

	return nil
}

// TODO add option to move conflicting objects rather than removing them?
func (r *DiffSvc) RemoveDiffObjects(ctx context.Context, id entity.DiffFixID, location entity.DiffLocation, user string, versioned bool) error {
	objectNames, err := r.fixRemoveSetStore.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("unable to get objects to remove: %w", err)
	}

	if len(objectNames) == 0 {
		return nil
	}

	client, err := r.clients.AsCommon(ctx, location.Storage, user)
	if err != nil {
		return fmt.Errorf("unable to obtain client: %w", err)
	}

	if !versioned {
		removeErrs := client.RemoveObjects(ctx, location.Bucket, objectNames)
		for _, err := range removeErrs {
			if !errors.Is(err, dom.ErrNotFound) {
				return fmt.Errorf("unable to remove diff objects: %w", err)
			}
		}
		return nil
	}

	listErrs := []error{}
	namesAndVersions := []objstore.NameAndVersion{}
	for _, objectName := range objectNames {
		versionIter := client.ListObjects(ctx, location.Bucket, objstore.WithPrefix(objectName), objstore.WithVersions())

		for versionInfo, err := range versionIter {
			if err != nil {
				listErrs = append(listErrs, fmt.Errorf("unable to list version: %w", err))
				continue
			}
			namesAndVersions = append(namesAndVersions, objstore.NameAndVersion{
				Name:    objectName,
				Version: versionInfo.VersionID,
			})
		}
	}

	removeErrs := client.RemoveVersionedObjects(ctx, location.Bucket, namesAndVersions)

	if len(listErrs) > 0 || len(removeErrs) > 0 {
		joinErr := errors.Join(errors.Join(listErrs...), errors.Join(removeErrs...))
		return fmt.Errorf("unable to delete object versions: %w", joinErr)
	}

	return nil
}

func (r *DiffSvc) EnsureObjectsDeleted(ctx context.Context, id entity.DiffFixID, location entity.DiffLocation, user string, versioned bool, makePayload func(object string, isDir bool) (any, error)) error {
	objectsToRemove, err := r.fixRemoveSetStore.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("unable to get objects to remove: %w", err)
	}

	client, err := r.clients.AsCommon(ctx, location.Storage, user)
	if err != nil {
		return fmt.Errorf("unable to obtain client: %w", err)
	}

	for _, object := range objectsToRemove {
		exists, err := client.ObjectExists(ctx, location.Bucket, object)
		if err != nil {
			return fmt.Errorf("unable to check if object exists: %w", err)
		}
		if exists {
			return fmt.Errorf("object %s is still present", object)
		}

		// TODO 1. add info about delete marker to stat
		// 2. use stat instead of exist check
		// No point to check here until fix is applied to Ceph
		// if !exists && versioned {
		// }

		if _, err := r.fixRemoveSetStore.Remove(ctx, id, object); err != nil {
			return fmt.Errorf("unable to remove object from fix set: %w", err)
		}
	}

	objectsToCopy, err := r.fixCopySetStore.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("unable to get objects to copy: %w", err)
	}

	for _, object := range objectsToCopy {
		payload, err := makePayload(object.Name, object.IsDir)
		if err != nil {
			return fmt.Errorf("unable to make payload: %w", err)
		}
		if err := r.queueSvc.EnqueueTask(ctx, payload); err != nil {
			return fmt.Errorf("unable to enqueue diff copy task: %w", err)
		}
	}

	if _, err := r.fixRemoveSetStore.Drop(ctx, id); err != nil {
		return fmt.Errorf("unable to clean fix set store: %w", err)
	}

	return nil
}
