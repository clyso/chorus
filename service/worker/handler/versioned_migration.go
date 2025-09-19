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
	"time"

	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/rclone"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
)

const (
	CMaxBufferedMoveObjectVersions = 100
)

type VersionedMigrationCtrl struct {
	svc      *VersionedMigrationSvc
	queueSvc tasks.QueueService
}

func NewVersionedMigrationCtrl(svc *VersionedMigrationSvc, queueSvc tasks.QueueService) *VersionedMigrationCtrl {
	return &VersionedMigrationCtrl{
		svc:      svc,
		queueSvc: queueSvc,
	}
}

func (r *VersionedMigrationCtrl) HandleObjectVersionList(ctx context.Context, t *asynq.Task) error {
	var listVersionsPayload tasks.ListObjectVersionsPayload
	if err := json.Unmarshal(t.Payload(), &listVersionsPayload); err != nil {
		return fmt.Errorf("unable to unmarshal payload: %w", err)
	}

	user := listVersionsPayload.ID.User()
	fromBucket, toBucket := listVersionsPayload.ID.FromToBuckets(listVersionsPayload.Bucket)

	objectVersionID := entity.NewVersionedObjectID(listVersionsPayload.ID.FromStorage(), fromBucket, listVersionsPayload.Prefix)
	replicationID := entity.NewBucketRepliationPolicy(user, listVersionsPayload.ID.FromStorage(), fromBucket, listVersionsPayload.ID.ToStorage(), toBucket)

	if err := r.svc.ListVersions(ctx, objectVersionID, replicationID); err != nil {
		return fmt.Errorf("unable to list obejct versions: %w", err)
	}

	migratePayload := tasks.MigrateVersionedObjectPayload(listVersionsPayload)

	err := r.queueSvc.EnqueueTask(ctx, migratePayload)
	if err != nil {
		return fmt.Errorf("migration bucket list obj: unable to enqueue copy obj task: %w", err)
	}

	return nil
}

func (r *VersionedMigrationCtrl) HandleVersionedObjectMigration(ctx context.Context, t *asynq.Task) error {
	var migratePayload tasks.MigrateVersionedObjectPayload
	if err := json.Unmarshal(t.Payload(), &migratePayload); err != nil {
		return fmt.Errorf("unable to unmarshal payload: %w", err)
	}

	user := migratePayload.ID.User()
	fromBucket, toBucket := migratePayload.ID.FromToBuckets(migratePayload.Bucket)
	replicationID := entity.NewBucketRepliationPolicy(user, migratePayload.ID.FromStorage(), fromBucket, migratePayload.ID.ToStorage(), toBucket)

	if err := r.svc.MigrateVersions(ctx, replicationID, migratePayload.Prefix); err != nil {
		return fmt.Errorf("unable to migrate object version: %w", err)
	}

	return nil
}

type VersionedMigrationSvc struct {
	policySvc policy.Service
	copySvc   rclone.CopySvc

	objectVersionInfoStore *store.ObjectVersionInfoStore

	objectLocker *store.ObjectLocker

	pauseRetryInterval time.Duration
}

func NewVersionedMigrationSvc(policySvc policy.Service, copySvc rclone.CopySvc,
	objectVersionInfoStore *store.ObjectVersionInfoStore, objectLocker *store.ObjectLocker,
	pauseRetryInterval time.Duration) *VersionedMigrationSvc {
	return &VersionedMigrationSvc{
		policySvc:              policySvc,
		copySvc:                copySvc,
		objectVersionInfoStore: objectVersionInfoStore,
		objectLocker:           objectLocker,
		pauseRetryInterval:     pauseRetryInterval,
	}
}

func (r *VersionedMigrationSvc) ListVersions(ctx context.Context, objectID entity.VersionedObjectID, replicationID entity.BucketReplicationPolicy) error {
	lastListedVersionInfo, err := r.objectVersionInfoStore.GetRight(ctx, objectID)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("unable to get last listed version for object: %w", err)
	}

	toFile := rclone.NewFile(objectID.Storage, objectID.Bucket, objectID.Name)

	var shouldSkipFirstListings bool
	if lastListedVersionInfo.Version != "" {
		shouldSkipFirstListings = true
	}

	versionInfoList, err := r.copySvc.GetVersionInfo(ctx, replicationID.User, toFile)
	if err != nil {
		return fmt.Errorf("unable to get version info: %w", err)
	}

	for _, info := range versionInfoList {
		if shouldSkipFirstListings {
			if info.Version == lastListedVersionInfo.Version {
				shouldSkipFirstListings = false
			}
			continue
		}
	}

	if err := r.objectVersionInfoStore.AddLeft(ctx, objectID, versionInfoList...); err != nil {
		return fmt.Errorf("unable to store version info list: %w", err)
	}

	return nil
}

func (r *VersionedMigrationSvc) MigrateVersions(ctx context.Context, replicationID entity.BucketReplicationPolicy, prefix string) error {
	toFile := rclone.File{
		Storage: replicationID.ToStorage,
		Bucket:  replicationID.ToBucket,
		Name:    prefix,
	}

	var destinationIsEmpty bool
	var shouldClearDestination bool
	migratedObjectVersionInfo, err := r.copySvc.GetLastMigratedVersionInfo(ctx, replicationID.User, toFile)
	switch {
	case errors.Is(err, dom.ErrNotFound):
		destinationIsEmpty = true
	case errors.Is(err, dom.ErrInvalidArg):
		shouldClearDestination = true
	case err != nil:
		return fmt.Errorf("unable to get migrated object version info: %w", err)
	}

	firstVersionToMigrateIdx := uint64(0)
	versionedObjectID := entity.NewVersionedObjectID(replicationID.FromStorage, replicationID.FromBucket, prefix)
	if !destinationIsEmpty {
		if migratedObjectVersionInfo.Version == "" {
			shouldClearDestination = true
		} else {
			versionIdx, err := r.objectVersionInfoStore.Find(ctx, versionedObjectID, migratedObjectVersionInfo)
			if errors.Is(err, dom.ErrNotFound) {
				shouldClearDestination = true
			} else if err != nil {
				return fmt.Errorf("unable to get object version index: %w", err)
			}

			firstVersionToMigrateIdx = uint64(versionIdx) + 1
		}
	}

	if shouldClearDestination {
		if err := r.copySvc.DeleteDestinationObject(ctx, replicationID.User, toFile); err != nil {
			return fmt.Errorf("unable to delete destination object: %w", err)
		}
	}

	// Not providing version to lock id, since migration should be performed in particular order
	objectLockID := entity.NewObjectLockID(replicationID.ToStorage, replicationID.ToBucket, prefix)
	lock, err := r.objectLocker.Lock(ctx, objectLockID)
	if err != nil {
		return fmt.Errorf("unable to acquire object lock: %w", err)
	}
	defer lock.Release(ctx)

	pager := store.NewPager(firstVersionToMigrateIdx, CMaxBufferedMoveObjectVersions)
	for {
		objectVersionInfoList, err := r.objectVersionInfoStore.GetPage(ctx, versionedObjectID, pager)
		if err != nil {
			return fmt.Errorf("unable to get object version info list: %w", err)
		}

		for _, info := range objectVersionInfoList {
			fromFile := rclone.NewVersionedFile(replicationID.FromStorage, replicationID.FromBucket, prefix, info.Version)
			toFile := rclone.NewVersionedFile(replicationID.ToStorage, replicationID.ToBucket, prefix, info.Version)

			if err = lock.Do(ctx, time.Second*2, func() error {
				return r.copySvc.CopyObject(ctx, replicationID.User, fromFile, toFile)
			}); err != nil {
				return fmt.Errorf("unable to copy object: %w", err)
			}
		}

		if len(objectVersionInfoList) < CMaxBufferedMoveObjectVersions {
			break
		}

		pager.From += CMaxBufferedMoveObjectVersions
	}

	if _, err := r.objectVersionInfoStore.Drop(ctx, versionedObjectID); err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("unable to drop object version list")
	}

	return nil
}
