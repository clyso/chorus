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

package api

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/tasks"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/service/worker/handler"
)

func DiffHandlers(
	credsSvc objstore.CredsService,
	queueSvc tasks.QueueService,
	checkSvc *handler.ConsistencyCheckSvc,
) pb.DiffServer {
	return &diffHandlers{
		credsSvc: credsSvc,
		queueSvc: queueSvc,
		checkSvc: checkSvc,
	}
}

var _ pb.DiffServer = &diffHandlers{}

type diffHandlers struct {
	queueSvc tasks.QueueService
	checkSvc *handler.ConsistencyCheckSvc
	credsSvc objstore.CredsService
}

func (h *diffHandlers) Start(ctx context.Context, req *pb.StartConsistencyCheckRequest) (*emptypb.Empty, error) {
	if err := h.validateStorageLocationsWithUser(req.Locations, req.User); err != nil {
		return nil, fmt.Errorf("unable to validate storage locations: %w", err)
	}

	locationCount := len(req.Locations)
	checkLocations := make([]entity.ConsistencyCheckLocation, 0, locationCount)
	taskLocations := make([]tasks.MigrateLocation, 0, locationCount)
	for _, reqLocation := range req.Locations {
		checkLocations = append(checkLocations, entity.NewConsistencyCheckLocation(reqLocation.Storage, reqLocation.Bucket))
		taskLocations = append(taskLocations, tasks.MigrateLocation{
			Storage: reqLocation.Storage,
			Bucket:  reqLocation.Bucket,
		})
	}

	shouldCheckVersions := !req.CheckOnlyLastVersions

	if shouldCheckVersions {
		var err error
		shouldCheckVersions, err = h.checkSvc.ShouldCheckVersions(ctx, req.User, checkLocations)
		if err != nil {
			return nil, fmt.Errorf("unable to determine if should check version: %w", err)
		}
	}

	withSizeCheck := !req.IgnoreSizes
	withEtagCheck := !req.IgnoreEtags && !req.IgnoreSizes
	checkID := entity.NewConsistencyCheckID(checkLocations...)
	settings := entity.NewConsistencyCheckSettings(shouldCheckVersions, withSizeCheck, withEtagCheck)
	if err := h.checkSvc.RegisterConsistencyCheck(ctx, checkID, settings); err != nil {
		return nil, fmt.Errorf("unable to start consistency check: %w", err)
	}

	consistencyCheckTask := tasks.ConsistencyCheckPayload{
		Locations:   taskLocations,
		User:        req.User,
		Versioned:   shouldCheckVersions,
		IgnoreEtags: req.IgnoreEtags,
		IgnoreSizes: req.IgnoreSizes,
	}
	if err := h.queueSvc.EnqueueTask(ctx, consistencyCheckTask); err != nil {
		return nil, fmt.Errorf("unable to enqueue consistency check task: %w", err)
	}

	return &emptypb.Empty{}, nil
}

func (h *diffHandlers) List(ctx context.Context, _ *emptypb.Empty) (*pb.ListConsistencyChecksResponse, error) {
	checkList, err := h.checkSvc.GetConsistencyCheckList(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to list consistency checks: %w", err)
	}

	consistencyChecks := make([]*pb.ConsistencyCheck, 0, len(checkList))
	for _, check := range checkList {
		locations := make([]*pb.MigrateLocation, 0, len(check.Locations))
		for _, checkLocation := range check.Locations {
			locations = append(locations, &pb.MigrateLocation{
				Storage: checkLocation.Storage,
				Bucket:  checkLocation.Bucket,
			})
		}
		consistencyChecks = append(consistencyChecks, &pb.ConsistencyCheck{
			Locations:  locations,
			Queued:     check.Queued,
			Completed:  check.Completed,
			Ready:      check.Ready,
			Consistent: check.Consistent,
			Versioned:  check.Versioned,
			WithSize:   check.WithSize,
			WithEtag:   check.WithEtag,
		})
	}

	return &pb.ListConsistencyChecksResponse{
		Checks: consistencyChecks,
	}, nil
}

func (h *diffHandlers) GetReport(ctx context.Context, req *pb.ConsistencyCheckRequest) (*pb.GetConsistencyCheckReportResponse, error) {
	if err := h.validateStorageLocations(req.Locations); err != nil {
		return nil, fmt.Errorf("unable to validate storage locations: %w", err)
	}

	checkLocations := make([]entity.ConsistencyCheckLocation, 0, len(req.Locations))
	for _, reqLocation := range req.Locations {
		checkLocations = append(checkLocations, entity.NewConsistencyCheckLocation(reqLocation.Storage, reqLocation.Bucket))
	}

	checkID := entity.NewConsistencyCheckID(checkLocations...)
	checkStatus, err := h.checkSvc.GetConsistencyCheckStatus(ctx, checkID)
	if err != nil {
		return nil, fmt.Errorf("unable to get consistency check status: %w", err)
	}

	return &pb.GetConsistencyCheckReportResponse{
		Check: &pb.ConsistencyCheck{
			Locations:  req.Locations,
			Queued:     checkStatus.Queued,
			Completed:  checkStatus.Completed,
			Ready:      checkStatus.Ready,
			Consistent: checkStatus.Consistent,
			Versioned:  checkStatus.Versioned,
			WithSize:   checkStatus.WithSize,
			WithEtag:   checkStatus.WithEtag,
		},
	}, nil
}

func (h *diffHandlers) GetReportEntries(ctx context.Context, req *pb.GetConsistencyCheckReportEntriesRequest) (*pb.GetConsistencyCheckReportEntriesResponse, error) {
	if err := h.validateStorageLocations(req.Locations); err != nil {
		return nil, fmt.Errorf("unable to validate storage locations: %w", err)
	}

	checkLocations := make([]entity.ConsistencyCheckLocation, 0, len(req.Locations))
	for _, reqLocation := range req.Locations {
		checkLocations = append(checkLocations, entity.NewConsistencyCheckLocation(reqLocation.Storage, reqLocation.Bucket))
	}

	checkID := entity.NewConsistencyCheckID(checkLocations...)
	reportPage, err := h.checkSvc.GetConsistencyCheckReportEntries(ctx, checkID, req.Cursor, req.PageSize)
	if err != nil {
		return nil, fmt.Errorf("unable to get consistency sets page: %w", err)
	}

	entries := make([]*pb.ConsistencyCheckReportEntry, 0, len(reportPage.Entries))
	for _, reportEntry := range reportPage.Entries {
		storageEntries := make([]*pb.ConsistencyCheckStorageEntry, 0, len(reportEntry.StorageEntries))
		for _, entry := range reportEntry.StorageEntries {
			storageEntries = append(storageEntries, &pb.ConsistencyCheckStorageEntry{
				Storage:   entry.Location.Storage,
				VersionId: entry.VersionID,
				Bucket:    entry.Location.Bucket,
			})
		}
		entries = append(entries, &pb.ConsistencyCheckReportEntry{
			Object:         reportEntry.Object,
			VersionIdx:     reportEntry.VersionIndex,
			Size:           reportEntry.Size,
			Etag:           reportEntry.Etag,
			StorageEntries: storageEntries,
		})
	}

	return &pb.GetConsistencyCheckReportEntriesResponse{
		Entries: entries,
		Cursor:  reportPage.Cursor,
	}, nil
}

func (h *diffHandlers) DeleteReport(ctx context.Context, req *pb.ConsistencyCheckRequest) (*emptypb.Empty, error) {
	if err := h.validateStorageLocations(req.Locations); err != nil {
		return nil, fmt.Errorf("unable to validate storage locations: %w", err)
	}

	checkLocations := make([]entity.ConsistencyCheckLocation, 0, len(req.Locations))
	for _, reqLocation := range req.Locations {
		checkLocations = append(checkLocations, entity.NewConsistencyCheckLocation(reqLocation.Storage, reqLocation.Bucket))
	}

	checkID := entity.NewConsistencyCheckID(checkLocations...)
	if err := h.checkSvc.DeleteConsistencyCheck(ctx, checkID); err != nil {
		return nil, fmt.Errorf("unable to delete consistency check: %w", err)
	}

	return nil, nil
}

var ccSupportedStorTypes = map[dom.StorageType]bool{
	dom.S3:    true,
	dom.Swift: true,
}

func (h *diffHandlers) validateStorageLocations(locations []*pb.MigrateLocation) error {
	if len(locations) < 2 {
		return errors.New("at least 2 migration locations should be provided")
	}

	for idx, location := range locations {
		if location.Bucket == "" {
			return fmt.Errorf("location %d bucket is empty", idx)
		}
		if location.Storage == "" {
			return fmt.Errorf("location %d storage is empty", idx)
		}
		storType, ok := h.credsSvc.Storages()[location.Storage]
		if !ok {
			return fmt.Errorf("unable to find storage %s in config", location.Storage)
		}
		if !ccSupportedStorTypes[storType] {
			return fmt.Errorf("storage %s of type %s is not supported storage location", location.Storage, storType)
		}
	}

	return nil
}

func (h *diffHandlers) validateStorageLocationsWithUser(locations []*pb.MigrateLocation, user string) error {
	if len(locations) < 2 {
		return errors.New("at least 2 migration locations should be provided")
	}

	for idx, location := range locations {
		if location.Bucket == "" {
			return fmt.Errorf("location %d bucket is empty", idx)
		}
		if location.Storage == "" {
			return fmt.Errorf("location %d storage is empty", idx)
		}
		storType, ok := h.credsSvc.Storages()[location.Storage]
		if !ok {
			return fmt.Errorf("unable to find storage %s in config", location.Storage)
		}
		if !ccSupportedStorTypes[storType] {
			return fmt.Errorf("storage %s of type %s is not supported storage location", location.Storage, storType)
		}
		if err := h.credsSvc.HasUser(location.Storage, user); err != nil {
			return fmt.Errorf("%w: invalid storage location", err)
		}
	}

	return nil
}
