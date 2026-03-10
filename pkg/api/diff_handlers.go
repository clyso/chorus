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
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/service/worker/handler"
)

func DiffHandlers(
	credsSvc objstore.CredsService,
	diffSvc *handler.DiffSvc,
) pb.DiffServer {
	return &diffHandlers{
		credsSvc: credsSvc,
		diffSvc:  diffSvc,
	}
}

var _ pb.DiffServer = &diffHandlers{}

type diffHandlers struct {
	diffSvc  *handler.DiffSvc
	credsSvc objstore.CredsService
}

func (h *diffHandlers) Start(ctx context.Context, req *pb.StartDiffCheckRequest) (*emptypb.Empty, error) {
	if err := h.validateStorageLocationsWithUser(req.Locations, req.User); err != nil {
		return nil, fmt.Errorf("unable to validate storage locations: %w", err)
	}

	diffID := pbToDiffID(req.Locations)
	shouldCheckVersions := !req.CheckOnlyLastVersions
	if shouldCheckVersions {
		var err error
		shouldCheckVersions, err = h.diffSvc.ShouldCheckVersions(ctx, req.User, diffID.Locations)
		if err != nil {
			return nil, fmt.Errorf("unable to determine if should check version: %w", err)
		}
	}

	settings := entity.NewDiffSettings(req.User, shouldCheckVersions, req.IgnoreSizes, req.IgnoreEtags)

	if err := h.diffSvc.StartDiff(ctx, diffID, settings); err != nil {
		return nil, fmt.Errorf("unable to start diff check: %w", err)
	}

	return &emptypb.Empty{}, nil
}

func (h *diffHandlers) List(ctx context.Context, _ *emptypb.Empty) (*pb.ListDiffChecksResponse, error) {
	diffList, err := h.diffSvc.GetDiffList(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to list diff checks: %w", err)
	}

	diffs := make([]*pb.DiffCheck, 0, len(diffList))
	for _, diff := range diffList {
		diffs = append(diffs, diffStatusToPB(diff))
	}

	return &pb.ListDiffChecksResponse{
		Checks: diffs,
	}, nil
}

func (h *diffHandlers) GetReport(ctx context.Context, req *pb.DiffCheckRequest) (*pb.GetDiffCheckReportResponse, error) {
	if err := h.validateStorageLocations(req.Locations); err != nil {
		return nil, fmt.Errorf("unable to validate storage locations: %w", err)
	}

	diffID := pbToDiffID(req.Locations)
	diffStatus, err := h.diffSvc.GetDiffStatus(ctx, diffID)
	if err != nil {
		return nil, fmt.Errorf("unable to get diff check status: %w", err)
	}

	return &pb.GetDiffCheckReportResponse{
		Check: diffStatusToPB(diffStatus),
	}, nil
}

func (h *diffHandlers) GetReportEntries(ctx context.Context, req *pb.GetDiffCheckReportEntriesRequest) (*pb.GetDiffCheckReportEntriesResponse, error) {
	if err := h.validateStorageLocations(req.Locations); err != nil {
		return nil, fmt.Errorf("unable to validate storage locations: %w", err)
	}

	diffID := pbToDiffID(req.Locations)
	reportPage, err := h.diffSvc.GetDiffReportEntries(ctx, diffID, req.Cursor, req.PageSize)
	if err != nil {
		return nil, fmt.Errorf("unable to get diff sets page: %w", err)
	}

	entries := make([]*pb.DiffCheckReportEntry, 0, len(reportPage.Entries))
	for _, reportEntry := range reportPage.Entries {
		storageEntries := make([]*pb.DiffCheckStorageEntry, 0, len(reportEntry.StorageEntries))
		for _, entry := range reportEntry.StorageEntries {
			storageEntries = append(storageEntries, &pb.DiffCheckStorageEntry{
				Storage:   entry.Location.Storage,
				VersionId: entry.VersionID,
				Bucket:    entry.Location.Bucket,
			})
		}
		entries = append(entries, &pb.DiffCheckReportEntry{
			Object:         reportEntry.Object,
			VersionIdx:     reportEntry.VersionIndex,
			Size:           reportEntry.Size,
			Etag:           reportEntry.Etag,
			StorageEntries: storageEntries,
		})
	}

	return &pb.GetDiffCheckReportEntriesResponse{
		Entries: entries,
		Cursor:  reportPage.Cursor,
	}, nil
}

func (h *diffHandlers) DeleteReport(ctx context.Context, req *pb.DiffCheckRequest) (*emptypb.Empty, error) {
	if err := h.validateStorageLocations(req.Locations); err != nil {
		return nil, fmt.Errorf("unable to validate storage locations: %w", err)
	}

	diffID := pbToDiffID(req.Locations)
	if err := h.diffSvc.DeleteDiff(ctx, diffID); err != nil {
		return nil, fmt.Errorf("unable to delete diff check: %w", err)
	}

	return nil, nil
}

func (h *diffHandlers) Fix(ctx context.Context, req *pb.StartDiffFixRequest) (*emptypb.Empty, error) {
	if err := h.validateStorageLocations(req.Locations); err != nil {
		return nil, fmt.Errorf("unable to validate storage locations: %w", err)
	}

	diffID := pbToDiffID(req.Locations)
	source := entity.NewDiffLocation(req.Locations[req.SourceIndex].Storage, req.Locations[req.SourceIndex].Bucket)
	storageType := h.credsSvc.Storages()[req.Locations[0].Storage]
	if err := h.diffSvc.FixDiff(ctx, diffID, source, storageType); err != nil {
		return nil, fmt.Errorf("unable to fix diff: %w", err)
	}

	return nil, nil
}

func (h *diffHandlers) Restart(ctx context.Context, req *pb.DiffCheckRequest) (*emptypb.Empty, error) {
	if err := h.validateStorageLocations(req.Locations); err != nil {
		return nil, fmt.Errorf("unable to validate storage locations: %w", err)
	}

	diffID := pbToDiffID(req.Locations)
	if err := h.diffSvc.RestartDiff(ctx, diffID); err != nil {
		return nil, fmt.Errorf("unable to restart diff check: %w", err)
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

	var prevStorageType dom.StorageType
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
		if prevStorageType == "" {
			prevStorageType = storType
		} else if prevStorageType != storType {
			return errors.New("all storages should have the same type")
		}
	}

	return nil
}

func (h *diffHandlers) validateStorageLocationsWithUser(locations []*pb.MigrateLocation, user string) error {
	if len(locations) < 2 {
		return errors.New("at least 2 migration locations should be provided")
	}

	var prevStorageType dom.StorageType
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
		if prevStorageType == "" {
			prevStorageType = storType
		} else if prevStorageType != storType {
			return errors.New("all storages should have the same type")
		}
		if err := h.credsSvc.HasUser(location.Storage, user); err != nil {
			return fmt.Errorf("%w: invalid storage location", err)
		}

	}

	return nil
}

func pbToDiffID(in []*pb.MigrateLocation) entity.DiffID {
	if in == nil {
		return entity.NewDiffID()
	}
	diffLocations := make([]entity.DiffLocation, 0, len(in))
	for _, reqLocation := range in {
		diffLocations = append(diffLocations, entity.NewDiffLocation(reqLocation.Storage, reqLocation.Bucket))
	}
	return entity.NewDiffID(diffLocations...)
}

func diffLocationsToPB(in []entity.DiffLocation) []*pb.MigrateLocation {
	locations := make([]*pb.MigrateLocation, 0, len(in))
	for _, diffLocation := range in {
		locations = append(locations, &pb.MigrateLocation{
			Storage: diffLocation.Storage,
			Bucket:  diffLocation.Bucket,
		})
	}
	return locations
}

func diffStatusToPB(in entity.DiffStatus) *pb.DiffCheck {
	status := &pb.DiffCheck{
		Locations:   diffLocationsToPB(in.Locations),
		Queued:      in.Check.Queue.Queued,
		Completed:   in.Check.Queue.Completed,
		Ready:       in.Check.Queue.Ready,
		Consistent:  in.Check.Consistent,
		Versioned:   in.Check.Settings.Versioned,
		IgnoreSizes: in.Check.Settings.IgnoreSizes,
		IgnoreEtags: in.Check.Settings.IgnoreEtags,
	}

	if in.FixQueue != nil {
		status.FixQueued = in.FixQueue.Queued
		status.FixCompleted = in.FixQueue.Completed
		status.FixReady = in.FixQueue.Ready
	}

	return status
}
