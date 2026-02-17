/*
 * Copyright © 2026 Clyso GmbH
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
	"fmt"
	"strings"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/notifications"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/replication"
	"github.com/clyso/chorus/pkg/tasks"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

func WebhookHandlers(
	credsSvc objstore.CredsService,
	policySvc policy.Service,
	swiftReplSvc replication.Service,
	s3ReplSvc replication.Service,
) pb.WebhookServer {
	return &webhookHandlers{
		credsSvc:     credsSvc,
		policySvc:    policySvc,
		swiftReplSvc: swiftReplSvc,
		s3ReplSvc:    s3ReplSvc,
	}
}

var _ pb.WebhookServer = &webhookHandlers{}

type webhookHandlers struct {
	credsSvc     objstore.CredsService
	policySvc    policy.Service
	swiftReplSvc replication.Service
	s3ReplSvc    replication.Service
}

func (h *webhookHandlers) SwiftEvents(ctx context.Context, req *pb.SwiftEventsRequest) (*emptypb.Empty, error) {
	logger := zerolog.Ctx(ctx)

	storType, ok := h.credsSvc.Storages()[req.Storage]
	if !ok {
		logger.Warn().Str("storage", req.Storage).Msg("webhook: storage not found, skipping events")
		return &emptypb.Empty{}, nil
	}
	if storType != dom.Swift {
		logger.Warn().Str("storage", req.Storage).Str("type", string(storType)).Msg("webhook: storage is not SWIFT, skipping events")
		return &emptypb.Empty{}, nil
	}

	for i, evt := range req.Events {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := h.processSwiftEvent(ctx, logger, req.Storage, evt); err != nil {
			logger.Warn().Err(err).Int("event_index", i).Msg("skip swift event")
		}
	}

	return &emptypb.Empty{}, nil
}

func (h *webhookHandlers) processSwiftEvent(ctx context.Context, logger *zerolog.Logger, storage string, evt *pb.SwiftEvent) error {
	if evt.Account == "" {
		return fmt.Errorf("%w: account is required", dom.ErrInvalidArg)
	}

	switch evt.Operation {
	case pb.SwiftOperation_CONTAINER_UPDATE, pb.SwiftOperation_CONTAINER_DELETE,
		pb.SwiftOperation_OBJECT_CREATED, pb.SwiftOperation_OBJECT_METADATA_UPDATED, pb.SwiftOperation_OBJECT_DELETED:
		if evt.Container == "" {
			return fmt.Errorf("%w: container is required for %s", dom.ErrInvalidArg, evt.Operation)
		}
	}
	switch evt.Operation {
	case pb.SwiftOperation_OBJECT_CREATED, pb.SwiftOperation_OBJECT_METADATA_UPDATED, pb.SwiftOperation_OBJECT_DELETED:
		if evt.Object == "" {
			return fmt.Errorf("%w: object is required for %s", dom.ErrInvalidArg, evt.Operation)
		}
	}

	evtCtx, err := h.policySvc.BuildAgentContext(ctx, evt.Account, evt.Container)
	if err != nil {
		return fmt.Errorf("build agent context: %w", err)
	}

	task, err := swiftEventToTask(evt)
	if err != nil {
		return err
	}

	if err := h.swiftReplSvc.Replicate(evtCtx, storage, task); err != nil {
		return fmt.Errorf("replicate: %w", err)
	}
	logger.Debug().
		Str("account", evt.Account).
		Str("container", evt.Container).
		Str("object", evt.Object).
		Str("operation", evt.Operation.String()).
		Msg("swift event processed")
	return nil
}

func swiftEventToTask(evt *pb.SwiftEvent) (tasks.ReplicationTask, error) {
	switch evt.Operation {
	case pb.SwiftOperation_ACCOUNT_UPDATE:
		return &tasks.SwiftAccountUpdatePayload{
			Date: evt.Date,
		}, nil
	case pb.SwiftOperation_CONTAINER_UPDATE, pb.SwiftOperation_CONTAINER_DELETE:
		return &tasks.SwiftContainerUpdatePayload{
			Bucket: evt.Container,
			Date:   evt.Date,
		}, nil
	case pb.SwiftOperation_OBJECT_CREATED:
		return &tasks.SwiftObjectUpdatePayload{
			Bucket:       evt.Container,
			Object:       evt.Object,
			VersionID:    evt.VersionId,
			Etag:         evt.Etag,
			LastModified: evt.LastModified,
		}, nil
	case pb.SwiftOperation_OBJECT_METADATA_UPDATED:
		return &tasks.SwiftObjectMetaUpdatePayload{
			Bucket: evt.Container,
			Object: evt.Object,
			Date:   evt.Date,
		}, nil
	case pb.SwiftOperation_OBJECT_DELETED:
		return &tasks.SwiftObjectDeletePayload{
			Bucket:          evt.Container,
			Object:          evt.Object,
			VersionID:       evt.VersionId,
			Date:            evt.Date,
			DeleteMultipart: evt.DeleteMultipart,
		}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported swift operation %s", dom.ErrInvalidArg, evt.Operation)
	}
}

func (h *webhookHandlers) S3Notifications(ctx context.Context, req *pb.S3NotificationRequest) (*emptypb.Empty, error) {
	logger := zerolog.Ctx(ctx)

	storType, ok := h.credsSvc.Storages()[req.Storage]
	if !ok {
		logger.Warn().Str("storage", req.Storage).Msg("webhook: storage not found, skipping events")
		return &emptypb.Empty{}, nil
	}
	if storType != dom.S3 {
		logger.Warn().Str("storage", req.Storage).Str("type", string(storType)).Msg("webhook: storage is not S3, skipping events")
		return &emptypb.Empty{}, nil
	}

	// Group records by (user, bucket) to reduce BuildAgentContext calls.
	type bucketKey struct {
		user, bucket string
	}
	type s3RecordInfo struct {
		record *pb.S3EventRecord
		index  int
	}
	groups := make(map[bucketKey][]s3RecordInfo)
	for i, record := range req.Records {
		if record.S3 == nil || record.S3.ConfigurationId == "" {
			logger.Warn().Int("record_index", i).Msg("skip s3 notification record: missing s3 payload or configuration_id")
			continue
		}
		user, err := notifications.UserIDFromNotificationID(record.S3.ConfigurationId)
		if err != nil {
			logger.Warn().Err(err).Int("record_index", i).Msg("skip s3 notification record: invalid notification id")
			continue
		}
		bucket := ""
		if record.S3.Bucket != nil {
			bucket = record.S3.Bucket.Name
		}
		if bucket == "" {
			logger.Warn().Int("record_index", i).Msg("skip s3 notification record: missing bucket name")
			continue
		}
		key := bucketKey{user: user, bucket: bucket}
		groups[key] = append(groups[key], s3RecordInfo{record: record, index: i})
	}

	for key, records := range groups {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		evtCtx, err := h.policySvc.BuildAgentContext(ctx, key.user, key.bucket)
		if err != nil {
			logger.Warn().Err(err).Str("user", key.user).Str("bucket", key.bucket).Msg("skip s3 notification group: build agent context failed")
			continue
		}
		for _, ri := range records {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if err := h.processS3Record(evtCtx, logger, req.Storage, key.bucket, ri.record); err != nil {
				logger.Warn().Err(err).Int("record_index", ri.index).Msg("skip s3 notification record")
			}
		}
	}

	return &emptypb.Empty{}, nil
}

func (h *webhookHandlers) processS3Record(evtCtx context.Context, logger *zerolog.Logger, storage, bucket string, record *pb.S3EventRecord) error {
	objectKey := ""
	if record.S3.Object != nil {
		objectKey = record.S3.Object.Key
	}
	if objectKey == "" {
		return fmt.Errorf("%w: object key is required", dom.ErrInvalidArg)
	}

	var task tasks.ReplicationTask
	switch {
	case strings.Contains(record.EventName, "ObjectCreated"):
		size := int64(0)
		if record.S3.Object != nil {
			size = record.S3.Object.Size
		}
		task = &tasks.ObjectSyncPayload{
			Object: dom.Object{
				Bucket: bucket,
				Name:   objectKey,
			},
			ObjSize: size,
		}
	case strings.Contains(record.EventName, "ObjectRemoved"):
		task = &tasks.ObjectSyncPayload{
			Object: dom.Object{
				Bucket: bucket,
				Name:   objectKey,
			},
			Deleted: true,
		}
	default:
		return fmt.Errorf("%w: unsupported s3 event %s", dom.ErrInvalidArg, record.EventName)
	}

	if err := h.s3ReplSvc.Replicate(evtCtx, storage, task); err != nil {
		return fmt.Errorf("replicate: %w", err)
	}
	logger.Debug().
		Str("bucket", bucket).
		Str("object", objectKey).
		Str("event", record.EventName).
		Msg("s3 notification processed")
	return nil
}
