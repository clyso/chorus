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

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/replication"
	"github.com/clyso/chorus/pkg/tasks"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

func WebhookHandlers(
	credsSvc objstore.CredsService,
	policySvc policy.Service,
	replSvc replication.Service,
) pb.WebhookServer {
	return &webhookHandlers{
		credsSvc:  credsSvc,
		policySvc: policySvc,
		replSvc:   replSvc,
	}
}

var _ pb.WebhookServer = &webhookHandlers{}

type webhookHandlers struct {
	credsSvc  objstore.CredsService
	policySvc policy.Service
	replSvc   replication.Service
}

func (h *webhookHandlers) SwiftEvents(ctx context.Context, req *pb.SwiftEventsRequest) (*emptypb.Empty, error) {
	logger := zerolog.Ctx(ctx)

	storType, ok := h.credsSvc.Storages()[req.Storage]
	if !ok {
		return nil, fmt.Errorf("%w: storage %q not found", dom.ErrNotFound, req.Storage)
	}
	if storType != dom.Swift {
		return nil, fmt.Errorf("%w: storage %q is %s, expected SWIFT", dom.ErrInvalidArg, req.Storage, storType)
	}

	for i, evt := range req.Events {
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

	if err := h.replSvc.Replicate(evtCtx, storage, task); err != nil {
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
