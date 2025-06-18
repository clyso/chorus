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
	"net/http"
	"strings"
	"time"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/lock"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/containers"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"
)

func (s *svc) HandleContainerUpdate(ctx context.Context, t *asynq.Task) (err error) {
	var p tasks.ContainerUpdatePayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("ContainerUpdatePayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	logger := zerolog.Ctx(ctx)
	toBucket := p.Bucket
	if p.ToBucket != nil {
		toBucket = *p.ToBucket
	}

	paused, err := s.policySvc.IsReplicationPolicyPaused(ctx, p.FromAccount, p.Bucket, p.FromStorage, p.ToStorage, p.ToBucket)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			zerolog.Ctx(ctx).Err(err).Msg("drop replication task: replication policy not found")
			return nil
		}
		return err
	}
	if paused {
		return &dom.ErrRateLimitExceeded{RetryIn: s.conf.PauseRetryInterval}
	}

	if err = s.limit.StorReq(ctx, p.FromStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.FromStorage).Msg("rate limit error")
		return err
	}
	if err = s.limit.StorReq(ctx, p.ToStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ToStorage).Msg("rate limit error")
		return err
	}
	defer func() {
		if err != nil {
			return
		}
		verErr := s.policySvc.IncReplEventsDone(ctx, p.FromAccount, p.Bucket, p.FromStorage, p.ToStorage, p.ToBucket, p.CreatedAt)
		if verErr != nil {
			zerolog.Ctx(ctx).Err(verErr).Msg("unable to inc processed events")
		}
	}()

	release, refresh, err := s.locker.Lock(ctx, lock.BucketKey(p.ToStorage+p.ToAccount, toBucket))
	if err != nil {
		return err
	}
	defer release()
	err = lock.WithRefresh(ctx, func() error {
		return s.handleContainerUpdate(ctx, p)
	}, refresh, time.Second*2)

	return
}

func (s *svc) handleContainerUpdate(ctx context.Context, p tasks.ContainerUpdatePayload) (err error) {
	fromBucket, toBucket := p.Bucket, p.Bucket
	if p.ToBucket != nil {
		toBucket = *p.ToBucket
	}
	// setup swift clients:
	fromClient, err := s.swiftClients.For(ctx, p.FromStorage, p.FromAccount)
	if err != nil {
		return err
	}
	toClient, err := s.swiftClients.For(ctx, p.ToStorage, p.ToAccount)
	if err != nil {
		return err
	}
	// get container metadata from source:
	fromHeaders, fromMeta, err := getSwiftContainerMeta(ctx, fromClient, fromBucket)
	if errors.Is(err, dom.ErrNotFound) {
		// if source container does not exist, remove destination container
		return s.deleteSwiftContainer(ctx, toClient, toBucket)
	}
	if err != nil {
		return fmt.Errorf("failed to get source container %q headers: %w", fromBucket, err)
	}

	// get destination container metadata if exists:
	toHeaders, toMeta, err := getSwiftContainerMeta(ctx, toClient, toBucket)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("failed to get destination container %q headers: %w", toBucket, err)
	}

	// update destination container metadata
	updateOpts := containerCopyMetaRequest(fromHeaders, fromMeta, toHeaders, toMeta)
	err = containers.Update(ctx, toClient, toBucket, updateOpts).Err
	if err != nil {
		return fmt.Errorf("failed to update destination container %q headers: %w", toBucket, err)
	}
	return nil
}

func getSwiftContainerMeta(ctx context.Context, client *gophercloud.ServiceClient, containerName string) (headers *containers.GetHeader, meta map[string]string, err error) {
	res := containers.Get(ctx, client, containerName, containers.GetOpts{
		Newest: true,
	})
	if res.Err != nil {
		if gophercloud.ResponseCodeIs(err, http.StatusNotFound) {
			return nil, nil, dom.ErrNotFound
		}
		return nil, nil, res.Err
	}
	meta, err = res.ExtractMetadata()
	if err != nil {
		return nil, nil, err
	}
	if meta == nil {
		meta = make(map[string]string)
	}
	headers, err = res.Extract()
	if err != nil {
		return nil, nil, err
	}
	return
}

func containerCopyMetaRequest(fromHeaders *containers.GetHeader, fromMeta map[string]string, toHeaders *containers.GetHeader, toMeta map[string]string) *containers.UpdateOpts {
	updateOpts := containers.UpdateOpts{
		Metadata:         make(map[string]string),
		RemoveMetadata:   []string{},
		TempURLKey:       fromHeaders.TempURLKey,
		TempURLKey2:      fromHeaders.TempURLKey2,
		VersionsLocation: fromHeaders.VersionsLocation,
		HistoryLocation:  fromHeaders.HistoryLocation,
		VersionsEnabled:  &fromHeaders.VersionsEnabled,
	}

	// Copy all metadata from source
	for k, v := range fromMeta {
		updateOpts.Metadata[k] = v
	}

	// Remove metadata keys that exist in destination but not in source
	for k := range toMeta {
		if _, ok := fromMeta[k]; !ok {
			// Remove only if not exists in source
			updateOpts.RemoveMetadata = append(updateOpts.RemoveMetadata, k)
		}
	}

	// Copy Content-Type if present
	if fromHeaders.ContentType != "" {
		updateOpts.ContentType = &fromHeaders.ContentType
	}

	// ACLs: Read, Write
	if len(fromHeaders.Read) != 0 {
		aclRead := strings.Join(fromHeaders.Read, ",")
		updateOpts.ContainerRead = &aclRead
	}
	if len(fromHeaders.Write) != 0 {
		aclWrite := strings.Join(fromHeaders.Write, ",")
		updateOpts.ContainerWrite = &aclWrite
	}

	//TODO:swift do we need to migrate syncTo and syncKey? How to handle if syncTo was not yet migrated?
	if fromHeaders.SyncTo != "" {
		updateOpts.ContainerSyncTo = &fromHeaders.SyncTo
	}
	if fromHeaders.SyncKey != "" {
		updateOpts.ContainerSyncKey = &fromHeaders.SyncKey
	}

	// disable versioning
	if toHeaders.VersionsLocation != "" && fromHeaders.VersionsLocation == "" {
		updateOpts.RemoveVersionsLocation = "DELETE"
	}

	if toHeaders.HistoryLocation != "" && fromHeaders.HistoryLocation == "" {
		updateOpts.RemoveHistoryLocation = "DELETE"
	}

	if fromHeaders.VersionsEnabled != toHeaders.VersionsEnabled {
		updateOpts.VersionsEnabled = &fromHeaders.VersionsEnabled
	}

	return &updateOpts
}

func (s *svc) deleteSwiftContainer(ctx context.Context, client *gophercloud.ServiceClient, container string) error {
	err := containers.Delete(ctx, client, container).Err
	if gophercloud.ResponseCodeIs(err, http.StatusConflict) {
		// if destination container is not empty, retry task later
		return &dom.ErrRateLimitExceeded{RetryIn: s.conf.SwiftRetryInterval}
	}
	if gophercloud.ResponseCodeIs(err, http.StatusNotFound) {
		// destination container was already deleted. Exit.
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to delete destination container %q: %w", container, err)
	}
	// destination container was successfully deleted. Exit.
	return nil
}
