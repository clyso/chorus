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

package swift

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/containers"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/tasks"
)

func (s *svc) HandleContainerUpdate(ctx context.Context, t *asynq.Task) (err error) {
	var p tasks.SwiftContainerUpdatePayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("ContainerUpdatePayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	logger := zerolog.Ctx(ctx)
	_, toBucket := p.ID.FromToBuckets(p.Bucket)

	if err = s.limit.StorReq(ctx, p.ID.FromStorage()); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.FromStorage()).Msg("rate limit error")
		return err
	}
	if err = s.limit.StorReq(ctx, p.ID.ToStorage()); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.ToStorage()).Msg("rate limit error")
		return err
	}

	lock, err := s.bucketLocker.Lock(ctx, entity.NewBucketLockID(p.ID.ToStorage(), toBucket))
	if err != nil {
		return err
	}
	defer lock.Release(context.Background())
	err = lock.Do(ctx, time.Second*2, func() error {
		return s.ContainerUpdate(ctx, p)
	})

	return
}

func (s *svc) ContainerUpdate(ctx context.Context, p tasks.SwiftContainerUpdatePayload) (err error) {
	fromBucket, toBucket := p.ID.FromToBuckets(p.Bucket)
	// setup swift clients:
	fromClient, err := s.clients.AsSwift(ctx, p.ID.FromStorage(), p.ID.User())
	if err != nil {
		return err
	}
	toClient, err := s.clients.AsSwift(ctx, p.ID.ToStorage(), p.ID.User())
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
	// Container temp URL are not supported by Ceph RGW: https://docs.ceph.com/en/latest/radosgw/swift/#features-support
	delete(fromMeta, "Temp-Url-Key")
	delete(fromMeta, "Temp-Url-Key-2")

	// create container if it does not exist:
	if toHeaders == nil {
		cereateOpts := containers.CreateOpts{
			Metadata:         fromMeta,
			VersionsLocation: fromHeaders.VersionsLocation,
			// HistoryLocation is not supported by Ceph RGW: https://docs.ceph.com/en/latest/radosgw/swift/#features-support
			// HistoryLocation:  fromHeaders.HistoryLocation,
			VersionsEnabled: fromHeaders.VersionsEnabled,
			//TODO: map acls?
			ContainerRead:  strings.Join(fromHeaders.Read, ","),
			ContainerWrite: strings.Join(fromHeaders.Write, ","),
			// Ignore swift container sync headers.
			// ContainerSyncTo:  fromHeaders.SyncTo,
			// ContainerSyncKey: fromHeaders.SyncKey,
			ContentType: fromHeaders.ContentType,
			//TODO: check swift StoragePolicy - simply copy it to ceph will not work - InvalidLocationConstraint will be returned by RGW.
			// StoragePolicy:    fromHeaders.StoragePolicy,
		}
		err = containers.Create(ctx, toClient, toBucket, cereateOpts).Err
		if err != nil {
			return fmt.Errorf("failed to create destination container %q: %w", toBucket, err)
		}
	} else {
		// update destination container metadata
		updateOpts := containerCopyMetaRequest(fromHeaders, fromMeta, toHeaders, toMeta)
		err = containers.Update(ctx, toClient, toBucket, updateOpts).Err
		if err != nil {
			return fmt.Errorf("failed to update destination container %q headers: %w", toBucket, err)
		}
	}
	return nil
}

func getSwiftContainerMeta(ctx context.Context, client *gophercloud.ServiceClient, containerName string) (headers *containers.GetHeader, meta map[string]string, err error) {
	res := containers.Get(ctx, client, containerName, containers.GetOpts{
		Newest: true,
	})
	if res.Err != nil {
		if gophercloud.ResponseCodeIs(res.Err, http.StatusNotFound) {
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
		Metadata:       make(map[string]string, len(fromMeta)),
		RemoveMetadata: []string{},
		// Container temp URL are not supported by Ceph RGW: https://docs.ceph.com/en/latest/radosgw/swift/#features-support
		// TempURLKey:       fromHeaders.TempURLKey,
		// TempURLKey2:      fromHeaders.TempURLKey2,
		VersionsLocation: fromHeaders.VersionsLocation,
		// HistoryLocation is not supported by Ceph RGW: https://docs.ceph.com/en/latest/radosgw/swift/#features-support
		// HistoryLocation:  fromHeaders.HistoryLocation,
		VersionsEnabled: &fromHeaders.VersionsEnabled,
	}

	// Copy all metadata from source
	maps.Copy(updateOpts.Metadata, fromMeta)

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
		//TODO: map acls?
		aclRead := strings.Join(fromHeaders.Read, ",")
		updateOpts.ContainerRead = &aclRead
	}
	if len(fromHeaders.Write) != 0 {
		//TODO: map acls?
		aclWrite := strings.Join(fromHeaders.Write, ",")
		updateOpts.ContainerWrite = &aclWrite
	}

	// Ignore swift container sync headers.
	// if fromHeaders.SyncTo != "" {
	// 	updateOpts.ContainerSyncTo = &fromHeaders.SyncTo
	// }
	// if fromHeaders.SyncKey != "" {
	// 	updateOpts.ContainerSyncKey = &fromHeaders.SyncKey
	// }

	// disable versioning
	if toHeaders != nil && toHeaders.VersionsLocation != "" && fromHeaders.VersionsLocation == "" {
		updateOpts.RemoveVersionsLocation = "DELETE"
	}

	// HistoryLocation is not supported by Ceph RGW: https://docs.ceph.com/en/latest/radosgw/swift/#features-support
	// if toHeaders != nil && toHeaders.HistoryLocation != "" && fromHeaders.HistoryLocation == "" {
	// 	updateOpts.RemoveHistoryLocation = "DELETE"
	// }

	if toHeaders != nil && fromHeaders.VersionsEnabled != toHeaders.VersionsEnabled {
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
