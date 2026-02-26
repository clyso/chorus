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
	"net/http"
	"time"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/swift"
	"github.com/clyso/chorus/pkg/tasks"
)

func (s *svc) HandleObjectMetaUpdate(ctx context.Context, t *asynq.Task) (err error) {
	var p tasks.SwiftObjectMetaUpdatePayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("ObjectMetaUpdatePayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	logger := zerolog.Ctx(ctx)

	// acquire rate limits for source and destination storage before proceeding
	if err := s.rateLimit(ctx, p.ID.FromStorage(), swift.HeadObject); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.FromStorage()).Msg("rate limit error")
		return err
	}
	if err := s.rateLimit(ctx, p.ID.ToStorage(), swift.HeadObject, swift.PutObject); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.ToStorage()).Msg("rate limit error")
		return err
	}

	lock, err := s.objectLocker.Lock(ctx, entity.NewObjectLockID(p.ID.FromStorage(), p.Bucket, p.Object))
	if err != nil {
		return err
	}
	defer lock.Release(context.Background())

	return lock.Do(ctx, time.Second*2, func() error {
		return s.ObjectMetaUpdate(ctx, p)
	})
}

// ObjectMetaUpdate handles the object metadata update task, copying metadata from the source object to the destination object.
func (s *svc) ObjectMetaUpdate(ctx context.Context, p tasks.SwiftObjectMetaUpdatePayload) (err error) {
	logger := zerolog.Ctx(ctx)
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
	// get object metadata from source:
	fromHeaders, fromMeta, err := getSwiftObjectMeta(ctx, fromClient, fromBucket, p.Object)
	if errors.Is(err, dom.ErrNotFound) {
		// object was deleted from source, skip update obj meta task
		// event will be handled by object delete task
		logger.Info().Msgf("object %q in container %q was deleted from source, skip update object meta task", p.Object, fromBucket)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get source object %q headers: %w", p.Object, err)
	}

	// get destination object metadata:
	toHeaders, toMeta, err := getSwiftObjectMeta(ctx, toClient, toBucket, p.Object)
	if errors.Is(err, dom.ErrNotFound) {
		// object not exists in source, skip update obj meta task
		// meta will be sync'ed on next object create task
		logger.Info().Msgf("object %q in container %q was not found in destination, skip update object meta task", p.Object, toBucket)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get destination object %q headers: %w", toBucket, err)
	}

	// update destination object metadata
	updateOpts := objectCopyMetaRequest(fromHeaders, fromMeta, toHeaders, toMeta)
	err = objects.Update(ctx, toClient, toBucket, p.Object, updateOpts).Err
	if err != nil {
		return fmt.Errorf("failed to update destination object %q metadata: %w", p.Object, err)
	}
	return nil
}

func getSwiftObjectMeta(ctx context.Context, client *gophercloud.ServiceClient, containerName, objectName string) (headers *objects.GetHeader, meta map[string]string, err error) {
	res := objects.Get(ctx, client, containerName, objectName, objects.GetOpts{
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

func objectCopyMetaRequest(fromHeaders *objects.GetHeader, fromMeta map[string]string, toHeaders *objects.GetHeader, toMeta map[string]string) *objects.UpdateOpts {
	updateOpts := objects.UpdateOpts{
		Metadata:       make(map[string]string),
		RemoveMetadata: []string{},
	}

	// handle object expiration
	if !fromHeaders.DeleteAt.IsZero() {
		epoch := fromHeaders.DeleteAt.Unix()
		updateOpts.DeleteAt = &epoch
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

	return &updateOpts
}
