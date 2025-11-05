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
	"fmt"
	"net/http"
	"strings"
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

func (s *svc) HandleObjectUpdate(ctx context.Context, t *asynq.Task) (err error) {
	// setup:
	var p tasks.SwiftObjectUpdatePayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("ObjectUpdatePayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	logger := zerolog.Ctx(ctx)

	// check rate limits:
	if err = s.limit.StorReq(ctx, p.ID.FromStorage()); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.FromStorage()).Msg("rate limit error")
		return err
	}
	if err = s.limit.StorReq(ctx, p.ID.ToStorage()); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.ToStorage()).Msg("rate limit error")
		return err
	}

	// acquire lock for object update
	lock, err := s.objectLocker.Lock(ctx, entity.NewObjectLockID(p.ID.FromStorage(), p.Bucket, p.Object))
	if err != nil {
		return err
	}
	defer lock.Release(context.Background())

	// sync object:
	return lock.Do(ctx, time.Second*2, func() error {
		return s.ObjectUpdate(ctx, p)
	})
}

func (s *svc) ObjectUpdate(ctx context.Context, p tasks.SwiftObjectUpdatePayload) (err error) {
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
	// head object
	res := objects.Get(ctx, fromClient, fromBucket, p.Object, objects.GetOpts{})
	if res.Err != nil {
		if gophercloud.ResponseCodeIs(res.Err, http.StatusNotFound) {
			// object was deleted from source, skip update
			// object deletion will be handled by the object deletion task
			logger.Info().Msgf("object %q in container %q was deleted from source, skip update object content task", p.Object, fromBucket)
			return nil
		}
		return res.Err
	}
	fromHeaders, err := res.Extract()
	if err != nil {
		return fmt.Errorf("failed to extract source object %q headers: %w", p.Object, err)
	}
	fromMeta, err := res.ExtractMetadata()
	if err != nil {
		return fmt.Errorf("failed to extract source object %q metadata: %w", p.Object, err)
	}

	// check if object was synced to swift
	if taskLastModified, err := time.Parse(time.RFC3339, p.LastModified); err == nil {
		if fromHeaders.LastModified.Before(taskLastModified) {
			// retry later
			return &dom.ErrRateLimitExceeded{RetryIn: s.conf.SwiftRetryInterval}
		}
	}
	toReq := objects.CreateOpts{
		Content:         nil,
		Metadata:        fromMeta,
		ContentEncoding: fromHeaders.ContentEncoding,
		ContentType:     fromHeaders.ContentType,
		NoETag:          true,
	}
	if !fromHeaders.DeleteAt.IsZero() {
		toReq.DeleteAt = fromHeaders.DeleteAt.Unix()
	}
	isSlo := false
	if dlo := res.Header.Get("X-Object-Manifest"); dlo != "" {
		// don't fetch object body for DLO
		toReq.ObjectManifest = dlo
	} else {
		//fetch object body from source
		fromReq := &swift.DownloadOpts{
			DownloadOpts: &objects.DownloadOpts{
				IfModifiedSince: fromHeaders.LastModified.Add(-time.Second),
			},
		}
		isSlo = strings.EqualFold(res.Header.Get("X-Static-Large-Object"), "True")
		if isSlo {
			// fetch manifest for SLO
			fromReq.MultipartManifest = "get"
			fromReq.Raw = true // fetch raw content for SLO
			// put manifest to dest
			toReq.MultipartManifest = "put"
		}
		fromObjContent := objects.Download(ctx, fromClient, fromBucket, p.Object, fromReq)
		if fromObjContent.Err != nil {
			if gophercloud.ResponseCodeIs(fromObjContent.Err, http.StatusNotModified) {
				// retry later
				return &dom.ErrRateLimitExceeded{RetryIn: s.conf.SwiftRetryInterval}
			}
			return fmt.Errorf("failed to download source object %q: %w", p.Object, fromObjContent.Err)
		}
		defer fromObjContent.Body.Close()
		res, err := fromObjContent.Extract()
		if err != nil {
			return fmt.Errorf("failed to extract source object %q content: %w", p.Object, err)
		}
		toReq.ContentLength = res.ContentLength
		toReq.ContentType = res.ContentType
		toReq.Content = fromObjContent.Body
		// set ETag if not a manifest
		if !isSlo {
			toReq.NoETag = false
			toReq.ETag = res.ETag
			toReq.IfNoneMatch = res.ETag
		}
	}
	// upload to destination
	err = objects.Create(ctx, toClient, toBucket, p.Object, toReq).Err
	if err != nil {
		if gophercloud.ResponseCodeIs(err, http.StatusPreconditionFailed) {
			// object with given ETag already exists. Skip update.
			logger.Info().Err(err).Msgf("object %q in container %q already exists with the same etag", p.Object, toBucket)
			return nil
		}
		if gophercloud.ResponseCodeIs(err, http.StatusNotModified) {
			// object with given ETag already exists. Skip update.
			logger.Info().Err(err).Msgf("object %q in container %q already exists with the same etag", p.Object, toBucket)
			return nil
		}
		if isSlo && gophercloud.ResponseCodeIs(err, http.StatusNotFound) {
			// SLO failed because manifest parts are not copied yet. Try again later.
			return &dom.ErrRateLimitExceeded{RetryIn: s.conf.SwiftRetryInterval}
		}
		return fmt.Errorf("failed to upload object %q to container %q: %w", p.Object, toBucket, err)
	}
	return nil
}
