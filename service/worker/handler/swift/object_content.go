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
	"strings"
	"time"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/lock"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/swift"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"
)

func (s *svc) HandleObjectUpdate(ctx context.Context, t *asynq.Task) (err error) {
	// setup:
	var p tasks.SwiftObjectUpdatePayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("ObjectUpdatePayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	logger := zerolog.Ctx(ctx)
	toBucket := p.Bucket
	if p.ToBucket != nil {
		toBucket = *p.ToBucket
	}

	// check if replication policy is paused or removed:
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

	// check rate limits:
	if err = s.limit.StorReq(ctx, p.FromStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.FromStorage).Msg("rate limit error")
		return err
	}
	if err = s.limit.StorReq(ctx, p.ToStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ToStorage).Msg("rate limit error")
		return err
	}

	// acquire lock for object update
	release, refresh, err := s.locker.Lock(ctx, lock.ObjKey(p.ToStorage, dom.Object{
		Bucket: toBucket,
		Name:   p.Object,
	}))
	if err != nil {
		return err
	}
	defer release()

	// sync object:
	err = lock.WithRefresh(ctx, func() error {
		return s.handleObjectUpdate(ctx, p)
	}, refresh, time.Second*2)
	if err != nil {
		return err
	}

	// update replication progress:
	metaErr := s.policySvc.IncReplEventsDone(ctx, p.FromAccount, p.Bucket, p.FromStorage, p.ToStorage, p.ToBucket, p.CreatedAt)
	if metaErr != nil {
		logger.Err(metaErr).Msg("unable to inc processed events")
	}

	return nil
}

func (s *svc) handleObjectUpdate(ctx context.Context, p tasks.SwiftObjectUpdatePayload) (err error) {
	logger := zerolog.Ctx(ctx)
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
