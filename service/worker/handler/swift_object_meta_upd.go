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

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/lock"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"
)

func (s *svc) HandleObjectMetaUpdate(ctx context.Context, t *asynq.Task) (err error) {
	var p tasks.ObjectMetaUpdatePayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("ObjectMetaUpdatePayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	logger := zerolog.Ctx(ctx)
	fromBucket, toBucket := p.Bucket, p.Bucket
	if p.ToBucket != nil {
		toBucket = *p.ToBucket
	}

	//TODO:swift support account-level repl policy
	// paused, err := s.policySvc.IsReplicationPolicyPaused(ctx, xctx.GetUser(ctx), p.Object.Bucket, p.FromStorage, p.ToStorage, p.ToBucket)
	// if err != nil {
	// 	if errors.Is(err, dom.ErrNotFound) {
	// 		zerolog.Ctx(ctx).Err(err).Msg("drop replication task: replication policy not found")
	// 		return nil
	// 	}
	// 	return err
	// }
	// if paused {
	// 	return &dom.ErrRateLimitExceeded{RetryIn: s.conf.PauseRetryInterval}
	// }

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
		//TODO:swift support account-level repl policy
		verErr := s.policySvc.IncReplEventsDone(ctx, xctx.GetUser(ctx), p.Object.Bucket, p.FromStorage, p.ToStorage, p.ToBucket, p.CreatedAt)
		if verErr != nil {
			zerolog.Ctx(ctx).Err(verErr).Msg("unable to inc processed events")
		}
	}()

	release, refresh, err := s.locker.Lock(ctx, lock.ObjKey(p.ToStorage, dom.Object{
		Bucket: toBucket,
		Name:   p.Object,
	}))
	if err != nil {
		return err
	}
	defer release()
	err = lock.WithRefresh(ctx, func() error {
		// setup swift clients:
		fromClient, err := s.swiftClients.For(ctx, p.FromStorage, p.FromAccount)
		if err != nil {
			return err
		}
		toClient, err := s.swiftClients.For(ctx, p.ToStorage, p.ToAccount)
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
	}, refresh, time.Second*2)

	return
}

func getSwiftObjectMeta(ctx context.Context, client *gophercloud.ServiceClient, containerName, objectName string) (headers *objects.GetHeader, meta map[string]string, err error) {
	res := objects.Get(ctx, client, containerName, objectName, objects.GetOpts{
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
