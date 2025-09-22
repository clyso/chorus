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

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/lock"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"
)

func (s *svc) HandleObjectDelete(ctx context.Context, t *asynq.Task) (err error) {
	var p tasks.SwiftObjectDeletePayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("ObjectDeletePayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
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

	release, refresh, err := s.locker.Lock(ctx, lock.ObjKey(p.ToStorage, dom.Object{
		Bucket: toBucket,
		Name:   p.Object,
	}))
	if err != nil {
		return err
	}
	defer release()
	err = lock.WithRefresh(ctx, func() error {
		return s.handleObjectDelete(ctx, p)
	}, refresh, time.Second*2)

	return
}

// handleObjectMetaUpdate handles the object metadata update task, copying metadata from the source object to the destination object.
func (s *svc) handleObjectDelete(ctx context.Context, p tasks.SwiftObjectDeletePayload) (err error) {
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
	// check that object was deleted from source:
	err = objects.Get(ctx, fromClient, fromBucket, p.Object, objects.GetOpts{Newest: true}).Err
	if !gophercloud.ResponseCodeIs(err, http.StatusNotFound) {
		logger.Info().Msgf("object %q in container %q was not deleted from source, skip delete object task", p.Object, fromBucket)
		return nil
	}
	deleteOpts := objects.DeleteOpts{
		ObjectVersionID: p.VersionID,
	}
	if p.DeleteMultipart {
		// if delete multipart is set, we delete the object with all its parts
		deleteOpts.MultipartManifest = "delete"
	}
	err = objects.Delete(ctx, toClient, toBucket, p.Object, deleteOpts).Err
	if gophercloud.ResponseCodeIs(err, http.StatusNotFound) {
		// object already deleted in destination, no error
		return nil
	}
	if gophercloud.ResponseCodeIs(err, http.StatusOK) {
		// rgw may retun 200 OK for delete, which is unexpected for gophercloud
		return nil
	}
	return err
}
