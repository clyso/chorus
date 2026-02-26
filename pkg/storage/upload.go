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

package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/validate"
)

type UploadSvc struct {
	store *store.UserUploadStore
}

func NewUploadSvc(client redis.Cmdable) *UploadSvc {
	return &UploadSvc{
		store: store.NewUserUploadStore(client),
	}
}

func (r *UploadSvc) StoreUpload(ctx context.Context, id entity.UserUploadObjectID,
	object entity.UserUploadObject, ttl time.Duration) error {
	if err := validate.UserUploadObjectID(id); err != nil {
		return fmt.Errorf("unable to validate user upload object id: %w", err)
	}
	if err := validate.UserUploadObject(object); err != nil {
		return fmt.Errorf("unable to validate user upload object: %w", err)
	}
	if _, err := r.store.Add(ctx, id, object); err != nil {
		return fmt.Errorf("unable to add user upload object: %w", err)
	}
	_, _ = r.store.SetTTL(ctx, id, ttl)
	return nil
}

func (r *UploadSvc) UploadExists(ctx context.Context, id entity.UserUploadObjectID,
	object entity.UserUploadObject) (bool, error) {
	if err := validate.UserUploadObjectID(id); err != nil {
		return false, fmt.Errorf("unable to validate user upload object id: %w", err)
	}
	if err := validate.UserUploadObject(object); err != nil {
		return false, fmt.Errorf("unable to validate user upload object: %w", err)
	}
	contains, err := r.store.IsMember(ctx, id, object)
	if err != nil {
		return false, fmt.Errorf("unable to check if upload exists: %w", err)
	}
	return contains, nil
}

func (r *UploadSvc) UploadsExistForUser(ctx context.Context, user string) (bool, error) {
	if user == "" {
		return false, fmt.Errorf("%w: user is required to set uploadID", dom.ErrInvalidArg)
	}
	contains, err := r.store.HasIDs(ctx, user)
	if err != nil {
		return false, fmt.Errorf("unable to check if user uploads exist: %w", err)
	}
	return contains, nil
}

func (r *UploadSvc) UploadsExistForUserBucket(ctx context.Context, id entity.UserUploadObjectID) (bool, error) {
	if err := validate.UserUploadObjectID(id); err != nil {
		return false, fmt.Errorf("unable to validate user upload object id: %w", err)
	}
	contains, err := r.store.NotEmpty(ctx, id)
	if err != nil {
		return false, fmt.Errorf("unable to check if user bucket uploads exist: %w", err)
	}
	return contains, nil
}

func (r *UploadSvc) DeleteUpload(ctx context.Context, id entity.UserUploadObjectID, object entity.UserUploadObject) error {
	if err := validate.UserUploadObjectID(id); err != nil {
		return fmt.Errorf("unable to validate user upload object id: %w", err)
	}
	if err := validate.UserUploadObject(object); err != nil {
		return fmt.Errorf("unable to validate user upload object: %w", err)
	}
	if _, err := r.store.Remove(ctx, id, object); err != nil {
		return fmt.Errorf("unable to remove upload: %w", err)
	}
	return nil
}
