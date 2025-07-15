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

	"github.com/hibiken/asynq"
	"github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/storage"
	"github.com/clyso/chorus/pkg/tasks"
)

const (
	CChorusSourceVersionIDMetaHeader = "x-amz-meta-chorus-source-version-id"
)

type ListObjectVersionsAction struct {
	logger  *zerolog.Logger
	client  s3client.Client
	storage storage.Service
	bucket  string
	object  string
}

func NewListObjectVersionsAction(ctx context.Context, s *svc, payload tasks.ListObjectVersionsPayload) (*ListObjectVersionsAction, error) {
	ctx = log.WithBucket(ctx, payload.Bucket)
	logger := zerolog.Ctx(ctx)

	fromClient, err := s.clients.GetByName(ctx, payload.FromStorage)
	if err != nil {
		return nil, fmt.Errorf("unable to get %s storage client: %w", payload.FromStorage, err)
	}

	if err := s.limit.StorReq(ctx, payload.FromStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, payload.FromStorage).Msg("rate limit error")
		return nil, fmt.Errorf("unable to get storage rate limit: %w", err)
	}

	return &ListObjectVersionsAction{
		logger:  logger,
		client:  fromClient,
		storage: s.storageSvc,
		bucket:  payload.Bucket,
		object:  payload.Prefix,
	}, nil
}

func (r *ListObjectVersionsAction) Run(ctx context.Context) error {
	lastListedVersion, err := r.storage.GetLastListedObjectVersion(ctx, r.bucket, r.bucket)
	if err != nil {
		return fmt.Errorf("unable to get last listed version for object: %w", err)
	}

	var shouldSkipFirstListings bool
	if lastListedVersion != "" {
		shouldSkipFirstListings = true
	}

	objects := r.client.S3().ListObjects(ctx, r.bucket, minio.ListObjectsOptions{
		WithVersions: true,
		Prefix:       r.object,
	})

	var objectsListed uint64
	maxBufferedObjectVersions := 1000
	objectVersions := []string{}

	for object := range objects {
		if object.Err != nil {
			if err := r.storage.StoreObjectVersions(ctx, r.bucket, r.object, objectVersions...); err != nil {
				return fmt.Errorf("unable to store listed versions for object: %w", err)
			}
			return fmt.Errorf("unable to list versions: %w", object.Err)
		}
		// s3 library is not allowing to pass version id to list function
		// therefore list should be started over
		if shouldSkipFirstListings {
			if object.VersionID == lastListedVersion {
				shouldSkipFirstListings = false
			}
			continue
		}

		objectVersions = append(objectVersions, object.VersionID)
		objectsListed++

		if len(objectVersions) < maxBufferedObjectVersions {
			continue
		}

		if err = r.storage.StoreObjectVersions(ctx, r.bucket, r.object, objectVersions...); err != nil {
			return fmt.Errorf("unable to store listed versions for object: %w", err)
		}

		objectVersions = []string{}
	}

	if len(objectVersions) > 0 {
		if err = r.storage.StoreObjectVersions(ctx, r.bucket, r.object, objectVersions...); err != nil {
			return fmt.Errorf("unable to store listed versions for object: %w", err)
		}
	}

	return nil
}

type VersionedMigrationCtrl struct {
}

func (r *VersionedMigrationCtrl) HandleObjectVersionList(ctx context.Context, t *asynq.Task) error {
	var listVersionsPayload tasks.ListObjectVersionsPayload
	if err := json.Unmarshal(t.Payload(), &listVersionsPayload); err != nil {
		return fmt.Errorf("unable to unmarshal payload: %w", err)
	}

	var action ListObjectVersionsAction
	if err := action.Run(ctx); err != nil {
		return fmt.Errorf("unable to list obejct versions: %w", err)
	}

	return nil
}

// func (r *VersionedMigrationCtrl) f() error {
// 	ctx = log.WithBucket(ctx, listVersionsPayload.Bucket)
// 	logger := zerolog.Ctx(ctx)

// 	if err := s.limit.StorReq(ctx, listVersionsPayload.FromStorage); err != nil {
// 		logger.Debug().Err(err).Str(log.Storage, listVersionsPayload.FromStorage).Msg("rate limit error")
// 		return fmt.Errorf("unable to get storage rate limit: %w", err)
// 	}

// 	fromClient, err := s.clients.GetByName(ctx, listVersionsPayload.FromStorage)
// 	if err != nil {
// 		return fmt.Errorf("unable to get %s storage client: %w", listVersionsPayload.FromStorage, err)
// 	}

// 	lastListedVersion, err := s.storageSvc.GetLastListedObjectVersion(ctx, listVersionsPayload.Bucket, listVersionsPayload.Prefix)
// 	if err != nil {
// 		return fmt.Errorf("unable to get last listed version for object: %w", err)
// 	}

// 	var shouldSkipFirstListings bool
// 	if lastListedVersion != "" {
// 		shouldSkipFirstListings = true
// 	}

// 	objects := fromClient.S3().ListObjects(ctx, listVersionsPayload.Bucket, minio.ListObjectsOptions{
// 		WithVersions: true,
// 		Prefix:       listVersionsPayload.Prefix,
// 	})

// 	var objectsListed uint64
// 	maxBufferedObjectVersions := 1000
// 	objectVersions := []string{}
// 	for object := range objects {
// 		if object.Err != nil {
// 			if err = s.storageSvc.StoreObjectVersions(ctx, listVersionsPayload.Bucket, listVersionsPayload.Prefix, objectVersions...); err != nil {
// 				return fmt.Errorf("unable to store listed versions for object: %w", err)
// 			}
// 			return fmt.Errorf("unable to list versions: %w", object.Err)
// 		}
// 		// s3 library is not allowing to pass version id to list function
// 		// therefore list should be started over
// 		if shouldSkipFirstListings {
// 			if object.VersionID == lastListedVersion {
// 				shouldSkipFirstListings = false
// 			}
// 			continue
// 		}

// 		objectVersions = append(objectVersions, object.VersionID)
// 		objectsListed++

// 		if len(objectVersions) < maxBufferedObjectVersions {
// 			continue
// 		}

// 		if err = s.storageSvc.StoreObjectVersions(ctx, listVersionsPayload.Bucket, listVersionsPayload.Prefix, objectVersions...); err != nil {
// 			return fmt.Errorf("unable to store listed versions for object: %w", err)
// 		}

// 		objectVersions = []string{}
// 	}

// 	if len(objectVersions) > 0 {
// 		if err = s.storageSvc.StoreObjectVersions(ctx, listVersionsPayload.Bucket, listVersionsPayload.Prefix, objectVersions...); err != nil {
// 			return fmt.Errorf("unable to store listed versions for object: %w", err)
// 		}
// 	}

// 	err = s.policySvc.IncReplInitObjListed(ctx, xctx.GetUser(ctx), listVersionsPayload.Bucket, listVersionsPayload.FromStorage, listVersionsPayload.ToStorage, listVersionsPayload.ToBucket, 0, listVersionsPayload.GetDate())
// 	if err != nil {
// 		return fmt.Errorf("migration bucket list obj: unable to inc obj listed meta: %w", err)
// 	}

// 	migratePayload := tasks.MigrateVersionedObjectPayload{}

// 	_, err = tasks.NewTask(ctx, migratePayload)
// 	if err != nil {
// 		return fmt.Errorf("unable to enqueue consistency obj task: %w", err)
// 	}

// 	return nil
// }

type Action[T any] func(ctx context.Context, sharedState T) (Action[T], error)

func RunActions[T any](ctx context.Context, sharedState T, initAction Action[T]) error {
	currentAction := initAction
	for currentAction != nil {
		nextAction, err := currentAction(ctx, sharedState)
		if err != nil {
			return fmt.Errorf("unable to execute state: %w", err)
		}
		currentAction = nextAction
	}
	return nil
}

type VersionedObjectCopyState struct {
	logger            *zerolog.Logger
	user              string
	fromClient        s3client.Client
	fromBucket        string
	fromStorage       string
	toClient          s3client.Client
	toBucket          string
	toStorage         string
	objectName        string
	startVersionIndex int64
}

func (s *svc) SequentialCopyAction(ctx context.Context, state *VersionedObjectCopyState) (Action[*VersionedObjectCopyState], error) {
	maxVersionsFetched := int64(100)

	startIndex := state.startVersionIndex
	endIndex := state.startVersionIndex + maxVersionsFetched - 1
	versions, err := s.storageSvc.GetObjectVersions(ctx, state.fromBucket, state.objectName, startIndex, endIndex)
	if err != nil {
		return nil, fmt.Errorf("unable to fetch versions: %w", err)
	}

	for len(versions) > 0 {
		startIndex = endIndex + 1
		endIndex += maxVersionsFetched

		for range versions {
			// err := s.copyObject(ctx, state, version)
			if errors.Is(err, dom.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("unable to copy object: %w", err)
			}
		}

		versions, err = s.storageSvc.GetObjectVersions(ctx, state.fromBucket, state.objectName, startIndex, endIndex)
		if err != nil {
			return nil, fmt.Errorf("unable to fetch versions: %w", err)
		}
	}

	return nil, nil
}

func (s *svc) CheckObjectAction(ctx context.Context, state *VersionedObjectCopyState) (Action[*VersionedObjectCopyState], error) {
	var destinationIsEmpty bool
	objectStat, err := state.toClient.S3().StatObject(ctx, state.toBucket, state.objectName, minio.StatObjectOptions{})
	if err != nil {
		var errorResp minio.ErrorResponse
		ok := errors.As(err, &errorResp)
		if !ok {
			return nil, fmt.Errorf("unable to cast error to s3 error %w", err)
		}
		if errorResp.StatusCode == http.StatusNotFound {
			destinationIsEmpty = true
		}
	}

	if destinationIsEmpty {
		return s.SequentialCopyAction, nil
	}

	sourceVersionID := objectStat.Metadata.Get(CChorusSourceVersionIDMetaHeader)
	if sourceVersionID == "" {
		return s.RemoveObjectAction, nil
	}

	sourceVersionIndex, err := s.storageSvc.FindObjectVersionIndex(ctx, state.toBucket, state.objectName, sourceVersionID)
	if err != nil {
		return nil, fmt.Errorf("unable to find source version index: %w", err)
	}
	if sourceVersionIndex == -1 {
		return s.RemoveObjectAction, nil
	}

	state.startVersionIndex = sourceVersionIndex + 1

	return s.SequentialCopyAction, nil
}

func (s *svc) GetRateLimitAction(ctx context.Context, state *VersionedObjectCopyState) (Action[*VersionedObjectCopyState], error) {
	if err := s.limit.StorReq(ctx, state.toStorage); err != nil {
		state.logger.Debug().Err(err).Str(log.Storage, state.toStorage).Msg("rate limit error")
		return nil, fmt.Errorf("unable to get storage rate limit: %w", err)
	}

	if err := s.limit.StorReq(ctx, state.fromStorage); err != nil {
		state.logger.Debug().Err(err).Str(log.Storage, state.fromStorage).Msg("rate limit error")
		return nil, fmt.Errorf("unable to get storage rate limit: %w", err)
	}

	return s.CheckObjectAction, nil
}

func (s *svc) RemoveObjectAction(ctx context.Context, state *VersionedObjectCopyState) (Action[*VersionedObjectCopyState], error) {
	if err := state.toClient.S3().RemoveObject(ctx, state.toBucket, state.objectName, minio.RemoveObjectOptions{}); err != nil {
		return nil, fmt.Errorf("unable to remove object: %w", err)
	}
	return s.SequentialCopyAction, nil
}

func (s *svc) migrateObjectVersion(ctx context.Context, migratePayload *tasks.MigrateVersionedObjectPayload, user string) error {
	ctx = log.WithBucket(ctx, migratePayload.Bucket)
	logger := zerolog.Ctx(ctx)

	fromClient, err := s.clients.GetByName(ctx, migratePayload.FromStorage)
	if err != nil {
		return fmt.Errorf("unable to get %s storage client: %w", migratePayload.FromStorage, err)
	}

	toClient, err := s.clients.GetByName(ctx, migratePayload.ToStorage)
	if err != nil {
		return fmt.Errorf("unable to get %s storage client: %w", migratePayload.FromStorage, err)
	}

	fromBucket := migratePayload.Bucket
	var toBucket string
	if migratePayload.ToBucket == "" {
		toBucket = fromBucket
	} else {
		toBucket = migratePayload.ToBucket
	}

	sharedState := &VersionedObjectCopyState{
		logger:            logger,
		user:              user,
		fromClient:        fromClient,
		toClient:          toClient,
		fromBucket:        fromBucket,
		toBucket:          toBucket,
		fromStorage:       migratePayload.FromStorage,
		toStorage:         migratePayload.ToStorage,
		objectName:        migratePayload.Prefix,
		startVersionIndex: 0,
	}

	if err := RunActions(ctx, sharedState, s.GetRateLimitAction); err != nil {
		return fmt.Errorf("unable to run migration actions: %w", err)
	}

	return nil
}

func (s *svc) HandleVersionedObjectMigration(ctx context.Context, t *asynq.Task) error {
	var migratePayload tasks.MigrateVersionedObjectPayload
	if err := json.Unmarshal(t.Payload(), &migratePayload); err != nil {
		return fmt.Errorf("unable to unmarshal payload: %w", err)
	}

	user := xctx.GetUser(ctx)

	if err := s.migrateObjectVersion(ctx, &migratePayload, user); err != nil {
		return fmt.Errorf("unable to migrate object version: %w", err)
	}

	return nil
}
