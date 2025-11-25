/*
 * Copyright © 2024 Clyso GmbH
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

package handler

import (
	"context"
	"fmt"
	"time"

	"github.com/hibiken/asynq"

	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/rclone"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/storage"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
)

type Config struct {
	SwiftRetryInterval  time.Duration `yaml:"swiftRetryInterval"`
	PauseRetryInterval  time.Duration `yaml:"pauseRetryInterval"`
	SwitchRetryInterval time.Duration `yaml:"switchRetryInterval"`
	QueueUpdateInterval time.Duration `yaml:"queueUpdateInterval"`
}

type svc struct {
	clients                 objstore.Clients
	versionSvc              meta.VersionService
	listStateStore          *store.MigrationObjectListStateStore
	uploadSvc               *storage.UploadSvc
	rc                      rclone.Service
	queueSvc                tasks.QueueService
	limit                   ratelimit.RPM
	objectLocker            *store.ObjectLocker
	bucketLocker            *store.BucketLocker
	replicationstatusLocker *store.ReplicationStatusLocker
	conf                    *Config
	rclone.CopySvc
}

func New(conf *Config, clients objstore.Clients, versionSvc meta.VersionService,
	rc rclone.Service, queueSvc tasks.QueueService, uploadSvc *storage.UploadSvc,
	limit ratelimit.RPM, listStateStore *store.MigrationObjectListStateStore,
	objectLocker *store.ObjectLocker, bucketLocker *store.BucketLocker,
	replicationstatusLocker *store.ReplicationStatusLocker) *svc {
	return &svc{
		conf:                    conf,
		clients:                 clients,
		versionSvc:              versionSvc,
		listStateStore:          listStateStore,
		uploadSvc:               uploadSvc,
		rc:                      rc,
		queueSvc:                queueSvc,
		limit:                   limit,
		objectLocker:            objectLocker,
		bucketLocker:            bucketLocker,
		replicationstatusLocker: replicationstatusLocker,
	}
}

func (s *svc) getClients(ctx context.Context, user, fromStorage, toStorage string) (fromClient s3client.Client, toClient s3client.Client, err error) {
	fromClient, err = s.clients.AsS3(ctx, fromStorage, user)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get %q s3 client: %w: %w", fromStorage, err, asynq.SkipRetry)
	}

	toClient, err = s.clients.AsS3(ctx, toStorage, user)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get %q s3 client: %w: %w", toStorage, err, asynq.SkipRetry)
	}
	return
}
