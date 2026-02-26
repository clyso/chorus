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

	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/swift"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/service/worker/handler"
)

type svc struct {
	clients              objstore.Clients
	bucketListStateStore *store.MigrationBucketListStateStore
	objectListStateStore *store.MigrationObjectListStateStore
	queueSvc             tasks.QueueService
	limit                ratelimit.RPM
	objectLocker         *store.ObjectLocker
	bucketLocker         *store.BucketLocker
	userLocker           *store.UserLocker
	conf                 *handler.Config
}

func New(conf *handler.Config, clients objstore.Clients, bucketListStateStore *store.MigrationBucketListStateStore,
	objectListStateStore *store.MigrationObjectListStateStore, queueSvc tasks.QueueService,
	limit ratelimit.RPM, objectLocker *store.ObjectLocker, userLocker *store.UserLocker,
	bucketLocker *store.BucketLocker) *svc {
	return &svc{
		conf:                 conf,
		clients:              clients,
		bucketListStateStore: bucketListStateStore,
		objectListStateStore: objectListStateStore,
		queueSvc:             queueSvc,
		limit:                limit,
		userLocker:           userLocker,
		objectLocker:         objectLocker,
		bucketLocker:         bucketLocker,
	}
}

func (s *svc) rateLimit(ctx context.Context, storage string, methods ...swift.Method) error {
	if len(methods) == 0 {
		return nil
	}
	opts := make([]ratelimit.Opt, 0, len(methods))
	for _, m := range methods {
		opts = append(opts, ratelimit.SwiftMethod(m))
	}
	return s.limit.StorReqN(ctx, storage, len(methods), opts...)
}
