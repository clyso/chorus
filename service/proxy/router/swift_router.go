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

package router

import (
	"fmt"
	"net/http"

	"github.com/hibiken/asynq"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/storage"
	"github.com/clyso/chorus/pkg/swift"
	"github.com/clyso/chorus/pkg/tasks"
)

func NewSwiftRouter(
	versionSvc meta.VersionService,
	policySvc policy.Service,
	storageSvc storage.Service,
	limit ratelimit.RPM) Router {
	return &swiftRouter{
		versionSvc: versionSvc,
		policySvc:  policySvc,
		storageSvc: storageSvc,
		limit:      limit,
	}
}

type swiftRouter struct {
	versionSvc meta.VersionService
	policySvc  policy.Service
	storageSvc storage.Service
	limit      ratelimit.RPM
}

func (r *swiftRouter) Route(req *http.Request) (resp *http.Response, taskList []tasks.SyncTask, storage string, isApiErr bool, err error) {
	var (
		ctx     = req.Context()
		method  = xctx.GetSwiftMethod(req.Context())
		account = xctx.GetAccount(ctx)
		bucket  = xctx.GetBucket(ctx)
		object  = xctx.GetObject(ctx)
		task    tasks.SyncTask
	)

	switch method {
	case swift.GetInfo, swift.GetEndpoints:
		// forward to main storage
	case swift.GetAccount:
		// forward to main acc routing policy
	case swift.GetContainer:
		// forward to main bucket routing policy
	case swift.GetObject:
		// forward to main object routing policy

	case swift.PostAccount:
	case swift.HeadAccount:
	case swift.DeleteAccount:
	case swift.PutContainer:
	case swift.PostContainer:
	case swift.HeadContainer:
	case swift.DeleteContainer:
	case swift.PutObject:
	case swift.CopyObject:
	case swift.DeleteObject:
	case swift.HeadObject:
	case swift.PostObject:
	case swift.UndefinedMethod:
		// forward to main storage
	default:
	}

	if err == nil && task != nil {
		task.SetFrom(storage)
		if taskList == nil {
			taskList = []tasks.SyncTask{task}
		}
	}

	return
}
