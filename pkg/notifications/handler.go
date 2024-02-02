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

package notifications

import (
	"context"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/replication"
	"github.com/clyso/chorus/pkg/tasks"
)

type Handler struct {
	fromStorage string

	replSvc replication.Service
}

func NewHandler(fromStorage string, replSvc replication.Service) *Handler {
	return &Handler{fromStorage: fromStorage, replSvc: replSvc}
}

type ObjCreated struct {
	Bucket  string
	ObjKey  string
	ObjETag string
	ObjSize int64
}

func (h *Handler) PutObject(ctx context.Context, n ObjCreated) error {

	task := tasks.ObjectSyncPayload{
		Object: dom.Object{
			Bucket: n.Bucket,
			Name:   n.ObjKey,
		},
		Sync: tasks.Sync{
			FromStorage: h.fromStorage,
		},
		ObjSize: n.ObjSize,
	}
	return h.replSvc.Replicate(ctx, &task)
}

type ObjDeleted struct {
	Bucket string
	ObjKey string
}

func (h *Handler) DeleteObject(ctx context.Context, n ObjDeleted) error {
	task := tasks.ObjectSyncPayload{
		Object: dom.Object{
			Bucket: n.Bucket,
			Name:   n.ObjKey,
		},
		Sync: tasks.Sync{
			FromStorage: h.fromStorage,
		},
		Deleted: true,
	}
	return h.replSvc.Replicate(ctx, &task)
}
