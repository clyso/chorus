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
	task := tasks.ObjectDeletePayload{
		Object: dom.Object{
			Bucket: n.Bucket,
			Name:   n.ObjKey,
		},
		Sync: tasks.Sync{
			FromStorage: h.fromStorage,
		},
	}
	return h.replSvc.Replicate(ctx, &task)
}
