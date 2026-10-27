package router

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/s3"
)

type objVersionStub struct {
	meta.VersionService
	ver meta.Version
}

func (s objVersionStub) GetObj(context.Context, entity.UniversalReplicationID, dom.Object) (meta.Version, error) {
	return s.ver, nil
}

// The replication policy is removed when a zero-downtime switch starts, so the
// replication list in ctx is empty while the switch is in progress.
func TestAdjustObjReadRouteDuringZeroDowntimeSwitch(t *testing.T) {
	replID := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User: "user", FromStorage: "old", FromBucket: "bucket", ToStorage: "new", ToBucket: "bucket",
	})
	sw := entity.ReplicationSwitchInfo{
		ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: time.Hour},
	}
	sw.SetReplicationID(replID)

	ctx := xctx.SetInProgressZeroDowntime(context.Background(), sw)
	ctx = xctx.SetMethod(ctx, s3.GetObject)
	ctx = xctx.SetBucket(ctx, "bucket")
	ctx = xctx.SetObject(ctx, "obj")

	tests := []struct {
		name string
		ver  meta.Version
		want string
	}{
		{name: "not yet replicated: read from old main", ver: meta.Version{From: 2, To: 1}, want: "old"},
		{name: "replicated: read from new main", ver: meta.Version{From: 2, To: 2}, want: "new"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &s3Router{versionSvc: objVersionStub{ver: tt.ver}}
			got, err := r.adjustObjReadRoute(ctx, "new")
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}

	t.Run("routed storage is not part of switched replication", func(t *testing.T) {
		r := &s3Router{versionSvc: objVersionStub{ver: meta.Version{From: 2, To: 1}}}
		_, err := r.adjustObjReadRoute(ctx, "other")
		require.ErrorIs(t, err, dom.ErrInternal)
	})
}
