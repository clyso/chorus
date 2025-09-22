package swift

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/lock"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"
)

func (s *svc) HandleSwiftObjectMigration(ctx context.Context, t *asynq.Task) (err error) {
	// setup:
	var p tasks.SwiftObjectMigrationPayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("SwiftObjectMigrationPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	logger := zerolog.Ctx(ctx)

	// check if replication policy is paused or removed:
	paused, err := s.policySvc.IsReplicationPolicyPaused(ctx, p.FromAccount, p.FromContaier, p.FromStorage, p.ToStorage, &p.ToContaier)
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

	// check rate limits:
	if err = s.limit.StorReq(ctx, p.FromStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.FromStorage).Msg("rate limit error")
		return err
	}
	if err = s.limit.StorReq(ctx, p.ToStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ToStorage).Msg("rate limit error")
		return err
	}

	// acquire lock for object update
	release, refresh, err := s.locker.Lock(ctx, lock.ObjKey(p.ToStorage, dom.Object{
		Bucket: p.ToContaier,
		Name:   p.ObjName,
	}))
	if err != nil {
		return err
	}
	defer release()

	// sync object:
	err = lock.WithRefresh(ctx, func() error {
		return s.handleObjectUpdate(ctx, tasks.SwiftObjectUpdatePayload{
			Sync: tasks.Sync{
				FromStorage: p.FromStorage,
				FromAccount: p.FromAccount,
				ToStorage:   p.ToStorage,
				ToAccount:   p.ToAccount,
				ToBucket:    &p.ToContaier,
				CreatedAt:   time.Now(),
			},
			Bucket:       p.FromContaier,
			Object:       p.ObjName,
			VersionID:    p.ObjVersion,
			Etag:         p.ObjEtag,
			LastModified: p.ObjLastModified,
		})
	}, refresh, time.Second*2)
	if err != nil {
		return err
	}

	// update replication progress:
	metaErr := s.policySvc.IncReplInitObjDone(ctx, p.FromAccount, p.FromContaier, p.FromStorage, p.ToStorage, &p.ToContaier, p.ObjSize, time.Now())
	if metaErr != nil {
		logger.Err(metaErr).Msg("migration obj copy: unable to inc obj done meta")
	}

	return nil
}
