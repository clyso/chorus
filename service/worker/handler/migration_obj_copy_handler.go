package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/lock"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/rclone"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"
	"time"
)

func (s *svc) HandleMigrationObjCopy(ctx context.Context, t *asynq.Task) (err error) {
	var p tasks.MigrateObjCopyPayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("MigrateObjCopyPayload Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Bucket)
	ctx = log.WithObjName(ctx, p.Obj.Name)
	logger := zerolog.Ctx(ctx)

	paused, err := s.policySvc.IsReplicationPolicyPaused(ctx, xctx.GetUser(ctx), p.Bucket, p.FromStorage, p.ToStorage)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			zerolog.Ctx(ctx).Err(err).Msg("drop replication task: replication policy not found")
			return nil
		}
		return err
	}
	if paused {
		return &dom.ErrRateLimitExceeded{RetryIn: replicationPauseRetryInterval}
	}

	if err = s.limit.StorReq(ctx, p.FromStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.FromStorage).Msg("rate limit error")
		return err
	}
	if err = s.limit.StorReq(ctx, p.ToStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ToStorage).Msg("rate limit error")
		return err
	}

	domObj := dom.Object{
		Bucket:  p.Bucket,
		Name:    p.Obj.Name,
		Version: p.Obj.VersionID,
	}
	defer func() {
		// complete obj migration meta if not err:
		if err != nil {
			return
		}
		metaErr := s.policySvc.IncReplInitObjDone(ctx, xctx.GetUser(ctx), p.Bucket, p.FromStorage, p.ToStorage, p.Obj.Size, p.CreatedAt)
		if metaErr != nil {
			logger.Err(metaErr).Msg("migration obj copy: unable to inc obj done meta")
		}
	}()

	release, refresh, err := s.locker.Lock(ctx, lock.ObjKey(p.ToStorage, domObj))
	if err != nil {
		return err
	}
	defer release()
	objMeta, err := s.versionSvc.GetObj(ctx, domObj)
	if err != nil {
		return fmt.Errorf("migration obj copy: unable to get obj meta: %w", err)
	}
	fromVer, toVer := objMeta[p.FromStorage], objMeta[p.ToStorage]

	if fromVer != 0 && fromVer <= toVer {
		logger.Info().Int64("from_ver", fromVer).Int64("to_ver", toVer).Msg("migration obj copy: identical from/to obj version: skip copy")
		return nil
	}
	// 1. sync obj meta and content
	err = lock.WithRefresh(ctx, func() error {
		return s.rc.CopyTo(ctx, rclone.File{
			Storage: p.FromStorage,
			Bucket:  p.Bucket,
			Name:    p.Obj.Name,
		}, rclone.File{
			Storage: p.ToStorage,
			Bucket:  p.Bucket,
			Name:    p.Obj.Name,
		}, p.Obj.Size)
	}, refresh, time.Second*2)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			logger.Warn().Msg("migration obj copy: skip object sync: object missing in source")
			return nil
		}
		return fmt.Errorf("migration obj copy: unable to copy with rclone: %w", err)

	}

	fromClient, toClient, err := s.getClients(ctx, p.FromStorage, p.ToStorage)
	if err != nil {
		return fmt.Errorf("migration obj copy: unable to get %q s3 client: %v: %w", p.FromStorage, err, asynq.SkipRetry)
	}

	// 2. sync obj ACL
	err = s.syncObjectACL(ctx, fromClient, toClient, p.Bucket, p.Obj.Name)
	if err != nil {
		return err
	}

	// 3. sync obj tags
	err = s.syncObjectTagging(ctx, fromClient, toClient, p.Bucket, p.Obj.Name)
	if err != nil {
		return err
	}

	if fromVer != 0 {
		err = s.versionSvc.UpdateIfGreater(ctx, domObj, p.ToStorage, fromVer)
		if err != nil {
			return fmt.Errorf("migration obj copy: unable to update obj meta: %w", err)
		}
	}
	logger.Info().Msg("migration obj copy: done")

	return nil
}
