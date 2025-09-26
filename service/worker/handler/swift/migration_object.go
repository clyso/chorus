package swift

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/clyso/chorus/pkg/entity"
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

	// check rate limits:
	if err = s.limit.StorReq(ctx, p.ID.FromStorage()); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.FromStorage()).Msg("rate limit error")
		return err
	}
	if err = s.limit.StorReq(ctx, p.ID.ToStorage()); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.ToStorage()).Msg("rate limit error")
		return err
	}

	// acquire lock for object update
	lock, err := s.objectLocker.Lock(ctx, entity.NewObjectLockID(p.ID.FromStorage(), p.Bucket, p.ObjName))
	if err != nil {
		return err
	}
	defer lock.Release(context.Background())

	// sync object:
	return lock.Do(ctx, time.Second*2, func() error {
		task := tasks.SwiftObjectUpdatePayload{
			Bucket:       p.Bucket,
			Object:       p.ObjName,
			VersionID:    p.ObjVersion,
			Etag:         p.ObjEtag,
			LastModified: p.ObjLastModified,
		}
		task.SetReplicationID(p.ID)
		return s.ObjectUpdate(ctx, task)
	})
}
