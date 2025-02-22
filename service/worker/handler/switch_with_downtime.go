package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/lock"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"
)

func (s *svc) SwitchWithDowntime(ctx context.Context, t *asynq.Task) error {
	var p tasks.SwitchWithDowntimePayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("SwitchWithDowntimePayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	taskID, ok := asynq.GetTaskID(ctx)
	if !ok {
		// should never happen
		return errors.New("task id not found")
	}
	// acquire switch lock
	release, refresh, err := s.locker.Lock(ctx, lock.StringKey(taskID))
	if err != nil {
		return err
	}
	defer release()

	policy, err := s.policySvc.GetReplicationSwitchWithDowntime(ctx, policy.ReplicationID{
		User:     p.User,
		Bucket:   p.Bucket,
		From:     p.FromStorage,
		To:       p.ToStorage,
		ToBucket: p.ToBucket,
	})
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			zerolog.Ctx(ctx).Err(err).Msg("drop switch with downtime task: switch metadata not found")
			return nil
		}
		return err
	}

}
