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
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/tasks"
)

func (s *svc) HandleZeroDowntimeReplicationSwitch(ctx context.Context, t *asynq.Task) error {
	var p tasks.ZeroDowntimeReplicationSwitchPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("ZeroDowntimeReplicationSwitchPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	// switch not allowed for custom bucket name:
	if p.FromBucket != p.ToBucket {
		// should never happen
		return fmt.Errorf("invalid switch with downtime task: from and to bucket must be equal: %w", asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.FromBucket)
	policyID := p.GetReplicationID()

	// acquire exclusive lock to switch task:
	lock, err := s.replicationstatusLocker.Lock(ctx, policyID)
	if err != nil {
		return fmt.Errorf("unable to create lock: %w", err)
	}
	defer lock.Release(ctx)
	return lock.Do(ctx, time.Second*2, func() error {
		return s.handleZeroDowntimeReplicationSwitch(ctx, p)
	})
}

func (s *svc) handleZeroDowntimeReplicationSwitch(ctx context.Context, p tasks.ZeroDowntimeReplicationSwitchPayload) error {
	// get latest replication and switch state and execute switch state machine:
	replicationID := p.GetReplicationID()

	replStatus, err := s.policySvc.GetReplicationPolicyInfoExtended(ctx, replicationID)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			zerolog.Ctx(ctx).Err(err).Msg("drop switch with downtime task: replication metadata was deleted")
			return nil
		}
		return err
	}
	if !replStatus.IsArchived {
		zerolog.Ctx(ctx).Error().Msg("invalid replication state: replication is not archived")
	}
	switchPolicy, err := s.policySvc.GetReplicationSwitchInfo(ctx, replicationID)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			zerolog.Ctx(ctx).Error().Msg("drop switch with downtime task: switch metadata was deleted")
			return nil
		}
		return err
	}
	if !switchPolicy.IsZeroDowntime() {
		// wrong switch type - drop task
		// should never happen
		zerolog.Ctx(ctx).Error().Msg("drop switch with downtime task: switch is not switch with downtime")
		return nil
	}
	done := replStatus.InitDone() && replStatus.EventMigration.Unprocessed == 0
	// check if replication switch can be finished:
	if !done {
		// events queue is not drained yet - retry later
		return &dom.ErrRateLimitExceeded{RetryIn: s.conf.SwitchRetryInterval}
	}
	existsUploads, err := s.storageSvc.ExistsUploads(ctx, p.User, p.FromBucket)
	if err != nil {
		return err
	}
	if existsUploads {
		// there are pending multipart uploads - retry later
		return &dom.ErrRateLimitExceeded{RetryIn: s.conf.SwitchRetryInterval}
	}
	// all good - finish zero downtime replication switch:

	return s.policySvc.CompleteZeroDowntimeReplicationSwitch(ctx, replicationID)
}
