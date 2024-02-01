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
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"
)

func (s *svc) FinishReplicationSwitch(ctx context.Context, t *asynq.Task) error {
	var p tasks.FinishReplicationSwitchPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("FinishReplicationSwitchPayload Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Bucket)
	logger := zerolog.Ctx(ctx)

	replSwitch, err := s.policySvc.GetReplicationSwitch(ctx, p.User, p.Bucket)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			logger.Warn().Msg("unable to finish replication switch task: not found")
			return nil
		}
		return err
	}
	if replSwitch.IsDone {
		logger.Info().Msg("skip finish replication switch task: already done")
		return nil
	}

	for oldFollower := range replSwitch.GetOldFollowers() {
		oldRepl, err := s.policySvc.GetReplicationPolicyInfo(ctx, p.User, p.Bucket, replSwitch.OldMain, oldFollower)
		if err != nil {
			if errors.Is(err, dom.ErrNotFound) {
				continue
			}
			return err
		}
		if oldRepl.EventsDone < oldRepl.Events {
			return &dom.ErrRateLimitExceeded{RetryIn: s.conf.SwitchRetryInterval}
		}
		existsUploads, err := s.storageSvc.ExistsUploads(ctx, p.User, p.Bucket)
		if err != nil {
			return err
		}
		if existsUploads {
			return &dom.ErrRateLimitExceeded{RetryIn: s.conf.SwitchRetryInterval}
		}
	}
	err = s.policySvc.ReplicationSwitchDone(ctx, p.User, p.Bucket)
	if err != nil {
		return err
	}
	logger.Info().Msg("replication switch is done")
	return nil
}
