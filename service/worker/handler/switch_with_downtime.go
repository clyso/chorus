// Copyright 2025 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/tasks"
)

// check mock values:
var (
	checkResultIsEqual, checkResultIsInProgress = true, false
)

func (s *svc) HandleSwitchWithDowntime(ctx context.Context, t *asynq.Task) error {
	var p tasks.SwitchWithDowntimePayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("SwitchWithDowntimePayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	policyID := p.ID

	lock, err := s.replicationstatusLocker.Lock(ctx, policyID)
	if err != nil {
		return err
	}
	defer lock.Release(ctx)

	switchFunc := func() error {
		// get latest replication and switch state and execute switch state machine:
		replStatus, err := s.policySvc.GetReplicationPolicyInfoExtended(ctx, policyID)
		if err != nil {
			if errors.Is(err, dom.ErrNotFound) {
				zerolog.Ctx(ctx).Err(err).Msg("drop switch with downtime task: replication metadata was deleted")
				return nil
			}
			return err
		}
		if replStatus.IsPaused {
			// replication is paused - retry later
			return &dom.ErrRateLimitExceeded{RetryIn: s.conf.PauseRetryInterval}
		}
		switchPolicy, err := s.policySvc.GetReplicationSwitchInfo(ctx, policyID)
		if err != nil {
			if errors.Is(err, dom.ErrNotFound) {
				zerolog.Ctx(ctx).Err(err).Msg("drop switch with downtime task: switch metadata was deleted")
				return nil
			}
			return err
		}
		if switchPolicy.IsZeroDowntime() {
			// wrong switch type - drop task
			// should never happen
			zerolog.Ctx(ctx).Error().Msg("drop switch with downtime task: switch is not switch with downtime")
			return nil
		}
		// execute switch state machine:
		result, err := s.processSwitchWithDowntimeState(ctx, policyID, replStatus, switchPolicy)
		if err != nil {
			return err
		}
		if result.nextState.status != "" {
			// update switch status:
			err = s.policySvc.UpdateDowntimeSwitchStatus(ctx, policyID, result.nextState.status, result.nextState.message, result.nextState.startedAt, result.nextState.doneAt)
			if err != nil {
				return fmt.Errorf("unable to update switch status to %q-%q: %w", result.nextState.status, result.nextState.message, err)
			}
		}
		if result.retryLater {
			return &dom.ErrRateLimitExceeded{RetryIn: s.conf.SwitchRetryInterval}
		}
		return nil
	}

	return lock.Do(ctx, time.Second*2, switchFunc)
}

type switchResult struct {
	retryLater bool
	nextState  state
}

type state struct {
	status    entity.ReplicationSwitchStatus
	message   string
	startedAt *time.Time
	doneAt    *time.Time
}

func (s *svc) processSwitchWithDowntimeState(ctx context.Context, id entity.ReplicationStatusID, replStatus entity.ReplicationStatusExtended, switchStatus entity.ReplicationSwitchInfo) (switchResult, error) {
	// state machine for switch with downtime:
	// switch statement contain all states in logical order
	// each task handling iteration handles one state and returns new state or error
	switch switchStatus.LastStatus {
	// 1. Switch not started - check if it is time to start according to schedule:
	case entity.StatusNotStarted, entity.StatusSkipped, entity.StatusError, "":
		// handle the case where switch is not recurring (no cron) and was already attempted:
		alredyAttempted := switchStatus.LastStatus != entity.StatusNotStarted && switchStatus.LastStatus != ""
		_, isRecurring := switchStatus.GetCron()
		if alredyAttempted && !isRecurring {
			zerolog.Ctx(ctx).Error().Msgf("switch with downtime already executed with status %q and should not be retried: drop task", string(switchStatus.LastStatus))
			return switchResult{
				retryLater: false,
				nextState: state{
					status:  entity.StatusError,
					message: "switch already executed and should not be retried",
				},
			}, nil
		}

		// check if it is time to start switch:
		isTimeToStart, err := switchStatus.IsTimeToStart()
		if err != nil {
			return switchResult{}, fmt.Errorf("failed to check if it is time to start switch: %w", err)
		}
		if !isTimeToStart {
			// retry later:
			return switchResult{
				retryLater: true,
			}, nil
		}

		// time to start switch according to schedule
		// check if replication status conditions are met:
		if !replStatus.InitDone() {
			if switchStatus.StartOnInitDone {
				// retry later:
				return switchResult{
					retryLater: true,
				}, nil
			}
			// skip this switch iteration:
			return switchResult{
				retryLater: true,
				nextState: state{
					status:  entity.StatusSkipped,
					message: "init replication was not done",
				},
			}, nil
		}
		// check if max event lag condition is met:
		if maxLag, ok := switchStatus.GetMaxEventLag(); ok {
			currentLag := replStatus.EventMigration.Unprocessed
			if currentLag > int(maxLag) {
				// lag is too big to start the switch:
				// skip this switch iteration:
				return switchResult{
					retryLater: true,
					nextState: state{
						status:  entity.StatusSkipped,
						message: fmt.Sprintf("event lag not met: %d > %d", currentLag, maxLag),
					},
				}, nil
			}
		}
		// Start downtime window:
		downtimeStart := time.Now()
		return switchResult{
			retryLater: true,
			nextState: state{
				status:    entity.StatusInProgress,
				message:   "downtime window started",
				startedAt: &downtimeStart,
			},
		}, nil

	// 2. Switch in progress - check migration queue drain progress and max duration to complete, cancel, or check it later:
	case entity.StatusInProgress:
		isQueueDrained := replStatus.EventMigration.Unprocessed == 0
		if !isQueueDrained {
			// switch is still in progress, check if max duration exceeded:
			if switchStatus.LastStartedAt == nil {
				// should never happen
				return switchResult{}, fmt.Errorf("switch with downtime started at is nil")
			}
			if maxDuration, ok := switchStatus.GetMaxDuration(); ok && time.Since(*switchStatus.LastStartedAt) > maxDuration {
				// cancel switch and retry later:
				return switchResult{
					retryLater: true,
					nextState: state{
						status:  entity.StatusError,
						message: "max duration exceeded",
					},
				}, nil
			}
			// duration not exceeded: wait for queue to be drained:
			return switchResult{
				retryLater: true,
			}, nil
		}
		// queue is drained, initiate bucket contents check:
		_, _, err := s.checkBuckets(ctx, id, switchStatus.SkipBucketCheck)
		if err != nil {
			return switchResult{}, err
		}
		return switchResult{
			retryLater: true,
			nextState: state{
				status:  entity.StatusCheckInProgress,
				message: "queue is drained, start buckets content check",
			},
		}, nil

	// 3. Poll bucket check status to fail or complete switch:
	case entity.StatusCheckInProgress:
		if switchStatus.LastStartedAt == nil {
			// should never happen
			return switchResult{}, fmt.Errorf("switch with downtime started at is nil")
		}
		isEqual, isInProgress, err := s.checkBuckets(ctx, id, switchStatus.SkipBucketCheck)
		if err != nil {
			return switchResult{}, err
		}
		if isInProgress {
			// bucket check is still in progress, check if max duration exceeded:
			if maxDuration, ok := switchStatus.GetMaxDuration(); ok && time.Since(*switchStatus.LastStartedAt) > maxDuration {
				// cancel switch and retry later:
				return switchResult{
					retryLater: true,
					nextState: state{
						status:  entity.StatusError,
						message: "max duration exceeded",
					},
				}, nil
			}
			// wait for bucket check to complete:
			return switchResult{
				retryLater: true,
			}, nil
		}
		if !isEqual {
			// bucket contents are not equal - fail switch:
			return switchResult{
				retryLater: true,
				nextState: state{
					status:  entity.StatusError,
					message: "bucket contents are not equal",
				},
			}, nil
		}
		// switch done - complete switch:
		doneAt := time.Now()
		return switchResult{
			retryLater: false,
			nextState: state{
				status:  entity.StatusDone,
				message: "switch done",
				doneAt:  &doneAt,
			},
		}, nil

	case entity.StatusDone:
		// should never be reached because Done is terminal state and we don't retry task.
		// just in case double check that routing block is removed, switch routing and complete task:
		doneAt := time.Now()
		return switchResult{
			retryLater: false,
			nextState: state{
				status:  entity.StatusDone,
				message: "switch done",
				doneAt:  &doneAt,
			},
		}, nil
	default:
		return switchResult{}, fmt.Errorf("unknown switch policy status: %s", switchStatus.LastStatus)
	}
}

func (s *svc) checkBuckets(_ context.Context, _ entity.ReplicationStatusID, skip bool) (isEqual, isInProgress bool, err error) {
	if skip {
		return true, false, nil
	}
	// todo: implement when https://github.com/clyso/chorus/issues/38 is done
	return checkResultIsEqual, checkResultIsInProgress, nil
}
