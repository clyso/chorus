package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

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
		return errors.New("task id not found in worker ctx")
	}

	// acquire exclusive lock to switch task:
	release, refresh, err := s.locker.Lock(ctx, lock.StringKey(taskID))
	if err != nil {
		return err
	}
	defer release()
	return lock.WithRefresh(ctx, func() error {
		// get latest replication and switch state and execute switch state machine:
		replStatus, err := s.policySvc.GetReplicationPolicyInfo(ctx, p.User, p.Bucket, p.FromStorage, p.ToStorage, p.ToBucket)
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
		policyID := policy.ReplicationID{
			User:     p.User,
			Bucket:   p.Bucket,
			From:     p.FromStorage,
			To:       p.ToStorage,
			ToBucket: p.ToBucket,
		}
		switchPolicy, err := s.policySvc.GetReplicationSwitchWithDowntime(ctx, policyID)
		if err != nil {
			if errors.Is(err, dom.ErrNotFound) {
				zerolog.Ctx(ctx).Err(err).Msg("drop switch with downtime task: switch metadata was deleted")
				return nil
			}
			return err
		}
		// execute switch state machine:
		result, err := s.processSwitchWithDowntimeState(ctx, policyID, replStatus, switchPolicy)
		if err != nil {
			return err
		}
		if result.nextState.status != "" {
			// update switch status:
			err = s.policySvc.UpdateSwitchWithDowntimeStatus(ctx, policyID, result.nextState.status, result.nextState.message, result.nextState.startedAt, result.nextState.doneAt)
			if err != nil {
				return fmt.Errorf("unable to update switch status to %q-%q: %w", result.nextState.status, result.nextState.message, err)
			}
		}
		if result.retryLater {
			return &dom.ErrRateLimitExceeded{RetryIn: s.conf.PauseRetryInterval}
		}
		return nil
	}, refresh, time.Second*2)
}

type switchResult struct {
	retryLater bool
	nextState  state
}

type state struct {
	status    policy.SwitchWithDowntimeStatus
	message   string
	startedAt *time.Time
	doneAt    *time.Time
}

func (s *svc) processSwitchWithDowntimeState(ctx context.Context, id policy.ReplicationID, replStatus policy.ReplicationPolicyStatus, switchStatus policy.SwitchWithDowntime) (switchResult, error) {
	// state machine for switch with downtime:
	// switch statement contain all states in logical order
	// each task handling iteration handles one state and returns new state or error
	switch switchStatus.LastStatus {
	// 1. Switch not started - check if it is time to start according to schedule:
	case policy.StatusNotStarted, policy.StatusSkipped, policy.StatusError, "":
		// cleanup routing block in case of error on previous iteration:
		if switchStatus.LastStatus == policy.StatusError {
			err := s.policySvc.DeleteRoutingBlock(ctx, id.From, id.Bucket)
			if err != nil && !errors.Is(err, dom.ErrNotFound) {
				return switchResult{}, nil
			}
		}
		// handle the case where switch is not recurring (no cron) and was already attempted:
		_, alredyAttempted := switchStatus.GetLastStartAt()
		_, isRecurring := switchStatus.GetCron()
		if alredyAttempted && !isRecurring {
			zerolog.Ctx(ctx).Error().Msgf("switch with downtime already executed with status %q and should not be retried: drop task", string(switchStatus.LastStatus))
			return switchResult{
				retryLater: false,
				nextState: state{
					status:  policy.StatusError,
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
					status:  policy.StatusSkipped,
					message: "init replication was not done",
				},
			}, nil
		}
		// check if max event lag condition is met:
		if maxLag, ok := switchStatus.GetMaxEventLag(); ok {
			currentLag := replStatus.Events - replStatus.EventsDone
			if currentLag > int64(maxLag) {
				// lag is too big to start the switch:
				// skip this switch iteration:
				return switchResult{
					retryLater: true,
					nextState: state{
						status:  policy.StatusSkipped,
						message: fmt.Sprintf("event lag not met: %d > %d", currentLag, maxLag),
					},
				}, nil
			}
		}
		// Start downtime window by blocking bucket writes
		err = s.policySvc.AddRoutingBlock(ctx, id.From, id.Bucket)
		if err != nil {
			return switchResult{}, err
		}
		downtimeStart := time.Now()
		return switchResult{
			retryLater: true,
			nextState: state{
				status:    policy.StatusInProgress,
				message:   "downtime window started",
				startedAt: &downtimeStart,
			},
		}, nil

	// 2. Switch in progress - check migration queue drain progress and max duration to complete, cancel, or check it later:
	case policy.StatusInProgress:
		isQueueDrained := replStatus.EventsDone >= replStatus.Events
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
						status:  policy.StatusError,
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
		_, _, err := s.checkBuckets(ctx, id)
		if err != nil {
			return switchResult{}, err
		}
		return switchResult{
			retryLater: true,
			nextState: state{
				status:  policy.StatusCheckInProgress,
				message: "queue is drained, start buckets content check",
			},
		}, nil

	// 3. Poll bucket check status to fail or complete switch:
	case policy.StatusCheckInProgress:
		if switchStatus.LastStartedAt == nil {
			// should never happen
			return switchResult{}, fmt.Errorf("switch with downtime started at is nil")
		}
		isEqual, isInProgress, err := s.checkBuckets(ctx, id)
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
						status:  policy.StatusError,
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
					status:  policy.StatusError,
					message: "bucket contents are not equal",
				},
			}, nil
		}
		// switch done - complete switch:
		err = s.policySvc.CompleteReplicationSwitchWithDowntime(ctx, id, switchStatus.ContinueReplication)
		if err != nil {
			return switchResult{}, err
		}
		doneAt := time.Now()
		return switchResult{
			retryLater: false,
			nextState: state{
				status:  policy.StatusDone,
				message: "switch done",
				doneAt:  &doneAt,
			},
		}, nil

	case policy.StatusDone:
		// should never be reached because Done is terminal state and we don't retry task.
		// just in case double check that routing block is removed, switch routing and complete task:
		err := s.policySvc.CompleteReplicationSwitchWithDowntime(ctx, id, switchStatus.ContinueReplication)
		if err != nil {
			return switchResult{}, err
		}
		doneAt := time.Now()
		return switchResult{
			retryLater: false,
			nextState: state{
				status:  policy.StatusDone,
				message: "switch done",
				doneAt:  &doneAt,
			},
		}, nil
	default:
		return switchResult{}, fmt.Errorf("unknown switch policy status: %s", switchStatus.LastStatus)
	}
}

func (s *svc) checkBuckets(_ context.Context, _ policy.ReplicationID) (isEqual, isInProgress bool, err error) {
	// todo: implement when https://github.com/clyso/chorus/issues/38 is done
	return true, true, nil
}
