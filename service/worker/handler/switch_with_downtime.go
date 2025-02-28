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
		result, err := s.processSwitchWithDowntime(ctx, p)
		if err != nil {
			return err
		}
		if result.newStatus != "" {
			// update switch status:
			err := s.policySvc.UpdateSwitchWithDowntimeStatus(ctx, policy.ReplicationID{
				User:     p.User,
				Bucket:   p.Bucket,
				From:     p.FromStorage,
				To:       p.ToStorage,
				ToBucket: p.ToBucket,
			}, result.newStatus, result.message)
			if err != nil {
				return fmt.Errorf("unable to update switch status to %q: %q", result.newStatus, err)
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
	newStatus  policy.SwitchWithDowntimeStatus
	message    string
}

func (s *svc) processSwitchWithDowntime(ctx context.Context, p tasks.SwitchWithDowntimePayload) (switchResult, error) {
	// check if replication is active
	paused, err := s.policySvc.IsReplicationPolicyPaused(ctx, p.User, p.Bucket, p.FromStorage, p.ToStorage, p.ToBucket)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			zerolog.Ctx(ctx).Err(err).Msg("drop switch task: replication policy not found")
			return switchResult{
				retryLater: false,
			}, nil
		}
		return switchResult{}, err
	}
	if paused {
		// replication is paused - retry later
		return switchResult{
			retryLater: true,
		}, nil
	}
	policyID := policy.ReplicationID{
		User:     p.User,
		Bucket:   p.Bucket,
		From:     p.FromStorage,
		To:       p.ToStorage,
		ToBucket: p.ToBucket,
	}
	// get switch policy:
	switchPolicy, err := s.policySvc.GetReplicationSwitchWithDowntime(ctx, policyID)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			zerolog.Ctx(ctx).Err(err).Msg("drop switch with downtime task: switch metadata was deleted")
			return switchResult{
				retryLater: false,
			}, nil
		}
		return switchResult{}, err
	}
	replStatus, err := s.policySvc.GetReplicationPolicyInfo(ctx, p.User, p.Bucket, p.FromStorage, p.ToStorage, p.ToBucket)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			zerolog.Ctx(ctx).Err(err).Msg("drop switch with downtime task: replication metadata was deleted")
			return switchResult{
				retryLater: false,
			}, nil
		}
		return switchResult{}, err
	}

	// state machine for switch with downtime:
	// switch statement contain all states in logical order
	// each task handling iteration handles one state and returns new state or error
	switch switchPolicy.LastStatus {
	// switch not started - check if it is time to start according to schedule:
	case policy.StatusNotStarted, policy.StatusSkipped, policy.StatusError, "":
		// cleanup routing block in case of previous error:
		if switchPolicy.LastStatus == policy.StatusError {
			err := s.policySvc.DeleteRoutingBlock(ctx, p.FromStorage, p.Bucket)
			if err != nil && !errors.Is(err, dom.ErrNotFound) {
				return switchResult{}, nil
			}
		}
		// don't retry if switch is not recurring and was already attempted:
		_, alredyAttempted := switchPolicy.GetLastStartAt()
		_, isRecurring := switchPolicy.GetCron()
		if alredyAttempted && !isRecurring {
			zerolog.Ctx(ctx).Error().Msgf("switch with downtime already executed with status %q and should not be retried: drop task", string(switchPolicy.LastStatus))
			return switchResult{
				retryLater: false,
				newStatus:  policy.StatusError,
				message:    "switch already executed and should not be retried",
			}, nil
		}

		// check if it is time to start switch:
		isTimeToStart, err := switchPolicy.IsTimeToStart()
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
			if switchPolicy.StartOnInitDone {
				// retry later:
				return switchResult{
					retryLater: true,
				}, nil
			}
			// skip this switch iteration:
			return switchResult{
				retryLater: true,
				newStatus:  policy.StatusSkipped,
				message:    "init replication was not done",
			}, nil
		}
		// check if max event lag condition is met:
		if maxLag, ok := switchPolicy.GetMaxEventLag(); ok {
			currentLag := replStatus.Events - replStatus.EventsDone
			if currentLag > int64(maxLag) {
				// lag is too big to start switch:
				// skip this switch iteration:
				return switchResult{
					retryLater: true,
					newStatus:  policy.StatusSkipped,
					message:    fmt.Sprintf("event lag not met: %d > %d", currentLag, maxLag),
				}, nil
			}
		}
		// Start downtime window by blocking bucket writes
		err = s.policySvc.AddRoutingBlock(ctx, p.FromStorage, p.Bucket)
		if err != nil {
			return switchResult{}, err
		}
		return switchResult{
			retryLater: true,
			newStatus:  policy.StatusInProgress,
			message:    "downtime window started",
		}, nil

	case policy.StatusInProgress:
		// switch in progress - check progress and max duration to complete cancel or check it later:
		isQueueDrained := replStatus.EventsDone >= replStatus.Events
		if !isQueueDrained {
			// switch is still in progress, check if max duration exceeded:
			if maxDuration, ok := switchPolicy.GetMaxDuration(); ok && time.Since(*switchPolicy.LastStartedAt) > maxDuration {
				// cancel switch and retry later:
				return switchResult{
					retryLater: true,
					newStatus:  policy.StatusError,
					message:    "max duration exceeded",
				}, nil
			}
			// wait for queue to be drained:
			return switchResult{
				retryLater: true,
			}, nil
		}
		// queue is drained, initiate bucket contents check:
		_, _, err = s.checkBuckets(ctx, p)
		if err != nil {
			return switchResult{}, err
		}
		return switchResult{
			retryLater: true,
			newStatus:  policy.StatusCheckInProgress,
			message:    "queue is drained, check bucket contents",
		}, nil

	case policy.StatusCheckInProgress:
		// poll bucket check status to fail or complete switch:
		isEqual, isInProgress, err := s.checkBuckets(ctx, p)
		if err != nil {
			return switchResult{}, err
		}
		if isInProgress {
			// bucket check is still in progress, check if max duration exceeded:
			if maxDuration, ok := switchPolicy.GetMaxDuration(); ok && time.Since(*switchPolicy.LastStartedAt) > maxDuration {
				// cancel switch and retry later:
				return switchResult{
					retryLater: true,
					newStatus:  policy.StatusError,
					message:    "max duration exceeded",
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
				newStatus:  policy.StatusError,
				message:    "bucket contents are not equal",
			}, nil
		}
		// switch done - complete switch:
		err = s.policySvc.CompleteReplicationSwitchWithDowntime(ctx, policyID, switchPolicy.ContinueReplication)
		if err != nil {
			return switchResult{}, err
		}
		return switchResult{
			retryLater: false,
			newStatus:  policy.StatusDone,
			message:    "switch done",
		}, nil

	case policy.StatusDone:
		// switch done double check that routing block is removed, switch routing and complete task:
		err = s.policySvc.CompleteReplicationSwitchWithDowntime(ctx, policyID, switchPolicy.ContinueReplication)
		if err != nil {
			return switchResult{}, err
		}
		return switchResult{
			retryLater: false,
			newStatus:  policy.StatusDone,
			message:    "switch done",
		}, nil
	default:
		return switchResult{}, fmt.Errorf("unknown switch policy status: %s", switchPolicy.LastStatus)
	}
}

func (s *svc) checkBuckets(_ context.Context, _ tasks.SwitchWithDowntimePayload) (isEqual, isInProgress bool, err error) {
	// todo: implement when https://github.com/clyso/chorus/issues/38 is done
	return true, true, nil
}
