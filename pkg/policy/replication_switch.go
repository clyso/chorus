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

package policy

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/adhocore/gronx"
	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/tasks"
)

var (
	// timeNow is a function to get current time
	// it is a variable so that it can be mocked in tests
	timeNow = time.Now
)

type SwitchDowntimeOpts struct {
	StartOnInitDone     bool          `redis:"onInitDone"`
	Cron                *string       `redis:"cron,omitempty"`
	StartAt             *time.Time    `redis:"startAt,omitempty"`
	MaxDuration         time.Duration `redis:"maxDuration,omitempty"`
	MaxEventLag         *uint32       `redis:"maxEventLag,omitempty"`
	SkipBucketCheck     bool          `redis:"skipBucketCheck,omitempty"`
	ContinueReplication bool          `redis:"continueReplication,omitempty"`
}

type SwitchZeroDowntimeOpts struct {
	MultipartTTL time.Duration `redis:"multipartTTL,omitempty"`
}

func (w *SwitchDowntimeOpts) GetCron() (string, bool) {
	if w != nil && w.Cron != nil && *w.Cron != "" {
		return *w.Cron, true
	}
	return "", false
}

func (w *SwitchDowntimeOpts) GetStartAt() (time.Time, bool) {
	if w != nil && w.StartAt != nil && !w.StartAt.IsZero() {
		return *w.StartAt, true
	}
	return time.Time{}, false
}

func (w *SwitchDowntimeOpts) GetMaxEventLag() (uint32, bool) {
	if w != nil && w.MaxEventLag != nil {
		return *w.MaxEventLag, true
	}
	return 0, false
}

func (w *SwitchDowntimeOpts) GetMaxDuration() (time.Duration, bool) {
	if w != nil && w.MaxDuration > 0 {
		return w.MaxDuration, true
	}
	return 0, false
}

// Reduced replication switch info for in progress zero downtime switch.
// Subset of SwitchInfo fields.
// Used by proxy to route requests to correct bucket during zero downtime switch.
type ZeroDowntimeSwitchInProgressInfo struct {
	ReplicationIDStr    string        `redis:"replicationID"`
	Status              SwitchStatus  `redis:"lastStatus"`
	MultipartTTL        time.Duration `redis:"multipartTTL"`
	ReplicationPriority uint8         `redis:"replPriority,omitempty"`
}

func (s *ZeroDowntimeSwitchInProgressInfo) ReplicationID() (ReplicationID, error) {
	return ReplicationIDFromStr(s.ReplicationIDStr)
}

// Contains all information about replication switch including its configuration and current status.
type SwitchInfo struct {
	// Options for downtime switch
	SwitchDowntimeOpts
	// Options for zero downtime switch
	SwitchZeroDowntimeOpts
	// Task priority of replication policy of this switch
	ReplicationPriority uint8 `redis:"replPriority,omitempty"`
	// ID of replication policy of this switch
	ReplicationIDStr string `redis:"replicationID"`
	// Time of switch creation
	CreatedAt time.Time `redis:"createdAt"`
	// Last status of switch
	LastStatus SwitchStatus `redis:"lastStatus,omitempty"`
	// Time of last switch was in progress
	LastStartedAt *time.Time `redis:"startedAt,omitempty"`
	// Time of last switch was done
	DoneAt *time.Time `redis:"doneAt,omitempty"`
	// History of switch status changes
	History []string `redis:"-"`
}

func (s *SwitchInfo) ReplicationID() (ReplicationID, error) {
	return ReplicationIDFromStr(s.ReplicationIDStr)
}

func (s *SwitchInfo) IsZeroDowntime() bool {
	return s.MultipartTTL > 0
}

func (s *SwitchInfo) GetLastStartAt() (time.Time, bool) {
	if s != nil && s.LastStartedAt != nil && !s.LastStartedAt.IsZero() {
		return *s.LastStartedAt, true
	}
	return time.Time{}, false
}

func (s *SwitchInfo) IsTimeToStart() (bool, error) {
	cron, cronSet := s.GetCron()
	startAt, startSet := s.GetStartAt()
	if !cronSet && !startSet {
		// start now
		return true, nil
	}
	// only cron or startAt can be set, not both
	if cronSet && startSet {
		return false, fmt.Errorf("only one of cron or startAt can be set")
	}
	// handle startAt:
	if startSet {
		return startAt.Before(timeNow()), nil
	}

	// handle cron:
	// calculate next tick after last cron execution
	prevStart, hadStart := s.GetLastStartAt()
	if hadStart {
		nextTick, err := gronx.NextTickAfter(cron, prevStart.Add(time.Second), true) // add sec to avoid same tick
		if err != nil {
			return false, err
		}
		// next <= now means it is time to start
		return nextTick.Compare(timeNow()) <= 0, nil
	}
	// if it is first execution, calculate next tick from switch creation time
	nextTick, err := gronx.NextTickAfter(cron, s.CreatedAt, true)
	if err != nil {
		return false, err
	}
	// next <= now means it is time to start
	return nextTick.Compare(timeNow()) <= 0, nil
}

type SwitchStatus string

const (
	// StatusNotStarted means that switch donwntime is not started yet
	// Relevant only for downtime switches
	StatusNotStarted SwitchStatus = "not_started"
	// StatusInProgress means that downtime is started, bucket is blocked until task queue is drained or timeout
	StatusInProgress SwitchStatus = "in_progress"
	// StatusCheckInProgress means that task queue is drained and bucket is blocked until src and dst bucket contents will be checked
	// Relevant only for downtime switches
	StatusCheckInProgress SwitchStatus = "check_in_progress"
	// StatusDone means that switch is successfully finished and data is routed to new bucket.
	StatusDone SwitchStatus = "done"
	// StatusError means that switch was aborted due to error
	// Relevant only for downtime switches
	StatusError SwitchStatus = "error"
	// StatusSkipped means that switch attempt was skipped because conditions were not met
	// Relevant only for downtime switches
	StatusSkipped SwitchStatus = "skipped"
)

func (s *SwitchStatus) UnmarshalBinary(data []byte) error {
	*s = SwitchStatus(data)
	return nil
}

func (s SwitchStatus) MarshalBinary() (data []byte, err error) {
	return []byte(s), nil
}

func (s *policySvc) SetDowntimeReplicationSwitch(ctx context.Context, replID ReplicationID, opts *SwitchDowntimeOpts) error {
	// check if switch already exists
	existing, err := s.GetReplicationSwitchInfo(ctx, replID)
	if err != nil {
		if !errors.Is(err, dom.ErrNotFound) {
			return fmt.Errorf("unable to get replication switch info: %w", err)
		}
		// not exists: fall through
	} else {
		// already exists:
		if existing.IsZeroDowntime() {
			return fmt.Errorf("%w: cannot update donwntime switch: there is existing zero downtime switch for given replication", dom.ErrAlreadyExists)
		}
		if existing.LastStatus == StatusInProgress || existing.LastStatus == StatusCheckInProgress {
			return fmt.Errorf("%w: cannot update downtime switch: switch is already in progress", dom.ErrInvalidArg)
		}
		if existing.LastStatus == StatusDone {
			return fmt.Errorf("%w: cannot update downtime switch: switch is already completed", dom.ErrInvalidArg)
		}
		// all good, update existing switch options:
		return s.updateDowntimeSwitchOpts(ctx, replID, opts)
	}

	// add new downtime switch
	// validate corresponding replication state:
	policy, err := s.GetReplicationPolicyInfo(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket)
	if err != nil {
		return fmt.Errorf("unable to get replication policy: %w", err)
	}
	if policy.AgentURL != "" {
		return fmt.Errorf("%w: cannot create downtime switch: given replication is agent based", dom.ErrInvalidArg)
	}
	forceStartNow := opts == nil || (opts.StartAt == nil && opts.Cron == nil && !opts.StartOnInitDone)
	if forceStartNow && !policy.InitDone() {
		return fmt.Errorf("%w: cannot create downtime switch: init replication is not done", dom.ErrInvalidArg)
	}

	policies, err := s.GetBucketReplicationPolicies(ctx, replID.User, replID.Bucket)
	if err != nil {
		return fmt.Errorf("unable to get replication policies: %w", err)
	}
	if len(policies.To) != 1 {
		return fmt.Errorf("%w: cannot create switch: existing bucket replication should have a single destination", dom.ErrInvalidArg)
	}
	var dest ReplicationPolicyDest
	var prio tasks.Priority
	for d, p := range policies.To {
		dest = d
		prio = p
	}
	if string(dest) != replID.To {
		return fmt.Errorf("%w: cannot create downtime switch: given replication is not routed to destination", dom.ErrInvalidArg)
	}
	info := &SwitchInfo{
		CreatedAt:           timeNow(),
		ReplicationIDStr:    replID.String(),
		LastStatus:          StatusNotStarted,
		ReplicationPriority: uint8(prio),
	}
	if opts != nil {
		info.SwitchDowntimeOpts = *opts
	}
	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, replID.SwitchKey(), info)
	if opts != nil {
		pipe.HSet(ctx, replID.SwitchKey(), opts)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("unable to set downtime switch: %w", err)
	}
	return nil
}

// updateDowntimeSwitchOpts goes through the opts struct and updates the corresponding fields in redis hash
// if a field is nil, it will be deleted from hash
// if a field is not nil, it will be set in hash
func (s *policySvc) updateDowntimeSwitchOpts(ctx context.Context, replID ReplicationID, opts *SwitchDowntimeOpts) error {
	deleteAll := opts == nil
	if deleteAll {
		opts = &SwitchDowntimeOpts{}
	}
	key := replID.SwitchKey()
	pipe := s.client.TxPipeline()

	val := reflect.ValueOf(*opts)
	typ := val.Type()
	for i := range typ.NumField() {
		field := typ.Field(i)
		redisTag := field.Tag.Get("redis")
		if redisTag == "" || redisTag == "-" {
			continue
		}
		redisTag = strings.Split(redisTag, ",")[0]

		if deleteAll {
			pipe.HDel(ctx, key, redisTag)
			continue
		}
		fieldVal := val.Field(i)
		//nolint:gocritic // keep if else for readability
		if field.Type.Kind() == reflect.Ptr && fieldVal.IsNil() {
			pipe.HDel(ctx, key, redisTag)
		} else if fieldVal.CanInterface() {
			if field.Type.Kind() == reflect.Ptr {
				pipe.HSet(ctx, key, redisTag, fieldVal.Elem().Interface())
			} else {
				pipe.HSet(ctx, key, redisTag, fieldVal.Interface())
			}
		} else {
			return fmt.Errorf("unable to update downtime switch field: %s", field.Name)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("unable to update downtime switch options: %w", err)
	}
	return nil
}

func (s *policySvc) AddZeroDowntimeReplicationSwitch(ctx context.Context, replID ReplicationID, opts *SwitchZeroDowntimeOpts) error {
	_, err := s.GetReplicationSwitchInfo(ctx, replID)
	if err != nil {
		if !errors.Is(err, dom.ErrNotFound) {
			return fmt.Errorf("unable to get replication switch info: %w", err)
		}
		// not exists: fall through
	} else {
		// already exists:
		return dom.ErrAlreadyExists
	}
	// validate corresponding replication state:
	policy, err := s.GetReplicationPolicyInfo(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket)
	if err != nil {
		return fmt.Errorf("unable to get replication policy: %w", err)
	}
	if policy.AgentURL != "" {
		return fmt.Errorf("%w: cannot create zero-downtime switch: given replication is agent based", dom.ErrInvalidArg)
	}
	if !policy.InitDone() {
		return fmt.Errorf("%w: cannot create zero-downtime switch: init replication is not done", dom.ErrInvalidArg)
	}
	if policy.IsPaused {
		return fmt.Errorf("%w: cannot create zero-downtime switch: replication is paused", dom.ErrInvalidArg)
	}
	policies, err := s.GetBucketReplicationPolicies(ctx, replID.User, replID.Bucket)
	if err != nil {
		return fmt.Errorf("unable to get replication policies: %w", err)
	}
	if len(policies.To) != 1 {
		return fmt.Errorf("%w: cannot create switch: existing bucket replication should have a single destination", dom.ErrInvalidArg)
	}
	var dest ReplicationPolicyDest
	var prio tasks.Priority
	for d, p := range policies.To {
		dest = d
		prio = p
	}
	if string(dest) != replID.To {
		return fmt.Errorf("%w: cannot create zero-downtime switch: given replication is not routed to destination", dom.ErrInvalidArg)
	}

	// validate routing policy
	toStorage, err := s.getRoutingPolicy(ctx, replID.User, replID.Bucket)
	if err != nil {
		return fmt.Errorf("unable to get routing policy: %w", err)
	}
	if toStorage != replID.From {
		return fmt.Errorf("%w: cannot create zero-downtime switch: given replication is not routed to destination", dom.ErrInvalidArg)
	}

	now := timeNow()
	pipe := s.client.TxPipeline()
	// switch routing
	pipe.Set(ctx, replID.RoutingKey(), replID.To, 0)
	// create switch metadata
	pipe.HSet(ctx, replID.SwitchKey(), SwitchInfo{
		ReplicationIDStr:    replID.String(),
		CreatedAt:           now,
		LastStatus:          StatusInProgress,
		ReplicationPriority: uint8(prio),
		// LastStartedAt:    &now,
	})
	// set time separately because of go-redis bug with *time.Time
	pipe.HSet(ctx, replID.SwitchKey(), "startedAt", now)
	pipe.HSet(ctx, replID.SwitchKey(), SwitchZeroDowntimeOpts{
		MultipartTTL: opts.MultipartTTL,
	})

	// archive replication
	if err = archiveReplicationWithClient(ctx, pipe, replID); err != nil {
		return fmt.Errorf("unable to archive replication: %w", err)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("unable to create zero downtime switch: %w", err)
	}
	return nil
}

func (s *policySvc) DeleteReplicationSwitch(ctx context.Context, replID ReplicationID) error {
	existing, err := s.GetReplicationSwitchInfo(ctx, replID)
	if err != nil {
		return err
	}
	pipe := s.client.TxPipeline()
	if existing.IsZeroDowntime() {
		if existing.LastStatus != StatusDone {
			//revert routing idempotently
			if err = addBucketRoutingPolicyWithClient(ctx, pipe, replID.User, replID.Bucket, replID.From, true); err != nil {
				return fmt.Errorf("unable to revert bucket routing policy: %w", err)
			}
		}
	} else {
		// delete routing block idempotently
		if err = deleteRoutingBlockWithClient(ctx, pipe, replID.From, replID.Bucket); err != nil {
			return fmt.Errorf("unable to delete routing block: %w", err)
		}
	}
	//delete switch metadata
	pipe.Del(ctx, replID.SwitchKey())
	// delete switch history
	pipe.Del(ctx, replID.SwitchHistoryKey())
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("unable to delete replication switch: %w", err)
	}
	return nil
}

func (s *policySvc) GetReplicationSwitchInfo(ctx context.Context, replID ReplicationID) (SwitchInfo, error) {
	var info SwitchInfo
	res := s.client.HGetAll(ctx, replID.SwitchKey())
	if res.Err() != nil {
		if errors.Is(res.Err(), redis.Nil) {
			return info, dom.ErrNotFound
		}
		return info, fmt.Errorf("unable to get replication switch info: %w", res.Err())
	}
	if len(res.Val()) == 0 {
		return info, dom.ErrNotFound
	}
	if err := res.Scan(&info); err != nil {
		if errors.Is(res.Err(), redis.Nil) {
			return info, dom.ErrNotFound
		}
		return info, fmt.Errorf("unable to scan replication switch info: %w", err)
	}
	if err := res.Scan(&info.SwitchDowntimeOpts); err != nil {
		return info, fmt.Errorf("unable to scan replication switch info downtime opts: %w", err)
	}
	if err := res.Scan(&info.SwitchZeroDowntimeOpts); err != nil {
		return info, fmt.Errorf("unable to scan replication switch info zero downtime opts: %w", err)
	}
	if info.ReplicationIDStr != replID.String() {
		return info, fmt.Errorf("%w: replication ID mismatch: expected %s, got %s", dom.ErrInvalidArg, replID.String(), info.ReplicationIDStr)
	}

	history, err := s.client.LRange(ctx, replID.SwitchHistoryKey(), 0, -1).Result()
	if err != nil {
		return info, fmt.Errorf("unable to get replication switch history: %w", err)
	}
	info.History = history
	return info, nil
}

func (s *policySvc) GetInProgressZeroDowntimeSwitchInfo(ctx context.Context, user string, bucket string) (ZeroDowntimeSwitchInProgressInfo, error) {
	info := ZeroDowntimeSwitchInProgressInfo{}
	key := ReplicationID{User: user, Bucket: bucket}.SwitchKey()
	err := s.client.HMGet(ctx, key, "multipartTTL", "lastStatus", "replicationID", "replPriority").Scan(&info)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return info, dom.ErrNotFound
		}
		return info, err
	}
	if info.MultipartTTL == 0 {
		// no multipart TTL means it is not zero downtime switch
		return info, dom.ErrNotFound
	}
	if info.Status != StatusInProgress {
		// only in progress switches are considered
		return info, dom.ErrNotFound
	}
	if _, err = info.ReplicationID(); err != nil {
		return info, fmt.Errorf("unable to get replication ID: %w", err)
	}
	return info, nil
}

// UpdateDowntimeSwitchStatus handles downtime switch status transitions
// and performs corresponding idempotent operations with replication and routing policies
func (s *policySvc) UpdateDowntimeSwitchStatus(ctx context.Context, replID ReplicationID, newStatus SwitchStatus, description string, startedAt *time.Time, doneAt *time.Time) error {
	// validate input
	if newStatus == "" || newStatus == StatusNotStarted {
		return fmt.Errorf("status cannot be %s: %w", newStatus, dom.ErrInvalidArg)
	}
	if startedAt != nil && doneAt != nil {
		return fmt.Errorf("cannot set both startedAt and doneAt: %w", dom.ErrInvalidArg)
	}
	switch newStatus {
	case StatusError, StatusSkipped, StatusCheckInProgress:
		if startedAt != nil {
			return fmt.Errorf("cannot set startedAt for status %s: %w", newStatus, dom.ErrInvalidArg)
		}
		if doneAt != nil {
			return fmt.Errorf("cannot set doneAt for status %s: %w", newStatus, dom.ErrInvalidArg)
		}
	case StatusInProgress:
		if startedAt == nil {
			return fmt.Errorf("startedAt is required for status in progress: %w", dom.ErrInvalidArg)
		}
		if doneAt != nil {
			return fmt.Errorf("cannot set doneAt for status in progress: %w", dom.ErrInvalidArg)
		}
	case StatusDone:
		if doneAt == nil {
			return fmt.Errorf("doneAt is required for status done: %w", dom.ErrInvalidArg)
		}
		if startedAt != nil {
			return fmt.Errorf("cannot set startedAt for status done: %w", dom.ErrInvalidArg)
		}
	}
	// validate status transition
	existing, err := s.GetReplicationSwitchInfo(ctx, replID)
	if err != nil {
		return err
	}
	switch existing.LastStatus {
	case "", StatusNotStarted, StatusError, StatusSkipped:
		// from starting status, only allowed transitions are to in progress, error or skipped
		if newStatus != StatusInProgress && newStatus != StatusError && newStatus != StatusSkipped {
			return fmt.Errorf("invalid status transition %s->%s: %w", existing.LastStatus, newStatus, dom.ErrInvalidArg)
		}
	case StatusInProgress:
		if newStatus != StatusCheckInProgress && newStatus != StatusError {
			return fmt.Errorf("invalid status transition %s->%s: %w", existing.LastStatus, newStatus, dom.ErrInvalidArg)
		}
	case StatusCheckInProgress:
		if newStatus != StatusDone && newStatus != StatusError {
			return fmt.Errorf("invalid status transition %s->%s: %w", existing.LastStatus, newStatus, dom.ErrInvalidArg)
		}

	case StatusDone:
		if newStatus != StatusDone {
			return fmt.Errorf("invalid status transition %s->%s: %w", existing.LastStatus, newStatus, dom.ErrInvalidArg)
		}
	}

	// update downtime switch status along with routing and replication policies in one transaction:
	pipe := s.client.TxPipeline()
	switch newStatus {
	// if switch in progress set routing block idempotently
	case StatusInProgress, StatusCheckInProgress:
		if err = addRoutingBlockWithClient(ctx, pipe, replID.From, replID.Bucket); err != nil {
			return fmt.Errorf("unable to add routing block: %w", err)
		}
	// if error, delete routing block idempotently
	case StatusError:
		if err = deleteRoutingBlockWithClient(ctx, pipe, replID.From, replID.Bucket); err != nil {
			return fmt.Errorf("unable to delete routing block: %w", err)
		}
	// if done, switch routing and replication to new bucket idempotently
	case StatusDone:
		if err = deleteRoutingBlockWithClient(ctx, pipe, replID.From, replID.Bucket); err != nil {
			return fmt.Errorf("unable to delete routing block: %w", err)
		}
		if err = addBucketRoutingPolicyWithClient(ctx, pipe, replID.User, replID.Bucket, replID.To, true); err != nil {
			return fmt.Errorf("unable to add bucket routing policy: %w", err)
		}
		if err = archiveReplicationWithClient(ctx, pipe, replID); err != nil {
			return fmt.Errorf("unable to archive replication: %w", err)
		}
		if existing.ContinueReplication {
			// create backwards replication policy
			key := fmt.Sprintf("p:repl:%s:%s", replID.User, replID.Bucket)
			val := fmt.Sprintf("%s:%s", replID.To, replID.From)
			pipe.ZAddNX(ctx, key, redis.Z{Member: val, Score: float64(existing.ReplicationPriority)})
			replBackID := replID
			replBackID.From, replBackID.To = replID.To, replID.From

			now := timeNow()
			statusKey := replBackID.StatusKey()
			res := ReplicationPolicyStatus{
				CreatedAt:      time.Now(),
				ListingStarted: true,
				//InitDoneAt:     &now,
			}
			pipe.HSet(ctx, statusKey, res)
			// set time separately because of go-redis bug with *time.Time
			pipe.HSet(ctx, statusKey, "init_done_at", now)
		}
	}
	// update switch status:
	pipe.HSet(ctx, replID.SwitchKey(), "lastStatus", string(newStatus))
	if startedAt != nil {
		pipe.HSet(ctx, replID.SwitchKey(), "startedAt", *startedAt)
	}
	if doneAt != nil {
		pipe.HSet(ctx, replID.SwitchKey(), "doneAt", *doneAt)
	}
	// update downtime switch status history
	history := fmt.Sprintf("%s | %s -> %s: %s", timeNow().Format(time.RFC3339), existing.LastStatus, newStatus, description)
	pipe.RPush(ctx, replID.SwitchHistoryKey(), history)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("unable to update downtime switch status: %w", err)
	}
	return nil
}

func (s *policySvc) CompleteZeroDowntimeReplicationSwitch(ctx context.Context, replID ReplicationID) error {
	info, err := s.GetReplicationSwitchInfo(ctx, replID)
	if err != nil {
		return err
	}
	if !info.IsZeroDowntime() {
		return fmt.Errorf("%w: cannot complete zero downtime switch: switch is not zero downtime", dom.ErrInvalidArg)
	}
	if info.LastStatus != StatusInProgress {
		return fmt.Errorf("%w: cannot complete zero downtime switch: switch is not in progress", dom.ErrInvalidArg)
	}
	now := timeNow()
	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, replID.SwitchKey(), "lastStatus", string(StatusDone))
	pipe.HSet(ctx, replID.SwitchKey(), "doneAt", now)
	// update downtime switch status history
	history := fmt.Sprintf("%s | %s -> %s: complete zero downtime switch", now.Format(time.RFC3339), info.LastStatus, StatusDone)
	pipe.RPush(ctx, replID.SwitchHistoryKey(), history)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("unable to update zero downtime switch status: %w", err)
	}
	return nil
}

func (s *policySvc) ListReplicationSwitchInfo(ctx context.Context) ([]SwitchInfo, error) {
	keys, err := s.client.Keys(ctx, "p:switch:*").Result()
	if err != nil {
		return nil, fmt.Errorf("unable to list replication switches: %w", err)
	}

	if len(keys) == 0 {
		return []SwitchInfo{}, nil
	}

	pipe := s.client.Pipeline()
	infoCmds := make([]*redis.MapStringStringCmd, len(keys))
	histCmds := make([]*redis.StringSliceCmd, len(keys))

	for i, key := range keys {
		infoCmds[i] = pipe.HGetAll(ctx, key)
		histCmds[i] = pipe.LRange(ctx, strings.Replace(key, "p:switch:", "p:switch-hist:", 1), 0, -1)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("failed to execute pipeline: %w", err)
	}
	res := make([]SwitchInfo, len(keys))
	for i, key := range keys {
		if err := infoCmds[i].Scan(&res[i]); err != nil {
			return nil, fmt.Errorf("unable to scan replication switch info %s: %w", key, err)
		}
		if err := infoCmds[i].Scan(&res[i].SwitchDowntimeOpts); err != nil {
			return nil, fmt.Errorf("unable to scan replication switch info downtime opts %s: %w", key, err)
		}
		if err := infoCmds[i].Scan(&res[i].SwitchZeroDowntimeOpts); err != nil {
			return nil, fmt.Errorf("unable to scan replication switch info zero downtime opts %s: %w", key, err)
		}
		res[i].History, err = histCmds[i].Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			return nil, fmt.Errorf("unable to get replication switch history %s: %w", key, err)
		}
	}
	return res, nil
}
