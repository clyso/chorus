package policy

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/adhocore/gronx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/redis/go-redis/v9"
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

// Reduced replication switch info for in progress zero downtime switch
type ZeroDowntimeSwitchInProgressInfo struct {
	ReplicationIDStr string                   `redis:"replicationID"`
	Status           SwitchWithDowntimeStatus `redis:"lastStatus"`
	MultipartTTL     time.Duration            `redis:"multipartTTL"`
}

func (s *ZeroDowntimeSwitchInProgressInfo) ReplicationID() (ReplicationID, error) {
	return ReplicationIDFromStr(s.ReplicationIDStr)
}

type SwitchInfo struct {
	SwitchDowntimeOpts
	SwitchZeroDowntimeOpts
	ReplicationIDStr string                   `redis:"replicationID"`
	CreatedAt        time.Time                `redis:"createdAt"`
	LastStatus       SwitchWithDowntimeStatus `redis:"lastStatus,omitempty"`
	LastStartedAt    *time.Time               `redis:"startedAt,omitempty"`
	DoneAt           *time.Time               `redis:"doneAt,omitempty"`
	History          []string                 `redis:"-"`
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

type SwitchWithDowntimeStatus string

func (s *SwitchWithDowntimeStatus) UnmarshalBinary(data []byte) error {
	*s = SwitchWithDowntimeStatus(data)
	return nil
}

func (s SwitchWithDowntimeStatus) MarshalBinary() (data []byte, err error) {
	return []byte(s), nil
}

const (
	// StatusNotStarted means that switch donwntime is not started yet
	StatusNotStarted SwitchWithDowntimeStatus = "not_started"
	// StatusInProgress means that downtime is started, bucket is blocked until task queue is drained or timeout
	StatusInProgress SwitchWithDowntimeStatus = "in_progress"
	// StatusCheckInProgress means that task queue is drained and bucket is blocked until src and dst bucket contents will be checked
	StatusCheckInProgress SwitchWithDowntimeStatus = "check_in_progress"
	// StatusDone means that switch is successfully finished and data is routed to new bucket.
	StatusDone SwitchWithDowntimeStatus = "done"
	// StatusError means that switch was aborted due to error
	StatusError SwitchWithDowntimeStatus = "error"
	// StatusSkipped means that switch attempt was skipped because conditions were not met
	StatusSkipped SwitchWithDowntimeStatus = "skipped"
)

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
			return fmt.Errorf("cannot update donwntime switch: there is existing zero downtime switch for given replication")
		}
		if existing.LastStatus == StatusInProgress || existing.LastStatus == StatusCheckInProgress {
			return fmt.Errorf("cannot update downtime switch: switch is already in progress")
		}
		if existing.LastStatus == StatusDone {
			return fmt.Errorf("cannot update downtime switch: switch is already completed")
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
		return fmt.Errorf("cannot create downtime switch: given replication is agent based")
	}
	forceStartNow := opts == nil || (opts.StartAt == nil && opts.Cron == nil && !opts.StartOnInitDone)
	if forceStartNow && !policy.InitDone() {
		return fmt.Errorf("cannot create downtime switch: init replication is not done")
	}

	policies, err := s.GetBucketReplicationPolicies(ctx, replID.User, replID.Bucket)
	if err != nil {
		return fmt.Errorf("unable to get replication policies: %w", err)
	}
	if len(policies.To) != 1 {
		return fmt.Errorf("cannot create switch: existing bucket replication should have a single destination")
	}
	info := &SwitchInfo{
		CreatedAt:        timeNow(),
		ReplicationIDStr: replID.String(),
		LastStatus:       StatusNotStarted,
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
		return fmt.Errorf("cannot create zero-downtime switch: given replication is agent based")
	}
	if !policy.InitDone() {
		return fmt.Errorf("cannot create zero-downtime switch: init replication is not done")
	}
	if policy.IsPaused {
		return fmt.Errorf("cannot create zero-downtime switch: replication is paused")
	}
	policies, err := s.GetBucketReplicationPolicies(ctx, replID.User, replID.Bucket)
	if err != nil {
		return fmt.Errorf("unable to get replication policies: %w", err)
	}
	if len(policies.To) != 1 {
		return fmt.Errorf("cannot create switch: existing bucket replication should have a single destination")
	}

	// validate routing policy
	toStorage, err := s.getRoutingPolicy(ctx, replID.User, replID.Bucket)
	if err != nil {
		return fmt.Errorf("unable to get routing policy: %w", err)
	}
	if toStorage != replID.From {
		return fmt.Errorf("cannot create zero-downtime switch: given replication is not routed to destination")
	}

	now := timeNow()
	pipe := s.client.TxPipeline()
	// switch routing
	pipe.Set(ctx, replID.RoutingKey(), replID.To, 0)
	// create switch metadata
	pipe.HSet(ctx, replID.SwitchKey(), SwitchInfo{
		ReplicationIDStr: replID.String(),
		CreatedAt:        now,
		LastStatus:       StatusInProgress,
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
	err := s.client.HMGet(ctx, key, "multipartTTL", "lastStatus", "replicationID").Scan(&info)
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
func (s *policySvc) UpdateDowntimeSwitchStatus(ctx context.Context, replID ReplicationID, newStatus SwitchWithDowntimeStatus, description string, startedAt *time.Time, doneAt *time.Time) error {
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
			pipe.ZAddNX(ctx, key, redis.Z{Member: val, Score: float64(tasks.PriorityDefault1)})
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
	pipe.LPush(ctx, replID.SwitchHistoryKey(), history)
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
		return fmt.Errorf("cannot complete zero downtime switch: switch is not zero downtime")
	}
	if info.LastStatus != StatusInProgress {
		return fmt.Errorf("cannot complete zero downtime switch: switch is not in progress")
	}
	now := timeNow()
	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, replID.SwitchKey(), "lastStatus", string(StatusDone))
	pipe.HSet(ctx, replID.SwitchKey(), "doneAt", now)
	// update downtime switch status history
	history := fmt.Sprintf("%s | %s -> %s: complete zero downtime switch", now.Format(time.RFC3339), info.LastStatus, StatusDone)
	pipe.LPush(ctx, replID.SwitchHistoryKey(), history)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("unable to update zero downtime switch status: %w", err)
	}
	return nil
}
