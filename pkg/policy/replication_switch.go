package policy

import (
	"context"
	"fmt"
	"time"

	"github.com/adhocore/gronx"
)

var (
	// timeNow is a function to get current time
	// it is a variable so that it can be mocked in tests
	timeNow = time.Now
)

type Window struct {
	StartOnInitDone bool           `redis:"onInitDone,omitempty"`
	Cron            *string        `redis:"cron,omitempty"`
	StartAt         *time.Time     `redis:"startAt,omitempty"`
	MaxDuration     *time.Duration `redis:"maxDuration,omitempty"`
	MaxEventLag     *uint32        `redis:"maxEventLag,omitempty"`
	SkipBucketCheck bool           `redis:"skipBucketCheck,omitempty"`
}

func (w *Window) GetCron() (string, bool) {
	if w != nil && w.Cron != nil && *w.Cron != "" {
		return *w.Cron, true
	}
	return "", false
}

func (w *Window) GetStartAt() (time.Time, bool) {
	if w != nil && w.StartAt != nil && !w.StartAt.IsZero() {
		return *w.StartAt, true
	}
	return time.Time{}, false
}

func (w *Window) GetMaxEventLag() (uint32, bool) {
	if w != nil && w.MaxEventLag != nil {
		return *w.MaxEventLag, true
	}
	return 0, false
}

func (w *Window) GetMaxDuration() (time.Duration, bool) {
	if w != nil && w.MaxDuration != nil && *w.MaxDuration > 0 {
		return *w.MaxDuration, true
	}
	return 0, false
}

type SwitchWithDowntime struct {
	Window
	ContinueReplication bool                     `redis:"continueReplication"`
	CreatedAt           time.Time                `redis:"createdAt"`
	LastStatus          SwitchWithDowntimeStatus `redis:"lastStatus"`
	LastStartedAt       *time.Time               `redis:"startedAt"`
	DoneAt              *time.Time               `redis:"doneAt,omitempty"`
	History             []string                 `redis:"-"`
}

func (s *SwitchWithDowntime) GetLastStartAt() (time.Time, bool) {
	if s != nil && s.LastStartedAt != nil && !s.LastStartedAt.IsZero() {
		return *s.LastStartedAt, true
	}
	return time.Time{}, false
}

func (s *SwitchWithDowntime) IsTimeToStart() (bool, error) {
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

func (s *policySvc) SetReplicationSwitchWithDowntime(ctx context.Context, replID ReplicationID, downtimeWindow *Window) error {
	panic("unimplemented")
}

func (s *policySvc) DeleteReplicationSwitchWithDowntime(ctx context.Context, replID ReplicationID) error {
	panic("unimplemented")
}

func (s *policySvc) GetReplicationSwitchWithDowntime(ctx context.Context, replID ReplicationID) (SwitchWithDowntime, error) {
	panic("unimplemented")
}

func (s *policySvc) UpdateSwitchWithDowntimeStatus(ctx context.Context, replID ReplicationID, newStatus SwitchWithDowntimeStatus, description string, startedAt, doneAt *time.Time) error {
	panic("unimplemented")
}
func (s *policySvc) CompleteReplicationSwitchWithDowntime(ctx context.Context, replID ReplicationID, continueReplication bool) error {
	panic("unimplemented")
}
