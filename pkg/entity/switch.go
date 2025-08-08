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

package entity

import (
	"fmt"
	"time"

	"github.com/adhocore/gronx"
)

var (
	// TimeNow is a function to get current time
	// it is a variable so that it can be mocked in tests
	TimeNow = time.Now
)

type ReplicationSwitchInfoID struct {
	User       string
	FromBucket string
}

func NewReplicationSwitchInfoID(user string, fromBucket string) ReplicationSwitchInfoID {
	return ReplicationSwitchInfoID{
		User:       user,
		FromBucket: fromBucket,
	}
}

type ReplicationSwitchStatus string

const (
	// StatusNotStarted means that switch donwntime is not started yet
	// Relevant only for downtime switches
	StatusNotStarted ReplicationSwitchStatus = "not_started"
	// StatusInProgress means that downtime is started, bucket is blocked until task queue is drained or timeout
	StatusInProgress ReplicationSwitchStatus = "in_progress"
	// StatusCheckInProgress means that task queue is drained and bucket is blocked until src and dst bucket contents will be checked
	// Relevant only for downtime switches
	StatusCheckInProgress ReplicationSwitchStatus = "check_in_progress"
	// StatusDone means that switch is successfully finished and data is routed to new bucket.
	StatusDone ReplicationSwitchStatus = "done"
	// StatusError means that switch was aborted due to error
	// Relevant only for downtime switches
	StatusError ReplicationSwitchStatus = "error"
	// StatusSkipped means that switch attempt was skipped because conditions were not met
	// Relevant only for downtime switches
	StatusSkipped ReplicationSwitchStatus = "skipped"
)

func (s *ReplicationSwitchStatus) UnmarshalBinary(data []byte) error {
	*s = ReplicationSwitchStatus(data)
	return nil
}

func (s ReplicationSwitchStatus) MarshalBinary() (data []byte, err error) {
	return []byte(s), nil
}

type ReplicationSwitchZeroDowntimeOpts struct {
	MultipartTTL time.Duration `redis:"multipartTTL,omitempty"`
}

type ReplicationSwitchDowntimeOpts struct {
	StartOnInitDone     bool          `redis:"onInitDone"`
	Cron                *string       `redis:"cron,omitempty"`
	StartAt             *time.Time    `redis:"startAt,omitempty"`
	MaxDuration         time.Duration `redis:"maxDuration,omitempty"`
	MaxEventLag         *uint32       `redis:"maxEventLag,omitempty"`
	SkipBucketCheck     bool          `redis:"skipBucketCheck,omitempty"`
	ContinueReplication bool          `redis:"continueReplication,omitempty"`
}

func (w *ReplicationSwitchDowntimeOpts) GetCron() (string, bool) {
	if w != nil && w.Cron != nil && *w.Cron != "" {
		return *w.Cron, true
	}
	return "", false
}

func (w *ReplicationSwitchDowntimeOpts) GetStartAt() (time.Time, bool) {
	if w != nil && w.StartAt != nil && !w.StartAt.IsZero() {
		return *w.StartAt, true
	}
	return time.Time{}, false
}

func (w *ReplicationSwitchDowntimeOpts) GetMaxEventLag() (uint32, bool) {
	if w != nil && w.MaxEventLag != nil {
		return *w.MaxEventLag, true
	}
	return 0, false
}

func (w *ReplicationSwitchDowntimeOpts) GetMaxDuration() (time.Duration, bool) {
	if w != nil && w.MaxDuration > 0 {
		return w.MaxDuration, true
	}
	return 0, false
}

// Reduced replication switch info for in progress zero downtime switch.
// Subset of SwitchInfo fields.
// Used by proxy to route requests to correct bucket during zero downtime switch.
type ZeroDowntimeSwitchInProgressInfo struct {
	ReplID           ReplicationStatusID     `redis:"-"`
	ReplicationIDStr string                  `redis:"replicationID"`
	Status           ReplicationSwitchStatus `redis:"lastStatus"`
	MultipartTTL     time.Duration           `redis:"multipartTTL"`
}

func (s *ZeroDowntimeSwitchInProgressInfo) ReplicationID() ReplicationStatusID {
	return s.ReplID
}

// Contains all information about replication switch including its configuration and current status.
type ReplicationSwitchInfo struct {
	// Options for downtime switch
	ReplicationSwitchDowntimeOpts
	// Options for zero downtime switch
	ReplicationSwitchZeroDowntimeOpts
	// ID of replication policy of this switch
	ReplID           ReplicationStatusID `redis:"-"`
	ReplicationIDStr string              `redis:"replicationID"`
	// Time of switch creation
	CreatedAt time.Time `redis:"createdAt"`
	// Last status of switch
	LastStatus ReplicationSwitchStatus `redis:"lastStatus,omitempty"`
	// Time of last switch was in progress
	LastStartedAt *time.Time `redis:"startedAt,omitempty"`
	// Time of last switch was done
	DoneAt *time.Time `redis:"doneAt,omitempty"`
	// History of switch status changes
	History []string `redis:"-"`
}

func (s *ReplicationSwitchInfo) ReplicationID() ReplicationStatusID {
	return s.ReplID
}

func (s *ReplicationSwitchInfo) IsZeroDowntime() bool {
	return s.MultipartTTL > 0
}

func (s *ReplicationSwitchInfo) GetLastStartAt() (time.Time, bool) {
	if s != nil && s.LastStartedAt != nil && !s.LastStartedAt.IsZero() {
		return *s.LastStartedAt, true
	}
	return time.Time{}, false
}

func (s *ReplicationSwitchInfo) IsTimeToStart() (bool, error) {
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
		return startAt.Before(TimeNow()), nil
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
		return nextTick.Compare(TimeNow()) <= 0, nil
	}
	// if it is first execution, calculate next tick from switch creation time
	nextTick, err := gronx.NextTickAfter(cron, s.CreatedAt, true)
	if err != nil {
		return false, err
	}
	// next <= now means it is time to start
	return nextTick.Compare(TimeNow()) <= 0, nil
}
