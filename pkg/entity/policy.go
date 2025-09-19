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
	"errors"
	"fmt"
	"time"

	"github.com/clyso/chorus/pkg/dom"
)

type BucketReplicationPolicyID struct {
	User       string
	FromBucket string
}

func NewBucketReplicationPolicyID(user string, fromBucket string) BucketReplicationPolicyID {
	return BucketReplicationPolicyID{
		User:       user,
		FromBucket: fromBucket,
	}
}

type BucketReplicationPolicyDestination struct {
	ToStorage string
	ToBucket  string
}

type UserReplicationPolicy struct {
	User        string
	FromStorage string
	ToStorage   string
}

func (p UserReplicationPolicy) LookupID() string {
	return p.User
}

func (p UserReplicationPolicy) RoutingID() string {
	return p.User
}

func (p UserReplicationPolicy) Validate() error {
	if p.User == "" {
		return fmt.Errorf("%w: user is required", dom.ErrInvalidArg)
	}
	if p.FromStorage == "" {
		return fmt.Errorf("%w: from storage is required", dom.ErrInvalidArg)
	}
	if p.ToStorage == "" {
		return fmt.Errorf("%w: to storage is required", dom.ErrInvalidArg)
	}
	if p.FromStorage == p.ToStorage {
		return fmt.Errorf("%w: from/to storages should differ", dom.ErrInvalidArg)
	}
	return nil
}

func NewUserReplicationPolicy(user string, fromStorage string, toStorage string) UserReplicationPolicy {
	return UserReplicationPolicy{
		User:        user,
		FromStorage: fromStorage,
		ToStorage:   toStorage,
	}
}

type BucketRoutingPolicyID struct {
	User   string
	Bucket string
}

func NewBucketRoutingPolicyID(user string, bucket string) BucketRoutingPolicyID {
	return BucketRoutingPolicyID{
		User:   user,
		Bucket: bucket,
	}
}

type ReplicationStatus struct {
	CreatedAt  time.Time `redis:"created_at"`
	IsArchived bool      `redis:"archived"`
	AgentURL   string    `redis:"agent_url,omitempty"`

	ArchivedAt *time.Time `redis:"archived_at,omitempty"`
}

type ReplicationStatusExtended struct {
	*ReplicationStatus
	Switch *ReplicationSwitchInfo `redis:"-"`

	// True if at least one of the queues is paused.
	IsPaused bool
	// Aggregated stats for initial migration queues.
	InitMigration QueueStats
	// Aggregated stats for event migration queues.
	EventMigration QueueStats
}

func (r *ReplicationStatusExtended) InitDone() bool {
	return r.InitMigration.Unprocessed == 0
}

type QueueStats struct {
	// Number of tasks left. Includes, new, in_progress, and retried tasks.
	Unprocessed int
	// Total number of successfully processed tasks.
	Done int
	// Failed  are tasks that exceeded maximum retries and were removed from the queue.
	Failed int
	// Age of the oldest pending task in the queue.
	Latency time.Duration
	// Approx bytes used by the queue and its tasks in Redis.
	MemoryUsage int64
}

type BucketReplicationPolicy struct {
	User        string
	FromStorage string
	FromBucket  string
	ToStorage   string
	ToBucket    string
}

func (p BucketReplicationPolicy) LookupID() BucketReplicationPolicyID {
	return BucketReplicationPolicyID{
		User:       p.User,
		FromBucket: p.FromBucket,
	}
}

func (p BucketReplicationPolicy) Destination() BucketReplicationPolicyDestination {
	return BucketReplicationPolicyDestination{
		ToStorage: p.ToStorage,
		ToBucket:  p.ToBucket,
	}
}

func (p BucketReplicationPolicy) RoutingID() BucketRoutingPolicyID {
	return BucketRoutingPolicyID{
		User:   p.User,
		Bucket: p.FromBucket,
	}
}

func (p BucketReplicationPolicy) Validate() error {
	errs := make([]error, 0)
	if p.User == "" {
		err := fmt.Errorf("%w: user is required", dom.ErrInvalidArg)
		errs = append(errs, err)
	}
	if p.FromStorage == "" {
		err := fmt.Errorf("%w: from storage is required", dom.ErrInvalidArg)
		errs = append(errs, err)
	}
	if p.FromBucket == "" {
		err := fmt.Errorf("%w: from bucket is required", dom.ErrInvalidArg)
		errs = append(errs, err)
	}
	if p.ToStorage == "" {
		err := fmt.Errorf("%w: to storage is required", dom.ErrInvalidArg)
		errs = append(errs, err)
	}
	if p.ToBucket == "" {
		err := fmt.Errorf("%w: to bucket is required", dom.ErrInvalidArg)
		errs = append(errs, err)
	}
	if p.FromStorage == p.ToStorage && p.FromBucket == p.ToBucket {
		err := fmt.Errorf("%w: from/to storages and/or buckets should differ", dom.ErrInvalidArg)
		errs = append(errs, err)
	}

	if len(errs) != 0 {
		return errors.Join(errs...)
	}
	return nil
}

func NewBucketRepliationPolicy(user string, fromStorage string, fromBucket string, toStorage string, toBucket string) BucketReplicationPolicy {
	return BucketReplicationPolicy{
		User:        user,
		FromStorage: fromStorage,
		FromBucket:  fromBucket,
		ToStorage:   toStorage,
		ToBucket:    toBucket,
	}
}
