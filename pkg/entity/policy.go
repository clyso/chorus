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
	"time"
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

type BucketReplicationPolicy struct {
	FromStorage string
	ToStorage   string
	ToBucket    string
}

func NewBucketReplicationPolicy(fromStorage string, toStorage string, toBucket string) BucketReplicationPolicy {
	return BucketReplicationPolicy{
		FromStorage: fromStorage,
		ToStorage:   toStorage,
		ToBucket:    toBucket,
	}
}

type UserReplicationPolicy struct {
	FromStorage string
	ToStorage   string
}

func NewUserReplicationPolicy(fromStorage string, toStorage string) UserReplicationPolicy {
	return UserReplicationPolicy{
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

	ListingStarted bool `redis:"listing_started"`

	HasSwitch bool `redis:"-"`
}

type ReplicationStatusExtended struct {
	*ReplicationStatus

	// True if at least one of the queues is paused.
	IsPaused bool
	// Aggregated stats for initial migration queues.
	InitMigration QueueStats
	// Aggregated stats for event migration queues.
	EventMigration QueueStats
}

func (r *ReplicationStatusExtended) InitDone() bool {
	return r.ListingStarted && r.InitMigration.Unprocessed == 0
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

type ReplicationStatusID struct {
	User        string
	FromStorage string
	FromBucket  string
	ToStorage   string
	ToBucket    string
}

func NewReplicationStatusID(user string, fromStorage string, fromBucket string, toStorage string, toBucket string) ReplicationStatusID {
	return ReplicationStatusID{
		User:        user,
		FromStorage: fromStorage,
		FromBucket:  fromBucket,
		ToStorage:   toStorage,
		ToBucket:    toBucket,
	}
}

type ReplicationPolicyDestination struct {
	Storage string
	Bucket  string
}

func NewBucketReplicationPolicyDestination(storage string, bucket string) ReplicationPolicyDestination {
	return ReplicationPolicyDestination{
		Storage: storage,
		Bucket:  bucket,
	}
}

func NewUserReplicationPolicyDestination(storage string) ReplicationPolicyDestination {
	return ReplicationPolicyDestination{
		Storage: storage,
	}
}

type StorageReplicationPolicies struct {
	FromStorage  string
	Destinations []ReplicationPolicyDestination
}
