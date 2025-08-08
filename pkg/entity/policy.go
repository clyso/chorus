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
	CreatedAt       time.Time `redis:"created_at"`
	IsPaused        bool      `redis:"paused"`
	IsArchived      bool      `redis:"archived"`
	InitObjListed   int64     `redis:"obj_listed"`
	InitObjDone     int64     `redis:"obj_done"`
	InitBytesListed int64     `redis:"bytes_listed"`
	InitBytesDone   int64     `redis:"bytes_done"`
	Events          int64     `redis:"events"`
	EventsDone      int64     `redis:"events_done"`
	AgentURL        string    `redis:"agent_url,omitempty"`

	InitDoneAt      *time.Time `redis:"init_done_at,omitempty"`
	LastEmittedAt   *time.Time `redis:"last_emitted_at,omitempty"`
	LastProcessedAt *time.Time `redis:"last_processed_at,omitempty"`
	ArchivedAt      *time.Time `redis:"archived_at,omitempty"`

	ListingStarted bool `redis:"listing_started"`

	HasSwitch bool `redis:"-"`
}

func (r *ReplicationStatus) InitDone() bool {
	return r.ListingStarted && r.InitDoneAt != nil && r.InitObjDone >= r.InitObjListed
}

type ReplicationStatusID struct {
	User        string
	FromStorage string
	FromBucket  string
	ToStorage   string
	ToBucket    string
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
