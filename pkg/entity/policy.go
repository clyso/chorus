package entity

import "time"

type BucketReplicationPolicyID struct {
	User       string
	FromBucket string
}

func NewBucketReplicationPolicyID(user string, fromBucket string) *BucketReplicationPolicyID {
	return &BucketReplicationPolicyID{
		User:       user,
		FromBucket: fromBucket,
	}
}

type BucketReplicationPolicy struct {
	FromStorage string
	ToStorage   string
	ToBucket    string
}

func NewBucketReplicationPolicy(fromStorage string, toStorage string, toBucket string) *BucketReplicationPolicy {
	return &BucketReplicationPolicy{
		FromStorage: fromStorage,
		ToStorage: toStorage,
		ToBucket: toBucket,
	}
}

type BucketRoutingPolicyID struct {
	User   string
	Bucket string
}

func NewBucketRoutingPolicyID(user string, bucket string) *BucketRoutingPolicyID {
	return &BucketRoutingPolicyID{
		User:       user,
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

type ReplicationStatusID struct {
	User        string
	FromStorage string
	FromBucket  string
	ToStorage   string
	ToBucket    string
}