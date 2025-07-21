package entity

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/redis/go-redis/v9"
)

type ReplicationSwitchID struct {
	User       string
	FromBucket string
}

func (r *ReplicationSwitchID) String() string {
	return fmt.Sprintf("%s:%s", r.User, r.FromBucket)
}

type BucketReplicationPolicyID struct {
	User       string
	FromBucket string
}

func (r *BucketReplicationPolicyID) ToTokens() ([]string, error) {
	return []string{r.User, r.FromBucket}, nil
}

func (r *BucketReplicationPolicyID) FromTokens(tokens []string) error {
	if len(tokens) != 2 {
		return dom.ErrInvalidArg
	}
	r.User = tokens[0]
	r.FromBucket = tokens[1]
	return nil
}

func (r *BucketReplicationPolicyID) String() string {
	return fmt.Sprintf("%s:%s", r.User, r.FromBucket)
}

type BucketReplicationPolicy struct {
	FromStorage string
	ToStorage   string
	ToBucket    string
}

type BucketRoutingPolicyID struct {
	User   string
	Bucket string
}

func (r *BucketRoutingPolicyID) String() string {
	return fmt.Sprintf("%s:%s", r.User, r.Bucket)
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

type UserRoutingPolicyStore struct {
	RedisIDKeyValue[string, string]
}

func NewUserRoutingPolicyStore(client redis.Cmdable) *UserRoutingPolicyStore {
	return &UserRoutingPolicyStore{
		*NewRedisIDKeyValue[string, string](client, "p:route:user",
			StringToSingleTokenConverter, SingleTokenToStringConverter,
			StringValueConverter, StringValueConverter),
	}
}

func TokensToBucketRoutingPolicyIDConverter(tokens []string) (BucketRoutingPolicyID, error) {
	return BucketRoutingPolicyID{
		User:   tokens[0],
		Bucket: tokens[1],
	}, nil
}

func BucketRoutingPolicyIDToTokensConverter(id BucketRoutingPolicyID) ([]string, error) {
	return []string{id.User, id.Bucket}, nil
}

type BucketRoutingPolicyStore struct {
	RedisIDKeyValue[BucketRoutingPolicyID, string]
}

func NewBucketRoutingPolicyStore(client redis.Cmdable) *BucketRoutingPolicyStore {
	return &BucketRoutingPolicyStore{
		*NewRedisIDKeyValue[BucketRoutingPolicyID, string](client, "p:route:bucket",
			BucketRoutingPolicyIDToTokensConverter, TokensToBucketRoutingPolicyIDConverter,
			StringValueConverter, StringValueConverter),
	}
}

type RoutingBlockStore struct {
	RedisIDKeySet[string, string]
}

func NewRoutingBlockStore(client redis.Cmdable) *RoutingBlockStore {
	return &RoutingBlockStore{
		*NewRedisIDKeySet(client, "p:route:block",
			StringToSingleTokenConverter, SingleTokenToStringConverter,
			StringValueConverter, StringValueConverter),
	}
}

func TokensToBucketReplicationPolicyIDConverter(tokens []string) (BucketReplicationPolicyID, error) {
	return BucketReplicationPolicyID{
		User:       tokens[0],
		FromBucket: tokens[1],
	}, nil
}

func BucketReplicationPolicyIDToTokensConverter(id BucketReplicationPolicyID) ([]string, error) {
	return []string{id.User, id.FromBucket}, nil
}

func BucketReplicationPolicyToStringConverter(value BucketReplicationPolicy) (string, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("unable to serialize bucket replication policy: %w", err)
	}
	return string(bytes), nil
}

func StringToBucketReplicationPolicyConverter(value string) (BucketReplicationPolicy, error) {
	var result BucketReplicationPolicy
	if err := json.Unmarshal([]byte(value), &result); err != nil {
		var noVal BucketReplicationPolicy
		return noVal, fmt.Errorf("unable to deserialize bucket replication policy: %w", err)
	}
	return result, nil
}

func Uint8ToFloat64Converter(value uint8) (float64, error) {
	return float64(value), nil
}

func Float64ToUint8Converter(value float64) (uint8, error) {
	return uint8(value), nil
}

type BucketReplicationPolicyStore struct {
	RedisIDKeySortedSet[BucketReplicationPolicyID, BucketReplicationPolicy, uint8]
}

func NewBucketReplicationPolicyStore(client redis.Cmdable) *BucketReplicationPolicyStore {
	return &BucketReplicationPolicyStore{
		*NewRedisIDKeySortedSet(client, "p:repl:bucket",
			BucketReplicationPolicyIDToTokensConverter, TokensToBucketReplicationPolicyIDConverter,
			BucketReplicationPolicyToStringConverter, StringToBucketReplicationPolicyConverter,
			Uint8ToFloat64Converter, Float64ToUint8Converter),
	}
}

func ReplicationStatusIDToTokensConverter(value ReplicationStatusID) ([]string, error) {
	return []string{value.User, value.FromStorage, value.FromBucket, value.ToStorage, value.ToBucket}, nil
}

func TokensToReplicationStatusIDConverter(values []string) (ReplicationStatusID, error) {
	return ReplicationStatusID{
		User:        values[0],
		FromStorage: values[1],
		FromBucket:  values[2],
		ToStorage:   values[3],
		ToBucket:    values[4],
	}, nil
}

type ReplicationStatusStore struct {
	RedisIDKeyHash[ReplicationStatusID, ReplicationStatus]
}

func NewReplicationStatusStore(client redis.Cmdable) *ReplicationStatusStore {
	return &ReplicationStatusStore{
		*NewRedisIDKeyHash[ReplicationStatusID, ReplicationStatus](
			client, "p:repl:status", ReplicationStatusIDToTokensConverter, TokensToReplicationStatusIDConverter),
	}
}

func (r *ReplicationStatusStore) SetListingStarted(ctx context.Context, id ReplicationStatusID) error {
	return r.SetFieldIfExists(ctx, id, "listing_started", true)
}

func (r *ReplicationStatusStore) SetLastEmittedAt(ctx context.Context, id ReplicationStatusID, value time.Time) error {
	return r.SetFieldIfExists(ctx, id, "last_emitted_at", value.UTC())
}

func (r *ReplicationStatusStore) IncrementEvents(ctx context.Context, id ReplicationStatusID) (int64, error) {
	return r.IncrementFieldIfExists(ctx, id, "events")
}
