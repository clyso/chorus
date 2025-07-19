package entity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/redis/go-redis/v9"
)

type Pager struct {
	From  uint64
	Count uint64
}

type Page[T any] struct {
	Entries []T
	Next    uint64
}

type ScoredSetEntry[T any] struct {
	Value T
	Score uint8
}

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

type PrioritizedBucketReplicationPolicy ScoredSetEntry[BucketReplicationPolicy]

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

func (r *ReplicationStatusID) String() string {
	return fmt.Sprintf("%s:%s:%s:%s:%s", r.User, r.FromStorage, r.FromBucket, r.ToStorage, r.ToBucket)
}

func (r *ReplicationStatusID) FromString(s string) error {
	parts := strings.Split(s, ":")
	partsCount := len(parts)
	if partsCount != 5 {
		return fmt.Errorf("expected to have 5 parts, but got %d", partsCount)
	}
	r.User = parts[0]
	r.FromStorage = parts[1]
	r.FromBucket = parts[2]
	r.ToStorage = parts[3]
	r.ToBucket = parts[4]
	return nil
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
		*NewRedisIDKeyValue[BucketRoutingPolicyID, string](client, "p:route:user",
			BucketRoutingPolicyIDToTokensConverter, TokensToBucketRoutingPolicyIDConverter,
			StringValueConverter, StringValueConverter),
	}
}

type RoutingBlockStore struct {
	prefix string
	client redis.Cmdable
}

func NewRoutingBlockStore(client redis.Cmdable) *RoutingBlockStore {
	return &RoutingBlockStore{
		prefix: "p:route:block",
		client: client,
	}
}

func (r *RoutingBlockStore) makeID(id string) string {
	return r.prefix + ":" + id
}

func (r *RoutingBlockStore) Add(ctx context.Context, id string, val ...string) (uint64, error) {
	key := r.makeID(id)
	affected, err := r.client.SAdd(ctx, key, val).Result()
	if err != nil {
		return 0, err
	}
	return uint64(affected), nil
}

func (r *RoutingBlockStore) Remove(ctx context.Context, id string, val ...string) (uint64, error) {
	key := r.makeID(id)
	affected, err := r.client.SRem(ctx, key, val).Result()
	if err != nil {
		return 0, err
	}
	return uint64(affected), nil
}

func (r *RoutingBlockStore) IsMember(ctx context.Context, id string, val string) (bool, error) {
	key := r.makeID(id)
	exists, err := r.client.SIsMember(ctx, key, val).Result()
	if err != nil {
		return false, err
	}
	return exists, nil
}

type BucketReplicationPolicyStore struct {
	prefix string
	client redis.Cmdable
}

func NewBucketReplicationPolicyStore(client redis.Cmdable) *BucketRoutingPolicyStore {
	return &BucketRoutingPolicyStore{
		prefix: "p:repl:bucket",
		client: client,
	}
}

func (r *BucketReplicationPolicyStore) Add(ctx context.Context, id BucketReplicationPolicyID, values ...PrioritizedBucketReplicationPolicy) (uint64, error) {
	key := r.makeID(id)
	scoredSetValues := make([]redis.Z, 0, len(values))
	for _, value := range values {
		serializedValue, err := json.Marshal(value.Value)
		if err != nil {
			return 0, err
		}
		scoredSetValue := redis.Z{Member: serializedValue, Score: float64(value.Score)}
		scoredSetValues = append(scoredSetValues, scoredSetValue)
	}

	affected, err := r.client.ZAddNX(ctx, key, scoredSetValues...).Result()
	return uint64(affected), err
}

func (r *BucketReplicationPolicyStore) GetAll(ctx context.Context, id BucketReplicationPolicyID) ([]PrioritizedBucketReplicationPolicy, error) {
	key := r.makeID(id)
	result, err := r.client.ZRangeWithScores(ctx, key, 0, -1).Result()
	if errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("%w: no replication from policy for user %q", dom.ErrNotFound, key)
	}
	if err != nil {
		return nil, err
	}

	policies := make([]PrioritizedBucketReplicationPolicy, 0, len(result))
	for _, resultEntry := range result {
		var policy PrioritizedBucketReplicationPolicy
		serializedValue, ok := resultEntry.Member.([]byte)
		if !ok {
			return nil, fmt.Errorf("unable to cast to bytes")
		}

		if err := json.Unmarshal(serializedValue, &policy.Value); err != nil {
			return nil, fmt.Errorf("unable deserialize value: %w", err)
		}

		policy.Score = uint8(resultEntry.Score)
		policies = append(policies, policy)
	}

	return policies, nil
}

func (r *BucketReplicationPolicyStore) Remove(ctx context.Context, id BucketReplicationPolicyID, values ...PrioritizedBucketReplicationPolicy) (uint64, error) {
	key := r.makeID(id)
	scoredSetMemebers := make([][]byte, 0, len(values))
	for _, value := range values {
		serializedValue, err := json.Marshal(value.Value)
		if err != nil {
			return 0, err
		}
		scoredSetMemebers = append(scoredSetMemebers, serializedValue)
	}
	affected, err := r.client.ZRem(ctx, key, scoredSetMemebers).Result()
	return uint64(affected), err
}

func (r *BucketReplicationPolicyStore) makeID(id BucketReplicationPolicyID) string {
	return r.prefix + ":" + id.String()
}

type ReplicationStatusStore struct {
	prefix string
	client redis.Cmdable
}

func NewReplicationStatusStore(client redis.Cmdable) *ReplicationStatusStore {
	return &ReplicationStatusStore{
		prefix: "p:repl:status",
		client: client,
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

func (r *ReplicationStatusStore) SetFieldIfExists(ctx context.Context, id ReplicationStatusID, fieldName string, value any) error {
	key := r.makeID(id)
	result, err := luaHSetEx.Run(ctx, r.client, []string{key}, fieldName, value).Result()
	if err != nil {
		return err
	}
	inc, ok := result.(int64)
	if !ok {
		return fmt.Errorf("%w: unable to cast luaHSetEx result %T to int64", dom.ErrInternal, result)
	}
	if inc == 0 {
		return dom.ErrNotFound
	}
	return nil
}

func (r *ReplicationStatusStore) SetField(ctx context.Context, id ReplicationStatusID, fieldName string, value any) (uint64, error) {
	key := r.makeID(id)
	affected, err := r.client.HSet(ctx, key, fieldName, value).Result()
	if err != nil {
		return 0, err
	}
	return uint64(affected), nil
}

func (r *ReplicationStatusStore) IncrementField(ctx context.Context, id ReplicationStatusID, fieldName string) (int64, error) {
	return r.IncrementFieldByN(ctx, id, fieldName, 1)
}

func (r *ReplicationStatusStore) IncrementFieldByN(ctx context.Context, id ReplicationStatusID, fieldName string, value int64) (int64, error) {
	key := r.makeID(id)
	count, err := r.client.HIncrBy(ctx, key, fieldName, value).Result()
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (r *ReplicationStatusStore) IncrementFieldIfExists(ctx context.Context, id ReplicationStatusID, fieldName string) (int64, error) {
	return r.IncrementFieldByNIfExists(ctx, id, fieldName, 1)
}

func (r *ReplicationStatusStore) IncrementFieldByNIfExists(ctx context.Context, id ReplicationStatusID, fieldName string, value int64) (int64, error) {
	key := r.makeID(id)
	result, err := luaHIncrByEx.Run(ctx, r.client, []string{key}, fieldName, value).Result()
	if err != nil {
		return 0, err
	}
	count, ok := result.(int64)
	if !ok {
		return 0, fmt.Errorf("%w: unable to cast luaHIncrByEx result %T to int64", dom.ErrInternal, result)
	}
	if count == 0 {
		return 0, dom.ErrNotFound
	}
	return count, nil
}

func (r *ReplicationStatusStore) Set(ctx context.Context, id ReplicationStatusID, info ReplicationStatus) error {
	key := r.makeID(id)
	if err := r.client.HSet(ctx, key, info).Err(); err != nil {
		return fmt.Errorf("unable to set value in hash: %w", err)
	}

	return nil
}

func (r *ReplicationStatusStore) Get(ctx context.Context, id ReplicationStatusID) (*ReplicationStatus, error) {
	key := r.makeID(id)
	cmd := r.client.HGetAll(ctx, key)
	if err := cmd.Err(); err != nil {
		return nil, err
	}
	entity := &ReplicationStatus{}
	if err := cmd.Scan(entity); err != nil {
		return nil, err
	}
	return entity, nil
}

func (r *ReplicationStatusStore) GetAllIDs(ctx context.Context, keyParts ...string) ([]ReplicationStatusID, error) {
	pager := Pager{
		From:  0,
		Count: 100,
	}

	ids := []ReplicationStatusID{}
	for {
		idPage, err := r.GetIDs(ctx, pager, keyParts...)
		if err != nil {
			return nil, err
		}
		ids = append(ids, idPage.Entries...)

		if idPage.Next == 0 {
			break
		}
		pager.From = idPage.Next
	}

	return ids, nil
}

func (r *ReplicationStatusStore) GetIDs(ctx context.Context, pager Pager, keyParts ...string) (*Page[ReplicationStatusID], error) {
	selector := r.makeWildcardSelector()
	keys, cursor, err := r.client.Scan(ctx, pager.From, selector, int64(pager.Count)).Result()
	if err != nil {
		return nil, err
	}

	ids := make([]ReplicationStatusID, 0, len(keys))
	for _, key := range keys {
		trimmedKey := strings.TrimPrefix(key, r.prefix+":")
		var id ReplicationStatusID
		if err := id.FromString(trimmedKey); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	return &Page[ReplicationStatusID]{
		Entries: ids,
		Next:    cursor,
	}, nil
}

// TODO make id method to use a set of key parts, rather than string method
// rename to make key
func (r *ReplicationStatusStore) makeID(id ReplicationStatusID) string {
	return r.prefix + ":" + id.String()
}

func (r *ReplicationStatusStore) makeWildcardSelector(keyParts ...string) string {
	joinedKeyParts := strings.Join(keyParts, ":")
	if joinedKeyParts == "" {
		return r.prefix + ":*"
	}
	return r.prefix + joinedKeyParts + ":*"
}
