package entity

import (
	"context"
	"encoding/json"
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

// type ScoredSetEntry[T any] struct {
// 	Value T
// 	Score uint8
// }

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
		User:   tokens[0],
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
