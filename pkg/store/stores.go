package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/redis/go-redis/v9"
)

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

func (r *UserRoutingPolicyStore) WithExecutor(exec Executor[redis.Pipeliner]) *UserRoutingPolicyStore {
	return NewUserRoutingPolicyStore(exec.Get())
}

func TokensToBucketRoutingPolicyIDConverter(tokens []string) (entity.BucketRoutingPolicyID, error) {
	return entity.BucketRoutingPolicyID{
		User:   tokens[0],
		Bucket: tokens[1],
	}, nil
}

func BucketRoutingPolicyIDToTokensConverter(id entity.BucketRoutingPolicyID) ([]string, error) {
	return []string{id.User, id.Bucket}, nil
}

type BucketRoutingPolicyStore struct {
	RedisIDKeyValue[entity.BucketRoutingPolicyID, string]
}

func NewBucketRoutingPolicyStore(client redis.Cmdable) *BucketRoutingPolicyStore {
	return &BucketRoutingPolicyStore{
		*NewRedisIDKeyValue[entity.BucketRoutingPolicyID, string](client, "p:route:bucket",
			BucketRoutingPolicyIDToTokensConverter, TokensToBucketRoutingPolicyIDConverter,
			StringValueConverter, StringValueConverter),
	}
}

func (r *BucketRoutingPolicyStore) WithExecutor(exec Executor[redis.Pipeliner]) *BucketRoutingPolicyStore {
	return NewBucketRoutingPolicyStore(exec.Get())
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

func (r *RoutingBlockStore) WithExecutor(exec Executor[redis.Pipeliner]) *RoutingBlockStore {
	return NewRoutingBlockStore(exec.Get())
}

func TokensToBucketReplicationPolicyIDConverter(tokens []string) (entity.BucketReplicationPolicyID, error) {
	return entity.BucketReplicationPolicyID{
		User:       tokens[0],
		FromBucket: tokens[1],
	}, nil
}

func BucketReplicationPolicyIDToTokensConverter(id entity.BucketReplicationPolicyID) ([]string, error) {
	return []string{id.User, id.FromBucket}, nil
}

func BucketReplicationPolicyToStringConverter(value entity.BucketReplicationPolicy) (string, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("unable to serialize bucket replication policy: %w", err)
	}
	return string(bytes), nil
}

func StringToBucketReplicationPolicyConverter(value string) (entity.BucketReplicationPolicy, error) {
	var result entity.BucketReplicationPolicy
	if err := json.Unmarshal([]byte(value), &result); err != nil {
		var noVal entity.BucketReplicationPolicy
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
	RedisIDKeySortedSet[entity.BucketReplicationPolicyID, entity.BucketReplicationPolicy, uint8]
}

func NewBucketReplicationPolicyStore(client redis.Cmdable) *BucketReplicationPolicyStore {
	return &BucketReplicationPolicyStore{
		*NewRedisIDKeySortedSet(client, "p:repl:bucket",
			BucketReplicationPolicyIDToTokensConverter, TokensToBucketReplicationPolicyIDConverter,
			BucketReplicationPolicyToStringConverter, StringToBucketReplicationPolicyConverter,
			Uint8ToFloat64Converter, Float64ToUint8Converter),
	}
}

func (r *BucketReplicationPolicyStore) WithExecutor(exec Executor[redis.Pipeliner]) *BucketReplicationPolicyStore {
	return NewBucketReplicationPolicyStore(exec.Get())
}

func ReplicationStatusIDToTokensConverter(value entity.ReplicationStatusID) ([]string, error) {
	return []string{value.User, value.FromStorage, value.FromBucket, value.ToStorage, value.ToBucket}, nil
}

func TokensToReplicationStatusIDConverter(values []string) (entity.ReplicationStatusID, error) {
	return entity.ReplicationStatusID{
		User:        values[0],
		FromStorage: values[1],
		FromBucket:  values[2],
		ToStorage:   values[3],
		ToBucket:    values[4],
	}, nil
}

type ReplicationStatusStore struct {
	RedisIDKeyHash[entity.ReplicationStatusID, entity.ReplicationStatus]
}

func NewReplicationStatusStore(client redis.Cmdable) *ReplicationStatusStore {
	return &ReplicationStatusStore{
		*NewRedisIDKeyHash[entity.ReplicationStatusID, entity.ReplicationStatus](
			client, "p:repl:status", ReplicationStatusIDToTokensConverter, TokensToReplicationStatusIDConverter),
	}
}

func (r *ReplicationStatusStore) WithExecutor(exec Executor[redis.Pipeliner]) *ReplicationStatusStore {
	return NewReplicationStatusStore(exec.Get())
}

func (r *ReplicationStatusStore) SetListingStarted(ctx context.Context, id entity.ReplicationStatusID) error {
	return r.SetFieldIfExists(ctx, id, "listing_started", true)
}

func (r *ReplicationStatusStore) SetLastEmittedAt(ctx context.Context, id entity.ReplicationStatusID, value time.Time) error {
	return r.SetFieldIfExists(ctx, id, "last_emitted_at", value.UTC())
}

func (r *ReplicationStatusStore) IncrementEvents(ctx context.Context, id entity.ReplicationStatusID) (int64, error) {
	return r.IncrementFieldIfExists(ctx, id, "events")
}

func (r *ReplicationStatusStore) SetArchieved(ctx context.Context, id entity.ReplicationStatusID) error {
	return r.SetFieldIfExists(ctx, id, "archived", true)
}

func (r *ReplicationStatusStore) SetArchievedAt(ctx context.Context, id entity.ReplicationStatusID, value time.Time) error {
	return r.SetFieldIfExists(ctx, id, "archived_at", value.UTC())
}

type ReplicationSwitchInfoStore struct {
	RedisIDKeyHash[entity.ReplicationSwitchInfoID, entity.ReplicationSwitchInfo]
}

func ReplicationSwitchIDToTokensConverter(id entity.ReplicationSwitchInfoID) ([]string, error) {
	return []string{id.User, id.FromBucket}, nil
}

func TokensToReplicationSwitchIDConverter(values []string) (entity.ReplicationSwitchInfoID, error) {
	return entity.ReplicationSwitchInfoID{
		User:       values[0],
		FromBucket: values[2],
	}, nil
}

func NewReplicationSwitchInfoStore(client redis.Cmdable) *ReplicationSwitchInfoStore {
	return &ReplicationSwitchInfoStore{
		*NewRedisIDKeyHash[entity.ReplicationSwitchInfoID, entity.ReplicationSwitchInfo](
			client, "p:repl:switch", ReplicationSwitchIDToTokensConverter, TokensToReplicationSwitchIDConverter),
	}
}

func (r *ReplicationSwitchInfoStore) WithExecutor(exec Executor[redis.Pipeliner]) *ReplicationSwitchInfoStore {
	return NewReplicationSwitchInfoStore(exec.Get())
}

func (r *ReplicationSwitchInfoStore) Get(ctx context.Context, id entity.ReplicationSwitchInfoID) (entity.ReplicationSwitchInfo, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		return entity.ReplicationSwitchInfo{}, fmt.Errorf("unable to make key: %w", err)
	}
	cmd := r.client.HGetAll(ctx, key)
	err = cmd.Err()
	if errors.Is(err, redis.Nil) {
		return entity.ReplicationSwitchInfo{}, fmt.Errorf("%w: %w", dom.ErrNotFound, err)
	}
	if err != nil {
		return entity.ReplicationSwitchInfo{}, fmt.Errorf("unable to get hash map: %w", err)
	}
	if len(cmd.Val()) == 0 {
		return entity.ReplicationSwitchInfo{}, dom.ErrNotFound
	}
	var info entity.ReplicationSwitchInfo
	if err := cmd.Scan(&info); err != nil {
		return entity.ReplicationSwitchInfo{}, fmt.Errorf("unable to scan replication switch info: %w", err)
	}
	if err := cmd.Scan(&info.ReplicationSwitchDowntimeOpts); err != nil {
		return entity.ReplicationSwitchInfo{}, fmt.Errorf("unable to scan replication switch info downtime opts: %w", err)
	}
	if err := cmd.Scan(&info.ReplicationSwitchZeroDowntimeOpts); err != nil {
		return entity.ReplicationSwitchInfo{}, fmt.Errorf("unable to scan replication switch info zero downtime opts: %w", err)
	}
	return info, nil
}

func (r *ReplicationSwitchInfoStore) GetZeroDowntimeInfo(ctx context.Context, id entity.ReplicationSwitchInfoID) (entity.ZeroDowntimeSwitchInProgressInfo, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		return entity.ZeroDowntimeSwitchInProgressInfo{}, fmt.Errorf("unable to make key: %w", err)
	}
	var info entity.ZeroDowntimeSwitchInProgressInfo
	cmd := r.client.HMGet(ctx, key, "multipartTTL", "lastStatus", "replicationID", "replPriority")
	err = cmd.Err()
	if errors.Is(err, redis.Nil) {
		return entity.ZeroDowntimeSwitchInProgressInfo{}, fmt.Errorf("%w: %w", dom.ErrNotFound, err)
	}
	if err != nil {
		return entity.ZeroDowntimeSwitchInProgressInfo{}, fmt.Errorf("unable to get hash map: %w", err)
	}
	if err := cmd.Scan(&info); err != nil {
		return entity.ZeroDowntimeSwitchInProgressInfo{}, fmt.Errorf("unable to scan zero downtime info: %w", err)
	}
	return info, nil
}

func (r *ReplicationSwitchInfoStore) SetWithDowntimeOpts(ctx context.Context, id entity.ReplicationSwitchInfoID, value entity.ReplicationSwitchInfo) error {
	key, err := r.MakeKey(id)
	if err != nil {
		return fmt.Errorf("unable to make key: %w", err)
	}
	pipe := r.client.TxPipeline()
	pipe.HSet(ctx, key, value)
	noValDowntimeOpts := entity.ReplicationSwitchDowntimeOpts{}
	if value.ReplicationSwitchDowntimeOpts != noValDowntimeOpts {
		pipe.HSet(ctx, key, value.ReplicationSwitchDowntimeOpts)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("unable to execute pipe: %w", err)
	}
	return nil
}

func (r *ReplicationSwitchInfoStore) UpdateDowntimeOpts(ctx context.Context, id entity.ReplicationSwitchInfoID, value *entity.ReplicationSwitchDowntimeOpts) error {
	key, err := r.MakeKey(id)
	if err != nil {
		return fmt.Errorf("unable to make key: %w", err)
	}
	var fieldMap map[string]any
	if value != nil {
		fieldMap = r.MakeFieldMap(*value)
	} else {
		fieldMap = r.MakeFieldMap(entity.ReplicationSwitchDowntimeOpts{})
	}
	pipe := r.client.TxPipeline()
	for k, v := range fieldMap {
		if value == nil || v == nil {
			pipe.HDel(ctx, key, k)
		} else {
			pipe.HSet(ctx, key, k, v)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("unable to execute pipe: %w", err)
	}
	return nil
}

func (r *ReplicationSwitchInfoStore) SetLastStatus(ctx context.Context, id entity.ReplicationSwitchInfoID, status entity.ReplicationSwitchStatus) error {
	return r.SetFieldIfExists(ctx, id, "lastStatus", status)
}

func (r *ReplicationSwitchInfoStore) SetDoneAt(ctx context.Context, id entity.ReplicationSwitchInfoID, value time.Time) error {
	return r.SetFieldIfExists(ctx, id, "doneAt", value.UTC())
}

func (r *ReplicationSwitchInfoStore) SetStartedAt(ctx context.Context, id entity.ReplicationSwitchInfoID, value time.Time) (uint64, error) {
	return r.SetField(ctx, id, "startedAt", value.UTC())
}

func (r *ReplicationSwitchInfoStore) SetMultipartTTL(ctx context.Context, id entity.ReplicationSwitchInfoID, value time.Duration) (uint64, error) {
	return r.SetField(ctx, id, "multipartTTL", value)
}

type ReplicationSwitchHistoryStore struct {
	RedisIDKeyList[entity.ReplicationSwitchInfoID, string]
}

func NewReplicationSwitchHistoryStore(client redis.Cmdable) *ReplicationSwitchHistoryStore {
	return &ReplicationSwitchHistoryStore{
		*NewRedisIDKeyList[entity.ReplicationSwitchInfoID, string](
			client, "p:repl:switch:hist", 
			ReplicationSwitchIDToTokensConverter, TokensToReplicationSwitchIDConverter,
			StringValueConverter, StringValueConverter),
	}
}

func (r *ReplicationSwitchHistoryStore) WithExecutor(exec Executor[redis.Pipeliner]) *ReplicationSwitchHistoryStore {
	return NewReplicationSwitchHistoryStore(exec.Get())
}
