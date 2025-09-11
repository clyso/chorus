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

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/rclone"
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
	RedisIDKeySet[entity.BucketReplicationPolicyID, entity.BucketReplicationPolicy]
}

func NewBucketReplicationPolicyStore(client redis.Cmdable) *BucketReplicationPolicyStore {
	return &BucketReplicationPolicyStore{
		*NewRedisIDKeySet(client, "p:repl:bucket",
			BucketReplicationPolicyIDToTokensConverter, TokensToBucketReplicationPolicyIDConverter,
			BucketReplicationPolicyToStringConverter, StringToBucketReplicationPolicyConverter),
	}
}

func (r *BucketReplicationPolicyStore) WithExecutor(exec Executor[redis.Pipeliner]) *BucketReplicationPolicyStore {
	return NewBucketReplicationPolicyStore(exec.Get())
}

func UserReplicationPolicyToStringConverter(value entity.UserReplicationPolicy) (string, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("unable to serialize bucket replication policy: %w", err)
	}
	return string(bytes), nil
}

func StringToUserReplicationPolicyConverter(value string) (entity.UserReplicationPolicy, error) {
	var result entity.UserReplicationPolicy
	if err := json.Unmarshal([]byte(value), &result); err != nil {
		var noVal entity.UserReplicationPolicy
		return noVal, fmt.Errorf("unable to deserialize bucket replication policy: %w", err)
	}
	return result, nil
}

type UserReplicationPolicyStore struct {
	RedisIDKeySortedSet[string, entity.UserReplicationPolicy, uint8]
}

func NewUserReplicationPolicyStore(client redis.Cmdable) *UserReplicationPolicyStore {
	return &UserReplicationPolicyStore{
		*NewRedisIDKeySortedSet(client, "p:repl:user",
			StringToSingleTokenConverter, SingleTokenToStringConverter,
			UserReplicationPolicyToStringConverter, StringToUserReplicationPolicyConverter,
			Uint8ToFloat64Converter, Float64ToUint8Converter),
	}
}

func (r *UserReplicationPolicyStore) WithExecutor(exec Executor[redis.Pipeliner]) *UserReplicationPolicyStore {
	return NewUserReplicationPolicyStore(exec.Get())
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

func (r *ReplicationStatusStore) SetListingStartedOp(ctx context.Context, id entity.ReplicationStatusID) OperationStatus {
	return r.SetFieldIfExistsOp(ctx, id, "listing_started", true)
}

func (r *ReplicationStatusStore) SetListingStarted(ctx context.Context, id entity.ReplicationStatusID) error {
	return r.SetListingStartedOp(ctx, id).Err()
}

func (r *ReplicationStatusStore) SetCreatedAtOp(ctx context.Context, id entity.ReplicationStatusID, value time.Time) OperationStatus {
	return r.SetFieldIfExistsOp(ctx, id, "created_at", value.UTC())
}

func (r *ReplicationStatusStore) SetCreatedAt(ctx context.Context, id entity.ReplicationStatusID, value time.Time) error {
	return r.SetCreatedAtOp(ctx, id, value.UTC()).Err()
}

func (r *ReplicationStatusStore) SetArchievedOp(ctx context.Context, id entity.ReplicationStatusID) OperationStatus {
	return r.SetFieldIfExistsOp(ctx, id, "archived", true)
}

func (r *ReplicationStatusStore) SetArchieved(ctx context.Context, id entity.ReplicationStatusID) error {
	return r.SetArchievedOp(ctx, id).Err()
}

func (r *ReplicationStatusStore) SetArchievedAtOp(ctx context.Context, id entity.ReplicationStatusID, value time.Time) OperationStatus {
	return r.SetFieldIfExistsOp(ctx, id, "archived_at", value.UTC())
}

func (r *ReplicationStatusStore) SetArchievedAt(ctx context.Context, id entity.ReplicationStatusID, value time.Time) error {
	return r.SetArchievedAtOp(ctx, id, value.UTC()).Err()
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
		FromBucket: values[1],
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

func (r *ReplicationSwitchInfoStore) GetOp(ctx context.Context, id entity.ReplicationSwitchInfoID) OperationResult[entity.ReplicationSwitchInfo] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[entity.ReplicationSwitchInfo](fmt.Errorf("unable to make key: %w", err))
	}
	cmd := r.client.HGetAll(ctx, key)

	collectFunc := func() (entity.ReplicationSwitchInfo, error) {
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

	return NewRedisOperationResult[entity.ReplicationSwitchInfo](collectFunc)
}

func (r *ReplicationSwitchInfoStore) Get(ctx context.Context, id entity.ReplicationSwitchInfoID) (entity.ReplicationSwitchInfo, error) {
	return r.GetOp(ctx, id).Get()
}

func (r *ReplicationSwitchInfoStore) GetReplicationKeyOp(ctx context.Context, id entity.ReplicationSwitchInfoID) OperationResult[string] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[string](fmt.Errorf("unable to make key: %w", err))
	}
	cmd := r.client.HGet(ctx, key, "replicationID")

	collectFunc := func() (string, error) {
		value, err := cmd.Result()
		if errors.Is(err, redis.Nil) {
			return "", fmt.Errorf("%w: %w", dom.ErrNotFound, err)
		}
		if err != nil {
			return "", fmt.Errorf("unable to get field map: %w", err)
		}
		return value, nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *ReplicationSwitchInfoStore) GetReplicationKey(ctx context.Context, id entity.ReplicationSwitchInfoID) (string, error) {
	return r.GetReplicationKeyOp(ctx, id).Get()
}

func (r *ReplicationSwitchInfoStore) GetZeroDowntimeInfoOp(ctx context.Context, id entity.ReplicationSwitchInfoID) OperationResult[entity.ZeroDowntimeSwitchInProgressInfo] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[entity.ZeroDowntimeSwitchInProgressInfo](fmt.Errorf("unable to make key: %w", err))
	}
	cmd := r.client.HMGet(ctx, key, "multipartTTL", "lastStatus", "replicationID", "replPriority")

	collectFunc := func() (entity.ZeroDowntimeSwitchInProgressInfo, error) {
		err = cmd.Err()
		if errors.Is(err, redis.Nil) {
			return entity.ZeroDowntimeSwitchInProgressInfo{}, fmt.Errorf("%w: %w", dom.ErrNotFound, err)
		}
		if err != nil {
			return entity.ZeroDowntimeSwitchInProgressInfo{}, fmt.Errorf("unable to get hash map: %w", err)
		}
		var info entity.ZeroDowntimeSwitchInProgressInfo
		if err := cmd.Scan(&info); err != nil {
			return entity.ZeroDowntimeSwitchInProgressInfo{}, fmt.Errorf("unable to scan zero downtime info: %w", err)
		}
		return info, nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *ReplicationSwitchInfoStore) GetZeroDowntimeInfo(ctx context.Context, id entity.ReplicationSwitchInfoID) (entity.ZeroDowntimeSwitchInProgressInfo, error) {
	return r.GetZeroDowntimeInfoOp(ctx, id).Get()
}

func (r *ReplicationSwitchInfoStore) SetWithDowntimeOptsOp(ctx context.Context, id entity.ReplicationSwitchInfoID, value entity.ReplicationSwitchInfo) OperationStatus {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to make key: %w", err))
	}

	pipe := r.client.TxPipeline()
	_ = pipe.HSet(ctx, key, value)
	noValDowntimeOpts := entity.ReplicationSwitchDowntimeOpts{}
	if value.ReplicationSwitchDowntimeOpts != noValDowntimeOpts {
		_ = pipe.HSet(ctx, key, value.ReplicationSwitchDowntimeOpts)
	}

	collectFunc := func() error {
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("unable to execute pipe: %w", err)
		}
		return nil
	}

	return NewRedisOperationStatus(collectFunc)
}

func (r *ReplicationSwitchInfoStore) SetWithDowntimeOpts(ctx context.Context, id entity.ReplicationSwitchInfoID, value entity.ReplicationSwitchInfo) error {
	return r.SetWithDowntimeOptsOp(ctx, id, value).Err()
}

func (r *ReplicationSwitchInfoStore) UpdateDowntimeOptsOp(ctx context.Context, id entity.ReplicationSwitchInfoID, value *entity.ReplicationSwitchDowntimeOpts) OperationStatus {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to make key: %w", err))
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
			_ = pipe.HDel(ctx, key, k)
		} else {
			_ = pipe.HSet(ctx, key, k, v)
		}
	}

	collectFunc := func() error {
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("unable to execute pipe: %w", err)
		}
		return nil
	}

	return NewRedisOperationStatus(collectFunc)
}

func (r *ReplicationSwitchInfoStore) UpdateDowntimeOpts(ctx context.Context, id entity.ReplicationSwitchInfoID, value *entity.ReplicationSwitchDowntimeOpts) error {
	return r.UpdateDowntimeOptsOp(ctx, id, value).Err()
}

func (r *ReplicationSwitchInfoStore) SetLastStatusOp(ctx context.Context, id entity.ReplicationSwitchInfoID, status entity.ReplicationSwitchStatus) OperationStatus {
	return r.SetFieldIfExistsOp(ctx, id, "lastStatus", status)
}

func (r *ReplicationSwitchInfoStore) SetLastStatus(ctx context.Context, id entity.ReplicationSwitchInfoID, status entity.ReplicationSwitchStatus) error {
	return r.SetLastStatusOp(ctx, id, status).Err()
}

func (r *ReplicationSwitchInfoStore) SetDoneAtOp(ctx context.Context, id entity.ReplicationSwitchInfoID, value time.Time) OperationStatus {
	return r.SetFieldIfExistsOp(ctx, id, "doneAt", value.UTC())
}

func (r *ReplicationSwitchInfoStore) SetDoneAt(ctx context.Context, id entity.ReplicationSwitchInfoID, value time.Time) error {
	return r.SetDoneAtOp(ctx, id, value).Err()
}

func (r *ReplicationSwitchInfoStore) SetStartedAtOp(ctx context.Context, id entity.ReplicationSwitchInfoID, value time.Time) OperationResult[uint64] {
	return r.SetFieldOp(ctx, id, "startedAt", value.UTC())
}

func (r *ReplicationSwitchInfoStore) SetStartedAt(ctx context.Context, id entity.ReplicationSwitchInfoID, value time.Time) (uint64, error) {
	return r.SetStartedAtOp(ctx, id, value).Get()
}

func (r *ReplicationSwitchInfoStore) SetMultipartTTLOp(ctx context.Context, id entity.ReplicationSwitchInfoID, value time.Duration) OperationResult[uint64] {
	return r.SetFieldOp(ctx, id, "multipartTTL", value)
}

func (r *ReplicationSwitchInfoStore) SetMultipartTTL(ctx context.Context, id entity.ReplicationSwitchInfoID, value time.Duration) (uint64, error) {
	return r.SetMultipartTTLOp(ctx, id, value).Get()
}

type ReplicationSwitchHistoryStore struct {
	RedisIDKeyList[entity.ReplicationSwitchInfoID, string]
}

func NewReplicationSwitchHistoryStore(client redis.Cmdable) *ReplicationSwitchHistoryStore {
	return &ReplicationSwitchHistoryStore{
		*NewRedisIDKeyList[entity.ReplicationSwitchInfoID, string](
			client, "p:repl:switchhist",
			ReplicationSwitchIDToTokensConverter, TokensToReplicationSwitchIDConverter,
			StringValueConverter, StringValueConverter),
	}
}

func (r *ReplicationSwitchHistoryStore) WithExecutor(exec Executor[redis.Pipeliner]) *ReplicationSwitchHistoryStore {
	return NewReplicationSwitchHistoryStore(exec.Get())
}

func (r *ReplicationSwitchHistoryStore) GetOp(ctx context.Context, id entity.ReplicationSwitchInfoID) OperationResult[[]string] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[[]string](fmt.Errorf("unable to make key: %w", err))
	}
	cmd := r.client.LRange(ctx, key, 0, -1)

	collectFunc := func() ([]string, error) {
		cmdVal, err := cmd.Result()
		if err != nil {
			return nil, fmt.Errorf("unable to get element range: %w", err)
		}
		values, err := r.deserializer.ConvertMulti(cmdVal)
		if err != nil {
			return nil, fmt.Errorf("unable to deserialize result: %w", err)
		}

		return values, nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *ReplicationSwitchHistoryStore) Get(ctx context.Context, id entity.ReplicationSwitchInfoID) ([]string, error) {
	return r.GetOp(ctx, id).Get()
}

type ReplicationStatusLocker struct {
	RedisIDKeyLocker[entity.ReplicationStatusID]
}

func NewReplicationStatusLocker(client redis.Cmdable, overlap time.Duration) *ReplicationStatusLocker {
	return &ReplicationStatusLocker{
		*NewRedisIDKeyLocker[entity.ReplicationStatusID](
			client, "lk:repl",
			ReplicationStatusIDToTokensConverter, TokensToReplicationStatusIDConverter,
			overlap),
	}
}

type UserLocker struct {
	RedisIDKeyLocker[string]
}

func NewUserLocker(client redis.Cmdable, overlap time.Duration) *UserLocker {
	return &UserLocker{
		*NewRedisIDKeyLocker[string](
			client, "lk:user",
			StringToSingleTokenConverter, SingleTokenToStringConverter,
			overlap),
	}
}

func TokensToObjectLockIDConverter(tokens []string) (entity.ObjectLockID, error) {
	return entity.ObjectLockID{
		Storage: tokens[0],
		Bucket:  tokens[1],
		Name:    tokens[2],
		Version: tokens[3],
	}, nil
}

func ObjectLockIDToTokensConverter(id entity.ObjectLockID) ([]string, error) {
	return []string{id.Storage, id.Bucket, id.Name, id.Version}, nil
}

type ObjectLocker struct {
	RedisIDKeyLocker[entity.ObjectLockID]
}

func NewObjectLocker(client redis.Cmdable, overlap time.Duration) *ObjectLocker {
	return &ObjectLocker{
		*NewRedisIDKeyLocker[entity.ObjectLockID](
			client, "lk:object",
			ObjectLockIDToTokensConverter, TokensToObjectLockIDConverter,
			overlap),
	}
}

func TokensToBucketLockIDConverter(tokens []string) (entity.BucketLockID, error) {
	return entity.BucketLockID{
		Storage: tokens[0],
		Bucket:  tokens[1],
	}, nil
}

func BucketLockIDToTokensConverter(id entity.BucketLockID) ([]string, error) {
	return []string{id.Storage, id.Bucket}, nil
}

type BucketLocker struct {
	RedisIDKeyLocker[entity.BucketLockID]
}

func NewBucketLocker(client redis.Cmdable, overlap time.Duration) *BucketLocker {
	return &BucketLocker{
		*NewRedisIDKeyLocker[entity.BucketLockID](
			client, "lk:bucket",
			BucketLockIDToTokensConverter, TokensToBucketLockIDConverter,
			overlap),
	}
}

type ObjectVersionInfoStore struct {
	RedisIDKeyList[entity.VersionedObjectID, rclone.ObjectVersionInfo]
}

func TokensToObjectVersionIDConverter(tokens []string) (entity.VersionedObjectID, error) {
	return entity.VersionedObjectID{
		Storage: tokens[0],
		Bucket:  tokens[1],
		Name:    tokens[2],
	}, nil
}

func ObjectVersionIDToTokensConverter(id entity.VersionedObjectID) ([]string, error) {
	return []string{id.Storage, id.Bucket, id.Name}, nil
}

func ObjectVersionInfoToStringConverter(value rclone.ObjectVersionInfo) (string, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("unable to marshal value: %w", err)
	}
	return string(bytes), nil
}

func StringToObjectVersionInfoConverter(value string) (rclone.ObjectVersionInfo, error) {
	result := rclone.ObjectVersionInfo{}
	if err := json.Unmarshal([]byte(value), &result); err != nil {
		return rclone.ObjectVersionInfo{}, fmt.Errorf("unable to unmarshal value: %w", err)
	}
	return result, nil
}

func NewObjectVersionInfoStore(client redis.Cmdable) *ObjectVersionInfoStore {
	return &ObjectVersionInfoStore{
		*NewRedisIDKeyList[entity.VersionedObjectID, rclone.ObjectVersionInfo](
			client, "p:repl:objectversion",
			ObjectVersionIDToTokensConverter, TokensToObjectVersionIDConverter,
			ObjectVersionInfoToStringConverter, StringToObjectVersionInfoConverter),
	}
}

func (r *ObjectVersionInfoStore) WithExecutor(exec Executor[redis.Pipeliner]) *ObjectVersionInfoStore {
	return NewObjectVersionInfoStore(exec.Get())
}
