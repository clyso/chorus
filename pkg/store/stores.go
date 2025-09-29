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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/rclone"
)

func TokensToBucketReplicationPolicyIDConverter(tokens []string) (entity.BucketReplicationPolicyID, error) {
	return entity.BucketReplicationPolicyID{
		User:       tokens[0],
		FromBucket: tokens[1],
	}, nil
}

func BucketReplicationPolicyIDToTokensConverter(id entity.BucketReplicationPolicyID) ([]string, error) {
	return []string{id.User, id.FromBucket}, nil
}

func BucketReplicationPolicyDestinationToStringConverter(value entity.BucketReplicationPolicyDestination) (string, error) {
	return value.ToStorage + ":" + value.ToBucket, nil
}

func StringToBucketReplicationPolicyDestinationConverter(value string) (entity.BucketReplicationPolicyDestination, error) {
	if value == "" {
		return entity.BucketReplicationPolicyDestination{}, fmt.Errorf("empty bucket replication policy destination")
	}
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return entity.BucketReplicationPolicyDestination{}, fmt.Errorf("invalid bucket replication policy destination format")
	}
	return entity.BucketReplicationPolicyDestination{
		ToStorage: parts[0],
		ToBucket:  parts[1],
	}, nil
}

func Uint8ToFloat64Converter(value uint8) (float64, error) {
	return float64(value), nil
}

func Float64ToUint8Converter(value float64) (uint8, error) {
	return uint8(value), nil
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

type ReplicationStatusLocker struct {
	RedisIDKeyLocker[entity.UniversalReplicationID]
}

func NewReplicationStatusLocker(client redis.Cmdable, overlap time.Duration) *ReplicationStatusLocker {
	return &ReplicationStatusLocker{
		*NewRedisIDKeyLocker[entity.UniversalReplicationID](
			client, "lk:repl",
			UniversalReplicationIDToTokensConverter, TokensToUniversalReplicationID,
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

func TokensToEmptyStructConverter(tokens []string) (struct{}, error) {
	return struct{}{}, nil
}

func EmptyStructToTokensConverter(id struct{}) ([]string, error) {
	return []string{}, nil
}

func ConsistencyCheckIDToStringConverter(value entity.ConsistencyCheckID) (string, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("unable to serialize consistency check id: %w", err)
	}
	return string(bytes), nil
}

func StringToConsistencyCheckIDConverter(value string) (entity.ConsistencyCheckID, error) {
	var result entity.ConsistencyCheckID
	if err := json.Unmarshal([]byte(value), &result); err != nil {
		var noVal entity.ConsistencyCheckID
		return noVal, fmt.Errorf("unable to deserialize bucket replication policy: %w", err)
	}
	return result, nil
}

type ConsistencyCheckIDStore struct {
	RedisIDKeySet[struct{}, entity.ConsistencyCheckID]
}

func NewConsistencyCheckIDStore(client redis.Cmdable) *ConsistencyCheckIDStore {
	return &ConsistencyCheckIDStore{
		*NewRedisIDKeySet(client, "cc:id",
			EmptyStructToTokensConverter, TokensToEmptyStructConverter,
			ConsistencyCheckIDToStringConverter, StringToConsistencyCheckIDConverter),
	}
}

func (r *ConsistencyCheckIDStore) WithExecutor(exec Executor[redis.Pipeliner]) *ConsistencyCheckIDStore {
	return NewConsistencyCheckIDStore(exec.Get())
}

func TokensToConsistencyCheckIDConverter(tokens []string) (entity.ConsistencyCheckID, error) {
	var result entity.ConsistencyCheckID
	decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(tokens[0]))
	if err := json.NewDecoder(decoder).Decode(&result); err != nil {
		return entity.ConsistencyCheckID{}, fmt.Errorf("unable to decode consistency check id: %w", err)
	}
	return result, nil
}

func ConsistencyCheckIDToTokensConverter(id entity.ConsistencyCheckID) ([]string, error) {
	var buf bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &buf)
	if err := json.NewEncoder(encoder).Encode(&id); err != nil {
		return nil, fmt.Errorf("unable to encode consistency check id: %w", err)
	}
	return []string{buf.String()}, nil
}

func TokensToConsistencyCheckObjectIDConverter(tokens []string) (entity.ConsistencyCheckObjectID, error) {
	consistencyCheckID, err := TokensToConsistencyCheckIDConverter([]string{tokens[0]})
	if err != nil {
		return entity.ConsistencyCheckObjectID{}, fmt.Errorf("unable to parse consistency check id: %w", err)
	}
	return entity.ConsistencyCheckObjectID{
		ConsistencyCheckID: consistencyCheckID,
		Storage:            tokens[1],
		Prefix:             tokens[2],
	}, nil
}

func ConsistencyCheckObjectIDToTokensConverter(id entity.ConsistencyCheckObjectID) ([]string, error) {
	tokens, err := ConsistencyCheckIDToTokensConverter(id.ConsistencyCheckID)
	if err != nil {
		return nil, fmt.Errorf("unable to get consistency check id tokens: %w", err)
	}
	return []string{tokens[0], id.Storage, id.Prefix}, nil
}

type ConsistencyCheckListStateStore struct {
	RedisIDKeyValue[entity.ConsistencyCheckObjectID, string]
}

func NewConsistencyCheckListStateStore(client redis.Cmdable) *ConsistencyCheckListStateStore {
	return &ConsistencyCheckListStateStore{
		*NewRedisIDKeyValue(client, "cc:listed",
			ConsistencyCheckObjectIDToTokensConverter, TokensToConsistencyCheckObjectIDConverter,
			StringValueConverter, StringValueConverter),
	}
}

func (r *ConsistencyCheckListStateStore) WithExecutor(exec Executor[redis.Pipeliner]) *ConsistencyCheckListStateStore {
	return NewConsistencyCheckListStateStore(exec.Get())
}

var (
	luaAddToConsistencySet = redis.NewScript(`redis.call("SADD", KEYS[1], ARGV[1])
local count = redis.call("SCARD", KEYS[1])
if count == tonumber(ARGV[2]) then
	redis.call("UNLINK", KEYS[1])
end
return 0`)
)

func TokensToConsistencyCheckSetIDConverter(tokens []string) (entity.ConsistencyCheckSetID, error) {
	consistencyCheckID, err := TokensToConsistencyCheckIDConverter([]string{tokens[0]})
	if err != nil {
		return entity.ConsistencyCheckSetID{}, fmt.Errorf("unable to parse consistency check id: %w", err)
	}
	versionIdx, err := strconv.ParseUint(tokens[2], 10, 64)
	if err != nil {
		return entity.ConsistencyCheckSetID{}, fmt.Errorf("unable to parse version index: %w", err)
	}
	return entity.ConsistencyCheckSetID{
		ConsistencyCheckID: consistencyCheckID,
		Object:             tokens[1],
		VersionIndex:       versionIdx,
		Etag:               tokens[3],
	}, nil
}

func ConsistencyCheckSetIDToTokensConverter(id entity.ConsistencyCheckSetID) ([]string, error) {
	tokens, err := ConsistencyCheckIDToTokensConverter(id.ConsistencyCheckID)
	if err != nil {
		return nil, fmt.Errorf("unable to get consistency check id tokens: %w", err)
	}
	return []string{tokens[0], id.Object, strconv.FormatUint(id.VersionIndex, 10), id.Etag}, nil
}

func ConsistencyCheckSetEntryToStringConverter(value entity.ConsistencyCheckSetEntry) (string, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("unable to serialize consistency check set entry: %w", err)
	}
	return string(bytes), nil
}

func StringToConsistencyCheckSetEntryConverter(value string) (entity.ConsistencyCheckSetEntry, error) {
	var result entity.ConsistencyCheckSetEntry
	if err := json.Unmarshal([]byte(value), &result); err != nil {
		var noVal entity.ConsistencyCheckSetEntry
		return noVal, fmt.Errorf("unable to deserialize consistency check set entry: %w", err)
	}
	return result, nil
}

type ConsistencyCheckSetStore struct {
	RedisIDKeySet[entity.ConsistencyCheckSetID, entity.ConsistencyCheckSetEntry]
}

func NewConsistencyCheckSetStore(client redis.Cmdable) *ConsistencyCheckSetStore {
	return &ConsistencyCheckSetStore{
		*NewRedisIDKeySet(client, "cc:set",
			ConsistencyCheckSetIDToTokensConverter, TokensToConsistencyCheckSetIDConverter,
			ConsistencyCheckSetEntryToStringConverter, StringToConsistencyCheckSetEntryConverter),
	}
}

func (r *ConsistencyCheckSetStore) WithExecutor(exec Executor[redis.Pipeliner]) *ConsistencyCheckSetStore {
	return NewConsistencyCheckSetStore(exec.Get())
}

func (r *ConsistencyCheckSetStore) AddOp(ctx context.Context, id entity.ConsistencyCheckSetID, entry entity.ConsistencyCheckSetEntry, storageCount uint8) OperationStatus {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to make key: %w", err))
	}

	serializedEntry, err := r.serializer.ConvertSingle(entry)
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to serialize entry: %w", err))
	}

	cmd := luaAddToConsistencySet.Eval(ctx, r.client, []string{key}, serializedEntry, storageCount)

	collectFunc := func() error {
		if err := cmd.Err(); err != nil {
			return fmt.Errorf("unable to execute script: %w", err)
		}
		return nil
	}

	return NewRedisOperationStatus(collectFunc)
}

func (r *ConsistencyCheckSetStore) Add(ctx context.Context, id entity.ConsistencyCheckSetID, entry entity.ConsistencyCheckSetEntry, storageCount uint8) error {
	return r.AddOp(ctx, id, entry, storageCount).Get()
}
