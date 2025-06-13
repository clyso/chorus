/*
 * Copyright © 2024 Clyso GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/tasks"
)

const (
	lastListedObjTTL = time.Hour * 8
)

var (
	luaDeleteKeysByPrefix = redis.NewScript(`local keys = redis.call('keys', ARGV[1])
if #keys >0 then
	return redis.call('DEL', unpack(keys))
else
	return 0
end`)

	luaAddToConsistencySet = redis.NewScript(`redis.call("SADD", KEYS[1], ARGV[1])
local count = redis.call("SCARD", KEYS[1])
if count == tonumber(ARGV[2]) then
	redis.call("UNLINK", KEYS[1])
end
return 0`)
)

type ConsistencyCheckObject struct {
	ConsistencyCheckID string
	Storage            string
	Prefix             string
}

type ConsistencyCheckRecord struct {
	ConsistencyCheckID string
	Storage            string
	Object             string
	ETag               string
	StorageCount       uint8
}

type ConsistencyCheckResultEntry struct {
	Object   string
	ETag     string
	Storages []string
}

type ConsistencyCheckResultPage struct {
	Entries []ConsistencyCheckResultEntry
	Cursor  uint64
}

type Service interface {
	GetLastListedObj(ctx context.Context, task tasks.MigrateBucketListObjectsPayload) (string, error)
	SetLastListedObj(ctx context.Context, task tasks.MigrateBucketListObjectsPayload, val string) error
	DelLastListedObj(ctx context.Context, task tasks.MigrateBucketListObjectsPayload) error
	CleanLastListedObj(ctx context.Context, fromStor, toStor, fromBucket string, toBucket *string) error

	StoreUploadID(ctx context.Context, user, bucket, object, uploadID string, ttl time.Duration) error
	DeleteUploadID(ctx context.Context, user, bucket, object, uploadID string) error
	ExistsUploadID(ctx context.Context, user, bucket, object, uploadID string) (bool, error)
	ExistsUploads(ctx context.Context, user, bucket string) (bool, error)

	GetLastListedConsistencyCheckObj(ctx context.Context, obj *ConsistencyCheckObject) (string, error)
	SetLastListedConsistencyCheckObj(ctx context.Context, obj *ConsistencyCheckObject, value string) error
	DeleteLastListedConsistencyCheckObj(ctx context.Context, obj *ConsistencyCheckObject) error
	DeleteAllLastListedConsistencyCheckObj(ctx context.Context, id string) error
	IncrementConsistencyCheckScheduledCounter(ctx context.Context, id string, count int64) error
	IncrementConsistencyCheckCompletedCounter(ctx context.Context, id string, count int64) error
	DecrementConsistencyCheckScheduledCounter(ctx context.Context, id string, count int64) error
	DecrementConsistencyCheckCompletedCounter(ctx context.Context, id string, count int64) error
	GetConsistencyCheckScheduledCounter(ctx context.Context, id string) (uint64, error)
	GetConsistencyCheckCompletedCounter(ctx context.Context, id string) (uint64, error)
	DeleteConsistencyCheckScheduledCounter(ctx context.Context, id string) error
	DeleteConsistencyCheckCompletedCounter(ctx context.Context, id string) error
	StoreConsistencyCheckID(ctx context.Context, id string) error
	DeleteConsistencyCheckID(ctx context.Context, id string) error
	ListConsistencyCheckIDs(ctx context.Context) ([]string, error)
	AddToConsistencyCheckSet(ctx context.Context, record *ConsistencyCheckRecord) error
	FindConsistencyCheckSets(ctx context.Context, id string) ([]ConsistencyCheckResultEntry, error)
	FindConsistencyCheckSetsPageable(ctx context.Context, id string, cursor uint64, pageSize int64) (*ConsistencyCheckResultPage, error)
	HasConsistencyCheckSets(ctx context.Context, id string) (bool, error)
	DeleteAllConsistencyCheckSets(ctx context.Context, id string) error
	SetConsistencyCheckReadiness(ctx context.Context, id string, ready bool) error
	GetConsistencyCheckReadiness(ctx context.Context, id string) (bool, error)
	DeleteConsistencyCheckReadiness(ctx context.Context, id string) error
	SetConsistencyCheckStorages(ctx context.Context, id string, storages []string) error
	GetConsistencyCheckStorages(ctx context.Context, id string) ([]string, error)
	DeleteConsistencyCheckStorages(ctx context.Context, id string) error
}

func New(client redis.UniversalClient) Service {
	return &svc{client: client}
}

type svc struct {
	client redis.UniversalClient
}

func (s *svc) CleanLastListedObj(ctx context.Context, fromStor string, toStor string, fromBucket string, toBucket *string) error {
	key := fmt.Sprintf("s:%s:%s:%s", fromStor, toStor, fromBucket)
	if toBucket != nil {
		key += ":" + *toBucket
	}
	if err := s.client.Del(ctx, key).Err(); err != nil {
		return err
	}
	prefix := key + ":*"
	return luaDeleteKeysByPrefix.Run(ctx, s.client, []string{}, prefix).Err()
}

func (s *svc) DelLastListedObj(ctx context.Context, task tasks.MigrateBucketListObjectsPayload) error {
	key := fmt.Sprintf("s:%s:%s:%s", task.FromStorage, task.ToStorage, task.Bucket)
	if task.ToBucket != nil {
		key += ":" + *task.ToBucket
	}
	if task.Prefix != "" {
		key += ":" + task.Prefix
	}
	return s.client.Del(ctx, key).Err()
}

func (s *svc) GetLastListedObj(ctx context.Context, task tasks.MigrateBucketListObjectsPayload) (string, error) {
	key := fmt.Sprintf("s:%s:%s:%s", task.FromStorage, task.ToStorage, task.Bucket)
	if task.ToBucket != nil {
		key += ":" + *task.ToBucket
	}
	if task.Prefix != "" {
		key += ":" + task.Prefix
	}
	val, err := s.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return val, err
}

func (s *svc) SetLastListedObj(ctx context.Context, task tasks.MigrateBucketListObjectsPayload, val string) error {
	key := fmt.Sprintf("s:%s:%s:%s", task.FromStorage, task.ToStorage, task.Bucket)
	if task.ToBucket != nil {
		key += ":" + *task.ToBucket
	}
	if task.Prefix != "" {
		key += ":" + task.Prefix
	}
	return s.client.Set(ctx, key, val, lastListedObjTTL).Err()
}

func (s *svc) StoreUploadID(ctx context.Context, user, bucket, object, uploadID string, ttl time.Duration) error {
	if user == "" {
		return fmt.Errorf("%w: user is required to set uploadID", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return fmt.Errorf("%w: bucket is required to set uploadID", dom.ErrInvalidArg)
	}
	if uploadID == "" {
		return fmt.Errorf("%w: uploadID is required", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("s:up:%s:%s", user, bucket)
	val := fmt.Sprintf("%s:%s", object, uploadID)
	err := s.client.SAdd(ctx, key, val).Err()
	if err != nil {
		return err
	}
	_ = s.client.Expire(ctx, key, ttl)
	return nil
}

func (s *svc) ExistsUploadID(ctx context.Context, user, bucket, object, uploadID string) (bool, error) {
	if user == "" {
		return false, fmt.Errorf("%w: user is required to set uploadID", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return false, fmt.Errorf("%w: bucket is required to set uploadID", dom.ErrInvalidArg)
	}
	if uploadID == "" {
		return false, fmt.Errorf("%w: uploadID is required", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("s:up:%s:%s", user, bucket)
	val := fmt.Sprintf("%s:%s", object, uploadID)
	return s.client.SIsMember(ctx, key, val).Result()
}

func (s *svc) ExistsUploads(ctx context.Context, user, bucket string) (bool, error) {
	if user == "" {
		return false, fmt.Errorf("%w: user is required to set uploadID", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return false, fmt.Errorf("%w: bucket is required to set uploadID", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("s:up:%s:%s", user, bucket)
	num, err := s.client.SCard(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return num > 0, nil
}

func (s *svc) DeleteUploadID(ctx context.Context, user, bucket, object, uploadID string) error {
	if user == "" {
		return fmt.Errorf("%w: user is required to set uploadID", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return fmt.Errorf("%w: bucket is required to set uploadID", dom.ErrInvalidArg)
	}
	if uploadID == "" {
		return fmt.Errorf("%w: uploadID is required", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("s:up:%s:%s", user, bucket)
	val := fmt.Sprintf("%s:%s", object, uploadID)
	return s.client.SRem(ctx, key, val).Err()
}

func (s *svc) GetLastListedConsistencyCheckObj(ctx context.Context, obj *ConsistencyCheckObject) (string, error) {
	key := fmt.Sprintf("ccv:l:%s:%s:%s", obj.ConsistencyCheckID, obj.Storage, obj.Prefix)
	cmd := s.client.Get(ctx, key)
	err := cmd.Err()

	switch {
	case errors.Is(err, redis.Nil):
		return "", nil
	case err != nil:
		return "", fmt.Errorf("unable to set last listed object: %w", err)
	default:
		return cmd.Val(), nil
	}
}

func (s *svc) SetLastListedConsistencyCheckObj(ctx context.Context, obj *ConsistencyCheckObject, value string) error {
	key := fmt.Sprintf("ccv:l:%s:%s:%s", obj.ConsistencyCheckID, obj.Storage, obj.Prefix)
	if err := s.client.Set(ctx, key, value, lastListedObjTTL).Err(); err != nil {
		return fmt.Errorf("unable to set last listed object: %w", err)
	}

	return nil
}

func (s *svc) DeleteLastListedConsistencyCheckObj(ctx context.Context, obj *ConsistencyCheckObject) error {
	key := fmt.Sprintf("ccv:l:%s:%s:%s", obj.ConsistencyCheckID, obj.Storage, obj.Prefix)
	if err := s.client.Unlink(ctx, key).Err(); err != nil {
		return fmt.Errorf("unable to delete last listed object: %w", err)
	}

	return nil
}

func (s *svc) DeleteAllLastListedConsistencyCheckObj(ctx context.Context, id string) error {
	keyPattern := fmt.Sprintf("ccv:l:%s:*", id)
	pipe := s.client.Pipeline()
	var cursor uint64

	for {
		keys, nextCursor, err := s.client.Scan(ctx, cursor, keyPattern, 0).Result()
		if err != nil {
			return fmt.Errorf("unable to scan keys: %w", err)
		}

		for _, key := range keys {
			_ = pipe.Unlink(ctx, key)
		}

		if nextCursor == 0 {
			break
		}

		cursor = nextCursor
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("unable to pipe unlink commands: %w", err)
	}

	return nil
}

func (s *svc) IncrementConsistencyCheckScheduledCounter(ctx context.Context, id string, count int64) error {
	key := fmt.Sprintf("ccv:c:%s:scheduled", id)
	if err := s.client.IncrBy(ctx, key, count).Err(); err != nil {
		return fmt.Errorf("unable to increment amount of scheduled consistency check tasks: %w", err)
	}

	return nil
}

func (s *svc) IncrementConsistencyCheckCompletedCounter(ctx context.Context, id string, count int64) error {
	key := fmt.Sprintf("ccv:c:%s:completed", id)
	if err := s.client.IncrBy(ctx, key, count).Err(); err != nil {
		return fmt.Errorf("unable to increment amount of completed consistency check tasks: %w", err)
	}

	return nil
}

func (s *svc) DecrementConsistencyCheckScheduledCounter(ctx context.Context, id string, count int64) error {
	key := fmt.Sprintf("ccv:c:%s:scheduled", id)
	if err := s.client.DecrBy(ctx, key, count).Err(); err != nil {
		return fmt.Errorf("unable to decrement amount of scheduled consistency check tasks: %w", err)
	}

	return nil
}

func (s *svc) DecrementConsistencyCheckCompletedCounter(ctx context.Context, id string, count int64) error {
	key := fmt.Sprintf("ccv:c:%s:completed", id)
	if err := s.client.DecrBy(ctx, key, count).Err(); err != nil {
		return fmt.Errorf("unable to decrement amount of completed consistency check tasks: %w", err)
	}

	return nil
}

func (s *svc) GetConsistencyCheckScheduledCounter(ctx context.Context, id string) (uint64, error) {
	key := fmt.Sprintf("ccv:c:%s:scheduled", id)
	count, err := s.client.Get(ctx, key).Uint64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("unable to get amount of scheduled consistency check tasks: %w", err)
	}

	return count, nil
}

func (s *svc) GetConsistencyCheckCompletedCounter(ctx context.Context, id string) (uint64, error) {
	key := fmt.Sprintf("ccv:c:%s:completed", id)
	count, err := s.client.Get(ctx, key).Uint64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("unable to get amount of scheduled consistency check tasks: %w", err)
	}

	return count, nil
}

func (s *svc) DeleteConsistencyCheckScheduledCounter(ctx context.Context, id string) error {
	key := fmt.Sprintf("ccv:c:%s:scheduled", id)
	if err := s.client.Unlink(ctx, key).Err(); err != nil {
		return fmt.Errorf("unable to delete counter for scheduled consistency check tasks: %w", err)
	}

	return nil
}

func (s *svc) DeleteConsistencyCheckCompletedCounter(ctx context.Context, id string) error {
	key := fmt.Sprintf("ccv:c:%s:completed", id)
	if err := s.client.Unlink(ctx, key).Err(); err != nil {
		return fmt.Errorf("unable to delete counter for completed consistency check tasks: %w", err)
	}

	return nil
}

func (s *svc) StoreConsistencyCheckID(ctx context.Context, id string) error {
	cmd := s.client.SAdd(ctx, "ccv:id", id)
	if cmd.Err() != nil {
		return fmt.Errorf("unable to add id to consistency check set: %w", cmd.Err())
	}

	affected := cmd.Val()
	if affected == 0 {
		return errors.New("consistency check id already exists")
	}

	return nil
}

func (s *svc) DeleteConsistencyCheckID(ctx context.Context, id string) error {
	cmd := s.client.SRem(ctx, "ccv:id", id)
	if cmd.Err() != nil {
		return fmt.Errorf("unable to delete from consistency check set: %w", cmd.Err())
	}

	return nil
}

func (s *svc) ListConsistencyCheckIDs(ctx context.Context) ([]string, error) {
	res, err := s.client.SMembers(ctx, "ccv:id").Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("unable to list set: %w", err)
	}

	return res, nil
}

func (s *svc) AddToConsistencyCheckSet(ctx context.Context, record *ConsistencyCheckRecord) error {
	key := fmt.Sprintf("ccv:s:%s:%s:%s", record.ConsistencyCheckID, record.Object, record.ETag)
	if err := luaAddToConsistencySet.Run(ctx, s.client, []string{key}, record.Storage, record.StorageCount).Err(); err != nil {
		return fmt.Errorf("unable to add object info to consistency check set: %w", err)
	}

	return nil
}

func (s *svc) FindConsistencyCheckSets(ctx context.Context, id string) ([]ConsistencyCheckResultEntry, error) {
	keyPattern := fmt.Sprintf("ccv:s:%s:*", id)
	var results []ConsistencyCheckResultEntry
	var cursor uint64

	for {
		keys, nextCursor, err := s.client.Scan(ctx, cursor, keyPattern, 0).Result()
		if err != nil {
			return nil, fmt.Errorf("unable to scan keys: %w", err)
		}

		for _, key := range keys {
			storages, err := s.client.SMembers(ctx, key).Result()
			if err != nil {
				return nil, fmt.Errorf("unable to read set members: %w", err)
			}

			parts := strings.Split(key, ":")
			partsCount := len(parts)
			result := ConsistencyCheckResultEntry{
				Object:   parts[partsCount-2],
				ETag:     parts[partsCount-1],
				Storages: storages,
			}
			results = append(results, result)
		}

		if nextCursor == 0 {
			break
		}

		cursor = nextCursor
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].Object < results[j].Object {
			return true
		}
		if results[i].Object > results[j].Object {
			return false
		}
		return results[i].ETag < results[j].ETag
	})

	return results, nil
}

func (s *svc) FindConsistencyCheckSetsPageable(ctx context.Context, id string, cursor uint64, pageSize int64) (*ConsistencyCheckResultPage, error) {
	keyPattern := fmt.Sprintf("ccv:s:%s:*", id)
	results := make([]ConsistencyCheckResultEntry, 0, pageSize)

	keys, nextCursor, err := s.client.Scan(ctx, cursor, keyPattern, pageSize).Result()
	if err != nil {
		return nil, fmt.Errorf("unable to scan keys: %w", err)
	}

	for _, key := range keys {
		storages, err := s.client.SMembers(ctx, key).Result()
		if err != nil {
			return nil, fmt.Errorf("unable to read set members: %w", err)
		}

		parts := strings.Split(key, ":")
		partsCount := len(parts)
		result := ConsistencyCheckResultEntry{
			Object:   parts[partsCount-2],
			ETag:     parts[partsCount-1],
			Storages: storages,
		}
		results = append(results, result)
	}

	return &ConsistencyCheckResultPage{
		Entries: results,
		Cursor:  nextCursor,
	}, nil
}

func (s *svc) HasConsistencyCheckSets(ctx context.Context, id string) (bool, error) {
	keyPattern := fmt.Sprintf("ccv:s:%s:*", id)

	keys, _, err := s.client.Scan(ctx, 0, keyPattern, 1).Result()
	if err != nil {
		return false, fmt.Errorf("unable to scan keys: %w", err)
	}

	if len(keys) != 0 {
		return true, nil
	}

	return false, nil
}

func (s *svc) DeleteAllConsistencyCheckSets(ctx context.Context, id string) error {
	keyPattern := fmt.Sprintf("ccv:s:%s:*", id)
	pipe := s.client.Pipeline()
	var cursor uint64

	for {
		keys, nextCursor, err := s.client.Scan(ctx, cursor, keyPattern, 0).Result()
		if err != nil {
			return fmt.Errorf("unable to scan keys: %w", err)
		}

		for _, key := range keys {
			pipe.Unlink(ctx, key)
		}

		if nextCursor == 0 {
			break
		}

		cursor = nextCursor
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("unable to pipe unlink commands: %w", err)
	}

	return nil
}

func (s *svc) SetConsistencyCheckReadiness(ctx context.Context, id string, ready bool) error {
	key := fmt.Sprintf("ccv:r:%s", id)
	if err := s.client.Set(ctx, key, ready, lastListedObjTTL).Err(); err != nil {
		return fmt.Errorf("unable to set readiness flag: %w", err)
	}
	return nil
}

func (s *svc) GetConsistencyCheckReadiness(ctx context.Context, id string) (bool, error) {
	key := fmt.Sprintf("ccv:r:%s", id)
	flag, err := s.client.Get(ctx, key).Bool()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("unable to get readiness flag: %w", err)
	}
	return flag, nil
}

func (s *svc) DeleteConsistencyCheckReadiness(ctx context.Context, id string) error {
	key := fmt.Sprintf("ccv:r:%s", id)
	if err := s.client.Unlink(ctx, key).Err(); err != nil {
		return fmt.Errorf("unable to delete readiness flag: %w", err)
	}
	return nil
}

func (s *svc) SetConsistencyCheckStorages(ctx context.Context, id string, storages []string) error {
	key := fmt.Sprintf("ccv:stor:%s", id)
	if err := s.client.SAdd(ctx, key, storages).Err(); err != nil {
		return fmt.Errorf("unable to set storage set: %w", err)
	}
	return nil
}

func (s *svc) GetConsistencyCheckStorages(ctx context.Context, id string) ([]string, error) {
	key := fmt.Sprintf("ccv:stor:%s", id)
	storages, err := s.client.SMembers(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("unable to get storage set: %w", err)
	}
	return storages, nil
}

func (s *svc) DeleteConsistencyCheckStorages(ctx context.Context, id string) error {
	key := fmt.Sprintf("ccv:stor:%s", id)
	if err := s.client.Unlink(ctx, key).Err(); err != nil {
		return fmt.Errorf("unable to delete stroage set: %w", err)
	}
	return nil
}
