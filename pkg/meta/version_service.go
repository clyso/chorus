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

package meta

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
)

type Destination struct {
	Storage string
	Bucket  string
}

func (d Destination) Validate() error {
	if d.Storage == "" {
		return fmt.Errorf("%w: empty storage in destination", dom.ErrInvalidArg)
	}
	if d.Bucket == "" {
		return fmt.Errorf("%w: empty bucket in destination", dom.ErrInvalidArg)
	}
	return nil
}

func (d Destination) IsFrom(replID entity.UniversalReplicationID) bool {
	fromBucket, toBucket := replID.FromToBuckets(d.Bucket)

	if d.Storage == replID.FromStorage() && fromBucket == d.Bucket {
		return true
	}
	if d.Storage == replID.ToStorage() && toBucket == d.Bucket {
		return false
	}
	// should not happen: destinations must match replication ID
	panic(fmt.Sprintf("destination storage/bucket %s/%s does not match replication ID %s", d.Storage, d.Bucket, replID.AsString()))
}

type Version struct {
	From int
	To   int
}

func (v Version) IsEmpty() bool {
	return v.From == 0 && v.To == 0
}

type VersionService interface {
	Cleanup(ctx context.Context, replID entity.UniversalReplicationID) error

	GetObj(ctx context.Context, replID entity.UniversalReplicationID, obj dom.Object) (Version, error)
	IncrementObj(ctx context.Context, replID entity.UniversalReplicationID, obj dom.Object, dest Destination) (int, error)
	UpdateIfGreater(ctx context.Context, replID entity.UniversalReplicationID, obj dom.Object, dest Destination, version int) error
	DeleteObjAll(ctx context.Context, replID entity.UniversalReplicationID, obj dom.Object) error

	GetACL(ctx context.Context, replID entity.UniversalReplicationID, obj dom.Object) (Version, error)
	IncrementACL(ctx context.Context, replID entity.UniversalReplicationID, obj dom.Object, dest Destination) (int, error)
	UpdateACLIfGreater(ctx context.Context, replID entity.UniversalReplicationID, obj dom.Object, dest Destination, version int) error

	GetTags(ctx context.Context, replID entity.UniversalReplicationID, obj dom.Object) (Version, error)
	IncrementTags(ctx context.Context, replID entity.UniversalReplicationID, obj dom.Object, dest Destination) (int, error)
	UpdateTagsIfGreater(ctx context.Context, replID entity.UniversalReplicationID, obj dom.Object, dest Destination, version int) error

	GetBucket(ctx context.Context, replID entity.UniversalReplicationID, bucket string) (Version, error)
	IncrementBucket(ctx context.Context, replID entity.UniversalReplicationID, bucket string, dest Destination) (int, error)
	UpdateBucketIfGreater(ctx context.Context, replID entity.UniversalReplicationID, bucket string, dest Destination, version int) error
	DeleteBucketAll(ctx context.Context, replID entity.UniversalReplicationID, bucket string) error

	GetBucketACL(ctx context.Context, replID entity.UniversalReplicationID, bucket string) (Version, error)
	IncrementBucketACL(ctx context.Context, replID entity.UniversalReplicationID, bucket string, dest Destination) (int, error)
	UpdateBucketACLIfGreater(ctx context.Context, replID entity.UniversalReplicationID, bucket string, dest Destination, version int) error

	GetBucketTags(ctx context.Context, replID entity.UniversalReplicationID, bucket string) (Version, error)
	IncrementBucketTags(ctx context.Context, replID entity.UniversalReplicationID, bucket string, dest Destination) (int, error)
	UpdateBucketTagsIfGreater(ctx context.Context, replID entity.UniversalReplicationID, bucket string, dest Destination, version int) error
}

func NewVersionService(client redis.UniversalClient) VersionService {
	return &versionSvc{client: client}
}

type versionSvc struct {
	client redis.UniversalClient
}

func (s *versionSvc) GetObj(ctx context.Context, replID entity.UniversalReplicationID, obj dom.Object) (Version, error) {
	return s.getObjVersion(ctx, replID, obj, payload)
}

func (s *versionSvc) IncrementObj(ctx context.Context, replID entity.UniversalReplicationID, obj dom.Object, dest Destination) (int, error) {
	keys, err := toRedisObjKeys(replID, obj, payload)
	if err != nil {
		return 0, err
	}
	return s.incrementDest(ctx, replID, keys, dest)
}

func (s *versionSvc) UpdateIfGreater(ctx context.Context, replID entity.UniversalReplicationID, obj dom.Object, dest Destination, version int) error {
	keys, err := toRedisObjKeys(replID, obj, payload)
	if err != nil {
		return err
	}
	return s.setDestIfGreater(ctx, replID, keys, dest, int(version))
}

func (s *versionSvc) DeleteObjAll(ctx context.Context, replID entity.UniversalReplicationID, obj dom.Object) error {
	key, fields, err := allRedisObjKeys(replID, obj)
	if err != nil {
		return err
	}
	return s.client.HDel(ctx, key, fields...).Err()
}

func (s *versionSvc) GetACL(ctx context.Context, replID entity.UniversalReplicationID, obj dom.Object) (Version, error) {
	return s.getObjVersion(ctx, replID, obj, acl)
}

func (s *versionSvc) IncrementACL(ctx context.Context, replID entity.UniversalReplicationID, obj dom.Object, dest Destination) (int, error) {
	keys, err := toRedisObjKeys(replID, obj, acl)
	if err != nil {
		return 0, err
	}
	return s.incrementDest(ctx, replID, keys, dest)
}

func (s *versionSvc) UpdateACLIfGreater(ctx context.Context, replID entity.UniversalReplicationID, obj dom.Object, dest Destination, version int) error {
	keys, err := toRedisObjKeys(replID, obj, acl)
	if err != nil {
		return err
	}
	return s.setDestIfGreater(ctx, replID, keys, dest, version)
}

func (s *versionSvc) GetTags(ctx context.Context, replID entity.UniversalReplicationID, obj dom.Object) (Version, error) {
	return s.getObjVersion(ctx, replID, obj, tags)
}

func (s *versionSvc) IncrementTags(ctx context.Context, replID entity.UniversalReplicationID, obj dom.Object, dest Destination) (int, error) {
	keys, err := toRedisObjKeys(replID, obj, tags)
	if err != nil {
		return 0, err
	}
	return s.incrementDest(ctx, replID, keys, dest)
}

func (s *versionSvc) UpdateTagsIfGreater(ctx context.Context, replID entity.UniversalReplicationID, obj dom.Object, dest Destination, version int) error {
	keys, err := toRedisObjKeys(replID, obj, tags)
	if err != nil {
		return err
	}
	return s.setDestIfGreater(ctx, replID, keys, dest, version)
}

func (s *versionSvc) GetBucket(ctx context.Context, replID entity.UniversalReplicationID, bucket string) (Version, error) {
	return s.getBucketVersion(ctx, replID, bucket, payload)
}

func (s *versionSvc) IncrementBucket(ctx context.Context, replID entity.UniversalReplicationID, bucket string, dest Destination) (int, error) {
	keys, err := toRedisBucketKeys(replID, bucket, payload)
	if err != nil {
		return 0, err
	}
	return s.incrementDest(ctx, replID, keys, dest)
}

func (s *versionSvc) UpdateBucketIfGreater(ctx context.Context, replID entity.UniversalReplicationID, bucket string, dest Destination, version int) error {
	keys, err := toRedisBucketKeys(replID, bucket, payload)
	if err != nil {
		return err
	}
	return s.setDestIfGreater(ctx, replID, keys, dest, version)
}

func (s *versionSvc) DeleteBucketAll(ctx context.Context, replID entity.UniversalReplicationID, bucket string) error {
	key, fields, err := allRedisBucketKeys(replID, bucket)
	if err != nil {
		return err
	}
	return s.client.HDel(ctx, key, fields...).Err()
}

func (s *versionSvc) GetBucketACL(ctx context.Context, replID entity.UniversalReplicationID, bucket string) (Version, error) {
	return s.getBucketVersion(ctx, replID, bucket, acl)
}

func (s *versionSvc) IncrementBucketACL(ctx context.Context, replID entity.UniversalReplicationID, bucket string, dest Destination) (int, error) {
	keys, err := toRedisBucketKeys(replID, bucket, acl)
	if err != nil {
		return 0, err
	}
	return s.incrementDest(ctx, replID, keys, dest)
}

func (s *versionSvc) UpdateBucketACLIfGreater(ctx context.Context, replID entity.UniversalReplicationID, bucket string, dest Destination, version int) error {
	keys, err := toRedisBucketKeys(replID, bucket, acl)
	if err != nil {
		return err
	}
	return s.setDestIfGreater(ctx, replID, keys, dest, version)
}

func (s *versionSvc) GetBucketTags(ctx context.Context, replID entity.UniversalReplicationID, bucket string) (Version, error) {
	return s.getBucketVersion(ctx, replID, bucket, tags)
}

func (s *versionSvc) IncrementBucketTags(ctx context.Context, replID entity.UniversalReplicationID, bucket string, dest Destination) (int, error) {
	keys, err := toRedisBucketKeys(replID, bucket, tags)
	if err != nil {
		return 0, err
	}
	return s.incrementDest(ctx, replID, keys, dest)
}

func (s *versionSvc) UpdateBucketTagsIfGreater(ctx context.Context, replID entity.UniversalReplicationID, bucket string, dest Destination, version int) error {
	keys, err := toRedisBucketKeys(replID, bucket, tags)
	if err != nil {
		return err
	}
	return s.setDestIfGreater(ctx, replID, keys, dest, version)
}

// luaDelKeysWithPrefix - server side scan and delete keys with given key prefix
//
// Input:
// KEYS[1] -> Key prefix pattern (e.g. "v:user1:*")
// --
// ARGV[1] -> COUNT for SCAN (optional, default 1000)
//
// Output:
// Returns 0 when done
var luaDelKeysWithPrefix = redis.NewScript(`
local pattern = KEYS[1]
local cursor = 0
local count = tonumber(ARGV[1]) or 1000
repeat
    local scan_result = redis.call('SCAN', cursor, 'MATCH', pattern, 'COUNT', count)
    cursor = tonumber(scan_result[1])
    local keys = scan_result[2]
    redis.call('UNLINK', unpack(keys))
until cursor == 0
return 0`)

func (s *versionSvc) Cleanup(ctx context.Context, replID entity.UniversalReplicationID) error {
	pattern := redisReplicationMATCH(replID)
	if !strings.HasSuffix(pattern, "*") {
		// exact match - single key deletion
		return s.client.Del(ctx, pattern).Err()
	}
	// wildcard match - server-side scan and delete
	_, err := luaDelKeysWithPrefix.Run(ctx, s.client, []string{pattern}, 1000).Result()
	return err
}

func (s *versionSvc) getObjVersion(ctx context.Context, replID entity.UniversalReplicationID, object dom.Object, kind versionKind) (Version, error) {
	keys, err := toRedisObjKeys(replID, object, kind)
	if err != nil {
		return Version{}, err
	}
	return s.getVersion(ctx, keys)
}

func (s *versionSvc) getBucketVersion(ctx context.Context, replID entity.UniversalReplicationID, bucket string, kind versionKind) (Version, error) {
	keys, err := toRedisBucketKeys(replID, bucket, kind)
	if err != nil {
		return Version{}, err
	}
	return s.getVersion(ctx, keys)
}

func (s *versionSvc) getVersion(ctx context.Context, keys redisKeys) (Version, error) {
	res, err := s.client.HMGet(ctx, keys.hashKey, keys.fromField, keys.toField).Result()
	if err != nil {
		return Version{}, err
	}
	if len(res) != 2 {
		return Version{}, fmt.Errorf("%w: unexpected HMGet result length %d", dom.ErrInternal, len(res))
	}
	var version Version
	if res[0] != nil {
		version.From, err = strconv.Atoi(res[0].(string))
		if err != nil {
			return Version{}, fmt.Errorf("%w: unable to parse from version %q", dom.ErrInternal, res[0])
		}
	}
	if res[1] != nil {
		version.To, err = strconv.Atoi(res[1].(string))
		if err != nil {
			return Version{}, fmt.Errorf("%w: unable to parse to version %q", dom.ErrInternal, res[1])
		}
	}
	return version, nil
}

func (s *versionSvc) incrementDest(ctx context.Context, replID entity.UniversalReplicationID, keys redisKeys, dest Destination) (int, error) {
	if err := dest.Validate(); err != nil {
		return 0, err
	}
	if dest.IsFrom(replID) {
		return s.incVersion(ctx, keys.hashKey, keys.fromField, keys.toField)
	}
	// swap fields for 'to' destination
	return s.incVersion(ctx, keys.hashKey, keys.toField, keys.fromField)
}

// luaHIncVersion - takes max value out of 2 hash fields (A,B), increments it, and assigns result to A.
//
//	A = max(A,B) + 1
//
// Input:
// KEYS[1] -> Redis hash Key
// --
// ARGV[1] -> First hash field (A)
// ARGV[2] -> Second hash field (B)
//
// Output:
// Returns incremented version
var luaHIncVersion = redis.NewScript(`
local max_ver = 0
local vals = redis.call('HMGET', KEYS[1], ARGV[1], ARGV[2])
for _, ver in ipairs(vals) do
	if ver then
		max_ver = math.max(max_ver, tonumber(ver))
	end
end
max_ver = max_ver + 1
redis.call('HSET',KEYS[1], ARGV[1], max_ver )
return max_ver`)

func (s *versionSvc) incVersion(ctx context.Context, key, a, b string) (int, error) {
	result, err := luaHIncVersion.Run(ctx, s.client, []string{key}, a, b).Result()
	if err != nil {
		return 0, err
	}
	inc, ok := result.(int64)
	if !ok {
		return 0, fmt.Errorf("%w: unable to cast luaHIncVersion result %T to int64", dom.ErrInternal, result)
	}
	if inc == 0 {
		return 0, dom.ErrNotFound
	}
	return int(inc), nil
}

func (s *versionSvc) setDestIfGreater(ctx context.Context, replID entity.UniversalReplicationID, keys redisKeys, dest Destination, version int) error {
	if err := dest.Validate(); err != nil {
		return err
	}
	if dest.IsFrom(replID) {
		return s.setIfGreater(ctx, keys.hashKey, keys.fromField, keys.toField, version)
	}
	// swap fields for 'to' destination
	return s.setIfGreater(ctx, keys.hashKey, keys.toField, keys.fromField, version)
}

// luaHSetIfExAndGreater(hash_key, field_A, field_B, X) - sets X to hash field A only if:
//  1. hash key exists
//  2. field B exists - checks concurrent delete
//  3. X <= B         - checks concurrent delete
//  3. X > A          - checks concurrent updates
//
// Input:
// KEYS[1] -> Redis hash Key
// --
// ARGV[1] -> First Hash field (A)
// ARGV[2] -> Second Hash field (B)
// ARGV[2] -> Integer value (X) to set to first hash field
//
// Output:
// Returns -1 if hash key does not exist or second field does not exist or less than given integer
// Returns 0 if given integer is not greater than current value
// Returns 1 if value was set
var luaHSetIfExAndGreater = redis.NewScript(`
if redis.call('exists',KEYS[1]) ~= 1 then return -1 end

local other = redis.call('hget', KEYS[1], ARGV[2])
if not other or tonumber(other) < tonumber(ARGV[3]) then return -1 end

local prev = redis.call('hget', KEYS[1], ARGV[1]);
if prev and (tonumber(prev) >= tonumber(ARGV[3])) then return 0
else 
	redis.call("hset", KEYS[1], ARGV[1], ARGV[3]);
	return 1
end`)

func (s *versionSvc) setIfGreater(ctx context.Context, key, a, b string, version int) error {
	if version <= 0 {
		zerolog.Ctx(ctx).Warn().Str("key", key).Str("field", a).Msgf("discarded attempt to set invalid version %d", version)
		return fmt.Errorf("%w: version value must be positive", dom.ErrInvalidArg)
	}
	result, err := luaHSetIfExAndGreater.Run(ctx, s.client, []string{key}, a, b, version).Result()
	if err != nil {
		return err
	}
	inc, ok := result.(int64)
	if !ok {
		return fmt.Errorf("%w: unable to cast luaHSetIfExAndGreater result %T to int64", dom.ErrInternal, result)
	}
	if inc == -1 {
		return fmt.Errorf("%w: unable to update replicated version for key %s", dom.ErrNotFound, key)
	}
	if inc == 0 {
		return fmt.Errorf("%w: unable to update replicated version for key %s", dom.ErrAlreadyExists, key)
	}
	return nil
}
