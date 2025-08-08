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
	"github.com/clyso/chorus/pkg/s3"
)

var (
	// key = version key, args {1: user, 2: bucket 3: storage}
	luaHIncVersion = redis.NewScript(`local max_ver = 0
	for _, ver in ipairs(redis.call('HVALS', KEYS[1])) do
		max_ver = math.max(max_ver, tonumber(ver))
	end
	max_ver = max_ver + 1
	redis.call('HSET',KEYS[1], ARGV[1], max_ver )
	return max_ver`)

	luaHSetIfExAndGreater = redis.NewScript(`
if redis.call('exists',KEYS[1]) ~= 1 then return -1 end
local prev = redis.call('hget', KEYS[1], ARGV[1]);
if prev and (tonumber(prev) >= tonumber(ARGV[2])) then return 0
else 
	redis.call("hset", KEYS[1], ARGV[1], ARGV[2]);
	return 1
end`)
	luaDelKeysWithPrefix = redis.NewScript(`local cursor = tonumber(ARGV[1]) or 0
local pattern = ARGV[2]
local count = tonumber(ARGV[3]) or 1000
local field = KEYS[1]

local scan_result = redis.call('SCAN', cursor, 'MATCH', pattern, 'COUNT', count)
local new_cursor = tonumber(scan_result[1])
local keys = scan_result[2]

for _, key in ipairs(keys) do
    redis.call('HDEL', key, field)
end

return new_cursor`)
)

// Destination - string alias for version destination
// can be storage or storage and bucket
type Destination string

func (d Destination) Parse() (storage string, bucket *string) {
	if dArr := strings.Split(string(d), ":"); len(dArr) == 2 {
		return dArr[0], &dArr[1]
	}
	return string(d), nil
}

// ToDest builds destination from storage name and optional bucket name
func ToDest(storage string, bucket string) Destination {
	if bucket == "" {
		return Destination(storage)
	}
	return Destination(fmt.Sprintf("%s:%s", storage, bucket))
}

type VersionService interface {
	// GetObj returns object content versions in s3 storages
	GetObj(ctx context.Context, obj dom.Object) (map[Destination]int64, error)
	// IncrementObj increments object content version in given storage
	IncrementObj(ctx context.Context, obj dom.Object, dest Destination) (int64, error)
	// UpdateIfGreater updates object content version in given storage if it is greater than previous value
	UpdateIfGreater(ctx context.Context, obj dom.Object, dest Destination, version int64) error
	// DeleteObjAll deletes all obj meta for all storages
	DeleteObjAll(ctx context.Context, obj dom.Object) error
	// DeleteBucketMeta deletes all versions info for bucket and version info for all bucket objects
	DeleteBucketMeta(ctx context.Context, dest Destination, bucket string) error

	// GetACL returns object ACL versions in s3 storages
	GetACL(ctx context.Context, obj dom.Object) (map[Destination]int64, error)
	// IncrementACL increments object ACL version in given storage
	IncrementACL(ctx context.Context, obj dom.Object, dest Destination) (int64, error)
	// UpdateACLIfGreater updates object ACL version in given storage if it is greater than previous value
	UpdateACLIfGreater(ctx context.Context, obj dom.Object, dest Destination, version int64) error

	// GetTags returns object Tagging versions in s3 storages
	GetTags(ctx context.Context, obj dom.Object) (map[Destination]int64, error)
	// IncrementTags increments object Tagging version in given storage
	IncrementTags(ctx context.Context, obj dom.Object, dest Destination) (int64, error)
	// UpdateTagsIfGreater updates object Tagging version in given storage if it is greater than previous value
	UpdateTagsIfGreater(ctx context.Context, obj dom.Object, dest Destination, version int64) error

	// GetBucket returns bucket content versions in s3 storages
	GetBucket(ctx context.Context, bucket string) (map[Destination]int64, error)
	// IncrementBucket increments bucket content version in given storage
	IncrementBucket(ctx context.Context, bucket string, dest Destination) (int64, error)
	// UpdateBucketIfGreater updates bucket content version in given storage if it is greater than previous value
	UpdateBucketIfGreater(ctx context.Context, bucket string, dest Destination, version int64) error
	// DeleteBucketAll deletes all bucket meta for all storages
	DeleteBucketAll(ctx context.Context, bucket string) error

	// GetBucketACL returns bucket ACL versions in s3 storages
	GetBucketACL(ctx context.Context, bucket string) (map[Destination]int64, error)
	// IncrementBucketACL increments bucket ACL version in given storage
	IncrementBucketACL(ctx context.Context, bucket string, dest Destination) (int64, error)
	// UpdateBucketACLIfGreater updates bucket ACL version in given storage if it is greater than previous value
	UpdateBucketACLIfGreater(ctx context.Context, bucket string, dest Destination, version int64) error

	// GetBucketTags returns bucket Tagging versions in s3 storages
	GetBucketTags(ctx context.Context, bucket string) (map[Destination]int64, error)
	// IncrementBucketTags increments bucket Tagging version in given storage
	IncrementBucketTags(ctx context.Context, bucket string, dest Destination) (int64, error)
	// UpdateBucketTagsIfGreater updates bucket Tagging version in given storage if it is greater than previous value
	UpdateBucketTagsIfGreater(ctx context.Context, bucket string, dest Destination, version int64) error
}

func NewVersionService(client redis.UniversalClient) VersionService {
	return &versionSvc{client: client}
}

type versionSvc struct {
	client redis.UniversalClient
}

func (s *versionSvc) GetObj(ctx context.Context, obj dom.Object) (map[Destination]int64, error) {
	return s.get(ctx, obj.Key())
}

func (s *versionSvc) get(ctx context.Context, key string) (map[Destination]int64, error) {
	resMap, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	res := map[Destination]int64{}
	for stor, verStr := range resMap {
		ver, err := strconv.Atoi(verStr)
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Interface("dom_object", key).Msgf("unable to parse %q version", stor)
			continue
		}
		res[Destination(stor)] = int64(ver)
	}
	return res, nil
}

func (s *versionSvc) IncrementObj(ctx context.Context, obj dom.Object, dest Destination) (int64, error) {
	return s.incVersion(ctx, obj.Key(), dest)
}

func (s *versionSvc) incVersion(ctx context.Context, key string, dest Destination) (int64, error) {
	result, err := luaHIncVersion.Run(ctx, s.client, []string{key}, string(dest)).Result()
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
	return inc, nil
}

func (s *versionSvc) UpdateIfGreater(ctx context.Context, obj dom.Object, dest Destination, version int64) error {
	return s.setIfGreater(ctx, obj.Key(), string(dest), int(version))
}

func (s *versionSvc) DeleteBucketMeta(ctx context.Context, dest Destination, bucket string) error {
	err := s3.ValidateBucketName(bucket)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("b:%s", bucket)
	if err = s.client.HDel(ctx, key, string(dest)).Err(); err != nil {
		return err
	}
	keyAcl := fmt.Sprintf("b:%s:a", bucket)
	if err = s.client.HDel(ctx, keyAcl, string(dest)).Err(); err != nil {
		return err
	}
	keyTags := fmt.Sprintf("b:%s:t", bucket)
	if err = s.client.HDel(ctx, keyTags, string(dest)).Err(); err != nil {
		return err
	}
	var cursor int64
	for {
		cursor, err = luaDelKeysWithPrefix.Run(ctx, s.client, []string{string(dest)}, cursor, bucket+":*").Int64()
		if err != nil {
			return err
		}
		if cursor == 0 {
			break
		}
	}
	return nil
}

func (s *versionSvc) DeleteObjAll(ctx context.Context, obj dom.Object) error {
	return s.client.Del(ctx, obj.Key(), obj.ACLKey(), obj.TagKey()).Err()
}

func (s *versionSvc) setIfGreater(ctx context.Context, key, field string, setTo int) error {
	if setTo <= 0 {
		zerolog.Ctx(ctx).Warn().Str("key", key).Str("field", field).Msgf("discarded attempt to set invalid version %d", setTo)
		return fmt.Errorf("%w: version value must be positive", dom.ErrInvalidArg)
	}
	result, err := luaHSetIfExAndGreater.Run(ctx, s.client, []string{key}, field, setTo).Result()
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

func (s *versionSvc) GetACL(ctx context.Context, obj dom.Object) (map[Destination]int64, error) {
	return s.get(ctx, obj.ACLKey())
}

func (s *versionSvc) IncrementACL(ctx context.Context, obj dom.Object, dest Destination) (int64, error) {
	return s.incVersion(ctx, obj.ACLKey(), dest)
}

func (s *versionSvc) UpdateACLIfGreater(ctx context.Context, obj dom.Object, dest Destination, version int64) error {
	return s.setIfGreater(ctx, obj.ACLKey(), string(dest), int(version))
}

func (s *versionSvc) GetTags(ctx context.Context, obj dom.Object) (map[Destination]int64, error) {
	return s.get(ctx, obj.TagKey())
}

func (s *versionSvc) IncrementTags(ctx context.Context, obj dom.Object, dest Destination) (int64, error) {
	return s.incVersion(ctx, obj.TagKey(), dest)
}

func (s *versionSvc) UpdateTagsIfGreater(ctx context.Context, obj dom.Object, dest Destination, version int64) error {
	return s.setIfGreater(ctx, obj.TagKey(), string(dest), int(version))
}

func (s *versionSvc) GetBucket(ctx context.Context, bucket string) (map[Destination]int64, error) {
	key := fmt.Sprintf("b:%s", bucket)
	return s.get(ctx, key)
}

func (s *versionSvc) IncrementBucket(ctx context.Context, bucket string, dest Destination) (int64, error) {
	key := fmt.Sprintf("b:%s", bucket)
	return s.incVersion(ctx, key, dest)
}

func (s *versionSvc) UpdateBucketIfGreater(ctx context.Context, bucket string, dest Destination, version int64) error {
	key := fmt.Sprintf("b:%s", bucket)
	return s.setIfGreater(ctx, key, string(dest), int(version))
}

func (s *versionSvc) DeleteBucketAll(ctx context.Context, bucket string) error {
	key := fmt.Sprintf("b:%s", bucket)
	keyAcl := fmt.Sprintf("b:%s:a", bucket)
	keyTags := fmt.Sprintf("b:%s:t", bucket)
	return s.client.Del(ctx, key, keyAcl, keyTags).Err()
}

func (s *versionSvc) GetBucketACL(ctx context.Context, bucket string) (map[Destination]int64, error) {
	key := fmt.Sprintf("b:%s:a", bucket)
	return s.get(ctx, key)
}

func (s *versionSvc) IncrementBucketACL(ctx context.Context, bucket string, dest Destination) (int64, error) {
	key := fmt.Sprintf("b:%s:a", bucket)
	return s.incVersion(ctx, key, dest)
}

func (s *versionSvc) UpdateBucketACLIfGreater(ctx context.Context, bucket string, dest Destination, version int64) error {
	key := fmt.Sprintf("b:%s:a", bucket)
	return s.setIfGreater(ctx, key, string(dest), int(version))
}

func (s *versionSvc) GetBucketTags(ctx context.Context, bucket string) (map[Destination]int64, error) {
	key := fmt.Sprintf("b:%s:t", bucket)
	return s.get(ctx, key)
}

func (s *versionSvc) IncrementBucketTags(ctx context.Context, bucket string, dest Destination) (int64, error) {
	key := fmt.Sprintf("b:%s:t", bucket)
	return s.incVersion(ctx, key, dest)
}

func (s *versionSvc) UpdateBucketTagsIfGreater(ctx context.Context, bucket string, dest Destination, version int64) error {
	key := fmt.Sprintf("b:%s:t", bucket)
	return s.setIfGreater(ctx, key, string(dest), int(version))
}
