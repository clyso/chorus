/*
 * Copyright © 2023 Clyso GmbH
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
	"errors"
	"fmt"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"strconv"
)

type VersionService interface {
	// GetObj returns object content versions in s3 storages
	GetObj(ctx context.Context, obj dom.Object) (map[string]int64, error)
	// IncrementObj increments object content version in given storage
	IncrementObj(ctx context.Context, obj dom.Object, storage string) (int64, error)
	// UpdateIfGreater updates object content version in given storage if it is greater than previous value
	UpdateIfGreater(ctx context.Context, obj dom.Object, storage string, version int64) error
	// DeleteObjAll deletes all obj meta for all storages
	DeleteObjAll(ctx context.Context, obj dom.Object) error
	// DeleteBucketMeta deletes meta of bucket objects for given storage.
	DeleteBucketMeta(ctx context.Context, storage, bucket string) error

	// GetACL returns object ACL versions in s3 storages
	GetACL(ctx context.Context, obj dom.Object) (map[string]int64, error)
	// IncrementACL increments object ACL version in given storage
	IncrementACL(ctx context.Context, obj dom.Object, storage string) (int64, error)
	// UpdateACLIfGreater updates object ACL version in given storage if it is greater than previous value
	UpdateACLIfGreater(ctx context.Context, obj dom.Object, storage string, version int64) error

	// GetTags returns object Tagging versions in s3 storages
	GetTags(ctx context.Context, obj dom.Object) (map[string]int64, error)
	// IncrementTags increments object Tagging version in given storage
	IncrementTags(ctx context.Context, obj dom.Object, storage string) (int64, error)
	// UpdateTagsIfGreater updates object Tagging version in given storage if it is greater than previous value
	UpdateTagsIfGreater(ctx context.Context, obj dom.Object, storage string, version int64) error

	// GetBucket returns bucket content versions in s3 storages
	GetBucket(ctx context.Context, bucket string) (map[string]int64, error)
	// IncrementBucket increments bucket content version in given storage
	IncrementBucket(ctx context.Context, bucket string, storage string) (int64, error)
	// UpdateBucketIfGreater updates bucket content version in given storage if it is greater than previous value
	UpdateBucketIfGreater(ctx context.Context, bucket string, storage string, version int64) error
	// DeleteBucketAll deletes all bucket meta for all storages
	DeleteBucketAll(ctx context.Context, bucket string) error

	// GetBucketACL returns bucket ACL versions in s3 storages
	GetBucketACL(ctx context.Context, bucket string) (map[string]int64, error)
	// IncrementBucketACL increments bucket ACL version in given storage
	IncrementBucketACL(ctx context.Context, bucket string, storage string) (int64, error)
	// UpdateBucketACLIfGreater updates bucket ACL version in given storage if it is greater than previous value
	UpdateBucketACLIfGreater(ctx context.Context, bucket string, storage string, version int64) error

	// GetBucketTags returns bucket Tagging versions in s3 storages
	GetBucketTags(ctx context.Context, bucket string) (map[string]int64, error)
	// IncrementBucketTags increments bucket Tagging version in given storage
	IncrementBucketTags(ctx context.Context, bucket string, storage string) (int64, error)
	// UpdateBucketTagsIfGreater updates bucket Tagging version in given storage if it is greater than previous value
	UpdateBucketTagsIfGreater(ctx context.Context, bucket string, storage string, version int64) error
}

func NewVersionService(client *redis.Client) VersionService {
	return &versionSvc{client: client}
}

type versionSvc struct {
	client *redis.Client
}

func (s *versionSvc) GetObj(ctx context.Context, obj dom.Object) (map[string]int64, error) {
	return s.get(ctx, obj.Key())
}

func (s *versionSvc) get(ctx context.Context, key string) (map[string]int64, error) {
	resMap, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	res := map[string]int64{}
	for stor, verStr := range resMap {
		ver, err := strconv.Atoi(verStr)
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Interface("dom_object", key).Msgf("unable to parse %q version", stor)
			continue
		}
		res[stor] = int64(ver)
	}
	return res, nil
}

func (s *versionSvc) IncrementObj(ctx context.Context, obj dom.Object, storage string) (int64, error) {
	return s.client.HIncrBy(ctx, obj.Key(), storage, 1).Result()
}

func (s *versionSvc) UpdateIfGreater(ctx context.Context, obj dom.Object, storage string, version int64) error {
	return s.setIfGreater(ctx, obj.Key(), storage, int(version))
}

func (s *versionSvc) DeleteBucketMeta(ctx context.Context, storage, bucket string) error {
	if err := s3.ValidateBucketName(bucket); err != nil {
		return err
	}
	iter := s.client.Scan(ctx, 0, bucket+":*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		err := s.client.HDel(ctx, key, storage).Err()
		if err != nil && err != redis.Nil {
			return fmt.Errorf("%w: unable to delete obj version", err)
		}
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("%w: iterate over obj meta error", err)
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
	const maxRetries = 1000
	// Transactional function.
	txf := func(tx *redis.Tx) error {
		// Get the current value or zero.
		currStr, err := s.client.HGet(ctx, key, field).Result()
		if err != nil && err != redis.Nil {
			return err
		}
		var currVer int
		if currStr != "" {
			currVer, err = strconv.Atoi(currStr)
			if err != nil {
				return err
			}
		}

		if setTo <= currVer {
			return nil
		}
		// Operation is commited only if the watched keys remain unchanged.
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, key, field, setTo)
			return nil
		})
		return err
	}

	// Retry if the key has been changed.
	for i := 0; i < maxRetries; i++ {
		err := s.client.Watch(ctx, txf, key)
		if err == nil {
			// Success.
			return nil
		}
		if err == redis.TxFailedErr {
			// Optimistic lock lost. Retry.
			continue
		}
		// Return any other error.
		return err
	}

	return errors.New("increment reached maximum number of retries")
}

func (s *versionSvc) GetACL(ctx context.Context, obj dom.Object) (map[string]int64, error) {
	return s.get(ctx, obj.ACLKey())
}

func (s *versionSvc) IncrementACL(ctx context.Context, obj dom.Object, storage string) (int64, error) {
	return s.client.HIncrBy(ctx, obj.ACLKey(), storage, 1).Result()
}

func (s *versionSvc) UpdateACLIfGreater(ctx context.Context, obj dom.Object, storage string, version int64) error {
	return s.setIfGreater(ctx, obj.ACLKey(), storage, int(version))
}

func (s *versionSvc) GetTags(ctx context.Context, obj dom.Object) (map[string]int64, error) {
	return s.get(ctx, obj.TagKey())
}

func (s *versionSvc) IncrementTags(ctx context.Context, obj dom.Object, storage string) (int64, error) {
	return s.client.HIncrBy(ctx, obj.TagKey(), storage, 1).Result()
}

func (s *versionSvc) UpdateTagsIfGreater(ctx context.Context, obj dom.Object, storage string, version int64) error {
	return s.setIfGreater(ctx, obj.TagKey(), storage, int(version))
}

func (s *versionSvc) GetBucket(ctx context.Context, bucket string) (map[string]int64, error) {
	key := fmt.Sprintf("b:%s", bucket)
	return s.get(ctx, key)
}

func (s *versionSvc) IncrementBucket(ctx context.Context, bucket string, storage string) (int64, error) {
	key := fmt.Sprintf("b:%s", bucket)
	return s.client.HIncrBy(ctx, key, storage, 1).Result()
}

func (s *versionSvc) UpdateBucketIfGreater(ctx context.Context, bucket string, storage string, version int64) error {
	key := fmt.Sprintf("b:%s", bucket)
	return s.setIfGreater(ctx, key, storage, int(version))
}

func (s *versionSvc) DeleteBucketAll(ctx context.Context, bucket string) error {
	key := fmt.Sprintf("b:%s", bucket)
	keyAcl := fmt.Sprintf("b:%s:a", bucket)
	keyTags := fmt.Sprintf("b:%s:t", bucket)
	return s.client.Del(ctx, key, keyAcl, keyTags).Err()
}

func (s *versionSvc) GetBucketACL(ctx context.Context, bucket string) (map[string]int64, error) {
	key := fmt.Sprintf("b:%s:a", bucket)
	return s.get(ctx, key)
}

func (s *versionSvc) IncrementBucketACL(ctx context.Context, bucket string, storage string) (int64, error) {
	key := fmt.Sprintf("b:%s:a", bucket)
	return s.client.HIncrBy(ctx, key, storage, 1).Result()
}

func (s *versionSvc) UpdateBucketACLIfGreater(ctx context.Context, bucket string, storage string, version int64) error {
	key := fmt.Sprintf("b:%s:a", bucket)
	return s.setIfGreater(ctx, key, storage, int(version))
}

func (s *versionSvc) GetBucketTags(ctx context.Context, bucket string) (map[string]int64, error) {
	key := fmt.Sprintf("b:%s:t", bucket)
	return s.get(ctx, key)
}

func (s *versionSvc) IncrementBucketTags(ctx context.Context, bucket string, storage string) (int64, error) {
	key := fmt.Sprintf("b:%s:t", bucket)
	return s.client.HIncrBy(ctx, key, storage, 1).Result()
}

func (s *versionSvc) UpdateBucketTagsIfGreater(ctx context.Context, bucket string, storage string, version int64) error {
	key := fmt.Sprintf("b:%s:t", bucket)
	return s.setIfGreater(ctx, key, storage, int(version))
}
