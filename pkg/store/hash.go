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
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/dom"
)

var (
	luaHSetEx    = redis.NewScript(`if redis.call('exists',KEYS[1]) == 1 then redis.call("hset", KEYS[1], ARGV[1], ARGV[2]); return 1 else return 0 end`)
	luaHIncrByEx = redis.NewScript(`if redis.call('exists',KEYS[1]) == 1 then return redis.call("hincrby", KEYS[1], ARGV[1], ARGV[2]) else return nil end`)
)

type RedisIDKeyHash[ID any, V any] struct {
	RedisIDCommonStore[ID]
}

func NewRedisIDKeyHash[ID any, V any](client redis.Cmdable, keyPrefix string,
	tokenizeID SingleToMultiValueConverter[ID, string], restoreID MultiToSingleValueConverter[string, ID]) *RedisIDKeyHash[ID, V] {
	return &RedisIDKeyHash[ID, V]{
		*NewRedisIDCommonStore(client, keyPrefix, tokenizeID, restoreID),
	}
}

func (r *RedisIDKeyHash[ID, V]) SetOp(ctx context.Context, id ID, value V) OperationStatus {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to make key: %w", err))
	}

	cmd := r.client.HSet(ctx, key, value)

	collectFunc := func() error {
		if err := cmd.Err(); err != nil {
			return fmt.Errorf("unable to set value in hash: %w", err)
		}
		return nil
	}

	return NewRedisOperationStatus(collectFunc)
}

func (r *RedisIDKeyHash[ID, V]) Set(ctx context.Context, id ID, value V) error {
	return r.SetOp(ctx, id, value).Get()
}

func (r *RedisIDKeyHash[ID, V]) Len(ctx context.Context, id ID) (int64, error) {
	return r.LenOp(ctx, id).Get()
}

func (r *RedisIDKeyHash[ID, V]) LenOp(ctx context.Context, id ID) OperationResult[int64] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[int64](fmt.Errorf("unable to make key: %w", err))
	}
	cmd := r.client.HLen(ctx, key)
	return NewRedisOperationResult(func() (int64, error) {
		return cmd.Result()
	})
}

func (r *RedisIDKeyHash[ID, V]) GetOp(ctx context.Context, id ID) OperationResult[V] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[V](fmt.Errorf("unable to make key: %w", err))
	}
	cmd := r.client.HGetAll(ctx, key)

	collectFunc := func() (V, error) {
		if err := cmd.Err(); err != nil {
			var noVal V
			return noVal, fmt.Errorf("unable to get hash map: %w", err)
		}
		if len(cmd.Val()) == 0 {
			var noVal V
			return noVal, fmt.Errorf("%w: hash map not found", dom.ErrNotFound)
		}
		var value V
		if err := cmd.Scan(&value); err != nil {
			var noVal V
			return noVal, fmt.Errorf("unable to scan hash map: %w", err)
		}
		return value, nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeyHash[ID, V]) Get(ctx context.Context, id ID) (V, error) {
	return r.GetOp(ctx, id).Get()
}

func (r *RedisIDKeyHash[ID, V]) GetFieldOp(ctx context.Context, id ID, fieldName string) OperationResult[string] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[string](fmt.Errorf("unable to make key: %w", err))
	}

	cmd := r.client.HGet(ctx, key, fieldName)

	collectFunc := func() (string, error) {
		value, err := cmd.Result()
		if errors.Is(err, redis.Nil) {
			return "", fmt.Errorf("%w: %w", dom.ErrNotFound, err)
		}
		if err != nil {
			return "", fmt.Errorf("unable to get field: %w", err)
		}
		return value, nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeyHash[ID, V]) GetField(ctx context.Context, id ID, fieldName string) (string, error) {
	return r.GetFieldOp(ctx, id, fieldName).Get()
}

func (r *RedisIDKeyHash[ID, V]) DelFieldOp(ctx context.Context, id ID, fieldName string) OperationResult[uint64] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[uint64](fmt.Errorf("unable to make key: %w", err))
	}

	cmd := r.client.HDel(ctx, key, fieldName)

	collectFunc := func() (uint64, error) {
		value, err := cmd.Result()
		if err != nil {
			return 0, fmt.Errorf("unable to delete field: %w", err)
		}
		return uint64(value), nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeyHash[ID, V]) DelField(ctx context.Context, id ID, fieldName string) (uint64, error) {
	return r.DelFieldOp(ctx, id, fieldName).Get()
}

func (r *RedisIDKeyHash[ID, V]) SetFieldIfExistsOp(ctx context.Context, id ID, fieldName string, value any) OperationStatus {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to make key: %w", err))
	}

	cmd := luaHSetEx.Eval(ctx, r.client, []string{key}, fieldName, value)

	collectFunc := func() error {
		result, err := cmd.Result()
		if err != nil {
			return fmt.Errorf("unable to execute script: %w", err)
		}
		affected, ok := result.(int64)
		if !ok {
			return fmt.Errorf("%w: unable to cast luaHSetEx result %T to int64", dom.ErrInternal, result)
		}
		if affected == 0 {
			return dom.ErrNotFound
		}
		return nil
	}

	return NewRedisOperationStatus(collectFunc)
}

func (r *RedisIDKeyHash[ID, V]) SetFieldIfExists(ctx context.Context, id ID, fieldName string, value any) error {
	return r.SetFieldIfExistsOp(ctx, id, fieldName, value).Get()
}

func (r *RedisIDKeyHash[ID, V]) SetFieldOp(ctx context.Context, id ID, fieldName string, value any) OperationResult[uint64] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[uint64](fmt.Errorf("unable to make key: %w", err))
	}

	cmd := r.client.HSet(ctx, key, fieldName, value)

	collectFunc := func() (uint64, error) {
		affected, err := cmd.Result()
		if err != nil {
			return 0, err
		}
		return uint64(affected), nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeyHash[ID, V]) SetField(ctx context.Context, id ID, fieldName string, value any) (uint64, error) {
	return r.SetFieldOp(ctx, id, fieldName, value).Get()
}

func (r *RedisIDKeyHash[ID, V]) IncrementFieldOp(ctx context.Context, id ID, fieldName string) OperationResult[int64] {
	return r.IncrementFieldByNOp(ctx, id, fieldName, 1)
}

func (r *RedisIDKeyHash[ID, V]) IncrementField(ctx context.Context, id ID, fieldName string) (int64, error) {
	return r.IncrementFieldOp(ctx, id, fieldName).Get()
}

func (r *RedisIDKeyHash[ID, V]) IncrementFieldByNOp(ctx context.Context, id ID, fieldName string, value int64) OperationResult[int64] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[int64](fmt.Errorf("unable to make key: %w", err))
	}

	cmd := r.client.HIncrBy(ctx, key, fieldName, value)

	collectFunc := func() (int64, error) {
		count, err := cmd.Result()
		if err != nil {
			return 0, fmt.Errorf("unable to execute increment: %w", err)
		}
		return count, nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeyHash[ID, V]) IncrementFieldByN(ctx context.Context, id ID, fieldName string, value int64) (int64, error) {
	return r.IncrementFieldByNOp(ctx, id, fieldName, value).Get()
}

func (r *RedisIDKeyHash[ID, V]) IncrementFieldIfExistsOp(ctx context.Context, id ID, fieldName string) OperationResult[int64] {
	return r.IncrementFieldByNIfExistsOp(ctx, id, fieldName, 1)
}

func (r *RedisIDKeyHash[ID, V]) IncrementFieldIfExists(ctx context.Context, id ID, fieldName string) (int64, error) {
	return r.IncrementFieldIfExistsOp(ctx, id, fieldName).Get()
}

func (r *RedisIDKeyHash[ID, V]) IncrementFieldByNIfExistsOp(ctx context.Context, id ID, fieldName string, value int64) OperationResult[int64] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[int64](fmt.Errorf("unable to make key: %w", err))
	}

	cmd := luaHIncrByEx.Eval(ctx, r.client, []string{key}, fieldName, value)

	collectFunc := func() (int64, error) {
		result, err := cmd.Result()
		if errors.Is(err, redis.Nil) {
			return 0, fmt.Errorf("%w: %w", dom.ErrNotFound, err)
		}
		if err != nil {
			return 0, err
		}
		count, ok := result.(int64)
		if !ok {
			return 0, fmt.Errorf("%w: unable to cast luaHIncrByEx result %T to int64", dom.ErrInternal, result)
		}
		return count, nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeyHash[ID, V]) IncrementFieldByNIfExists(ctx context.Context, id ID, fieldName string, value int64) (int64, error) {
	return r.IncrementFieldByNIfExistsOp(ctx, id, fieldName, value).Get()
}

func makeRedisFieldMap(value any) map[string]any {
	result := map[string]any{}
	reflectValue := reflect.ValueOf(value)
	reflectType := reflectValue.Type()

	for i := range reflectType.NumField() {
		field := reflectType.Field(i)
		redisTag := field.Tag.Get("redis")
		if redisTag == "" || redisTag == "-" {
			continue
		}

		redisTag = strings.Split(redisTag, ",")[0]

		fieldValue := reflectValue.Field(i)
		if !fieldValue.CanInterface() {
			result[redisTag] = nil
			continue
		}
		if field.Type.Kind() == reflect.Ptr && fieldValue.IsNil() {
			result[redisTag] = nil
			continue
		}
		if field.Type.Kind() == reflect.Ptr {
			result[redisTag] = fieldValue.Elem().Interface()
		} else {
			result[redisTag] = fieldValue.Interface()
		}
	}
	return result
}
