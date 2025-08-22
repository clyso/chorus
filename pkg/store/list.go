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

	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/dom"
)

type RedisIDKeyList[ID any, V any] struct {
	serializer   CollectionConverter[V, string]
	deserializer CollectionConverter[string, V]
	RedisIDCommonStore[ID]
}

func NewRedisIDKeyList[ID any, V any](client redis.Cmdable, idPrefix string,
	tokenizeID SingleToMultiValueConverter[ID, string], restoreID MultiToSingleValueConverter[string, ID],
	serializeValue SingleValueConverter[V, string], deserializeValue SingleValueConverter[string, V]) *RedisIDKeyList[ID, V] {
	return &RedisIDKeyList[ID, V]{
		serializer:         NewSimpleCollectionConverter(serializeValue),
		deserializer:       NewSimpleCollectionConverter(deserializeValue),
		RedisIDCommonStore: *NewRedisIDCommonStore[ID](client, idPrefix, tokenizeID, restoreID),
	}
}

func (r *RedisIDKeyList[ID, V]) GetAllOp(ctx context.Context, id ID) OperationResult[[]V] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[[]V](fmt.Errorf("unable to make key: %w", err))
	}

	cmd := r.client.LRange(ctx, key, 0, -1)

	collectFunc := func() ([]V, error) {
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

func (r *RedisIDKeyList[ID, V]) GetAll(ctx context.Context, id ID) ([]V, error) {
	return r.GetAllOp(ctx, id).Get()
}

func (r *RedisIDKeyList[ID, V]) GetPageOp(ctx context.Context, id ID, pager Pager) OperationResult[[]V] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[[]V](fmt.Errorf("unable to make key: %w", err))
	}

	cmd := r.client.LRange(ctx, key, int64(pager.From), int64(pager.From+pager.Count-1))

	collectFunc := func() ([]V, error) {
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

func (r *RedisIDKeyList[ID, V]) GetPage(ctx context.Context, id ID, pager Pager) ([]V, error) {
	return r.GetPageOp(ctx, id, pager).Get()
}

func (r *RedisIDKeyList[ID, V]) getOneOp(ctx context.Context, id ID, idx int64) OperationResult[V] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[V](fmt.Errorf("unable to make key: %w", err))
	}

	cmd := r.client.LIndex(ctx, key, idx)

	collectFunc := func() (V, error) {
		cmdVal, err := cmd.Result()
		if errors.Is(err, redis.Nil) {
			var noVal V
			return noVal, fmt.Errorf("%w: %w", dom.ErrNotFound, err)
		}
		if err != nil {
			var noVal V
			return noVal, fmt.Errorf("unable to get element of list: %w", err)
		}
		value, err := r.deserializer.ConvertSingle(cmdVal)
		if err != nil {
			var noVal V
			return noVal, fmt.Errorf("unable to deserialize value: %w", err)
		}
		return value, nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeyList[ID, V]) GetOneOp(ctx context.Context, id ID, idx uint64) OperationResult[V] {
	return r.getOneOp(ctx, id, int64(idx))
}

func (r *RedisIDKeyList[ID, V]) GetOne(ctx context.Context, id ID, idx uint64) (V, error) {
	return r.GetOneOp(ctx, id, idx).Get()
}

func (r *RedisIDKeyList[ID, V]) GetLeftOp(ctx context.Context, id ID) OperationResult[V] {
	return r.GetOneOp(ctx, id, 0)
}

func (r *RedisIDKeyList[ID, V]) GetLeft(ctx context.Context, id ID) (V, error) {
	return r.GetLeftOp(ctx, id).Get()
}

func (r *RedisIDKeyList[ID, V]) GetRightOp(ctx context.Context, id ID) OperationResult[V] {
	return r.getOneOp(ctx, id, -1)
}

func (r *RedisIDKeyList[ID, V]) GetRight(ctx context.Context, id ID) (V, error) {
	return r.GetRightOp(ctx, id).Get()
}

func (r *RedisIDKeyList[ID, V]) AddLeftOp(ctx context.Context, id ID, values ...V) OperationStatus {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to make key: %w", err))
	}
	serializedValues, err := r.serializer.ConvertMulti(values)
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to serialize values: %w", err))
	}

	cmd := r.client.LPush(ctx, key, serializedValues)

	collectFunc := func() error {
		if err := cmd.Err(); err != nil {
			return fmt.Errorf("unable to push values: %w", err)
		}
		return nil
	}

	return NewRedisOperationStatus(collectFunc)
}

func (r *RedisIDKeyList[ID, V]) AddLeft(ctx context.Context, id ID, values ...V) error {
	return r.AddLeftOp(ctx, id, values...).Get()
}

func (r *RedisIDKeyList[ID, V]) AddRightOp(ctx context.Context, id ID, values ...V) OperationStatus {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to make key: %w", err))
	}
	serializedValues, err := r.serializer.ConvertMulti(values)
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to serialize values: %w", err))
	}

	cmd := r.client.RPush(ctx, key, serializedValues)

	collectFunc := func() error {
		if err := cmd.Err(); err != nil {
			return fmt.Errorf("unable to push values: %w", err)
		}
		return nil
	}

	return NewRedisOperationStatus(collectFunc)
}

func (r *RedisIDKeyList[ID, V]) AddRight(ctx context.Context, id ID, values ...V) error {
	return r.AddRightOp(ctx, id, values...).Get()
}

func (r *RedisIDKeyList[ID, V]) FindOp(ctx context.Context, id ID, value V) OperationResult[int64] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[int64](fmt.Errorf("unable to make key: %w", err))
	}
	serializedValue, err := r.serializer.ConvertSingle(value)
	if err != nil {
		return NewRedisFailedOperationResult[int64](fmt.Errorf("unable to serialize value: %w", err))
	}

	cmd := r.client.LPos(ctx, key, serializedValue, redis.LPosArgs{})

	collectFunc := func() (int64, error) {
		idx, err := cmd.Result()
		if errors.Is(err, redis.Nil) {
			return 0, fmt.Errorf("%w: %w", dom.ErrNotFound, err)
		}
		if err != nil {
			return 0, fmt.Errorf("unable to locate value: %w", err)
		}
		return idx, nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeyList[ID, V]) Find(ctx context.Context, id ID, value V) (int64, error) {
	return r.FindOp(ctx, id, value).Get()
}
