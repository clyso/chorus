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

type RedisIDKeySet[ID any, V any] struct {
	serializer   CollectionConverter[V, string]
	deserializer CollectionConverter[string, V]
	RedisIDCommonStore[ID]
}

func NewRedisIDKeySet[ID any, V any](client redis.Cmdable, idPrefix string,
	tokenizeID SingleToMultiValueConverter[ID, string], restoreID MultiToSingleValueConverter[string, ID],
	serializeValue SingleValueConverter[V, string], deserializeValue SingleValueConverter[string, V]) *RedisIDKeySet[ID, V] {
	return &RedisIDKeySet[ID, V]{
		serializer:         NewSimpleCollectionConverter(serializeValue),
		deserializer:       NewSimpleCollectionConverter(deserializeValue),
		RedisIDCommonStore: *NewRedisIDCommonStore[ID](client, idPrefix, tokenizeID, restoreID),
	}
}

func (r *RedisIDKeySet[ID, V]) GetOp(ctx context.Context, id ID) OperationResult[[]V] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[[]V](fmt.Errorf("unable to make key: %w", err))
	}
	cmd := r.client.SMembers(ctx, key)

	collectFunc := func() ([]V, error) {
		if err := cmd.Err(); errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("%w: %w", dom.ErrNotFound, err)
		}
		values, err := r.deserializer.ConvertMulti(cmd.Val())
		if err != nil {
			return nil, fmt.Errorf("unable to deserialize result: %w", err)
		}
		return values, nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeySet[ID, V]) Get(ctx context.Context, id ID) ([]V, error) {
	return r.GetOp(ctx, id).Get()
}

func (r *RedisIDKeySet[ID, V]) AddOp(ctx context.Context, id ID, values ...V) OperationResult[uint64] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[uint64](fmt.Errorf("unable to make key: %w", err))
	}
	convertedValues, err := r.serializer.ConvertMulti(values)
	if err != nil {
		return NewRedisFailedOperationResult[uint64](fmt.Errorf("unable to serialize values: %w", err))
	}

	cmd := r.client.SAdd(ctx, key, convertedValues)

	collectFunc := func() (uint64, error) {
		affected, err := cmd.Result()
		if err != nil {
			return 0, fmt.Errorf("unable to add values to set: %w", err)
		}
		return uint64(affected), nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeySet[ID, V]) Add(ctx context.Context, id ID, values ...V) (uint64, error) {
	return r.AddOp(ctx, id, values...).Get()
}

func (r *RedisIDKeySet[ID, V]) RemoveOp(ctx context.Context, id ID, values ...V) OperationResult[uint64] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[uint64](fmt.Errorf("unable to make key: %w", err))
	}
	convertedValues, err := r.serializer.ConvertMulti(values)
	if err != nil {
		return NewRedisFailedOperationResult[uint64](fmt.Errorf("unable to serialize values: %w", err))
	}

	cmd := r.client.SRem(ctx, key, convertedValues)

	collectFunc := func() (uint64, error) {
		affected, err := cmd.Result()
		if err != nil {
			return 0, err
		}
		return uint64(affected), nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeySet[ID, V]) Remove(ctx context.Context, id ID, values ...V) (uint64, error) {
	return r.RemoveOp(ctx, id, values...).Get()
}

func (r *RedisIDKeySet[ID, V]) IsMemberOp(ctx context.Context, id ID, value V) OperationResult[bool] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[bool](fmt.Errorf("unable to make key: %w", err))
	}
	convertedValue, err := r.serializer.ConvertSingle(value)
	if err != nil {
		return NewRedisFailedOperationResult[bool](fmt.Errorf("unable to serialize value: %w", err))
	}

	cmd := r.client.SIsMember(ctx, key, convertedValue)

	collectFunc := func() (bool, error) {
		exists, err := cmd.Result()
		if err != nil {
			return false, err
		}
		return exists, nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeySet[ID, V]) IsMember(ctx context.Context, id ID, value V) (bool, error) {
	return r.IsMemberOp(ctx, id, value).Get()
}

func (r *RedisIDKeySet[ID, V]) LenOp(ctx context.Context, id ID) OperationResult[uint64] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[uint64](fmt.Errorf("unable to make key: %w", err))
	}

	cmd := r.client.SCard(ctx, key)

	collectFunc := func() (uint64, error) {
		card, err := cmd.Result()
		if err != nil {
			return 0, err
		}
		return uint64(card), nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeySet[ID, V]) Len(ctx context.Context, id ID) (uint64, error) {
	return r.LenOp(ctx, id).Get()
}

func (r *RedisIDKeySet[ID, V]) EmptyOp(ctx context.Context, id ID) OperationResult[bool] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[bool](fmt.Errorf("unable to make key: %w", err))
	}

	cmd := r.client.SCard(ctx, key)

	collectFunc := func() (bool, error) {
		card, err := cmd.Result()
		if err != nil {
			return false, err
		}
		return card == 0, nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeySet[ID, V]) Empty(ctx context.Context, id ID) (bool, error) {
	return r.EmptyOp(ctx, id).Get()
}

func (r *RedisIDKeySet[ID, V]) NotEmptyOp(ctx context.Context, id ID) OperationResult[bool] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[bool](fmt.Errorf("unable to make key: %w", err))
	}

	cmd := r.client.SCard(ctx, key)

	collectFunc := func() (bool, error) {
		card, err := cmd.Result()
		if err != nil {
			return false, err
		}
		return card != 0, nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeySet[ID, V]) NotEmpty(ctx context.Context, id ID) (bool, error) {
	return r.NotEmptyOp(ctx, id).Get()
}
