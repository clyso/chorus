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

type RedisIDKeyValue[ID any, V any] struct {
	serializeValue   SingleValueConverter[V, string]
	deserializeValue SingleValueConverter[string, V]
	RedisIDCommonStore[ID]
}

func NewRedisIDKeyValue[ID any, V any](
	client redis.Cmdable, keyPrefix string,
	tokenizeID SingleToMultiValueConverter[ID, string], restoreID MultiToSingleValueConverter[string, ID],
	serializeValue SingleValueConverter[V, string], deserializeValue SingleValueConverter[string, V]) *RedisIDKeyValue[ID, V] {
	return &RedisIDKeyValue[ID, V]{
		serializeValue:     serializeValue,
		deserializeValue:   deserializeValue,
		RedisIDCommonStore: *NewRedisIDCommonStore[ID](client, keyPrefix, tokenizeID, restoreID),
	}
}

func (r *RedisIDKeyValue[ID, V]) GetOp(ctx context.Context, id ID) OperationResult[V] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[V](fmt.Errorf("unable to make key: %w", err))
	}
	cmd := r.client.Get(ctx, key)

	collectFunc := func() (V, error) {
		err = cmd.Err()
		if errors.Is(err, redis.Nil) {
			var noVal V
			return noVal, fmt.Errorf("%w: %w", dom.ErrNotFound, err)
		}
		if err != nil {
			var noVal V
			return noVal, fmt.Errorf("unable to get value: %w", err)
		}
		val, err := r.deserializeValue(cmd.Val())
		if err != nil {
			var noVal V
			return noVal, fmt.Errorf("unable to extract value: %w", err)
		}
		return val, nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeyValue[ID, V]) Get(ctx context.Context, id ID) (V, error) {
	return r.GetOp(ctx, id).Get()
}

func (r *RedisIDKeyValue[ID, V]) SetOp(ctx context.Context, id ID, value V) OperationStatus {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to make key: %w", err))
	}
	serializedValue, err := r.serializeValue(value)
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to serialize value: %w", err))
	}

	cmd := r.client.Set(ctx, key, serializedValue, 0)

	collectFunc := func() error {
		if err := cmd.Err(); err != nil {
			return err
		}
		return nil
	}

	return NewRedisOperationStatus(collectFunc)
}

func (r *RedisIDKeyValue[ID, V]) Set(ctx context.Context, id ID, value V) error {
	return r.SetOp(ctx, id, value).Get()
}

func (r *RedisIDKeyValue[ID, V]) SetIfNotExistsOp(ctx context.Context, id ID, value V) OperationStatus {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to make key: %w", err))
	}
	serializedValue, err := r.serializeValue(value)
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to serialize value: %w", err))
	}

	cmd := r.client.SetNX(ctx, key, serializedValue, 0)

	collectFunc := func() error {
		set, err := cmd.Result()
		if err != nil {
			return err
		}
		if !set {
			return dom.ErrAlreadyExists
		}
		return nil
	}

	return NewRedisOperationStatus(collectFunc)
}

func (r *RedisIDKeyValue[ID, V]) SetIfNotExists(ctx context.Context, id ID, value V) error {
	return r.SetIfNotExistsOp(ctx, id, value).Get()
}
