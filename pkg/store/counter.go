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
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type Numeric interface {
	int | int64 | uint64 | float32 | float64
}

func SerializeNumber[T Numeric](value T) (string, error) {
	var result string
	var err error

	switch typedValue := any(value).(type) {
	case int:
		result = strconv.Itoa(typedValue)
	case int64:
		result = strconv.FormatInt(typedValue, 10)
	case uint64:
		result = strconv.FormatUint(typedValue, 10)
	case float32:
		result = strconv.FormatFloat(float64(typedValue), 'f', -1, 32)
	case float64:
		result = strconv.FormatFloat(typedValue, 'f', -1, 64)
	default:
		err = fmt.Errorf("unsupported variable type %T", value)
	}

	if err != nil {
		return "", fmt.Errorf("unable to serialize number: %w", err)
	}

	return result, nil
}

func DeserializeNumber[T Numeric](value string) (T, error) {
	var result T
	var genericResult any
	var err error

	switch any(result).(type) {
	case int:
		genericResult, err = strconv.Atoi(value)
	case int64:
		genericResult, err = strconv.ParseInt(value, 10, 64)
	case uint64:
		genericResult, err = strconv.ParseUint(value, 10, 64)
	case float32:
		var f64 float64
		f64, err = strconv.ParseFloat(value, 32)
		genericResult = float32(f64)
	case float64:
		genericResult, err = strconv.ParseFloat(value, 64)
	default:
		err = fmt.Errorf("unsupported variable type %T", value)
	}

	if err != nil {
		return result, fmt.Errorf("unable to deserialize number: %w", err)
	}

	result = genericResult.(T)

	return result, nil
}

type RedisIDKeyCounter[ID any, V Numeric] struct {
	RedisIDKeyValue[ID, V]
}

func NewRedisIDKeyCounter[ID any, V Numeric](client redis.Cmdable, keyPrefix string,
	tokenizeID SingleToMultiValueConverter[ID, string], restoreID MultiToSingleValueConverter[string, ID]) *RedisIDKeyCounter[ID, V] {
	return &RedisIDKeyCounter[ID, V]{
		*NewRedisIDKeyValue[ID, V](client, keyPrefix, tokenizeID, restoreID, SerializeNumber, DeserializeNumber),
	}
}

func (r *RedisIDKeyCounter[ID, V]) WithExecutorProvider(executor Executor[redis.Cmdable]) *RedisIDKeyCounter[ID, V] {
	return &RedisIDKeyCounter[ID, V]{
		*NewRedisIDKeyValue[ID, V](executor.Get(), r.keyPrefix, r.tokenizeID, r.restoreID, SerializeNumber, DeserializeNumber),
	}
}

func (r *RedisIDKeyCounter[ID, V]) IncrementOp(ctx context.Context, id ID) OperationResult[V] {
	return r.IncrementByNOp(ctx, id, 1)
}

func (r *RedisIDKeyCounter[ID, V]) Increment(ctx context.Context, id ID) (V, error) {
	return r.IncrementOp(ctx, id).Get()
}

func (r *RedisIDKeyCounter[ID, V]) IncrementByNOp(ctx context.Context, id ID, n V) OperationResult[V] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[V](fmt.Errorf("unable to make key: %w", err))
	}
	return r.incrementOp(ctx, key, n)
}

func (r *RedisIDKeyCounter[ID, V]) IncrementByN(ctx context.Context, id ID, n V) (V, error) {
	return r.IncrementByNOp(ctx, id, n).Get()
}

func (r *RedisIDKeyCounter[ID, V]) DecrementOp(ctx context.Context, id ID) OperationResult[V] {
	return r.DecrementByNOp(ctx, id, 1)
}

func (r *RedisIDKeyCounter[ID, V]) Decrement(ctx context.Context, id ID) (V, error) {
	return r.DecrementOp(ctx, id).Get()
}

func (r *RedisIDKeyCounter[ID, V]) DecrementByNOp(ctx context.Context, id ID, n V) OperationResult[V] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[V](fmt.Errorf("unable to make key: %w", err))
	}
	return r.decrementOp(ctx, key, n)
}

func (r *RedisIDKeyCounter[ID, V]) DecrementByN(ctx context.Context, id ID, n V) (V, error) {
	return r.DecrementByNOp(ctx, id, n).Get()
}

func (r *RedisIDKeyCounter[K, V]) decrementOp(ctx context.Context, key string, n V) OperationResult[V] {
	var op OperationResult[V]
	var err error

	switch any(n).(type) {
	case int, int64, uint64:
		op = r.incrementIntegerOp(ctx, key, -int64(n))
	case float32, float64:
		op = r.incrementFloatOp(ctx, key, -float64(n))
	default:
		err = fmt.Errorf("unsupported variable type %T", n)
	}

	if err != nil {
		return NewRedisFailedOperationResult[V](fmt.Errorf("unable to create command: %w", err))
	}

	return op
}

func (r *RedisIDKeyCounter[K, V]) incrementOp(ctx context.Context, key string, n V) OperationResult[V] {
	var op OperationResult[V]
	var err error

	switch any(n).(type) {
	case int, int64, uint64:
		op = r.incrementIntegerOp(ctx, key, int64(n))
	case float32, float64:
		op = r.incrementFloatOp(ctx, key, float64(n))
	default:
		err = fmt.Errorf("unsupported variable type %T", n)
	}

	if err != nil {
		return NewRedisFailedOperationResult[V](fmt.Errorf("unable to create command: %w", err))
	}

	return op
}

func (r *RedisIDKeyCounter[K, V]) incrementIntegerOp(ctx context.Context, key string, n int64) OperationResult[V] {
	cmd := r.client.IncrBy(ctx, key, n)

	collectFunc := func() (V, error) {
		result, err := cmd.Result()
		if err != nil {
			return 0, fmt.Errorf("unable to increment: %w", err)
		}
		return V(result), nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeyCounter[K, V]) incrementFloatOp(ctx context.Context, key string, n float64) OperationResult[V] {
	cmd := r.client.IncrByFloat(ctx, key, n)

	collectFunc := func() (V, error) {
		result, err := cmd.Result()
		if err != nil {
			return 0, fmt.Errorf("unable to increment: %w", err)
		}
		return V(result), nil
	}

	return NewRedisOperationResult(collectFunc)
}
