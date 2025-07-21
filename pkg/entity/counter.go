package entity

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

func (r *RedisIDKeyCounter[ID, V]) Increment(ctx context.Context, id ID) (V, error) {
	return r.IncrementByN(ctx, id, 1)
}

func (r *RedisIDKeyCounter[ID, V]) IncrementByN(ctx context.Context, id ID, n V) (V, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		var noVal V
		return noVal, fmt.Errorf("unable to make key: %w", err)
	}
	val, err := r.increment(ctx, key, n)
	if err != nil {
		var noVal V
		return noVal, fmt.Errorf("unable to increment counter: %w", err)
	}
	return val, nil
}

func (r *RedisIDKeyCounter[ID, V]) Decrement(ctx context.Context, id ID) (V, error) {
	return r.DecrementByN(ctx, id, 1)
}

func (r *RedisIDKeyCounter[ID, V]) DecrementByN(ctx context.Context, id ID, n V) (V, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		var noVal V
		return noVal, fmt.Errorf("unable to make key: %w", err)
	}
	val, err := r.decrement(ctx, key, n)
	if err != nil {
		var noVal V
		return noVal, fmt.Errorf("unable to decrement counter: %w", err)
	}
	return val, nil
}

func (r *RedisIDKeyCounter[K, V]) increment(ctx context.Context, key string, n V) (V, error) {
	var value V
	var genericValue any
	var err error

	switch typedN := any(n).(type) {
	case int:
		var result int64
		result, err = r.client.IncrBy(ctx, key, int64(typedN)).Result()
		genericValue = int(result)
	case int64:
		genericValue, err = r.client.IncrBy(ctx, key, typedN).Result()
	case uint64:
		genericValue, err = r.client.IncrBy(ctx, key, int64(typedN)).Uint64()
	case float32:
		var result float64
		result, err = r.client.IncrByFloat(ctx, key, float64(typedN)).Result()
		genericValue = float32(result)
	case float64:
		genericValue, err = r.client.IncrByFloat(ctx, key, typedN).Result()
	default:
		err = fmt.Errorf("unsupported variable type %T", value)
	}

	if err != nil {
		return value, fmt.Errorf("unable to change value: %w", err)
	}

	value = genericValue.(V)

	return value, nil
}

func (r *RedisIDKeyCounter[K, V]) decrement(ctx context.Context, key string, n V) (V, error) {
	var value V
	var genericValue any
	var err error

	switch typedN := any(n).(type) {
	case int:
		var result int64
		result, err = r.client.DecrBy(ctx, key, int64(typedN)).Result()
		genericValue = int(result)
	case int64:
		genericValue, err = r.client.DecrBy(ctx, key, typedN).Result()
	case uint64:
		genericValue, err = r.client.DecrBy(ctx, key, int64(typedN)).Uint64()
	case float32:
		var result float64
		result, err = r.client.IncrByFloat(ctx, key, float64(-typedN)).Result()
		genericValue = float32(result)
	case float64:
		genericValue, err = r.client.IncrByFloat(ctx, key, -typedN).Result()
	default:
		err = fmt.Errorf("unsupported variable type %T", value)
	}

	if err != nil {
		return value, fmt.Errorf("unable to change value: %w", err)
	}

	value = genericValue.(V)

	return value, nil
}
