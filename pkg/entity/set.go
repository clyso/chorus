package entity

import (
	"context"
	"errors"
	"fmt"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/redis/go-redis/v9"
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

func (r *RedisIDKeySet[ID, V]) Get(ctx context.Context, id ID) ([]V, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		return nil, fmt.Errorf("unable to make key: %w", err)
	}
	cmd := r.client.SMembers(ctx, key)
	if err := cmd.Err(); errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("%w: %w", dom.ErrNotFound, err)
	}
	values, err := r.deserializer.ConvertMulti(cmd.Val())
	if err != nil {
		return nil, fmt.Errorf("unable to get counter %s: %w", id, err)
	}
	return values, nil
}

func (r *RedisIDKeySet[ID, V]) Add(ctx context.Context, id ID, values ...V) (uint64, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		return 0, fmt.Errorf("unable to make key: %w", err)
	}
	convertedValues, err := r.serializer.ConvertMulti(values)
	if err != nil {
		return 0, fmt.Errorf("unable to convert values: %w", err)
	}
	affected, err := r.client.SAdd(ctx, key, convertedValues).Result()
	if err != nil {
		return 0, fmt.Errorf("unable to add values to set: %w", id, err)
	}
	return uint64(affected), nil
}

func (r *RedisIDKeySet[ID, V]) Remove(ctx context.Context, id ID, values ...V) (uint64, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		return 0, fmt.Errorf("unable to make key: %w", err)
	}
	convertedValues, err := r.serializer.ConvertMulti(values)
	if err != nil {
		return 0, fmt.Errorf("unable to convert values: %w", err)
	}
	affected, err := r.client.SRem(ctx, key, convertedValues).Result()
	if err != nil {
		return 0, err
	}
	return uint64(affected), nil
}

func (r *RedisIDKeySet[ID, V]) IsMember(ctx context.Context, id ID, value V) (bool, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		return false, fmt.Errorf("unable to make key: %w", err)
	}
	convertedValue, err := r.serializer.ConvertSingle(value)
	if err != nil {
		return false, fmt.Errorf("unable to convert values: %w", err)
	}
	exists, err := r.client.SIsMember(ctx, key, convertedValue).Result()
	if err != nil {
		return false, err
	}
	return exists, nil
}
