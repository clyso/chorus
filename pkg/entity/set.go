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

func NewRedisIDKeySet[ID any, V Numeric](client redis.Cmdable, idPrefix string,
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
	err = cmd.Err()
	if errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("%w: %w", dom.ErrNotFound, err)
	}
	val, err := r.extractValue(cmd.Val())
	if err != nil {
		return nil, fmt.Errorf("unable to get counter %s: %w", id, err)
	}
	return val, nil
}

func (r *RedisIDKeySet[ID, V]) Add(ctx context.Context, id ID, val ...V) error {
	key, err := r.MakeKey(id)
	if err != nil {
		return fmt.Errorf("unable to make key: %w", err)
	}
	affected, err := r.client.SAdd(ctx, key, val).Result()
	if err != nil {
		return fmt.Errorf("unable to add values to set: %w", id, err)
	}
	return nil
}

func (r *RedisIDKeySet[ID, V]) extractValue(values []string) ([]V, error) {

	return nil, nil
}
