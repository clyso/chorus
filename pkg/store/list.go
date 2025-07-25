package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/redis/go-redis/v9"
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

func (r *RedisIDKeyList[ID, V]) GetAll(ctx context.Context, id ID) ([]V, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		return nil, fmt.Errorf("unable to make key: %w", err)
	}
	cmdVal, err := r.client.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("unable to get element range: %w", err)
	}
	values, err := r.deserializer.ConvertMulti(cmdVal)
	if err != nil {
		return nil, fmt.Errorf("unable to deserialize result: %w", err)
	}
	return values, nil
}

func (r *RedisIDKeyList[ID, V]) GetPage(ctx context.Context, id ID, pager Pager) ([]V, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		return nil, fmt.Errorf("unable to make key: %w", err)
	}
	cmdVal, err := r.client.LRange(ctx, key, int64(pager.From), int64(pager.From + pager.Count - 1)).Result()
	if err != nil {
		return nil, fmt.Errorf("unable to locate select versions: %w", err)
	}
	values, err := r.deserializer.ConvertMulti(cmdVal)
	if err != nil {
		return nil, fmt.Errorf("unable to deserialize result: %w", err)
	}
	return values, nil
}

func (r *RedisIDKeyList[ID, V]) GetOne(ctx context.Context, id ID, idx uint64) (V, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		var noVal V
		return noVal, fmt.Errorf("unable to make key: %w", err)
	}
	cmdVal, err := r.client.LIndex(ctx, key, int64(idx)).Result()
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

func (r *RedisIDKeyList[ID, V]) AddLeft(ctx context.Context, id ID, values ...V) error {
	key, err := r.MakeKey(id)
	if err != nil {
		return fmt.Errorf("unable to make key: %w", err)
	}
	serializedValues, err := r.serializer.ConvertMulti(values)
	if err != nil {
		return fmt.Errorf("unable to serialize values: %w", err)
	}
	if err := r.client.LPush(ctx, key, serializedValues).Err(); err != nil {
		return fmt.Errorf("unable to push values: %w", err)
	}
	return nil
}

func (r *RedisIDKeyList[ID, V]) AddRight(ctx context.Context, id ID, values ...V) error {
	key, err := r.MakeKey(id)
	if err != nil {
		return fmt.Errorf("unable to make key: %w", err)
	}
	serializedValues, err := r.serializer.ConvertMulti(values)
	if err != nil {
		return fmt.Errorf("unable to serialize values: %w", err)
	}
	if err := r.client.RPush(ctx, key, serializedValues).Err(); err != nil {
		return fmt.Errorf("unable to push values: %w", err)
	}
	return nil
}

func (r *RedisIDKeyList[ID, V]) Find(ctx context.Context, id ID, value V) (int64, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		return 0, fmt.Errorf("unable to make key: %w", err)
	}
	serializedValue, err := r.serializer.ConvertSingle(value)
	if err != nil {
		return 0, fmt.Errorf("unable to serialize value: %w", err)
	}
	idx, err := r.client.LPos(ctx, key, serializedValue, redis.LPosArgs{}).Result()
	if errors.Is(err, redis.Nil) {
		return 0, fmt.Errorf("%w: %w", dom.ErrNotFound, err)
	}
	if err != nil {
		return 0, fmt.Errorf("unable to locate value: %w", err)
	}
	return idx, nil
}
