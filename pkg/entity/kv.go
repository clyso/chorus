package entity

import (
	"context"
	"errors"
	"fmt"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/redis/go-redis/v9"
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

func (r *RedisIDKeyValue[ID, V]) Get(ctx context.Context, id ID) (V, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		var noVal V
		return noVal, fmt.Errorf("unable to make key: %w", err)
	}
	cmd := r.client.Get(ctx, key)
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

func (r *RedisIDKeyValue[ID, V]) Set(ctx context.Context, id ID, value V) error {
	key, err := r.MakeKey(id)
	if err != nil {
		return fmt.Errorf("unable to make key: %w", err)
	}
	serializedValue, err := r.serializeValue(value)
	if err != nil {
		return fmt.Errorf("unable to serialize value: %w", err)
	}
	if err := r.client.Set(ctx, key, serializedValue, 0).Err(); err != nil {
		return err
	}
	return nil
}

func (r *RedisIDKeyValue[ID, V]) SetIfNotExists(ctx context.Context, id ID, value V) error {
	key, err := r.MakeKey(id)
	if err != nil {
		return fmt.Errorf("unable to make key: %w", err)
	}
	serializedValue, err := r.serializeValue(value)
	if err != nil {
		return fmt.Errorf("unable to serialize value: %w", err)
	}
	set, err := r.client.SetNX(ctx, key, serializedValue, 0).Result()
	if err != nil {
		return err
	}
	if !set {
		return dom.ErrAlreadyExists
	}
	return nil
}
