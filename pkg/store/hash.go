package store

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/redis/go-redis/v9"
)

var (
	luaHSetEx    = redis.NewScript(`if redis.call('exists',KEYS[1]) == 1 then redis.call("hset", KEYS[1], ARGV[1], ARGV[2]); return 1 else return 0 end`)
	luaHIncrByEx = redis.NewScript(`if redis.call('exists',KEYS[1]) == 1 then return redis.call("hincrby", KEYS[1], ARGV[1], ARGV[2]) else return 0 end`)
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

func (r *RedisIDKeyHash[ID, V]) Set(ctx context.Context, id ID, value V) error {
	key, err := r.MakeKey(id)
	if err != nil {
		return fmt.Errorf("unable to make key: %w", err)
	}
	if err := r.client.HSet(ctx, key, value).Err(); err != nil {
		return fmt.Errorf("unable to set value in hash: %w", err)
	}
	return nil
}

func (r *RedisIDKeyHash[ID, V]) Get(ctx context.Context, id ID) (V, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		var noVal V
		return noVal, fmt.Errorf("unable to make key: %w", err)
	}
	cmd := r.client.HGetAll(ctx, key)
	if err := cmd.Err(); err != nil {
		var noVal V
		return noVal, err
	}
	var value V
	if err := cmd.Scan(&value); err != nil {
		var noVal V
		return noVal, err
	}
	return value, nil
}

func (r *RedisIDKeyHash[ID, V]) SetFieldIfExists(ctx context.Context, id ID, fieldName string, value any) error {
	key, err := r.MakeKey(id)
	if err != nil {
		return fmt.Errorf("unable to make key: %w", err)
	}
	result, err := luaHSetEx.Run(ctx, r.client, []string{key}, fieldName, value).Result()
	if err != nil {
		return err
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

func (r *RedisIDKeyHash[ID, V]) SetField(ctx context.Context, id ID, fieldName string, value any) (uint64, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		return 0, fmt.Errorf("unable to make key: %w", err)
	}
	affected, err := r.client.HSet(ctx, key, fieldName, value).Result()
	if err != nil {
		return 0, err
	}
	return uint64(affected), nil
}

func (r *RedisIDKeyHash[ID, V]) IncrementField(ctx context.Context, id ID, fieldName string) (int64, error) {
	return r.IncrementFieldByN(ctx, id, fieldName, 1)
}

func (r *RedisIDKeyHash[ID, V]) IncrementFieldByN(ctx context.Context, id ID, fieldName string, value int64) (int64, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		return 0, fmt.Errorf("unable to make key: %w", err)
	}
	count, err := r.client.HIncrBy(ctx, key, fieldName, value).Result()
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (r *RedisIDKeyHash[ID, V]) IncrementFieldIfExists(ctx context.Context, id ID, fieldName string) (int64, error) {
	return r.IncrementFieldByNIfExists(ctx, id, fieldName, 1)
}

func (r *RedisIDKeyHash[ID, V]) IncrementFieldByNIfExists(ctx context.Context, id ID, fieldName string, value int64) (int64, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		return 0, fmt.Errorf("unable to make key: %w", err)
	}
	result, err := luaHIncrByEx.Run(ctx, r.client, []string{key}, fieldName, value).Result()
	if err != nil {
		return 0, err
	}
	count, ok := result.(int64)
	if !ok {
		return 0, fmt.Errorf("%w: unable to cast luaHIncrByEx result %T to int64", dom.ErrInternal, result)
	}
	if count == 0 {
		return 0, dom.ErrNotFound
	}
	return count, nil
}

func (r *RedisIDKeyHash[ID, V]) MakeFieldMap(value any) map[string]any {
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
