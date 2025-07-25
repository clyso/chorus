package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/redis/go-redis/v9"
)

type ScoredSetEntry[V any, S any] struct {
	Value V
	Score S
}

type ScoredSetEntryToZConverter[V any, S any] struct {
	convertValue SingleValueConverter[V, string]
	convertScore SingleValueConverter[S, float64]
}

func NewScoredSetEntryToZConverter[V any, S any](
	convertValue SingleValueConverter[V, string], convertScore SingleValueConverter[S, float64]) *ScoredSetEntryToZConverter[V, S] {
	return &ScoredSetEntryToZConverter[V, S]{
		convertValue: convertValue,
		convertScore: convertScore,
	}
}

func (r *ScoredSetEntryToZConverter[V, S]) Convert(entry ScoredSetEntry[V, S]) (redis.Z, error) {
	convertedValue, err := r.convertValue(entry.Value)
	if err != nil {
		return redis.Z{}, fmt.Errorf("unable to convert value: %w", err)
	}
	convertedScore, err := r.convertScore(entry.Score)
	if err != nil {
		return redis.Z{}, fmt.Errorf("unable to convert score: %w", err)
	}
	return redis.Z{
		Score:  convertedScore,
		Member: convertedValue,
	}, nil
}

type ZToScoredSetEntryConverter[V any, S any] struct {
	convertValue SingleValueConverter[string, V]
	convertScore SingleValueConverter[float64, S]
}

func NewZToScoredSetEntryConverter[V any, S any](
	convertValue SingleValueConverter[string, V], convertScore SingleValueConverter[float64, S]) *ZToScoredSetEntryConverter[V, S] {
	return &ZToScoredSetEntryConverter[V, S]{
		convertValue: convertValue,
		convertScore: convertScore,
	}
}

func (r *ZToScoredSetEntryConverter[V, S]) Convert(entry redis.Z) (ScoredSetEntry[V, S], error) {
	stringZMember, ok := entry.Member.(string)
	if !ok {
		var noVal ScoredSetEntry[V, S]
		return noVal, fmt.Errorf("unable to cast %+v to string", entry.Member)
	}
	convertedValue, err := r.convertValue(stringZMember)
	if err != nil {
		var noVal ScoredSetEntry[V, S]
		return noVal, fmt.Errorf("unable to convert value: %w", err)
	}
	convertedScore, err := r.convertScore(entry.Score)
	if err != nil {
		var noVal ScoredSetEntry[V, S]
		return noVal, fmt.Errorf("unable to convert score: %w", err)
	}
	return ScoredSetEntry[V, S]{
		Score: convertedScore,
		Value: convertedValue,
	}, nil
}

type RedisIDKeySortedSet[ID any, V any, S any] struct {
	zSerializer   CollectionConverter[ScoredSetEntry[V, S], redis.Z]
	zDeserializer CollectionConverter[redis.Z, ScoredSetEntry[V, S]]
	vSerializer   CollectionConverter[V, string]
	RedisIDCommonStore[ID]
}

func NewRedisIDKeySortedSet[ID any, V any, S any](client redis.Cmdable, idPrefix string,
	tokenizeID SingleToMultiValueConverter[ID, string], restoreID MultiToSingleValueConverter[string, ID],
	serializeValue SingleValueConverter[V, string], deserializeValue SingleValueConverter[string, V],
	serializeScore SingleValueConverter[S, float64], deserializeScore SingleValueConverter[float64, S]) *RedisIDKeySortedSet[ID, V, S] {
	return &RedisIDKeySortedSet[ID, V, S]{
		zSerializer:        NewSimpleCollectionConverter(NewScoredSetEntryToZConverter(serializeValue, serializeScore).Convert),
		zDeserializer:      NewSimpleCollectionConverter(NewZToScoredSetEntryConverter(deserializeValue, deserializeScore).Convert),
		vSerializer:        NewSimpleCollectionConverter(serializeValue),
		RedisIDCommonStore: *NewRedisIDCommonStore[ID](client, idPrefix, tokenizeID, restoreID),
	}
}

func (r *RedisIDKeySortedSet[ID, V, S]) Add(ctx context.Context, id ID, values ...ScoredSetEntry[V, S]) (uint64, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		return 0, fmt.Errorf("unable to make key: %w", err)
	}
	scoredSetValues, err := r.zSerializer.ConvertMulti(values)
	if err != nil {
		return 0, fmt.Errorf("unable to serialiye value: %w", err)
	}
	affected, err := r.client.ZAdd(ctx, key, scoredSetValues...).Result()
	return uint64(affected), err
}

func (r *RedisIDKeySortedSet[ID, V, S]) AddIfNotExists(ctx context.Context, id ID, values ...ScoredSetEntry[V, S]) (uint64, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		return 0, fmt.Errorf("unable to make key: %w", err)
	}
	scoredSetValues, err := r.zSerializer.ConvertMulti(values)
	if err != nil {
		return 0, fmt.Errorf("unable to serialize values: %w", err)
	}
	affected, err := r.client.ZAddNX(ctx, key, scoredSetValues...).Result()
	return uint64(affected), err
}

func (r *RedisIDKeySortedSet[ID, V, S]) GetAll(ctx context.Context, id ID) ([]ScoredSetEntry[V, S], error) {
	key, err := r.MakeKey(id)
	if err != nil {
		return nil, fmt.Errorf("unable to make key: %w", err)
	}
	result, err := r.client.ZRangeWithScores(ctx, key, 0, -1).Result()
	if errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("%w: %w", dom.ErrNotFound, err)
	}
	if err != nil {
		return nil, err
	}
	entries, err := r.zDeserializer.ConvertMulti(result)
	if err != nil {
		return nil, fmt.Errorf("unable to deserialize values: %w", err)
	}
	return entries, nil
}

func (r *RedisIDKeySortedSet[ID, V, S]) Remove(ctx context.Context, id ID, values ...V) (uint64, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		return 0, fmt.Errorf("unable to make key: %w", err)
	}
	scoredSetValues, err := r.vSerializer.ConvertMulti(values)
	if err != nil {
		return 0, fmt.Errorf("unable to serialize values: %w", err)
	}
	affected, err := r.client.ZRem(ctx, key, scoredSetValues).Result()
	return uint64(affected), err
}
