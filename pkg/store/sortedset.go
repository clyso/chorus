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

type ScoredSetEntry[V any, S any] struct {
	Value V
	Score S
}

func NewScoredSetEntry[V any, S any](value V, score S) *ScoredSetEntry[V, S] {
	return &ScoredSetEntry[V, S]{
		Value: value,
		Score: score,
	}
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

func (r *RedisIDKeySortedSet[ID, V, S]) IsMemberOp(ctx context.Context, id ID, value V) OperationResult[bool] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[bool](fmt.Errorf("unable to make key: %w", err))
	}
	serializedValue, err := r.vSerializer.ConvertSingle(value)
	if err != nil {
		return NewRedisFailedOperationResult[bool](fmt.Errorf("unable to serialize value: %w", err))
	}

	cmd := r.client.ZRank(ctx, key, serializedValue)

	collectFunc := func() (bool, error) {
		err = cmd.Err()
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("unable to rank value: %w", err)
		}
		return true, nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeySortedSet[ID, V, S]) IsMember(ctx context.Context, id ID, value V) (bool, error) {
	return r.IsMemberOp(ctx, id, value).Get()
}

func (r *RedisIDKeySortedSet[ID, V, S]) AddOp(ctx context.Context, id ID, values ...ScoredSetEntry[V, S]) OperationResult[uint64] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[uint64](fmt.Errorf("unable to make key: %w", err))
	}
	scoredSetValues, err := r.zSerializer.ConvertMulti(values)
	if err != nil {
		return NewRedisFailedOperationResult[uint64](fmt.Errorf("unable to serialize values: %w", err))
	}

	cmd := r.client.ZAdd(ctx, key, scoredSetValues...)

	collectFunc := func() (uint64, error) {
		affected, err := cmd.Result()
		if err != nil {
			return 0, fmt.Errorf("unable to add value: %w", err)
		}
		return uint64(affected), err
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeySortedSet[ID, V, S]) Add(ctx context.Context, id ID, values ...ScoredSetEntry[V, S]) (uint64, error) {
	return r.AddOp(ctx, id, values...).Get()
}

func (r *RedisIDKeySortedSet[ID, V, S]) AddIfNotExistsOp(ctx context.Context, id ID, values ...ScoredSetEntry[V, S]) OperationResult[uint64] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[uint64](fmt.Errorf("unable to make key: %w", err))
	}
	scoredSetValues, err := r.zSerializer.ConvertMulti(values)
	if err != nil {
		return NewRedisFailedOperationResult[uint64](fmt.Errorf("unable to serialize values: %w", err))
	}

	cmd := r.client.ZAddNX(ctx, key, scoredSetValues...)

	collectFunc := func() (uint64, error) {
		affected, err := cmd.Result()
		if err != nil {
			return 0, fmt.Errorf("unable to add value: %w", err)
		}
		return uint64(affected), err
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeySortedSet[ID, V, S]) AddIfNotExists(ctx context.Context, id ID, values ...ScoredSetEntry[V, S]) (uint64, error) {
	return r.AddIfNotExistsOp(ctx, id, values...).Get()
}

func (r *RedisIDKeySortedSet[ID, V, S]) GetAllOp(ctx context.Context, id ID) OperationResult[[]ScoredSetEntry[V, S]] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[[]ScoredSetEntry[V, S]](fmt.Errorf("unable to make key: %w", err))
	}

	cmd := r.client.ZRangeWithScores(ctx, key, 0, -1)

	collectFunc := func() ([]ScoredSetEntry[V, S], error) {
		result, err := cmd.Result()
		if errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("%w: %w", dom.ErrNotFound, err)
		}
		if err != nil {
			return nil, fmt.Errorf("unable to get range: %w", err)
		}
		entries, err := r.zDeserializer.ConvertMulti(result)
		if err != nil {
			return nil, fmt.Errorf("unable to deserialize values: %w", err)
		}
		return entries, nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeySortedSet[ID, V, S]) GetAll(ctx context.Context, id ID) ([]ScoredSetEntry[V, S], error) {
	return r.GetAllOp(ctx, id).Get()
}

func (r *RedisIDKeySortedSet[ID, V, S]) SizeOp(ctx context.Context, id ID) OperationResult[uint64] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[uint64](fmt.Errorf("unable to make key: %w", err))
	}

	cmd := r.client.ZCard(ctx, key)

	collectFunc := func() (uint64, error) {
		size, err := cmd.Result()
		if err != nil {
			return 0, fmt.Errorf("unable to get cardinality: %w", err)
		}
		return uint64(size), err
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeySortedSet[ID, V, S]) Size(ctx context.Context, id ID) (uint64, error) {
	return r.SizeOp(ctx, id).Get()
}

func (r *RedisIDKeySortedSet[ID, V, S]) RemoveOp(ctx context.Context, id ID, values ...V) OperationResult[uint64] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[uint64](fmt.Errorf("unable to make key: %w", err))
	}
	scoredSetValues, err := r.vSerializer.ConvertMulti(values)
	if err != nil {
		return NewRedisFailedOperationResult[uint64](fmt.Errorf("unable to serialize values: %w", err))
	}

	cmd := r.client.ZRem(ctx, key, scoredSetValues)

	collectFunc := func() (uint64, error) {
		affected, err := cmd.Result()
		if err != nil {
			return 0, fmt.Errorf("unable to remove value: %w", err)
		}
		return uint64(affected), err
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDKeySortedSet[ID, V, S]) Remove(ctx context.Context, id ID, values ...V) (uint64, error) {
	return r.RemoveOp(ctx, id, values...).Get()
}
