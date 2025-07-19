package entity

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type ScoredCollectionConverter[F any, T any] struct {
	convertValue SingleValueConverter[F, T]
	convertScore SingleValueConverter[F, T]
}

type ScoredSetEntry[T any] struct {
	Value T
	Score uint8
}

type RedisIDKeySortedSet[ID any, V any] struct {
	serializer   CollectionConverter[ScoredSetEntry[V], redis.Z]
	deserializer CollectionConverter[redis.Z, ScoredSetEntry[V]]
	RedisIDCommonStore[ID]
}

func (r *RedisIDKeySortedSet[ID, V]) Add(ctx context.Context, id ID, values ...ScoredSetEntry[V]) (uint64, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		return 0, fmt.Errorf("unable to make key: %w", err)
	}
	scoredSetValues := make([]redis.Z, 0, len(values))
	for _, value := range values {
		serializedValue, err := json.Marshal(value.Value)
		if err != nil {
			return 0, err
		}
		scoredSetValue := redis.Z{Member: serializedValue, Score: float64(value.Score)}
		scoredSetValues = append(scoredSetValues, scoredSetValue)
	}

	affected, err := r.client.ZAddNX(ctx, key, scoredSetValues...).Result()
	return uint64(affected), err
}
