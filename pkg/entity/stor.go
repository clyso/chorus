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

package entity

// import (
// 	"context"
// 	"errors"
// 	"fmt"

// 	"github.com/clyso/chorus/pkg/dom"
// 	"github.com/redis/go-redis/v9"
// )

// const (
// 	CKeyPartsDelimiter = ":"
// )

// type CompositeID interface {
// 	fmt.Stringer
// }

// type PrimitiveID interface {
// 	string
// }

// type Numeric interface {
// 	int | int64 | uint64 | float32 | float64
// }

// type Set[K any, V any] interface {
// 	Get(ctx context.Context, key K) ([]V, error)
// 	Set(ctx context.Context, key K, val []V) error
// 	Add(ctx context.Context, key K, val V) error
// 	Remove(ctx context.Context, key K, val V) error
// }

// type Counter[K any, V Numeric] interface {
// 	GroupExecutorProvider() ExecutorProvider[any]
// 	WithExecutorProvider(provider ExecutorProvider[any]) Counter[K, V]
// 	Get(ctx context.Context, key K) (V, error)
// 	Set(ctx context.Context, key K, val V) error
// 	Increment(ctx context.Context, key K) (V, error)
// 	IncrementByN(ctx context.Context, key K, n V) (V, error)
// 	Decrement(ctx context.Context, key K) (V, error)
// 	DecrementByN(ctx context.Context, key K, n V) (V, error)
// }

// type RedisCompositeKeySet[K CompositeID, V any] struct {
// 	RedisCompositeKeyValueStore[K]
// }

// func NewRedisCompositeKeySet[K CompositeID, V Numeric](client redis.Cmdable, idPrefix string) *RedisCompositeKeyCounter[K, V] {
// 	return &RedisCompositeKeyCounter[K, V]{
// 		*NewRedisCompositeKeyValueStore[K](client, idPrefix),
// 	}
// }

// func (r *RedisCompositeKeySet[K, V]) Get(ctx context.Context, key K) ([]V, error) {
// 	id := r.MakeKey(key)
// 	cmd := r.client.SMembers(ctx, id)
// 	err := cmd.Err()
// 	if errors.Is(err, redis.Nil) {
// 		return nil, fmt.Errorf("%w: %w", dom.ErrNotFound, err)
// 	}
// 	val, err := r.extractValue(cmd)
// 	if err != nil {
// 		return nil, fmt.Errorf("unable to get counter %s: %w", id, err)
// 	}
// 	return val, nil
// }

// func (r *RedisCompositeKeySet[K, V]) Set(ctx context.Context, key K, val V) error {
// 	id := r.MakeKey(key)
// 	if err := r.client.Set(ctx, id, val, 0); err != nil {
// 		return fmt.Errorf("unable to set counter %s value: %w", id, err)
// 	}
// 	return nil
// }

// func (r *RedisCompositeKeySet[K, V]) extractValue(cmd *redis.StringSliceCmd) ([]V, error) {
// 	return nil, nil
// }

// type RedisKeyValueStore struct {
// 	idPrefix string
// 	client   redis.Cmdable
// }

// func NewRedisKeyValueStore(client redis.Cmdable, idPrefix string) *RedisKeyValueStore {
// 	return &RedisKeyValueStore{
// 		client:   client,
// 		idPrefix: idPrefix,
// 	}
// }

// func (r *RedisKeyValueStore) MakeID(key string) string {
// 	return r.idPrefix + CKeyPartsDelimiter + key
// }

// func (r *RedisKeyValueStore) GroupExecutorProvider() *RedisExecutorProvider {
// 	return &RedisExecutorProvider{
// 		client: r.client.Pipeline(),
// 	}
// }

// type RedisCompositeKeyValueStore[K CompositeID] struct {
// 	RedisKeyValueStore
// }

// func NewRedisCompositeKeyValueStore[K CompositeID](client redis.Cmdable, idPrefix string) *RedisCompositeKeyValueStore[K] {
// 	return &RedisCompositeKeyValueStore[K]{
// 		*NewRedisKeyValueStore(client, idPrefix),
// 	}
// }

// func (r *RedisCompositeKeyValueStore[K]) MakeID(key K) string {
// 	return r.RedisKeyValueStore.MakeID(key.String())
// }

// type Executor[T any] interface {
// 	Get() T
// 	Exec() error
// }

// type RedisExecutorProvider struct {
// 	client redis.Pipeliner
// }

// func NewRedisExecutorProvider(client redis.Pipeliner) *RedisExecutorProvider {
// 	return &RedisExecutorProvider{
// 		client: client,
// 	}
// }

// func (r *RedisExecutorProvider) GetExecutor() redis.Cmdable {
// 	return r.client
// }
