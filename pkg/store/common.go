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
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

const (
	CKeyPartsDelimiter = ":"
	CWildcardSelector  = "*"
)

type ErrorCollector func() error

type ValueCollector[V any] func() (V, error)

type OperationStatus interface {
	Get() error
}

type OperationResult[T any] interface {
	Get() (T, error)
}

type RedisOperationStatus struct {
	collect ErrorCollector
}

func NewRedisOperationStatus(confirm ErrorCollector) *RedisOperationStatus {
	return &RedisOperationStatus{
		collect: confirm,
	}
}

func (r *RedisOperationStatus) Get() error {
	if err := r.collect(); err != nil {
		return fmt.Errorf("unable to collect result: %w", err)
	}
	return nil
}

type RedisFailedOperationStatus struct {
	err error
}

func NewRedisFailedOperationStatus(err error) *RedisFailedOperationStatus {
	return &RedisFailedOperationStatus{
		err: err,
	}
}

func (r *RedisFailedOperationStatus) Get() error {
	return fmt.Errorf("unable to create result: %w", r.err)
}

type RedisFailedOperationResult[T any] struct {
	err error
}

func NewRedisFailedOperationResult[T any](err error) *RedisFailedOperationResult[T] {
	return &RedisFailedOperationResult[T]{
		err: err,
	}
}

func (r *RedisFailedOperationResult[T]) Get() (T, error) {
	var noVal T
	return noVal, fmt.Errorf("unable to create command: %w", r.err)
}

type RedisOperationResult[T any] struct {
	collect ValueCollector[T]
}

func NewRedisOperationResult[T any](collect ValueCollector[T]) *RedisOperationResult[T] {
	return &RedisOperationResult[T]{
		collect: collect,
	}
}

func (r *RedisOperationResult[T]) Get() (T, error) {
	result, err := r.collect()
	if err != nil {
		var noVal T
		return noVal, fmt.Errorf("unable to collect result: %w", err)
	}

	return result, nil
}

type Pager struct {
	From  uint64
	Count uint64
}

type Page[T any] struct {
	Entries []T
	Next    uint64
}

func StringValueConverter(value string) (string, error) {
	return value, nil
}

func SingleTokenToStringConverter(tokens []string) (string, error) {
	return tokens[0], nil
}

func StringToSingleTokenConverter(id string) ([]string, error) {
	return []string{id}, nil
}

type SimpleCollectionConverter[F any, T any] struct {
	convert SingleValueConverter[F, T]
}

func NewSimpleCollectionConverter[F any, T any](convert SingleValueConverter[F, T]) *SimpleCollectionConverter[F, T] {
	return &SimpleCollectionConverter[F, T]{
		convert: convert,
	}
}

func (r *SimpleCollectionConverter[F, T]) ConvertSingle(value F) (T, error) {
	return r.convert(value)
}

func (r *SimpleCollectionConverter[F, T]) ConvertMulti(values []F) ([]T, error) {
	result := make([]T, 0, len(values))
	for _, value := range values {
		convertedValue, err := r.convert(value)
		if err != nil {
			return nil, fmt.Errorf("unable to convert value: %w", err)
		}
		result = append(result, convertedValue)
	}
	return result, nil
}

type CollectionConverter[F any, T any] interface {
	ConvertSingle(F) (T, error)
	ConvertMulti([]F) ([]T, error)
}

type SingleValueConverter[F any, T any] func(F) (T, error)

type MultiValueConverter[F any, T any] func([]F) ([]T, error)

type SingleToMultiValueConverter[F any, T any] func(F) ([]T, error)

type MultiToSingleValueConverter[F any, T any] func([]F) (T, error)

type RedisIDCommonStore[ID any] struct {
	tokenizeID SingleToMultiValueConverter[ID, string]
	restoreID  MultiToSingleValueConverter[string, ID]
	RedisCommonStore
}

func NewRedisIDCommonStore[ID any](client redis.Cmdable, keyPrefix string, tokenizeID SingleToMultiValueConverter[ID, string], restoreID MultiToSingleValueConverter[string, ID]) *RedisIDCommonStore[ID] {
	return &RedisIDCommonStore[ID]{
		tokenizeID:       tokenizeID,
		restoreID:        restoreID,
		RedisCommonStore: *NewRedisCommonStore(client, keyPrefix),
	}
}

func (r *RedisIDCommonStore[ID]) GetAllIDs(ctx context.Context, keyParts ...string) ([]ID, error) {
	pager := Pager{
		From:  0,
		Count: 100,
	}

	ids := []ID{}
	for {
		idPage, err := r.GetIDs(ctx, pager, keyParts...)
		if err != nil {
			return nil, err
		}
		ids = append(ids, idPage.Entries...)

		if idPage.Next == 0 {
			break
		}
		pager.From = idPage.Next
	}

	return ids, nil
}

func (r *RedisIDCommonStore[ID]) GetIDsOp(ctx context.Context, pager Pager, keyParts ...string) OperationResult[Page[ID]] {
	selector := r.MakeWildcardSelector(keyParts...)
	cmd := r.client.Scan(ctx, pager.From, selector, int64(pager.Count))

	collectFunc := func() (Page[ID], error) {
		keys, cursor, err := cmd.Result()
		if err != nil {
			return Page[ID]{}, fmt.Errorf("unable to scan keys: %w", err)
		}

		ids := make([]ID, 0, len(keys))
		for _, key := range keys {
			id, err := r.RestoreID(key)
			if err != nil {
				return Page[ID]{}, fmt.Errorf("unable to restore id: %w", err)
			}
			ids = append(ids, id)
		}

		return Page[ID]{
			Entries: ids,
			Next:    cursor,
		}, nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDCommonStore[ID]) GetIDs(ctx context.Context, pager Pager, keyParts ...string) (Page[ID], error) {
	return r.GetIDsOp(ctx, pager, keyParts...).Get()
}

func (r *RedisIDCommonStore[ID]) DropOp(ctx context.Context, id ID) OperationResult[uint64] {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[uint64](fmt.Errorf("unable to make key: %w", err))
	}

	cmd := r.client.Unlink(ctx, key)

	collectFunc := func() (uint64, error) {
		affected, err := cmd.Result()
		if err != nil {
			return 0, fmt.Errorf("unable to unlink key: %w", err)
		}
		return uint64(affected), nil
	}

	return NewRedisOperationResult(collectFunc)
}

func (r *RedisIDCommonStore[ID]) Drop(ctx context.Context, id ID) (uint64, error) {
	return r.DropOp(ctx, id).Get()
}

func (r *RedisIDCommonStore[ID]) MakeKey(id ID) (string, error) {
	idTokens, err := r.tokenizeID(id)
	if err != nil {
		return "", fmt.Errorf("unable to tokenize id: %w", err)
	}
	return r.RedisCommonStore.MakeKey(idTokens...), nil
}

func (r *RedisIDCommonStore[ID]) RestoreID(key string) (ID, error) {
	idTokens := r.SplitKey(key)
	id, err := r.restoreID(idTokens)
	if err != nil {
		var noVal ID
		return noVal, fmt.Errorf("unable to restore id: %w", err)
	}
	return id, nil
}

type RedisCommonStore struct {
	keyPrefix string
	client    redis.Cmdable
}

func NewRedisCommonStore(client redis.Cmdable, keyPrefix string) *RedisCommonStore {
	return &RedisCommonStore{
		client:    client,
		keyPrefix: keyPrefix,
	}
}

func (r *RedisCommonStore) JoinParts(keyParts ...string) string {
	return strings.Join(keyParts, CKeyPartsDelimiter)
}

func (r *RedisCommonStore) MakeKey(keyParts ...string) string {
	return r.JoinParts(append([]string{r.keyPrefix}, keyParts...)...)
}

func (r *RedisCommonStore) SplitKey(key string) []string {
	trimmedKey := strings.TrimPrefix(key, r.keyPrefix+CKeyPartsDelimiter)
	return strings.Split(trimmedKey, CKeyPartsDelimiter)
}

func (r *RedisCommonStore) MakeWildcardSelector(keyParts ...string) string {
	if len(keyParts) == 0 {
		return r.JoinParts(r.keyPrefix, CWildcardSelector)
	}
	parts := append([]string{r.keyPrefix}, keyParts...)
	parts = append(parts, CWildcardSelector)
	return r.JoinParts(parts...)
}

func (r *RedisCommonStore) GroupExecutor() *RedisExecutor {
	return NewRedisExecutor(r.client.Pipeline())
}

func (r *RedisCommonStore) TxExecutor() *RedisExecutor {
	return NewRedisExecutor(r.client.TxPipeline())
}

type Executor[T any] interface {
	Get() T
	Exec(ctx context.Context) error
}

type RedisExecutor struct {
	client redis.Pipeliner
}

func NewRedisExecutor(client redis.Pipeliner) *RedisExecutor {
	return &RedisExecutor{
		client: client,
	}
}

func (r *RedisExecutor) Get() redis.Pipeliner {
	return r.client
}

func (r *RedisExecutor) Exec(ctx context.Context) error {
	_, err := r.client.Exec(ctx)
	if err != nil {
		return fmt.Errorf("unable to execute command group: %w", err)
	}
	return nil
}
