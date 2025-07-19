package entity

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
	Exec() error
}

type RedisExecutor struct {
	client redis.Pipeliner
}

func NewRedisExecutor(client redis.Pipeliner) *RedisExecutor {
	return &RedisExecutor{
		client: client,
	}
}

func (r *RedisExecutor) Get() redis.Cmdable {
	return r.client
}

func (r *RedisExecutor) Exec(ctx context.Context) error {
	_, err := r.client.Exec(ctx)
	if err != nil {
		return fmt.Errorf("unable to execute command group: %w", err)
	}
	return nil
}
