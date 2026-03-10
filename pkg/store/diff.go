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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/entity"
)

func TokensToEmptyStructConverter(tokens []string) (struct{}, error) {
	return struct{}{}, nil
}

func EmptyStructToTokensConverter(id struct{}) ([]string, error) {
	return []string{}, nil
}

func DiffIDToStringConverter(value entity.DiffID) (string, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("unable to serialize diff id: %w", err)
	}
	return string(bytes), nil
}

func StringToDiffIDConverter(value string) (entity.DiffID, error) {
	var result entity.DiffID
	if err := json.Unmarshal([]byte(value), &result); err != nil {
		var noVal entity.DiffID
		return noVal, fmt.Errorf("unable to deserialize bucket replication policy: %w", err)
	}
	return result, nil
}

type DiffIDStore struct {
	RedisIDKeySet[struct{}, entity.DiffID]
}

func NewDiffIDStore(client redis.Cmdable) *DiffIDStore {
	return &DiffIDStore{
		*NewRedisIDKeySet(client, "cc:id",
			EmptyStructToTokensConverter, TokensToEmptyStructConverter,
			DiffIDToStringConverter, StringToDiffIDConverter),
	}
}

func (r *DiffIDStore) WithExecutor(exec Executor[redis.Pipeliner]) *DiffIDStore {
	return NewDiffIDStore(exec.Get())
}

func TokensToDiffIDConverter(tokens []string) (entity.DiffID, error) {
	var result entity.DiffID
	decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(tokens[0]))
	if err := json.NewDecoder(decoder).Decode(&result); err != nil {
		return entity.DiffID{}, fmt.Errorf("unable to decode diff id: %w", err)
	}
	return result, nil
}

func DiffIDToTokensConverter(id entity.DiffID) ([]string, error) {
	var buf bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &buf)
	if err := json.NewEncoder(encoder).Encode(&id); err != nil {
		return nil, fmt.Errorf("unable to encode diff id: %w", err)
	}
	if err := encoder.Close(); err != nil {
		return nil, fmt.Errorf("unable to finalize encoding of diff id: %w", err)
	}
	return []string{buf.String()}, nil
}

func DiffSettingsToStringConverter(value entity.DiffSettings) (string, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("unable to serialize diff settings: %w", err)
	}
	return string(bytes), nil
}

func StringToDiffSettingsConverter(value string) (entity.DiffSettings, error) {
	var result entity.DiffSettings
	if err := json.Unmarshal([]byte(value), &result); err != nil {
		var noVal entity.DiffSettings
		return noVal, fmt.Errorf("unable to deserialize diff set entry: %w", err)
	}
	return result, nil
}

type DiffSettingsStore struct {
	RedisIDKeyValue[entity.DiffID, entity.DiffSettings]
}

func NewDiffSettingsStore(client redis.Cmdable) *DiffSettingsStore {
	return &DiffSettingsStore{
		*NewRedisIDKeyValue(client, "cc:settings",
			DiffIDToTokensConverter, TokensToDiffIDConverter,
			DiffSettingsToStringConverter, StringToDiffSettingsConverter),
	}
}

func (r *DiffSettingsStore) WithExecutor(exec Executor[redis.Pipeliner]) *DiffSettingsStore {
	return NewDiffSettingsStore(exec.Get())
}

func TokensToDiffObjectIDConverter(tokens []string) (entity.DiffObjectID, error) {
	diffID, err := TokensToDiffIDConverter([]string{tokens[0]})
	if err != nil {
		return entity.DiffObjectID{}, fmt.Errorf("unable to parse diff id: %w", err)
	}
	return entity.DiffObjectID{
		DiffID:  diffID,
		Storage: tokens[1],
		Prefix:  tokens[2],
	}, nil
}

func DiffObjectIDToTokensConverter(id entity.DiffObjectID) ([]string, error) {
	tokens, err := DiffIDToTokensConverter(id.DiffID)
	if err != nil {
		return nil, fmt.Errorf("unable to get diff id tokens: %w", err)
	}
	return []string{tokens[0], id.Storage, id.Prefix}, nil
}

type DiffListStateStore struct {
	RedisIDKeyValue[entity.DiffObjectID, string]
}

func NewDiffListStateStore(client redis.Cmdable) *DiffListStateStore {
	return &DiffListStateStore{
		*NewRedisIDKeyValue(client, "cc:listed",
			DiffObjectIDToTokensConverter, TokensToDiffObjectIDConverter,
			StringValueConverter, StringValueConverter),
	}
}

func (r *DiffListStateStore) WithExecutor(exec Executor[redis.Pipeliner]) *DiffListStateStore {
	return NewDiffListStateStore(exec.Get())
}

var (
	luaAddToDiffSet = redis.NewScript(`redis.call("SADD", KEYS[1], ARGV[1])
local count = redis.call("SCARD", KEYS[1])
if count == tonumber(ARGV[2]) then
	redis.call("UNLINK", KEYS[1])
end
return 0`)
)

func TokensToDiffSetIDConverter(tokens []string) (entity.DiffSetID, error) {
	diffID, err := TokensToDiffIDConverter([]string{tokens[0]})
	if err != nil {
		return entity.DiffSetID{}, fmt.Errorf("unable to parse diff id: %w", err)
	}
	versionIdx, err := strconv.ParseUint(tokens[2], 10, 64)
	if err != nil {
		return entity.DiffSetID{}, fmt.Errorf("unable to parse version index: %w", err)
	}
	size, err := strconv.ParseUint(tokens[3], 10, 64)
	if err != nil {
		return entity.DiffSetID{}, fmt.Errorf("unable to parse size: %w", err)
	}
	return entity.DiffSetID{
		DiffID:       diffID,
		Object:       tokens[1],
		VersionIndex: versionIdx,
		Size:         size,
		Etag:         tokens[4],
	}, nil
}

func DiffSetIDToTokensConverter(id entity.DiffSetID) ([]string, error) {
	tokens, err := DiffIDToTokensConverter(id.DiffID)
	if err != nil {
		return nil, fmt.Errorf("unable to get diff id tokens: %w", err)
	}
	return []string{tokens[0], id.Object, strconv.FormatUint(id.VersionIndex, 10), strconv.FormatUint(id.Size, 10), id.Etag}, nil
}

func DiffSetEntryToStringConverter(value entity.DiffSetEntry) (string, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("unable to serialize diff set entry: %w", err)
	}
	return string(bytes), nil
}

func StringToDiffSetEntryConverter(value string) (entity.DiffSetEntry, error) {
	var result entity.DiffSetEntry
	if err := json.Unmarshal([]byte(value), &result); err != nil {
		var noVal entity.DiffSetEntry
		return noVal, fmt.Errorf("unable to deserialize diff set entry: %w", err)
	}
	return result, nil
}

type DiffSetStore struct {
	RedisIDKeySet[entity.DiffSetID, entity.DiffSetEntry]
}

func NewDiffSetStore(client redis.Cmdable) *DiffSetStore {
	return &DiffSetStore{
		*NewRedisIDKeySet(client, "cc:set",
			DiffSetIDToTokensConverter, TokensToDiffSetIDConverter,
			DiffSetEntryToStringConverter, StringToDiffSetEntryConverter),
	}
}

func (r *DiffSetStore) WithExecutor(exec Executor[redis.Pipeliner]) *DiffSetStore {
	return NewDiffSetStore(exec.Get())
}

func (r *DiffSetStore) AddOp(ctx context.Context, id entity.DiffSetID, entry entity.DiffSetEntry, storageCount uint8) OperationStatus {
	key, err := r.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to make key: %w", err))
	}

	serializedEntry, err := r.serializer.ConvertSingle(entry)
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to serialize entry: %w", err))
	}

	cmd := luaAddToDiffSet.Eval(ctx, r.client, []string{key}, serializedEntry, storageCount)

	collectFunc := func() error {
		if err := cmd.Err(); err != nil {
			return fmt.Errorf("unable to execute script: %w", err)
		}
		return nil
	}

	return NewRedisOperationStatus(collectFunc)
}

func (r *DiffSetStore) Add(ctx context.Context, id entity.DiffSetID, entry entity.DiffSetEntry, storageCount uint8) error {
	return r.AddOp(ctx, id, entry, storageCount).Get()
}

func TokensToDiffFixIDConverter(tokens []string) (entity.DiffFixID, error) {
	diffID, err := TokensToDiffIDConverter([]string{tokens[0]})
	if err != nil {
		return entity.DiffFixID{}, fmt.Errorf("unable to parse diff id: %w", err)
	}
	return entity.DiffFixID{
		DiffID:      diffID,
		Destination: tokens[1],
	}, nil
}

func DiffFixIDToTokensConverter(id entity.DiffFixID) ([]string, error) {
	tokens, err := DiffIDToTokensConverter(id.DiffID)
	if err != nil {
		return nil, fmt.Errorf("unable to get diff id tokens: %w", err)
	}
	return []string{tokens[0], id.Destination}, nil
}

func DiffFixCopyObjectToStringConverter(value entity.DiffFixCopyObject) (string, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("unable to serialize diff set entry: %w", err)
	}
	return string(bytes), nil
}

func StringDiffFixCopyObjectConverter(value string) (entity.DiffFixCopyObject, error) {
	var result entity.DiffFixCopyObject
	if err := json.Unmarshal([]byte(value), &result); err != nil {
		var noVal entity.DiffFixCopyObject
		return noVal, fmt.Errorf("unable to deserialize diff set entry: %w", err)
	}
	return result, nil
}

type DiffFixCopySetStore struct {
	RedisIDKeySet[entity.DiffFixID, entity.DiffFixCopyObject]
}

func NewDiffFixCopySetStore(client redis.Cmdable) *DiffFixCopySetStore {
	return &DiffFixCopySetStore{
		*NewRedisIDKeySet(client, "cc:fixcopyset",
			DiffFixIDToTokensConverter, TokensToDiffFixIDConverter,
			DiffFixCopyObjectToStringConverter, StringDiffFixCopyObjectConverter),
	}
}

func (r *DiffFixCopySetStore) WithExecutor(exec Executor[redis.Pipeliner]) *DiffFixCopySetStore {
	return NewDiffFixCopySetStore(exec.Get())
}

type DiffFixRemoveSetStore struct {
	RedisIDKeySet[entity.DiffFixID, string]
}

func NewDiffFixRemoveSetStore(client redis.Cmdable) *DiffFixRemoveSetStore {
	return &DiffFixRemoveSetStore{
		*NewRedisIDKeySet(client, "cc:fixremoveset",
			DiffFixIDToTokensConverter, TokensToDiffFixIDConverter,
			StringValueConverter, StringValueConverter),
	}
}

func (r *DiffFixRemoveSetStore) WithExecutor(exec Executor[redis.Pipeliner]) *DiffFixRemoveSetStore {
	return NewDiffFixRemoveSetStore(exec.Get())
}
