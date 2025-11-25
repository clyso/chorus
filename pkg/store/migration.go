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
	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/entity"
)

func TokensToMigrationObjectIDConverter(tokens []string) (entity.MigrationObjectID, error) {
	return entity.MigrationObjectID{
		FromStorage: tokens[0],
		FromBucket:  tokens[1],
		ToStorage:   tokens[2],
		ToBucket:    tokens[3],
		Prefix:      tokens[4],
	}, nil
}

func MigrationObjectIDToTokensConverter(id entity.MigrationObjectID) ([]string, error) {
	return []string{id.FromStorage, id.FromBucket, id.ToStorage, id.ToBucket, id.Prefix}, nil
}

type MigrationObjectListStateStore struct {
	RedisIDKeyValue[entity.MigrationObjectID, string]
}

func NewMigrationObjectListStateStore(client redis.Cmdable) *MigrationObjectListStateStore {
	return &MigrationObjectListStateStore{
		*NewRedisIDKeyValue(client, "m:objectlist",
			MigrationObjectIDToTokensConverter, TokensToMigrationObjectIDConverter,
			StringValueConverter, StringValueConverter),
	}
}

func (r *MigrationObjectListStateStore) WithExecutor(exec Executor[redis.Pipeliner]) *MigrationObjectListStateStore {
	return NewMigrationObjectListStateStore(exec.Get())
}

func TokensToMigrationBucketIDConverter(tokens []string) (entity.MigrationBucketID, error) {
	return entity.MigrationBucketID{
		User:        tokens[0],
		FromStorage: tokens[1],
		FromBucket:  tokens[2],
		ToStorage:   tokens[3],
		ToBucket:    tokens[4],
	}, nil
}

func MigrationBucketIDToTokensConverter(id entity.MigrationBucketID) ([]string, error) {
	return []string{id.User, id.FromStorage, id.FromBucket, id.ToStorage, id.ToBucket}, nil
}

type MigrationBucketListStateStore struct {
	RedisIDKeyValue[entity.MigrationBucketID, string]
}

func NewMigrationBucketListStateStore(client redis.Cmdable) *MigrationBucketListStateStore {
	return &MigrationBucketListStateStore{
		*NewRedisIDKeyValue(client, "m:bucketlist",
			MigrationBucketIDToTokensConverter, TokensToMigrationBucketIDConverter,
			StringValueConverter, StringValueConverter),
	}
}

func (r *MigrationBucketListStateStore) WithExecutor(exec Executor[redis.Pipeliner]) *MigrationBucketListStateStore {
	return NewMigrationBucketListStateStore(exec.Get())
}
