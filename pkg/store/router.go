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
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/entity"
)

func TokensToUserUploadObjectIDConverter(tokens []string) (entity.UserUploadObjectID, error) {
	return entity.UserUploadObjectID{
		User:   tokens[0],
		Bucket: tokens[1],
	}, nil
}

func UserUploadObjectIDToTokensConverter(id entity.UserUploadObjectID) ([]string, error) {
	return []string{id.User, id.Bucket}, nil
}

func UserUploadObjectToStringConverter(value entity.UserUploadObject) (string, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("unable to serialize user upload object: %w", err)
	}
	return string(bytes), nil
}

func StringToUserUploadObjectConverter(value string) (entity.UserUploadObject, error) {
	var result entity.UserUploadObject
	if err := json.Unmarshal([]byte(value), &result); err != nil {
		var noVal entity.UserUploadObject
		return noVal, fmt.Errorf("unable to deserialize user upload object: %w", err)
	}
	return result, nil
}

type UserUploadStore struct {
	RedisIDKeySet[entity.UserUploadObjectID, entity.UserUploadObject]
}

func NewUserUploadStore(client redis.Cmdable) *UserUploadStore {
	return &UserUploadStore{
		*NewRedisIDKeySet(client, "r:upload",
			UserUploadObjectIDToTokensConverter, TokensToUserUploadObjectIDConverter,
			UserUploadObjectToStringConverter, StringToUserUploadObjectConverter),
	}
}
