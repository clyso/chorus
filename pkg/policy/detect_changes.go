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

package policy

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/dom"
)

const (
	minSchemaVersion     = 3
	currentSchemaVersion = 3
	schemaVersionKey     = "chorus_schema"
)

var supportedSchemaVersions = map[int]string{
	0: "v0.5.15 or older",
	1: "v0.5.15 or older",
	2: "v0.6.0",
}

func CheckSchemaCompatibility(ctx context.Context, chorusVersion string, client redis.UniversalClient) error {
	// Check current schema version in Redis
	version, err := client.Get(ctx, schemaVersionKey).Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("failed to get schema version: %w", err)
	}
	if version >= minSchemaVersion {
		return nil
	}

	// Check for existing keys
	keys, _, err := client.Scan(ctx, 0, "*", 5).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("failed to scan for existing keys: %w", err)
	}

	onlySchemaVersionKey := len(keys) == 1 && keys[0] == schemaVersionKey

	if !onlySchemaVersionKey && len(keys) > 0 {
		supportedVersion := supportedSchemaVersions[version]
		return fmt.Errorf("%w: Chorus version %s is incompatible with existing Redis schema from previous Chorus installations. Please remove all data in Redis to continue with the current version, or use Chorus %s", dom.ErrInternal, chorusVersion, supportedVersion)
	}
	// No legacy keys found, consider schema compatible
	// Set the current schema version to not check again next time
	err = client.Set(ctx, schemaVersionKey, currentSchemaVersion, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to set schema version: %w", err)
	}
	return nil
}
