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

package swift

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/swift"
	"github.com/clyso/chorus/pkg/tasks"
)

func (s *svc) HandleSwiftObjectMigration(ctx context.Context, t *asynq.Task) (err error) {
	// setup:
	var p tasks.SwiftObjectMigrationPayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("SwiftObjectMigrationPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	logger := zerolog.Ctx(ctx)

	// acquire rate limits for source and destination storage before proceeding
	if err := s.rateLimit(ctx, p.ID.FromStorage(), swift.GetObject); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.FromStorage()).Msg("rate limit error")
		return err
	}
	if err := s.rateLimit(ctx, p.ID.ToStorage(), swift.PutObject); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.ToStorage()).Msg("rate limit error")
		return err
	}

	// acquire lock for object update
	lock, err := s.objectLocker.Lock(ctx, entity.NewObjectLockID(p.ID.FromStorage(), p.Bucket, p.ObjName))
	if err != nil {
		return err
	}
	defer lock.Release(context.Background())

	// sync object:
	return lock.Do(ctx, time.Second*2, func() error {
		task := tasks.SwiftObjectUpdatePayload{
			Bucket:       p.Bucket,
			Object:       p.ObjName,
			VersionID:    p.ObjVersion,
			Etag:         p.ObjEtag,
			LastModified: p.ObjLastModified,
		}
		task.SetReplicationID(p.ID)
		return s.ObjectUpdate(ctx, task)
	})
}
