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

package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/lock"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"
)

func (s *svc) HandleSwiftAccountMigration(ctx context.Context, t *asynq.Task) (err error) {
	var p tasks.SwiftAccountMigrationPayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("SwiftAccountMigrationPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	logger := zerolog.Ctx(ctx)

	replProlicy, err := s.policySvc.GetUserReplicationPolicies(ctx, p.FromAccount)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			zerolog.Ctx(ctx).Err(err).Msg("drop replication task: replication policy not found")
			return nil
		}
		return err
	}
	if replProlicy.From != p.FromStorage {
		zerolog.Ctx(ctx).Err(err).Msg("drop replication task: replication policy does not match storage")
		return nil
	}
	if _, ok := replProlicy.To[policy.ReplicationPolicyDest(p.ToStorage)]; !ok {
		zerolog.Ctx(ctx).Err(err).Msg("drop replication task: replication policy does not match storage")
		return nil
	}

	if err = s.limit.StorReq(ctx, p.FromStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.FromStorage).Msg("rate limit error")
		return err
	}
	fromClient, err := s.swiftClients.For(ctx, p.FromStorage, p.FromAccount)
	if err != nil {
		return err
	}

	lastObjName, err := s.storageSvc.GetLastListedObj(ctx, p)
	if err != nil {
		return err
	}

	return err
}
