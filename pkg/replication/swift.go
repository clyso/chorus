/*
 * Copyright © 2024 Clyso GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package replication

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/tasks"
)

func NewSwift(queueSvc tasks.QueueService, policySvc policy.Service) Service {
	return &swiftSVC{
		queueSvc:  queueSvc,
		policySvc: policySvc,
	}
}

type swiftSVC struct {
	queueSvc  tasks.QueueService
	policySvc policy.Service
}

func (s *swiftSVC) Replicate(ctx context.Context, routedTo string, task tasks.ReplicationTask) error {
	if task == nil {
		zerolog.Ctx(ctx).Info().Msg("replication task is nil")
		return nil
	}
	zerolog.Ctx(ctx).Debug().Msg("creating replication task")

	// 1. find repl rule(-s)
	replications := xctx.GetReplications(ctx)
	if len(replications) == 0 {
		return nil
	}

	// Handle all swift tasks here
	// There is no task-specific logic for SWIFT because chorus does not track obj/bucket versions metadata in Redis (unlike S3).
	// Versions are needed only for zero-downtime migration which is currently not supported for SWIFT.
	// Swift is using multiple buckets to implement obj versioning and multipart upload
	// and allows cross-account and cross-bucket references in API for symlinks and COPY.
	// This and eventual consistency makes zero-downtime implementation very tricky.

	// 3. fan out tasks for each destination
	for _, replID := range replications {
		task.SetReplicationID(replID)
		err := s.queueSvc.EnqueueTask(ctx, task)
		if err != nil {
			return fmt.Errorf("unable to fan out replication task to %+v: %w", replID, err)
		}
	}

	return nil
}
