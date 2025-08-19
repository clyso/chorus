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

package tasks

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hibiken/asynq"

	"github.com/clyso/chorus/pkg/dom"
)

type QueueService interface {
	IsEmpty(ctx context.Context, queueName string) (bool, error)
	UnprocessedCount(ctx context.Context, queueName string) (int, error)
	IsPaused(ctx context.Context, queueName string) (bool, error)
	Resume(ctx context.Context, queueName string) error
	Pause(ctx context.Context, queueName string) error
}

func NewQueueService(inspector *asynq.Inspector) *queueService {
	return &queueService{
		inspector: inspector,
	}
}

var _ QueueService = (*queueService)(nil)

type queueService struct {
	inspector *asynq.Inspector
}

func (q *queueService) UnprocessedCount(ctx context.Context, queueName string) (int, error) {
	info, err := q.getInfo(ctx, queueName)
	if err != nil {
		return 0, err
	}
	return unprocessedCount(info), nil
}

func unprocessedCount(info *asynq.QueueInfo) int {
	return info.Pending + info.Active + info.Scheduled + info.Retry
}

func (q *queueService) IsEmpty(ctx context.Context, queueName string) (bool, error) {
	info, err := q.getInfo(ctx, queueName)
	if err != nil {
		return false, err
	}
	return unprocessedCount(info) == 0, nil
}

func (q *queueService) IsPaused(ctx context.Context, queueName string) (bool, error) {
	info, err := q.getInfo(ctx, queueName)
	if err != nil {
		return false, err
	}
	return info.Paused, nil
}

func (q *queueService) getInfo(_ context.Context, queueName string) (*asynq.QueueInfo, error) {
	info, err := q.inspector.GetQueueInfo(queueName)
	if err != nil {
		if strings.HasSuffix(err.Error(), "does not exist") {
			return nil, fmt.Errorf("%w: queue %s does not exist", dom.ErrNotFound, queueName)
		}
		return nil, fmt.Errorf("get queue info for %s: %w", queueName, err)
	}
	return info, nil
}

func (q *queueService) Pause(ctx context.Context, queueName string) error {
	err := q.inspector.PauseQueue(queueName)
	if err != nil {
		if errors.Is(err, asynq.ErrQueueNotFound) {
			return fmt.Errorf("%w: queue %s does not exist", dom.ErrNotFound, queueName)
		}
		// ignore error if queue is already paused
		if strings.HasSuffix(err.Error(), "is already paused") {
			return nil
		}
	}
	return err
}

func (q *queueService) Resume(ctx context.Context, queueName string) error {
	err := q.inspector.UnpauseQueue(queueName)
	if err != nil {
		if errors.Is(err, asynq.ErrQueueNotFound) {
			return fmt.Errorf("%w: queue %s does not exist", dom.ErrNotFound, queueName)
		}
		// ignore error if queue is not paused
		if strings.HasSuffix(err.Error(), "is not paused") {
			return nil
		}
	}
	return err
}
