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
	"fmt"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
)

var _ QueueService = (*QueueServiceMock)(nil)

type QueueServiceMock struct {
	Queues map[string]int
	Paused map[string]bool
}

// InitReplicationInProgress test helper to initialize queues for replication in progress
func (q *QueueServiceMock) InitReplicationInProgress(id entity.ReplicationStatusID) {
	queues := InitMigrationQueues(id)
	for _, queue := range queues {
		q.Queues[queue] = 1 // not empty
	}
}

// InitReplicationDone test helper to make init replication queues empty
func (q *QueueServiceMock) InitReplicationDone(id entity.ReplicationStatusID) {
	queues := InitMigrationQueues(id)
	for _, queue := range queues {
		q.Queues[queue] = 0 // empty
	}
}

// EventReplicationInProgress test helper to make event replication queue non-empty
func (q *QueueServiceMock) EventReplicationInProgress(id entity.ReplicationStatusID) {
	queues := EventMigrationQueues(id)
	for _, queue := range queues {
		q.Queues[queue] = 1 // not empty
	}
}

func (q *QueueServiceMock) EventReplicationLag(id entity.ReplicationStatusID, lag int) {
	queues := EventMigrationQueues(id)
	for _, queue := range queues {
		q.Queues[queue] = lag
	}
}

// EventReplicationDone test helper to make event replication queue empty
func (q *QueueServiceMock) EventReplicationDone(id entity.ReplicationStatusID) {
	queues := EventMigrationQueues(id)
	for _, queue := range queues {
		q.Queues[queue] = 0 // empty
	}
}

func Reset(q *QueueServiceMock) {
	q.Queues = make(map[string]int)
	q.Paused = make(map[string]bool)
}

func (q *QueueServiceMock) UnprocessedCount(ctx context.Context, queueName string) (int, error) {
	size, exists := q.Queues[queueName]
	if !exists {
		return 0, fmt.Errorf("%w: queue %s does not exist", dom.ErrNotFound, queueName)
	}
	return size, nil
}

func (q *QueueServiceMock) Delete(ctx context.Context, queueName string, force bool) error {
	empty, err := q.IsEmpty(ctx, queueName)
	if err != nil {
		return err
	}
	if !force && !empty {
		return fmt.Errorf("%w: queue %s is not empty, cannot delete without force", dom.ErrInvalidArg, queueName)
	}
	delete(q.Queues, queueName)
	delete(q.Paused, queueName)
	return nil
}

func (q *QueueServiceMock) IsEmpty(ctx context.Context, queueName string) (bool, error) {
	size, exists := q.Queues[queueName]
	if !exists {
		return false, fmt.Errorf("%w: queue %s does not exist", dom.ErrNotFound, queueName)
	}
	return size == 0, nil
}

func (q *QueueServiceMock) IsPaused(ctx context.Context, queueName string) (bool, error) {
	_, err := q.IsEmpty(ctx, queueName)
	if err != nil {
		return false, err
	}
	return q.Paused[queueName], nil
}

func (q *QueueServiceMock) Pause(ctx context.Context, queueName string) error {
	_, err := q.IsEmpty(ctx, queueName)
	if err != nil {
		return err
	}
	q.Paused[queueName] = true
	return nil
}

func (q *QueueServiceMock) Resume(ctx context.Context, queueName string) error {
	_, err := q.IsEmpty(ctx, queueName)
	if err != nil {
		return err
	}
	q.Paused[queueName] = false
	return nil
}
