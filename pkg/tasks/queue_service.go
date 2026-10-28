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
	"time"

	"github.com/hibiken/asynq"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
)

type QueueService interface {
	UnprocessedCount(ctx context.Context, ignoreNotFound bool, queues ...string) (int, error)
	IsPaused(ctx context.Context, queueName string) (bool, error)
	Resume(ctx context.Context, queueName string) error
	Pause(ctx context.Context, queueName string) error
	Delete(ctx context.Context, queueName string, force bool) error
	// Stop pauses queues, cancels active tasks and waits until asynq reports none active.
	// Handlers of canceled tasks may still be finishing. Queues stay paused.
	Stop(ctx context.Context, queues ...string) error
	// Purge requires stopped queues: asynq refuses to delete a queue with active tasks.
	Purge(ctx context.Context, queues ...string) error
	Stats(ctx context.Context, queueName string) (*QueueStats, error)
	EnqueueTask(ctx context.Context, task any) error
}

type QueueStats struct {
	// Current number of tasks present in the queue (asynq QueueInfo.Size).
	// Includes pending, active, scheduled, retry, aggregating AND archived
	// (permanently failed) tasks. It is a live snapshot, not a lifetime total,
	// and it does NOT include tasks that were already processed and deleted.
	Unprocessed int
	// Total number of times tasks in this queue finished processing, counting
	// every retry and archival. asynq bumps this counter on success AND on each
	// failed attempt, so it is NOT the number of successful tasks on its own.
	// Successful tasks = ProcessedTotal - FailedTotal.
	ProcessedTotal int
	// Total number of failed processing attempts (retries + archived tasks).
	FailedTotal int

	// Paused indicates whether the queue is paused.
	// If true, tasks in the queue will not be processed.
	Paused bool

	// Total number of bytes that the queue and its tasks require to be stored in redis.
	// It is an approximate memory usage value in bytes since the value is computed by sampling.
	MemoryUsage int64

	// Latency of the queue, measured by the oldest pending task in the queue.
	Latency time.Duration
}

func NewQueueService(client *asynq.Client, inspector *asynq.Inspector) *queueService {
	return &queueService{
		inspector: inspector,
		client:    client,
	}
}

var _ QueueService = (*queueService)(nil)

type queueService struct {
	inspector *asynq.Inspector
	client    *asynq.Client
}

func (q *queueService) EnqueueTask(ctx context.Context, task any) error {
	return enqueueAny(ctx, q.client, task)
}

func (q *queueService) Stats(ctx context.Context, queueName string) (*QueueStats, error) {
	info, err := q.getInfo(ctx, queueName)
	if err != nil {
		return nil, err
	}
	return &QueueStats{
		Unprocessed:    info.Size,
		ProcessedTotal: info.ProcessedTotal,
		FailedTotal:    info.FailedTotal,
		Paused:         info.Paused,
		MemoryUsage:    info.MemoryUsage,
		Latency:        info.Latency,
	}, nil
}

func (q *queueService) Delete(ctx context.Context, queueName string, force bool) error {
	err := q.inspector.DeleteQueue(queueName, force)
	if errors.Is(err, asynq.ErrQueueNotFound) {
		return fmt.Errorf("unable to delete queue %s: %w", queueName, dom.ErrNotFound)
	}
	if errors.Is(err, asynq.ErrQueueNotEmpty) {
		return fmt.Errorf("unable to delete non-empty queue %s: %w", queueName, dom.ErrInvalidArg)
	}
	return err
}

const (
	// a task held by a dead worker stays active until its asynq lease expires, so waiting longer is pointless
	stopTimeout      = 10 * time.Second
	stopPollInterval = 50 * time.Millisecond
)

func (q *queueService) Stop(ctx context.Context, queues ...string) error {
	// pause sets the flag even for missing queues, so later enqueued tasks are not processed
	for _, queue := range queues {
		if err := q.Pause(ctx, queue); err != nil {
			return fmt.Errorf("unable to pause queue %s: %w", queue, err)
		}
	}
	deadline := time.Now().Add(stopTimeout)
	for {
		// a task dequeued concurrently with pause may become active after the previous listing
		active, err := q.cancelActive(queues)
		if err != nil {
			return err
		}
		if active == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w: %d tasks are still active after %s, queues are paused, retry later", dom.ErrInternal, active, stopTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(stopPollInterval):
		}
	}
}

func (q *queueService) cancelActive(queues []string) (int, error) {
	const pageSize = 1000
	active := 0
	for _, queue := range queues {
		for page := 1; ; page++ {
			list, err := q.inspector.ListActiveTasks(queue, asynq.PageSize(pageSize), asynq.Page(page))
			if errors.Is(err, asynq.ErrQueueNotFound) {
				break
			}
			if err != nil {
				return 0, fmt.Errorf("unable to list active tasks of queue %s: %w", queue, err)
			}
			for _, task := range list {
				if err := q.inspector.CancelProcessing(task.ID); err != nil {
					return 0, fmt.Errorf("unable to cancel task %s of queue %s: %w", task.ID, queue, err)
				}
			}
			active += len(list)
			if len(list) < pageSize {
				break
			}
		}
	}
	return active, nil
}

func (q *queueService) Purge(ctx context.Context, queues ...string) error {
	for _, queue := range queues {
		err := q.Delete(ctx, queue, true)
		if errors.Is(err, dom.ErrNotFound) {
			// missing queue may still have the paused flag
			if err := q.Resume(ctx, queue); err != nil {
				return fmt.Errorf("unable to remove paused flag of queue %s: %w", queue, err)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("unable to delete queue %s: %w", queue, err)
		}
	}
	return nil
}

func StopAndPurgeReplicationQueues(ctx context.Context, svc QueueService, id entity.UniversalReplicationID) error {
	queues := AllReplicationQueues(id)
	if err := svc.Stop(ctx, queues...); err != nil {
		return fmt.Errorf("unable to stop replication queues: %w", err)
	}
	if err := svc.Purge(ctx, queues...); err != nil {
		return fmt.Errorf("unable to purge replication queues: %w", err)
	}
	return nil
}

func (q *queueService) UnprocessedCount(ctx context.Context, ignoreNotFound bool, queues ...string) (int, error) {
	if len(queues) == 0 {
		return 0, fmt.Errorf("%w: no queues specified", dom.ErrInvalidArg)
	}
	count := 0
	for _, queue := range queues {
		info, err := q.getInfo(ctx, queue)
		if ignoreNotFound && errors.Is(err, dom.ErrNotFound) {
			continue
		}
		if err != nil {
			return 0, err
		}
		count += info.Size
	}
	return count, nil
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
		if errors.Is(err, asynq.ErrQueueNotFound) {
			return nil, fmt.Errorf("%w: queue %s does not exist", dom.ErrNotFound, queueName)
		}
		return nil, fmt.Errorf("get queue info for %s: %w", queueName, err)
	}
	return info, nil
}

func (q *queueService) Pause(ctx context.Context, queueName string) error {
	err := q.inspector.PauseQueue(queueName)
	if err != nil {
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
		// ignore error if queue is not paused
		if strings.HasSuffix(err.Error(), "is not paused") {
			return nil
		}
	}
	return err
}
