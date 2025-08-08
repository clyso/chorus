package tasks

import (
	"context"
	"fmt"
	"sync"

	"github.com/clyso/chorus/pkg/dom"
)

var _ QueueService = (*QueueServiceMock)(nil)

type QueueServiceMock struct {
	Queues map[string]bool
	Paused map[string]bool
	sync.RWMutex
}

func (q *QueueServiceMock) Delete(ctx context.Context, queueName string, force bool) error {
	empty, err := q.IsEmpty(ctx, queueName)
	if err != nil {
		return err
	}
	if !force && !empty {
		return fmt.Errorf("%w: queue %s is not empty, cannot delete without force", dom.ErrInvalidArg, queueName)
	}
	q.Lock()
	defer q.Unlock()
	delete(q.Queues, queueName)
	delete(q.Paused, queueName)
	return nil
}

func (q *QueueServiceMock) IsEmpty(ctx context.Context, queueName string) (bool, error) {
	q.RLock()
	defer q.RUnlock()
	empty, exists := q.Queues[queueName]
	if !exists {
		return false, fmt.Errorf("%w: queue %s does not exist", dom.ErrNotFound, queueName)
	}
	return empty, nil
}

func (q *QueueServiceMock) IsPaused(ctx context.Context, queueName string) (bool, error) {
	_, err := q.IsEmpty(ctx, queueName)
	if err != nil {
		return false, err
	}
	q.RLock()
	defer q.RUnlock()
	return q.Paused[queueName], nil
}

func (q *QueueServiceMock) Pause(ctx context.Context, queueName string) error {
	_, err := q.IsEmpty(ctx, queueName)
	if err != nil {
		return err
	}
	q.Lock()
	defer q.Unlock()
	q.Paused[queueName] = true
	return nil
}

func (q *QueueServiceMock) Resume(ctx context.Context, queueName string) error {
	_, err := q.IsEmpty(ctx, queueName)
	if err != nil {
		return err
	}
	q.Lock()
	defer q.Unlock()
	q.Paused[queueName] = false
	return nil
}
