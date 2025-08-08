package tasks

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/hibiken/asynq"
)

type QueueService interface {
	IsEmpty(ctx context.Context, queueName string) (bool, error)
	IsPaused(ctx context.Context, queueName string) (bool, error)
	Resume(ctx context.Context, queueName string) error
	Pause(ctx context.Context, queueName string) error
	Delete(ctx context.Context, queueName string, force bool) error
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

func (q *queueService) Delete(ctx context.Context, queueName string, force bool) error {
	err := q.inspector.DeleteQueue(queueName, force)
	if errors.Is(err, asynq.ErrQueueNotFound) {
		return fmt.Errorf("%w: queue %s does not exist", dom.ErrNotFound, queueName)
	}
	if errors.Is(err, asynq.ErrQueueNotEmpty) {
		return fmt.Errorf("%w: queue %s is not empty, cannot delete without force", dom.ErrInvalidArg, queueName)
	}
	return err
}

func (q *queueService) IsEmpty(ctx context.Context, queueName string) (bool, error) {
	info, err := q.inspector.GetQueueInfo(queueName)
	if err != nil {
		if strings.HasSuffix(err.Error(), "does not exist") {
			return false, fmt.Errorf("%w: queue %s does not exist", dom.ErrNotFound, queueName)
		}
		return false, fmt.Errorf("get queue info for %s: %w", queueName, err)
	}
	return isQueueEmpty(info), nil
}

func isQueueEmpty(info *asynq.QueueInfo) bool {
	return info.Pending == 0 && info.Active == 0 && info.Scheduled == 0 && info.Retry == 0
}

func (q *queueService) IsPaused(ctx context.Context, queueName string) (bool, error) {
	info, err := q.inspector.GetQueueInfo(queueName)
	if err != nil {
		if errors.Is(err, asynq.ErrQueueNotFound) {
			return false, fmt.Errorf("%w: queue %s does not exist", dom.ErrNotFound, queueName)
		}
		return false, fmt.Errorf("get queue info for %s: %w", queueName, err)
	}
	return info.Paused, nil
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
