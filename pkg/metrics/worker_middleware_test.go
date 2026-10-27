package metrics

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
)

func TestWorkerMiddleware_FailedTasks(t *testing.T) {
	r := require.New(t)
	taskType := "test:failed-tasks"
	value := func(c *prometheus.CounterVec) float64 {
		m := &dto.Metric{}
		r.NoError(c.WithLabelValues("", taskType).Write(m))
		return m.GetCounter().GetValue()
	}
	failed := func() float64 { return value(failedCounter) }
	processed := func() float64 { return value(processedCounter) }
	run := func(err error) error {
		h := WorkerMiddleware()(asynq.HandlerFunc(func(context.Context, *asynq.Task) error { return err }))
		return h.ProcessTask(context.Background(), asynq.NewTask(taskType, nil))
	}

	r.NoError(run(nil))
	r.EqualValues(0, failed())

	r.Error(run(errors.New("copy failed")))
	r.EqualValues(1, failed())

	// postponed by a rate limit or a busy lock: not a failure
	postponed := fmt.Errorf("%w: lock not obtained", &dom.ErrRateLimitExceeded{RetryIn: time.Second})
	r.ErrorIs(run(postponed), postponed)
	r.EqualValues(1, failed())

	r.EqualValues(3, processed())
}
