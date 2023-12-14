package metrics

import (
	"context"
	"github.com/hibiken/asynq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics variables.

func WorkerMiddleware() asynq.MiddlewareFunc {
	var (
		processedCounter = promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "worker_processed_tasks_total",
				Help: "The total number of processed tasks",
			},
			[]string{"queue", "task_type"},
		)

		failedCounter = promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "worker_failed_tasks_total",
				Help: "The total number of times processing failed",
			},
			[]string{"queue", "task_type"},
		)

		inProgressGauge = promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "worker_in_progress_tasks",
				Help: "The number of tasks currently being processed",
			},
			[]string{"queue", "task_type"},
		)

		taskDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "worker_task_duration_seconds",
			Help:    "Task processing time in seconds.",
			Buckets: prometheus.ExponentialBucketsRange(0.1, 600, 11),
		}, []string{"queue", "task_type"})
	)
	return func(next asynq.Handler) asynq.Handler {
		return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
			queue, _ := asynq.GetQueueName(ctx)
			timer := prometheus.NewTimer(taskDuration.WithLabelValues(queue, t.Type()))

			inProgressGauge.WithLabelValues(queue, t.Type()).Inc()
			err := next.ProcessTask(ctx, t)
			timer.ObserveDuration()
			inProgressGauge.WithLabelValues(queue, t.Type()).Dec()
			if err != nil {
				failedCounter.WithLabelValues(queue, t.Type()).Inc()
			}
			processedCounter.WithLabelValues(queue, t.Type()).Inc()
			return err
		})
	}
}
