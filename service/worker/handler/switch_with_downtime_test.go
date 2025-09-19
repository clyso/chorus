package handler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/entity"
)

func Test_SwitchWithDowntimeStateMachine(t *testing.T) {
	worker := &svc{}

	now := time.Now()
	hourAgo := now.Add(-time.Hour)
	minuteAgo := now.Add(-time.Minute)

	ctx := t.Context()
	replications := map[string]entity.UniversalReplicationID{
		"user": entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
			User:        "user",
			FromStorage: "from",
			ToStorage:   "to",
		}),
		"bucket": entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
			User:        "user",
			FromBucket:  "bucket",
			FromStorage: "from",
			ToStorage:   "to",
			ToBucket:    "bucket",
		}),
	}

	for replType, id := range replications {
		for _, status := range []entity.ReplicationSwitchStatus{entity.StatusNotStarted, entity.StatusError, entity.StatusSkipped, ""} {
			t.Run(replType+" from "+string(status)+" to in_progress", func(t *testing.T) {
				r := require.New(t)

				nextState, err := worker.processSwitchWithDowntimeState(ctx, id, entity.ReplicationStatusExtended{
					ReplicationStatus: &entity.ReplicationStatus{
						CreatedAt: hourAgo,
					},
					IsPaused: false,
					InitMigration: entity.QueueStats{
						// init done
						Unprocessed: 0,
					},
					EventMigration: entity.QueueStats{},
				}, entity.ReplicationSwitchInfo{
					ReplicationSwitchDowntimeOpts: entity.ReplicationSwitchDowntimeOpts{
						Cron: stringPtr("@5minutes"),
					},
					CreatedAt:     hourAgo,
					LastStartedAt: &hourAgo,
					LastStatus:    status,
				})
				r.NoError(err)
				r.True(nextState.retryLater)
				r.Equal(entity.StatusInProgress, nextState.nextState.status)
			})
			t.Run(replType+" from "+string(status)+" to error - already retried", func(t *testing.T) {
				r := require.New(t)

				nextState, err := worker.processSwitchWithDowntimeState(ctx, id, entity.ReplicationStatusExtended{
					ReplicationStatus: &entity.ReplicationStatus{
						CreatedAt: hourAgo,
					},
					IsPaused: false,
					InitMigration: entity.QueueStats{
						// init done
						Unprocessed: 0,
					},
					EventMigration: entity.QueueStats{},
				}, entity.ReplicationSwitchInfo{
					ReplicationSwitchDowntimeOpts: entity.ReplicationSwitchDowntimeOpts{
						StartAt: &now,
					},
					CreatedAt:     hourAgo,
					LastStartedAt: &hourAgo,
					LastStatus:    status,
				})
				r.NoError(err)
				if status != entity.StatusNotStarted && status != "" {
					r.False(nextState.retryLater, "don't retry one-time switch if it was already attempted")
					r.Equal(entity.StatusError, nextState.nextState.status)
				} else {
					r.True(nextState.retryLater)
					r.Equal(entity.StatusInProgress, nextState.nextState.status)
				}
			})
			t.Run(replType+" from "+string(status)+" to retry later", func(t *testing.T) {
				r := require.New(t)

				nextState, err := worker.processSwitchWithDowntimeState(ctx, id, entity.ReplicationStatusExtended{}, entity.ReplicationSwitchInfo{
					ReplicationSwitchDowntimeOpts: entity.ReplicationSwitchDowntimeOpts{
						// run every hour
						Cron: stringPtr("@hourly"),
					},
					CreatedAt: hourAgo,
					// was recently attempted
					LastStartedAt: &minuteAgo,
					LastStatus:    status,
				})
				r.NoError(err)
				r.True(nextState.retryLater)
				r.Empty(nextState.nextState.status, "no status change")
			})
			t.Run(replType+" from "+string(status)+" retry later - init not done", func(t *testing.T) {
				r := require.New(t)

				nextState, err := worker.processSwitchWithDowntimeState(ctx, id, entity.ReplicationStatusExtended{
					ReplicationStatus: &entity.ReplicationStatus{
						CreatedAt: hourAgo,
					},
					IsPaused: false,
					InitMigration: entity.QueueStats{
						// init not done
						Unprocessed: 1,
					},
					EventMigration: entity.QueueStats{},
				}, entity.ReplicationSwitchInfo{
					ReplicationSwitchDowntimeOpts: entity.ReplicationSwitchDowntimeOpts{
						// wait for init done
						StartOnInitDone: true,
						Cron:            stringPtr("@5minutes"),
					},
					CreatedAt:     hourAgo,
					LastStartedAt: &hourAgo,
					LastStatus:    status,
				})
				r.NoError(err)
				r.True(nextState.retryLater)
				r.Empty(nextState.nextState.status, "no status change")
			})
			t.Run(replType+" from "+string(status)+" to skipped - init not done", func(t *testing.T) {
				r := require.New(t)

				nextState, err := worker.processSwitchWithDowntimeState(ctx, id, entity.ReplicationStatusExtended{
					ReplicationStatus: &entity.ReplicationStatus{
						CreatedAt: hourAgo,
					},
					IsPaused: false,
					InitMigration: entity.QueueStats{
						// init not done
						Unprocessed: 1,
					},
					EventMigration: entity.QueueStats{},
				}, entity.ReplicationSwitchInfo{
					ReplicationSwitchDowntimeOpts: entity.ReplicationSwitchDowntimeOpts{
						// skip if init not done
						StartOnInitDone: false,
						Cron:            stringPtr("@5minutes"),
					},
					CreatedAt:     hourAgo,
					LastStartedAt: &hourAgo,
					LastStatus:    status,
				})
				r.NoError(err)
				r.True(nextState.retryLater)
				r.EqualValues(entity.StatusSkipped, nextState.nextState.status)
			})
			t.Run(replType+" from "+string(status)+" to skipped - event lag not met", func(t *testing.T) {
				r := require.New(t)

				nextState, err := worker.processSwitchWithDowntimeState(ctx, id, entity.ReplicationStatusExtended{
					ReplicationStatus: &entity.ReplicationStatus{
						CreatedAt: hourAgo,
					},
					IsPaused: false,
					InitMigration: entity.QueueStats{
						// init done
						Unprocessed: 0,
					},
					EventMigration: entity.QueueStats{
						// event lag is 10
						Unprocessed: 10,
					},
				}, entity.ReplicationSwitchInfo{
					ReplicationSwitchDowntimeOpts: entity.ReplicationSwitchDowntimeOpts{
						// skip if init not done
						StartOnInitDone: false,
						Cron:            stringPtr("@5minutes"),
						// max event lag is 9
						MaxEventLag: uint32Ptr(9),
					},
					CreatedAt:     hourAgo,
					LastStartedAt: &hourAgo,
					LastStatus:    status,
				})
				r.NoError(err)
				r.True(nextState.retryLater)
				r.EqualValues(entity.StatusSkipped, nextState.nextState.status)
			})
		}

		t.Run(replType+" in_progress wait queue drain", func(t *testing.T) {
			r := require.New(t)

			nextState, err := worker.processSwitchWithDowntimeState(ctx, id, entity.ReplicationStatusExtended{
				ReplicationStatus: &entity.ReplicationStatus{
					CreatedAt: hourAgo,
				},
				IsPaused: false,
				InitMigration: entity.QueueStats{
					// init done
					Unprocessed: 0,
				},
				EventMigration: entity.QueueStats{
					// queue not drained
					Unprocessed: 1,
				},
			}, entity.ReplicationSwitchInfo{
				ReplicationSwitchDowntimeOpts: entity.ReplicationSwitchDowntimeOpts{
					Cron: stringPtr("@5minutes"),
				},
				CreatedAt:     hourAgo,
				LastStartedAt: &hourAgo,
				LastStatus:    entity.StatusInProgress,
			})
			r.NoError(err)
			r.True(nextState.retryLater)
			r.Empty(nextState.nextState.status, "no status change")
		})
		t.Run(replType+"in_progress to error: drain timeout", func(t *testing.T) {
			r := require.New(t)

			nextState, err := worker.processSwitchWithDowntimeState(ctx, id, entity.ReplicationStatusExtended{
				ReplicationStatus: &entity.ReplicationStatus{
					CreatedAt: hourAgo,
				},
				IsPaused: false,
				InitMigration: entity.QueueStats{
					// init done
					Unprocessed: 0,
				},
				EventMigration: entity.QueueStats{
					// queue not drained
					Unprocessed: 1,
				},
			}, entity.ReplicationSwitchInfo{
				ReplicationSwitchDowntimeOpts: entity.ReplicationSwitchDowntimeOpts{
					Cron: stringPtr("@5minutes"),
					// max duration is 59 minutes
					MaxDuration: (time.Minute * 59),
				},
				CreatedAt: hourAgo,
				// started 1 hour ago
				LastStartedAt: &hourAgo,
				LastStatus:    entity.StatusInProgress,
			})
			r.NoError(err)
			r.True(nextState.retryLater)
			r.EqualValues(entity.StatusError, nextState.nextState.status)
		})
		t.Run(replType+" in_progress to check_in_progress", func(t *testing.T) {
			r := require.New(t)

			nextState, err := worker.processSwitchWithDowntimeState(ctx, id, entity.ReplicationStatusExtended{
				ReplicationStatus: &entity.ReplicationStatus{
					CreatedAt: hourAgo,
				},
				IsPaused: false,
				InitMigration: entity.QueueStats{
					// init done
					Unprocessed: 0,
				},
				EventMigration: entity.QueueStats{
					// queue drained
					Unprocessed: 0,
				},
			}, entity.ReplicationSwitchInfo{
				ReplicationSwitchDowntimeOpts: entity.ReplicationSwitchDowntimeOpts{
					Cron: stringPtr("@5minutes"),
				},
				CreatedAt: hourAgo,
				// started 1 hour ago
				LastStartedAt: &hourAgo,
				LastStatus:    entity.StatusInProgress,
			})
			r.NoError(err)
			r.True(nextState.retryLater)
			r.EqualValues(entity.StatusCheckInProgress, nextState.nextState.status)
		})
		t.Run(replType+" check_in_progress wait for check complete", func(t *testing.T) {
			r := require.New(t)
			// mock that check is still in progress
			checkResultIsInProgress = true

			nextState, err := worker.processSwitchWithDowntimeState(ctx, id, entity.ReplicationStatusExtended{
				ReplicationStatus: &entity.ReplicationStatus{
					CreatedAt: hourAgo,
				},
				IsPaused: false,
				InitMigration: entity.QueueStats{
					// init done
					Unprocessed: 0,
				},
				EventMigration: entity.QueueStats{
					// queue drained
					Unprocessed: 0,
				},
			}, entity.ReplicationSwitchInfo{
				ReplicationSwitchDowntimeOpts: entity.ReplicationSwitchDowntimeOpts{
					Cron: stringPtr("@5minutes"),
				},
				CreatedAt: hourAgo,
				// started 1 hour ago
				LastStartedAt: &hourAgo,
				LastStatus:    entity.StatusCheckInProgress,
			})
			r.NoError(err)
			r.True(nextState.retryLater)
			r.Empty(nextState.nextState.status, "no status change")
		})
		t.Run(replType+" check_in_progress to error duration exceeded", func(t *testing.T) {
			r := require.New(t)
			// mock that check is still in progress
			checkResultIsInProgress = true

			nextState, err := worker.processSwitchWithDowntimeState(ctx, id, entity.ReplicationStatusExtended{
				ReplicationStatus: &entity.ReplicationStatus{
					CreatedAt: hourAgo,
				},
				IsPaused: false,
				InitMigration: entity.QueueStats{
					// init done
					Unprocessed: 0,
				},
				EventMigration: entity.QueueStats{
					// queue drained
					Unprocessed: 0,
				},
			}, entity.ReplicationSwitchInfo{
				ReplicationSwitchDowntimeOpts: entity.ReplicationSwitchDowntimeOpts{
					Cron: stringPtr("@5minutes"),
					// max duration is 59 minutes
					MaxDuration: (time.Minute * 59),
				},
				CreatedAt: hourAgo,
				// started 1 hour ago
				LastStartedAt: &hourAgo,
				LastStatus:    entity.StatusCheckInProgress,
			})
			r.NoError(err)
			r.True(nextState.retryLater)
			r.EqualValues(entity.StatusError, nextState.nextState.status)
		})
		t.Run(replType+" check_in_progress to error buckets not equal", func(t *testing.T) {
			r := require.New(t)
			// mock that check is done and not equal
			checkResultIsInProgress = false
			checkResultIsEqual = false

			nextState, err := worker.processSwitchWithDowntimeState(ctx, id, entity.ReplicationStatusExtended{
				ReplicationStatus: &entity.ReplicationStatus{
					CreatedAt: hourAgo,
				},
				IsPaused: false,
				InitMigration: entity.QueueStats{
					// init done
					Unprocessed: 0,
				},
				EventMigration: entity.QueueStats{
					// queue drained
					Unprocessed: 0,
				},
			}, entity.ReplicationSwitchInfo{
				ReplicationSwitchDowntimeOpts: entity.ReplicationSwitchDowntimeOpts{
					Cron: stringPtr("@5minutes"),
				},
				CreatedAt: hourAgo,
				// started 1 hour ago
				LastStartedAt: &hourAgo,
				LastStatus:    entity.StatusCheckInProgress,
			})
			r.NoError(err)
			r.True(nextState.retryLater)
			r.EqualValues(entity.StatusError, nextState.nextState.status)
		})
		t.Run(replType+" check_in_progress to done", func(t *testing.T) {
			r := require.New(t)
			// mock that check is done and buckets are equal
			checkResultIsInProgress = false
			checkResultIsEqual = true

			nextState, err := worker.processSwitchWithDowntimeState(ctx, id, entity.ReplicationStatusExtended{
				ReplicationStatus: &entity.ReplicationStatus{
					CreatedAt: hourAgo,
				},
				IsPaused: false,
				InitMigration: entity.QueueStats{
					// init done
					Unprocessed: 0,
				},
				EventMigration: entity.QueueStats{
					// queue drained
					Unprocessed: 0,
				},
			}, entity.ReplicationSwitchInfo{
				ReplicationSwitchDowntimeOpts: entity.ReplicationSwitchDowntimeOpts{
					Cron:                stringPtr("@5minutes"),
					ContinueReplication: true,
				},
				CreatedAt: hourAgo,
				// started 1 hour ago
				LastStartedAt: &hourAgo,
				LastStatus:    entity.StatusCheckInProgress,
			})
			r.NoError(err)
			r.False(nextState.retryLater, "no retry - terminal state")
			r.EqualValues(entity.StatusDone, nextState.nextState.status)
		})
		t.Run(replType+"done to done", func(t *testing.T) {
			r := require.New(t)

			nextState, err := worker.processSwitchWithDowntimeState(ctx, id, entity.ReplicationStatusExtended{
				ReplicationStatus: &entity.ReplicationStatus{
					CreatedAt: hourAgo,
				},
				IsPaused: false,
				InitMigration: entity.QueueStats{
					// init done
					Unprocessed: 0,
				},
				EventMigration: entity.QueueStats{
					// queue drained
					Unprocessed: 0,
				},
			}, entity.ReplicationSwitchInfo{
				ReplicationSwitchDowntimeOpts: entity.ReplicationSwitchDowntimeOpts{
					Cron:                stringPtr("@5minutes"),
					ContinueReplication: true,
				},
				CreatedAt: hourAgo,
				// started 1 hour ago
				LastStartedAt: &hourAgo,
				LastStatus:    entity.StatusDone,
			})
			r.NoError(err)
			r.False(nextState.retryLater, "no retry - terminal state")
			r.EqualValues(entity.StatusDone, nextState.nextState.status)
		})
	}
}

func stringPtr(s string) *string {
	return &s
}

func uint32Ptr(i uint32) *uint32 {
	return &i
}
