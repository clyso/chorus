package handler

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/tasks"
)

func Test_SwitchWithDowntimeStateMachine(t *testing.T) {
	worker := &switchSvc{}

	now := time.Now()
	hourAgo := now.Add(-time.Hour)

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
					// attempted at this very instant
					// otherwise causes problems if run at the very beginning of an hour
					LastStartedAt: &now,
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

type zeroDowntimePolicyStub struct {
	policy.Service
	status    entity.ReplicationStatusExtended
	completed bool
}

func (p *zeroDowntimePolicyStub) GetReplicationPolicyInfoExtended(context.Context, entity.UniversalReplicationID) (entity.ReplicationStatusExtended, error) {
	return p.status, nil
}

func (p *zeroDowntimePolicyStub) CompleteZeroDowntimeReplicationSwitch(context.Context, entity.UniversalReplicationID) error {
	p.completed = true
	return nil
}

type s3ClientsStub struct {
	objstore.Clients
	client s3client.Client
}

func (c s3ClientsStub) AsS3(_ context.Context, storage, user string) (s3client.Client, error) {
	if storage != "from" || user != "user" {
		return nil, fmt.Errorf("unexpected client %s/%s", storage, user)
	}
	return c.client, nil
}

func Test_ZeroDowntimeSwitchWaitsForPreSwitchUploads(t *testing.T) {
	const bucket = "bucket"
	now := time.Now()
	// fake storage clock: all uploads are initiated at this time
	uploadAt := now.Add(-2 * time.Hour)

	replications := map[string]entity.UniversalReplicationID{
		"user": entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
			User:        "user",
			FromStorage: "from",
			ToStorage:   "to",
		}),
		"bucket": entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
			User:        "user",
			FromBucket:  bucket,
			FromStorage: "from",
			ToStorage:   "to",
			ToBucket:    bucket,
		}),
	}
	tests := []struct {
		name         string
		startedAt    time.Time
		ttl          time.Duration
		noBucket     bool
		createUpload bool
		wantComplete bool
	}{
		{name: "no uploads", startedAt: now.Add(-30 * time.Minute), ttl: time.Hour, wantComplete: true},
		{name: "no bucket on old storage", startedAt: now.Add(-30 * time.Minute), ttl: time.Hour, noBucket: true, wantComplete: true},
		{name: "pre-switch upload within ttl", startedAt: now.Add(-90 * time.Minute), ttl: 2 * time.Hour, createUpload: true, wantComplete: false},
		{name: "pre-switch upload ttl exceeded", startedAt: now.Add(-90 * time.Minute), ttl: time.Hour, createUpload: true, wantComplete: true},
		{name: "abandoned upload started ttl before switch", startedAt: now.Add(-30 * time.Minute), ttl: time.Hour, createUpload: true, wantComplete: true},
		{name: "upload started on old storage after switch", startedAt: now.Add(-3 * time.Hour), ttl: 24 * time.Hour, createUpload: true, wantComplete: false},
	}
	for replType, id := range replications {
		for _, tt := range tests {
			t.Run(replType+" "+tt.name, func(t *testing.T) {
				r := require.New(t)
				ctx := t.Context()

				backend := s3mem.New()
				fake := gofakes3.New(backend, gofakes3.WithTimeSource(gofakes3.FixedTimeSource(uploadAt)), gofakes3.WithTimeSkewLimit(0))
				ts := httptest.NewServer(fake.Server())
				t.Cleanup(ts.Close)
				client, err := s3client.NewClient(ctx, metrics.NewS3Service(false), s3.StorageAddress{Address: ts.URL, HttpTimeout: time.Minute},
					s3.CredentialsV4{AccessKeyID: "key", SecretAccessKey: "secret"}, "from", "user")
				r.NoError(err)
				if !tt.noBucket {
					r.NoError(backend.CreateBucket(bucket))
				}
				if tt.createUpload {
					_, err = client.AWS().CreateMultipartUploadWithContext(ctx, &aws_s3.CreateMultipartUploadInput{
						Bucket: stringPtr(bucket),
						Key:    stringPtr("obj"),
					})
					r.NoError(err)
				}

				policyStub := &zeroDowntimePolicyStub{status: entity.ReplicationStatusExtended{
					ReplicationStatus: &entity.ReplicationStatus{IsArchived: true},
					Switch: &entity.ReplicationSwitchInfo{
						ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: tt.ttl},
						LastStartedAt:                     &tt.startedAt,
						LastStatus:                        entity.StatusInProgress,
					},
				}}
				worker := NewSwitchSvc(&Config{SwitchRetryInterval: time.Second}, policyStub, s3ClientsStub{client: client}, nil)

				err = worker.handleZeroDowntimeReplicationSwitch(ctx, tasks.ZeroDowntimeReplicationSwitchPayload{ID: id})
				if tt.wantComplete {
					r.NoError(err)
					r.True(policyStub.completed)
					return
				}
				var rlErr *dom.ErrRateLimitExceeded
				r.ErrorAs(err, &rlErr)
				r.False(policyStub.completed)
			})
		}
	}
}
