package handler

import (
	"context"
	"testing"
	"time"

	"github.com/clyso/chorus/pkg/policy"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func Test_SwitchWithDowntimeStateMachine(t *testing.T) {
	worker := &svc{}

	now := time.Now()
	hourAgo := now.Add(-time.Hour)
	minuteAgo := now.Add(-time.Minute)

	ctx := context.Background()
	id := policy.ReplicationID{
		User:     "user",
		Bucket:   "bucket",
		From:     "from",
		To:       "to",
		ToBucket: stringPtr("toBucket"),
	}

	for _, status := range []policy.SwitchWithDowntimeStatus{policy.StatusNotStarted, policy.StatusError, policy.StatusSkipped, ""} {
		t.Run("from "+string(status)+" to in_progress", func(t *testing.T) {
			r := require.New(t)
			policyMock := &policy.MockService{}
			if status == policy.StatusError {
				policyMock.On("DeleteRoutingBlock", mock.Anything, "from", "bucket").Return(nil).Once()
			}
			policyMock.On("AddRoutingBlock", mock.Anything, "from", "bucket").Return(nil).Once()
			worker.policySvc = policyMock

			nextState, err := worker.processSwitchWithDowntimeState(ctx, id, policy.ReplicationPolicyStatus{
				CreatedAt: hourAgo,
				IsPaused:  false,
				// init done
				InitObjListed:  1,
				InitObjDone:    1,
				InitDoneAt:     &hourAgo,
				ListingStarted: true,
			}, policy.SwitchWithDowntime{
				Window: policy.Window{
					Cron: stringPtr("@5minutes"),
				},
				CreatedAt:     hourAgo,
				LastStartedAt: &hourAgo,
				LastStatus:    status,
			})
			r.NoError(err)
			r.True(nextState.retryLater)
			r.Equal(policy.StatusInProgress, nextState.nextState.status)
			r.NotNil(nextState.nextState.startedAt)
			r.Nil(nextState.nextState.doneAt)
			policyMock.AssertExpectations(t)
			if status != policy.StatusError {
				// test cleanup only on error
				policyMock.AssertNotCalled(t, "DeleteRoutingBlock", mock.Anything, mock.Anything, mock.Anything)
			}
		})
		t.Run("from "+string(status)+" to error - already retried", func(t *testing.T) {
			r := require.New(t)
			policyMock := &policy.MockService{}
			if status == policy.StatusError {
				policyMock.On("DeleteRoutingBlock", mock.Anything, "from", "bucket").Return(nil).Once()
			}
			if status == policy.StatusNotStarted || status == "" {
				policyMock.On("AddRoutingBlock", mock.Anything, "from", "bucket").Return(nil).Once()
			}
			worker.policySvc = policyMock

			nextState, err := worker.processSwitchWithDowntimeState(ctx, id, policy.ReplicationPolicyStatus{
				CreatedAt: hourAgo,
				IsPaused:  false,
				// init done
				InitObjListed:  1,
				InitObjDone:    1,
				InitDoneAt:     &hourAgo,
				ListingStarted: true,
			}, policy.SwitchWithDowntime{
				Window: policy.Window{
					StartAt: &now,
				},
				CreatedAt:     hourAgo,
				LastStartedAt: &hourAgo,
				LastStatus:    status,
			})
			r.NoError(err)
			if status != policy.StatusNotStarted && status != "" {
				r.False(nextState.retryLater, "don't retry one-time switch if it was already attempted")
				r.Equal(policy.StatusError, nextState.nextState.status)
				r.Nil(nextState.nextState.startedAt)
				r.Nil(nextState.nextState.doneAt)
			} else {
				r.True(nextState.retryLater)
				r.Equal(policy.StatusInProgress, nextState.nextState.status)
				r.NotNil(nextState.nextState.startedAt)
				r.Nil(nextState.nextState.doneAt)
			}
			policyMock.AssertExpectations(t)
			if status != policy.StatusError {
				// test cleanup only on error
				policyMock.AssertNotCalled(t, "DeleteRoutingBlock", mock.Anything, mock.Anything, mock.Anything)
			}
		})
		t.Run("from "+string(status)+" to retry later", func(t *testing.T) {
			r := require.New(t)
			policyMock := &policy.MockService{}
			if status == policy.StatusError {
				policyMock.On("DeleteRoutingBlock", mock.Anything, "from", "bucket").Return(nil).Once()
			}
			worker.policySvc = policyMock

			nextState, err := worker.processSwitchWithDowntimeState(ctx, id, policy.ReplicationPolicyStatus{}, policy.SwitchWithDowntime{
				Window: policy.Window{
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
			policyMock.AssertExpectations(t)
			if status != policy.StatusError {
				// test cleanup only on error
				policyMock.AssertNotCalled(t, "DeleteRoutingBlock", mock.Anything, mock.Anything, mock.Anything)
			}
		})
		t.Run("from "+string(status)+" retry later - init not done", func(t *testing.T) {
			r := require.New(t)
			policyMock := &policy.MockService{}
			if status == policy.StatusError {
				policyMock.On("DeleteRoutingBlock", mock.Anything, "from", "bucket").Return(nil).Once()
			}
			worker.policySvc = policyMock

			nextState, err := worker.processSwitchWithDowntimeState(ctx, id, policy.ReplicationPolicyStatus{
				// init not done
				CreatedAt:      hourAgo,
				InitObjListed:  1,
				InitObjDone:    0,
				ListingStarted: true,
			}, policy.SwitchWithDowntime{
				Window: policy.Window{
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
			policyMock.AssertExpectations(t)
			if status != policy.StatusError {
				// test cleanup only on error
				policyMock.AssertNotCalled(t, "DeleteRoutingBlock", mock.Anything, mock.Anything, mock.Anything)
			}
		})
		t.Run("from "+string(status)+" to skipped - init not done", func(t *testing.T) {
			r := require.New(t)
			policyMock := &policy.MockService{}
			if status == policy.StatusError {
				policyMock.On("DeleteRoutingBlock", mock.Anything, "from", "bucket").Return(nil).Once()
			}
			worker.policySvc = policyMock

			nextState, err := worker.processSwitchWithDowntimeState(ctx, id, policy.ReplicationPolicyStatus{
				CreatedAt:      hourAgo,
				IsPaused:       false,
				InitObjListed:  1,
				InitObjDone:    0,
				ListingStarted: true,
			}, policy.SwitchWithDowntime{
				Window: policy.Window{
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
			r.EqualValues(policy.StatusSkipped, nextState.nextState.status)
			r.Nil(nextState.nextState.startedAt)
			r.Nil(nextState.nextState.doneAt)
			policyMock.AssertExpectations(t)
			if status != policy.StatusError {
				// test cleanup only on error
				policyMock.AssertNotCalled(t, "DeleteRoutingBlock", mock.Anything, mock.Anything, mock.Anything)
			}
		})
		t.Run("from "+string(status)+" to skipped - event lag not met", func(t *testing.T) {
			r := require.New(t)
			policyMock := &policy.MockService{}
			if status == policy.StatusError {
				policyMock.On("DeleteRoutingBlock", mock.Anything, "from", "bucket").Return(nil).Once()
			}
			worker.policySvc = policyMock

			nextState, err := worker.processSwitchWithDowntimeState(ctx, id, policy.ReplicationPolicyStatus{
				CreatedAt: hourAgo,
				IsPaused:  false,
				// init done
				InitObjListed:  1,
				InitObjDone:    1,
				InitDoneAt:     &hourAgo,
				ListingStarted: true,
				// event lag is 10
				Events:     15,
				EventsDone: 5,
			}, policy.SwitchWithDowntime{
				Window: policy.Window{
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
			r.EqualValues(policy.StatusSkipped, nextState.nextState.status)
			r.Nil(nextState.nextState.startedAt)
			r.Nil(nextState.nextState.doneAt)
			policyMock.AssertExpectations(t)
			if status != policy.StatusError {
				// test cleanup only on error
				policyMock.AssertNotCalled(t, "DeleteRoutingBlock", mock.Anything, mock.Anything, mock.Anything)
			}
		})
	}

	t.Run("in_progress wait queue drain", func(t *testing.T) {
		r := require.New(t)
		policyMock := &policy.MockService{}
		worker.policySvc = policyMock

		nextState, err := worker.processSwitchWithDowntimeState(ctx, id, policy.ReplicationPolicyStatus{
			CreatedAt: hourAgo,
			IsPaused:  false,
			// init done
			InitObjListed: 1,
			InitObjDone:   1,
			// queue not drained
			Events:         1,
			EventsDone:     0,
			InitDoneAt:     &hourAgo,
			ListingStarted: true,
		}, policy.SwitchWithDowntime{
			Window: policy.Window{
				Cron: stringPtr("@5minutes"),
			},
			CreatedAt:     hourAgo,
			LastStartedAt: &hourAgo,
			LastStatus:    policy.StatusInProgress,
		})
		r.NoError(err)
		r.True(nextState.retryLater)
		r.Empty(nextState.nextState.status, "no status change")
		r.Nil(nextState.nextState.startedAt)
		r.Nil(nextState.nextState.doneAt)
		policyMock.AssertExpectations(t)
	})
	t.Run("in_progress to error: drain timeout", func(t *testing.T) {
		r := require.New(t)
		policyMock := &policy.MockService{}
		worker.policySvc = policyMock

		nextState, err := worker.processSwitchWithDowntimeState(ctx, id, policy.ReplicationPolicyStatus{
			CreatedAt: hourAgo,
			IsPaused:  false,
			// init done
			InitObjListed: 1,
			InitObjDone:   1,
			// queue not drained
			Events:         1,
			EventsDone:     0,
			InitDoneAt:     &hourAgo,
			ListingStarted: true,
		}, policy.SwitchWithDowntime{
			Window: policy.Window{
				Cron: stringPtr("@5minutes"),
				// max duration is 59 minutes
				MaxDuration: durationPtr(time.Minute * 59),
			},
			CreatedAt: hourAgo,
			// started 1 hour ago
			LastStartedAt: &hourAgo,
			LastStatus:    policy.StatusInProgress,
		})
		r.NoError(err)
		r.True(nextState.retryLater)
		r.EqualValues(policy.StatusError, nextState.nextState.status)
		r.Nil(nextState.nextState.startedAt)
		r.Nil(nextState.nextState.doneAt)
		policyMock.AssertExpectations(t)
	})
	t.Run("in_progress to check_in_progress", func(t *testing.T) {
		r := require.New(t)
		policyMock := &policy.MockService{}
		worker.policySvc = policyMock

		nextState, err := worker.processSwitchWithDowntimeState(ctx, id, policy.ReplicationPolicyStatus{
			CreatedAt: hourAgo,
			IsPaused:  false,
			// init done
			InitObjListed: 1,
			InitObjDone:   1,
			// queue drained
			Events:         1,
			EventsDone:     1,
			InitDoneAt:     &hourAgo,
			ListingStarted: true,
		}, policy.SwitchWithDowntime{
			Window: policy.Window{
				Cron: stringPtr("@5minutes"),
			},
			CreatedAt: hourAgo,
			// started 1 hour ago
			LastStartedAt: &hourAgo,
			LastStatus:    policy.StatusInProgress,
		})
		r.NoError(err)
		r.True(nextState.retryLater)
		r.EqualValues(policy.StatusCheckInProgress, nextState.nextState.status)
		r.Nil(nextState.nextState.startedAt)
		r.Nil(nextState.nextState.doneAt)
		policyMock.AssertExpectations(t)
	})
	t.Run("check_in_progress wait for check complete", func(t *testing.T) {
		r := require.New(t)
		policyMock := &policy.MockService{}
		worker.policySvc = policyMock
		// mock that check is still in progress
		checkResultIsInProgress = true

		nextState, err := worker.processSwitchWithDowntimeState(ctx, id, policy.ReplicationPolicyStatus{
			CreatedAt: hourAgo,
			IsPaused:  false,
			// init done
			InitObjListed: 1,
			InitObjDone:   1,
			// queue drained
			Events:         1,
			EventsDone:     1,
			InitDoneAt:     &hourAgo,
			ListingStarted: true,
		}, policy.SwitchWithDowntime{
			Window: policy.Window{
				Cron: stringPtr("@5minutes"),
			},
			CreatedAt: hourAgo,
			// started 1 hour ago
			LastStartedAt: &hourAgo,
			LastStatus:    policy.StatusCheckInProgress,
		})
		r.NoError(err)
		r.True(nextState.retryLater)
		r.Empty(nextState.nextState.status, "no status change")
		r.Nil(nextState.nextState.startedAt)
		r.Nil(nextState.nextState.doneAt)
		policyMock.AssertExpectations(t)
	})
	t.Run("check_in_progress to error duration exceeded", func(t *testing.T) {
		r := require.New(t)
		policyMock := &policy.MockService{}
		worker.policySvc = policyMock
		// mock that check is still in progress
		checkResultIsInProgress = true

		nextState, err := worker.processSwitchWithDowntimeState(ctx, id, policy.ReplicationPolicyStatus{
			CreatedAt: hourAgo,
			IsPaused:  false,
			// init done
			InitObjListed: 1,
			InitObjDone:   1,
			// queue drained
			Events:         1,
			EventsDone:     1,
			InitDoneAt:     &hourAgo,
			ListingStarted: true,
		}, policy.SwitchWithDowntime{
			Window: policy.Window{
				Cron: stringPtr("@5minutes"),
				// max duration is 59 minutes
				MaxDuration: durationPtr(time.Minute * 59),
			},
			CreatedAt: hourAgo,
			// started 1 hour ago
			LastStartedAt: &hourAgo,
			LastStatus:    policy.StatusCheckInProgress,
		})
		r.NoError(err)
		r.True(nextState.retryLater)
		r.EqualValues(policy.StatusError, nextState.nextState.status)
		r.Nil(nextState.nextState.startedAt)
		r.Nil(nextState.nextState.doneAt)
		policyMock.AssertExpectations(t)
	})
	t.Run("check_in_progress to error buckets not equal", func(t *testing.T) {
		r := require.New(t)
		policyMock := &policy.MockService{}
		worker.policySvc = policyMock
		// mock that check is done and not equal
		checkResultIsInProgress = false
		checkResultIsEqual = false

		nextState, err := worker.processSwitchWithDowntimeState(ctx, id, policy.ReplicationPolicyStatus{
			CreatedAt: hourAgo,
			IsPaused:  false,
			// init done
			InitObjListed: 1,
			InitObjDone:   1,
			// queue drained
			Events:         1,
			EventsDone:     1,
			InitDoneAt:     &hourAgo,
			ListingStarted: true,
		}, policy.SwitchWithDowntime{
			Window: policy.Window{
				Cron: stringPtr("@5minutes"),
			},
			CreatedAt: hourAgo,
			// started 1 hour ago
			LastStartedAt: &hourAgo,
			LastStatus:    policy.StatusCheckInProgress,
		})
		r.NoError(err)
		r.True(nextState.retryLater)
		r.EqualValues(policy.StatusError, nextState.nextState.status)
		r.Nil(nextState.nextState.startedAt)
		r.Nil(nextState.nextState.doneAt)
		policyMock.AssertExpectations(t)
	})
	t.Run("check_in_progress to done", func(t *testing.T) {
		r := require.New(t)
		policyMock := &policy.MockService{}
		// mock that check is done and buckets are equal
		checkResultIsInProgress = false
		checkResultIsEqual = true
		//mock policy CompleteReplicationSwitchWithDowntime
		policyMock.On("CompleteReplicationSwitchWithDowntime", mock.Anything, id, true).Return(nil).Once()
		worker.policySvc = policyMock

		nextState, err := worker.processSwitchWithDowntimeState(ctx, id, policy.ReplicationPolicyStatus{
			CreatedAt: hourAgo,
			IsPaused:  false,
			// init done
			InitObjListed: 1,
			InitObjDone:   1,
			// queue drained
			Events:         1,
			EventsDone:     1,
			InitDoneAt:     &hourAgo,
			ListingStarted: true,
		}, policy.SwitchWithDowntime{
			Window: policy.Window{
				Cron: stringPtr("@5minutes"),
			},
			ContinueReplication: true,
			CreatedAt:           hourAgo,
			// started 1 hour ago
			LastStartedAt: &hourAgo,
			LastStatus:    policy.StatusCheckInProgress,
		})
		r.NoError(err)
		r.False(nextState.retryLater, "no retry - terminal state")
		r.EqualValues(policy.StatusDone, nextState.nextState.status)
		r.Nil(nextState.nextState.startedAt)
		r.NotNil(nextState.nextState.doneAt)
		policyMock.AssertExpectations(t)
	})
	t.Run("done to done", func(t *testing.T) {
		r := require.New(t)
		policyMock := &policy.MockService{}
		//mock policy CompleteReplicationSwitchWithDowntime
		policyMock.On("CompleteReplicationSwitchWithDowntime", mock.Anything, id, true).Return(nil).Once()
		worker.policySvc = policyMock

		nextState, err := worker.processSwitchWithDowntimeState(ctx, id, policy.ReplicationPolicyStatus{
			CreatedAt: hourAgo,
			IsPaused:  false,
			// init done
			InitObjListed: 1,
			InitObjDone:   1,
			// queue drained
			Events:         1,
			EventsDone:     1,
			InitDoneAt:     &hourAgo,
			ListingStarted: true,
		}, policy.SwitchWithDowntime{
			Window: policy.Window{
				Cron: stringPtr("@5minutes"),
			},
			ContinueReplication: true,
			CreatedAt:           hourAgo,
			// started 1 hour ago
			LastStartedAt: &hourAgo,
			LastStatus:    policy.StatusDone,
		})
		r.NoError(err)
		r.False(nextState.retryLater, "no retry - terminal state")
		r.EqualValues(policy.StatusDone, nextState.nextState.status)
		r.Nil(nextState.nextState.startedAt)
		r.NotNil(nextState.nextState.doneAt)
		policyMock.AssertExpectations(t)
	})
}

func stringPtr(s string) *string {
	return &s
}

func uint32Ptr(i uint32) *uint32 {
	return &i
}

func durationPtr(d time.Duration) *time.Duration {
	return &d
}
