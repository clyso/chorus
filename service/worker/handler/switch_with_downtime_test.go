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
	inHour := now.Add(time.Hour)

	ctx := context.Background()
	id := policy.ReplicationID{
		User:     "user",
		Bucket:   "bucket",
		From:     "from",
		To:       "to",
		ToBucket: stringPtr("toBucket"),
	}

	t.Run("not_started", func(t *testing.T) {
		t.Run("if state error - cleanup block", func(t *testing.T) {
			r := require.New(t)
			policyMock := &policy.MockService{}
			policyMock.On("DeleteRoutingBlock", mock.Anything, "from", "bucket").Return(nil).Once()
			worker.policySvc = policyMock

			nextState, err := worker.processSwitchWithDowntimeState(ctx, id, policy.ReplicationPolicyStatus{}, policy.SwitchWithDowntime{
				Window: policy.Window{
					StartAt: &inHour,
				},
				CreatedAt:  now,
				LastStatus: policy.StatusError,
			})
			r.NoError(err)
			r.True(nextState.retryLater)
			r.Empty(nextState.nextState.status)
			policyMock.AssertExpectations(t)
		})
	})

}

func stringPtr(s string) *string {
	return &s
}
