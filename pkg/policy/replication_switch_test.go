package policy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/testutil"
)

func timePtr(t time.Time) *time.Time {
	return &t
}

func stringPtr(s string) *string {
	return &s
}

func uint32Ptr(u uint32) *uint32 {
	return &u
}

func TestSwitchWithDowntime_IsTimeToStart(t *testing.T) {
	type fields struct {
		Window        entity.ReplicationSwitchDowntimeOpts
		LastStatus    entity.ReplicationSwitchStatus
		CreatedAt     time.Time
		LastStartedAt *time.Time
	}
	tests := []struct {
		name        string
		currentTime time.Time
		fields      fields
		want        bool
		wantErr     bool
	}{
		{
			name: "nothing set - start now",
			fields: fields{
				Window: entity.ReplicationSwitchDowntimeOpts{
					Cron:    nil,
					StartAt: nil,
				},
				LastStartedAt: nil,
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "all empty - start now",
			fields: fields{
				Window: entity.ReplicationSwitchDowntimeOpts{
					Cron:    stringPtr(""),
					StartAt: timePtr(time.Time{}),
				},
				LastStartedAt: nil,
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "error - both cron and startAt set",
			fields: fields{
				Window: entity.ReplicationSwitchDowntimeOpts{
					Cron:    stringPtr("0 * * * *"),
					StartAt: timePtr(time.Now()),
				},
				LastStartedAt: nil,
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "error - both cron and startAt set",
			fields: fields{
				Window: entity.ReplicationSwitchDowntimeOpts{
					Cron:    stringPtr("0 * * * *"),
					StartAt: timePtr(time.Now()),
				},
				LastStartedAt: nil,
			},
			want:    false,
			wantErr: true,
		},
		// startAt tests:
		{
			name:        "not start: startAt is in the future",
			currentTime: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
			fields: fields{
				Window: entity.ReplicationSwitchDowntimeOpts{
					StartAt: timePtr(time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC)),
				},
				LastStartedAt: nil,
			},
			want:    false,
			wantErr: false,
		},
		{
			name:        "start: startAt is in the past",
			currentTime: time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
			fields: fields{
				Window: entity.ReplicationSwitchDowntimeOpts{
					StartAt: timePtr(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)),
				},
				LastStartedAt: nil,
			},
			want:    true,
			wantErr: false,
		},
		// cron tests:
		{
			name:        "no start: cron first start not expired",
			currentTime: time.Date(2021, 1, 1, 0, 59, 0, 0, time.UTC), // 00:59
			fields: fields{
				Window: entity.ReplicationSwitchDowntimeOpts{
					Cron: stringPtr("0 * * * *"), // every hour
				},
				// first start
				LastStartedAt: nil,
				CreatedAt:     time.Date(2021, 1, 1, 0, 0, 1, 0, time.UTC), // 00:00:01 - next start at 01:00
			},
			want:    false,
			wantErr: false,
		},
		{
			name:        "start: cron first start expired",
			currentTime: time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC), // 01:00
			fields: fields{
				Window: entity.ReplicationSwitchDowntimeOpts{
					Cron: stringPtr("0 * * * *"), // every hour
				},
				// first start
				LastStartedAt: nil,
				CreatedAt:     time.Date(2021, 1, 1, 0, 0, 1, 0, time.UTC), // 00:00:01 - next start at 01:00
			},
			want:    true,
			wantErr: false,
		},
		{
			name:        "no start: cron recurring start not expired",
			currentTime: time.Date(2021, 1, 1, 0, 59, 0, 0, time.UTC), // 00:59
			fields: fields{
				Window: entity.ReplicationSwitchDowntimeOpts{
					Cron: stringPtr("0 * * * *"), // every hour
				},
				// first start
				LastStartedAt: timePtr(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)), // 00:00
				CreatedAt:     time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),          // ignored in favor of LastStartedAt
			},
			want:    false,
			wantErr: false,
		},
		{
			name:        "start: cron recurring start is expired",
			currentTime: time.Date(2021, 1, 1, 1, 1, 0, 0, time.UTC), // 01:01
			fields: fields{
				Window: entity.ReplicationSwitchDowntimeOpts{
					Cron: stringPtr("0 * * * *"), // every hour
				},
				// first start
				LastStartedAt: timePtr(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)), // 00:00
				CreatedAt:     time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),          // ignored in favor of LastStartedAt
			},
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.currentTime.IsZero() {
				// setup current time mock:
				entity.TimeNow = func() time.Time {
					return tt.currentTime
				}
				defer func() {
					entity.TimeNow = time.Now
				}()
			}
			s := &entity.ReplicationSwitchInfo{
				ReplicationSwitchDowntimeOpts: tt.fields.Window,
				LastStatus:                    tt.fields.LastStatus,
				LastStartedAt:                 tt.fields.LastStartedAt,
				CreatedAt:                     tt.fields.CreatedAt,
			}
			got, err := s.IsTimeToStart()
			if (err != nil) != tt.wantErr {
				t.Errorf("SwitchWithDowntime.IsTimeToStart() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SwitchWithDowntime.IsTimeToStart() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPolicySvc_UpdateDowntimeSwitchOpts(t *testing.T) {
	client := testutil.SetupRedis(t)
	ctx := context.TODO()
	queuesMock := &tasks.QueueServiceMock{}
	tasks.Reset(queuesMock)
	svc := NewService(client, queuesMock)
	replID := entity.ReplicationStatusID{
		User:        "u",
		FromBucket:  "b",
		FromStorage: "f",
		ToStorage:   "t",
		ToBucket:    "b",
	}
	replSwitchID := entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket)

	tests := []struct {
		name    string
		before  *entity.ReplicationSwitchDowntimeOpts
		opts    *entity.ReplicationSwitchDowntimeOpts
		wantErr bool
	}{
		{
			name: "set switch with optional values",
			opts: &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone: true,
				Cron:            stringPtr("0 0 * * *"),
				MaxDuration:     (10 * time.Second),
				MaxEventLag:     nil, // Should trigger HDEL
			},
			wantErr: false,
		},
		{
			name: "set switch with all values",
			opts: &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone:     true,
				Cron:                stringPtr("0 0 * * *"),
				StartAt:             timePtr(time.Now()),
				MaxDuration:         (10 * time.Second),
				MaxEventLag:         uint32Ptr(100),
				SkipBucketCheck:     true,
				ContinueReplication: true,
			},
			wantErr: false,
		},
		{
			name: "update existing data with non-empty opts",
			before: &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone:     true,
				Cron:                stringPtr("0 0 * * *"),
				StartAt:             timePtr(time.Now()),
				MaxDuration:         (10 * time.Second),
				MaxEventLag:         uint32Ptr(100),
				SkipBucketCheck:     true,
				ContinueReplication: true,
			},
			opts: &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone:     false,
				Cron:                stringPtr("1 1 * * *"),
				StartAt:             timePtr(time.Now().Add(1 * time.Hour)),
				MaxDuration:         (18 * time.Second),
				MaxEventLag:         uint32Ptr(123),
				SkipBucketCheck:     false,
				ContinueReplication: false,
			},
			wantErr: false,
		},
		{
			name: "update existing data with empty opts",
			before: &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone:     true,
				Cron:                stringPtr("0 0 * * *"),
				StartAt:             timePtr(time.Now()),
				MaxDuration:         (10 * time.Second),
				MaxEventLag:         uint32Ptr(100),
				SkipBucketCheck:     true,
				ContinueReplication: true,
			},
			opts:    &entity.ReplicationSwitchDowntimeOpts{}, // All pointers nil, bools false
			wantErr: false,
		},
		{
			name: "delete existing data with nil opts",
			before: &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone:     true,
				Cron:                stringPtr("0 0 * * *"),
				StartAt:             timePtr(time.Now()),
				MaxDuration:         (10 * time.Second),
				MaxEventLag:         uint32Ptr(100),
				SkipBucketCheck:     true,
				ContinueReplication: true,
			},
			opts:    nil,
			wantErr: false,
		},
		{
			name: "update existing data with new values",
			before: &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone: true,
				Cron:            stringPtr("0 0 * * *"),
				MaxDuration:     (10 * time.Second),
				MaxEventLag:     uint32Ptr(100),
			},
			opts: &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone: false,
				Cron:            stringPtr("1 1 * * *"),
				MaxDuration:     (20 * time.Second),
				MaxEventLag:     nil, // Should delete this field
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear Redis state
			if err := client.FlushAll(ctx).Err(); err != nil {
				t.Fatalf("failed to flush Redis: %v", err)
			}
			// reset queues mock
			tasks.Reset(queuesMock)

			// Set initial state if provided
			if tt.before != nil {
				if err := svc.updateDowntimeSwitchOpts(ctx, replID, tt.before); err != nil {
					t.Fatalf("failed to set initial state: %v", err)
				}
			}

			// Run the update
			err := svc.updateDowntimeSwitchOpts(ctx, replID, tt.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("updateDowntimeSwitchOpts() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			switchKey, err := svc.replicationSwitchStore.MakeKey(replSwitchID)
			assert.NoError(t, err)

			if tt.opts == nil {
				// Check if all fields were deleted
				res, err := client.HGetAll(ctx, switchKey).Result()
				assert.NoError(t, err)
				assert.Empty(t, res)
			} else {
				// Check if all fields were set correctly
				var got entity.ReplicationSwitchDowntimeOpts
				err = client.HGetAll(ctx, switchKey).Scan(&got)
				assert.NoError(t, err)
				if tt.opts.StartAt != nil || got.StartAt != nil {
					if tt.opts.StartAt == nil || got.StartAt == nil {
						t.Errorf("StartAt mismatch: got %v, want %v", got.StartAt, tt.opts.StartAt)
						return
					}
					assert.True(t, got.StartAt.Truncate(time.Second).Equal(tt.opts.StartAt.Truncate(time.Second)),
						"StartAt mismatch: got %v, want %v", got.StartAt, tt.opts.StartAt)
					// Temporarily nil out StartAt for the Equal check
					got.StartAt = nil
					tt.opts.StartAt = nil
				}
				assert.EqualValues(t, *tt.opts, got)
			}
		})
	}
}

func Test_policySvc_SetDowntimeReplicationSwitch(t *testing.T) {
	c := testutil.SetupRedis(t)
	ctx := context.TODO()

	queuesMock := &tasks.QueueServiceMock{}
	tasks.Reset(queuesMock)
	svc := NewService(c, queuesMock)
	replID := entity.ReplicationStatusID{
		User:        "u",
		FromBucket:  "b",
		FromStorage: "f",
		ToStorage:   "t",
		ToBucket:    "b",
	}
	replIDCopy := replID
	replIDCopy.ToStorage = "asdf"
	validSwitch := &entity.ReplicationSwitchDowntimeOpts{
		StartOnInitDone:     false,
		Cron:                stringPtr("0 0 * * *"),
		StartAt:             nil,
		MaxDuration:         (3 * time.Hour),
		MaxEventLag:         uint32Ptr(100),
		SkipBucketCheck:     true,
		ContinueReplication: true,
	}
	//setup time mock
	testTime := time.Now()
	entity.TimeNow = func() time.Time {
		return testTime
	}
	defer func() {
		entity.TimeNow = time.Now
	}()

	t.Run("create new", func(t *testing.T) {
		t.Run("validate against replication policy", func(t *testing.T) {
			r := require.New(t)
			// cleanup redis
			r.NoError(c.FlushAll(ctx).Err())
			// reset queues mock
			tasks.Reset(queuesMock)

			// canot create switch for non-existing replication
			err := svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.Error(err, "replication not exists")

			// create replication but to other destination
			r.NoError(svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket), replID.FromStorage, true))
			r.NoError(svc.AddBucketReplicationPolicy(ctx, replIDCopy, nil))
			queuesMock.InitReplicationInProgress(replIDCopy)
			// try again and get error
			err = svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.Error(err, "replication not exists")
			_, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.Error(err, "switch was not created")

			//create correct replication but now there are 2 destinations which is not allowed
			r.NoError(svc.AddBucketReplicationPolicy(ctx, replID, nil))
			queuesMock.InitReplicationInProgress(replID)
			// try again and get error
			err = svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.Error(err, "only one destination allowed")
			_, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.Error(err, "switch was not created")

			// delete first replication and check that it works
			r.NoError(svc.DeleteReplication(ctx, replIDCopy))
			err = svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.NoError(err, "success")
			_, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.NoError(err, "switch was created")
		})
		t.Run("replication using agent not allowed", func(t *testing.T) {
			r := require.New(t)
			// cleanup redis
			r.NoError(c.FlushAll(ctx).Err())
			// reset queues mock
			tasks.Reset(queuesMock)

			//create replication with agent which is not allowed
			r.NoError(svc.AddBucketReplicationPolicy(ctx, replID, stringPtr("http://example.com")))
			queuesMock.InitReplicationInProgress(replID)
			err := svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.Error(err, "replication using agent")
			_, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.Error(err, "switch was not created")

			// check that similar replication without agent works
			r.NoError(svc.DeleteReplication(ctx, replID))
			r.NoError(svc.AddBucketReplicationPolicy(ctx, replID, nil))
			queuesMock.InitReplicationInProgress(replID)
			err = svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.NoError(err, "success")
		})
		t.Run("err: init replication not done for immediate switch", func(t *testing.T) {
			r := require.New(t)
			// cleanup redis
			r.NoError(c.FlushAll(ctx).Err())
			// reset queues mock
			tasks.Reset(queuesMock)
			queuesMock.InitReplicationInProgress(replID)
			err := svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.Error(err, "replication not exists")

			// create replication with init not done
			r.NoError(svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket), replID.FromStorage, true))
			r.NoError(svc.AddBucketReplicationPolicy(ctx, replID, nil))

			// create switch with immediate start
			immediateSwitch := &entity.ReplicationSwitchDowntimeOpts{}
			err = svc.SetDowntimeReplicationSwitch(ctx, replID, immediateSwitch)
			r.Error(err, "init replication not done")
		})
		t.Run("success crud", func(t *testing.T) {
			r := require.New(t)
			// cleanup redis
			r.NoError(c.FlushAll(ctx).Err())
			// reset queues mock
			tasks.Reset(queuesMock)

			// create replication
			r.NoError(svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket), replID.FromStorage, true))
			r.NoError(svc.AddBucketReplicationPolicy(ctx, replID, nil))
			queuesMock.InitReplicationInProgress(replID)

			// create switch
			err := svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.NoError(err)
			// check that switch was created
			got, err := svc.GetReplicationSwitchInfo(ctx, replID)
			r.NoError(err)
			r.False(got.IsZeroDowntime())
			r.True(testTime.Equal(got.CreatedAt))
			gotID := got.ReplicationID()
			r.NoError(err)
			r.Equal(replID, gotID)
			r.Equal(entity.StatusNotStarted, got.LastStatus)
			r.EqualValues(*validSwitch, got.ReplicationSwitchDowntimeOpts)
			_, err = svc.GetInProgressZeroDowntimeSwitchInfo(ctx, entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket))
			r.ErrorIs(err, dom.ErrNotFound, "no in progress zero downtime switch")
			// check that routing policy was not changed
			routeToStorage, err := svc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket))
			r.NoError(err)
			r.Equal(replID.FromStorage, routeToStorage, "routing policy was not changed")
			// check that replication policy was not changed
			replications, err := svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(replID.User, replID.FromBucket))
			r.NoError(err)
			r.Len(replications.Destinations, 1)
			r.Equal(entity.NewBucketReplicationPolicyDestination(replID.ToStorage, replID.ToBucket), replications.Destinations[0])
			_, err = svc.GetInProgressZeroDowntimeSwitchInfo(ctx, entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket))
			r.ErrorIs(err, dom.ErrNotFound, "no in progress switch zero downtime")

			//delete switch
			r.NoError(svc.DeleteReplicationSwitch(ctx, replID))
			_, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.ErrorIs(err, dom.ErrNotFound, "switch was deleted")
			// check that routing policy was not changed
			routeToStorage, err = svc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket))
			r.NoError(err)
			r.Equal(replID.FromStorage, routeToStorage, "routing policy was not changed")
			// check that replication policy was not changed
			replications, err = svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(replID.User, replID.FromBucket))
			r.NoError(err)
			r.Len(replications.Destinations, 1)
			r.Equal(entity.NewBucketReplicationPolicyDestination(replID.ToStorage, replID.ToBucket), replications.Destinations[0])
		})
	})
	t.Run("update existing", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			r := require.New(t)
			// cleanup redis
			r.NoError(c.FlushAll(ctx).Err())
			// reset queues mock
			tasks.Reset(queuesMock)

			// create replication
			r.NoError(svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket), replID.FromStorage, true))
			r.NoError(svc.AddBucketReplicationPolicy(ctx, replID, nil))
			queuesMock.InitReplicationInProgress(replID)

			// create existing switch
			err := svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.NoError(err)
			existing, err := svc.GetReplicationSwitchInfo(ctx, replID)
			r.NoError(err)

			// update switch
			inHour := time.Now().Add(1 * time.Hour)
			updated := &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone:     true,
				Cron:                nil,
				StartAt:             &inHour,
				MaxDuration:         7 * time.Minute,
				MaxEventLag:         uint32Ptr(200),
				SkipBucketCheck:     false,
				ContinueReplication: true,
			}
			err = svc.SetDowntimeReplicationSwitch(ctx, replID, updated)
			r.NoError(err)

			got, err := svc.GetReplicationSwitchInfo(ctx, replID)
			r.NoError(err)
			r.False(got.IsZeroDowntime())
			r.True(existing.CreatedAt.Equal(got.CreatedAt), "created at should not change")
			gotID := got.ReplicationID()
			r.NoError(err)
			r.Equal(replID, gotID)
			r.Equal(existing.ReplicationIDStr, got.ReplicationIDStr)
			r.Equal(entity.StatusNotStarted, got.LastStatus)
			r.Equal(existing.LastStatus, got.LastStatus)
			// compare StartAt time separately:
			r.True(inHour.Equal(*got.StartAt))
			r.True(updated.StartAt.Equal(*got.StartAt))
			got.StartAt = nil
			updated.StartAt = nil
			r.EqualValues(*updated, got.ReplicationSwitchDowntimeOpts)

			// update with empty opts
			err = svc.SetDowntimeReplicationSwitch(ctx, replID, &entity.ReplicationSwitchDowntimeOpts{})
			r.NoError(err)
			got, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.NoError(err)
			r.False(got.IsZeroDowntime())
			r.True(existing.CreatedAt.Equal(got.CreatedAt), "created at should not change")
			r.EqualValues(entity.ReplicationSwitchDowntimeOpts{}, got.ReplicationSwitchDowntimeOpts)
		})
		t.Run("err: existing switch is zero downtime", func(t *testing.T) {
			r := require.New(t)
			// cleanup redis
			r.NoError(c.FlushAll(ctx).Err())
			// reset queues mock
			tasks.Reset(queuesMock)
			// create replication
			r.NoError(svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket), replID.FromStorage, true))
			r.NoError(svc.AddBucketReplicationPolicy(ctx, replID, nil))
			queuesMock.InitReplicationInProgress(replID)
			// finish init replication
			r.NoError(svc.ObjListStarted(ctx, replID))
			queuesMock.InitReplicationDone(replID)
			// r.NoError(svc.IncReplInitObjListed(ctx, replID, 0, time.Now()))
			// r.NoError(svc.IncReplInitObjDone(ctx, replID, 0, time.Now()))

			// create zero downtime switch
			err := svc.AddZeroDowntimeReplicationSwitch(ctx, replID, &entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: 15*time.Minute + 30*time.Second})
			r.NoError(err)

			// try to update with downtime switch - should fail
			err = svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.Error(err)

			// switch is still zero downtime
			got, err := svc.GetReplicationSwitchInfo(ctx, replID)
			r.NoError(err)
			r.True(got.IsZeroDowntime())
		})
		t.Run("update switch in status", func(t *testing.T) {
			allStatuses := []entity.ReplicationSwitchStatus{
				entity.StatusNotStarted,
				entity.StatusInProgress,
				entity.StatusCheckInProgress,
				entity.StatusDone,
				entity.StatusSkipped,
				entity.StatusError,
			}
			inHour := time.Now().Add(1 * time.Hour)
			updated := &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone:     true,
				Cron:                nil,
				StartAt:             &inHour,
				MaxDuration:         7 * time.Minute,
				MaxEventLag:         uint32Ptr(200),
				SkipBucketCheck:     false,
				ContinueReplication: true,
			}
			for _, status := range allStatuses {
				t.Run(string(status), func(t *testing.T) {
					r := require.New(t)
					r.NoError(c.FlushAll(ctx).Err())
					// reset queues mock
					tasks.Reset(queuesMock)
					setupDowntimeSwitchState(t, svc, queuesMock, replID, validSwitch, status)
					if status == entity.StatusInProgress || status == entity.StatusCheckInProgress || status == entity.StatusDone {
						// update not allowed
						err := svc.SetDowntimeReplicationSwitch(ctx, replID, updated)
						r.Error(err)
					} else {
						// update allowed
						err := svc.SetDowntimeReplicationSwitch(ctx, replID, updated)
						r.NoError(err)

						got, err := svc.GetReplicationSwitchInfo(ctx, replID)
						r.NoError(err)
						r.False(got.IsZeroDowntime())
						r.Equal(status, got.LastStatus)
						// compare StartAt time separately:
						r.NotNil(got.StartAt, got) //fails skip and err
						r.True(inHour.Equal(*got.StartAt))
						r.True(updated.StartAt.Equal(*got.StartAt))
						got.StartAt = nil
						updCopy := *updated
						updCopy.StartAt = nil
						r.EqualValues(updCopy, got.ReplicationSwitchDowntimeOpts)
					}
				})
			}
		})
	})
}

func Test_policySvc_AddZeroDowntimeSwitch(t *testing.T) {
	c := testutil.SetupRedis(t)
	ctx := context.TODO()

	queuesMock := &tasks.QueueServiceMock{}
	tasks.Reset(queuesMock)
	svc := NewService(c, queuesMock)
	replID := entity.ReplicationStatusID{
		User:        "u",
		FromBucket:  "b",
		FromStorage: "f",
		ToStorage:   "t",
		ToBucket:    "b",
	}
	replIDCopy := replID
	replIDCopy.ToStorage = "asdf"
	validSwitch := &entity.ReplicationSwitchZeroDowntimeOpts{
		MultipartTTL: 15*time.Minute + 30*time.Second,
	}
	//setup time mock
	testTime := time.Now()
	entity.TimeNow = func() time.Time {
		return testTime
	}
	defer func() {
		entity.TimeNow = time.Now
	}()

	t.Run("error cases", func(t *testing.T) {
		r := require.New(t)
		// cleanup redis
		r.NoError(c.FlushAll(ctx).Err())
		// reset queues mock
		tasks.Reset(queuesMock)

		// canot create switch for non-existing replication
		err := svc.AddZeroDowntimeReplicationSwitch(ctx, replID, validSwitch)
		r.Error(err, "replication not exists")

		// create replication but to wrong destination
		r.NoError(svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket), replID.FromStorage, true))
		r.NoError(svc.AddBucketReplicationPolicy(ctx, replIDCopy, nil))
		queuesMock.InitReplicationInProgress(replID)
		// try again
		err = svc.AddZeroDowntimeReplicationSwitch(ctx, replID, validSwitch)
		r.Error(err, "replication not exists")
		_, err = svc.GetReplicationSwitchInfo(ctx, replID)
		r.Error(err, "switch was not created")

		//create replication but now there are 2 destinations which is not allowed
		r.NoError(svc.AddBucketReplicationPolicy(ctx, replID, nil))
		queuesMock.InitReplicationInProgress(replID)
		// try again
		err = svc.AddZeroDowntimeReplicationSwitch(ctx, replID, validSwitch)
		r.Error(err, "only one destination allowed")
		_, err = svc.GetReplicationSwitchInfo(ctx, replID)
		r.Error(err, "switch was not created")

		// delete first replication and check that it works
		r.NoError(svc.DeleteReplication(ctx, replIDCopy))
		err = svc.AddZeroDowntimeReplicationSwitch(ctx, replID, validSwitch)
		r.Error(err, "replication not done")

		// finish init replication
		r.NoError(svc.ObjListStarted(ctx, replID))
		queuesMock.InitReplicationDone(replID)

		err = svc.AddZeroDowntimeReplicationSwitch(ctx, replID, validSwitch)
		r.NoError(err, "replication not done")
		_, err = svc.GetReplicationSwitchInfo(ctx, replID)
		r.NoError(err, "switch was created")

		err = svc.AddZeroDowntimeReplicationSwitch(ctx, replID, validSwitch)
		r.Error(err, "cannot update existing switch")
	})
	t.Run("success crud", func(t *testing.T) {
		r := require.New(t)
		// cleanup redis
		r.NoError(c.FlushAll(ctx).Err())
		// reset queues mock
		tasks.Reset(queuesMock)

		_, err := svc.GetInProgressZeroDowntimeSwitchInfo(ctx, entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket))
		r.ErrorIs(err, dom.ErrNotFound, "no in progress switch")

		// create replication
		r.NoError(svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket), replID.FromStorage, true))
		r.NoError(svc.AddBucketReplicationPolicy(ctx, replID, nil))
		queuesMock.InitReplicationInProgress(replID)
		// finish init replication
		r.NoError(svc.ObjListStarted(ctx, replID))
		queuesMock.InitReplicationDone(replID)

		// create switch
		err = svc.AddZeroDowntimeReplicationSwitch(ctx, replID, validSwitch)
		r.NoError(err)
		info, err := svc.GetInProgressZeroDowntimeSwitchInfo(ctx, entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket))
		r.NoError(err)
		r.EqualValues(validSwitch.MultipartTTL, info.MultipartTTL)
		r.EqualValues(replID, info.ReplID)
		r.EqualValues(entity.StatusInProgress, info.Status)

		got, err := svc.GetReplicationSwitchInfo(ctx, replID)
		r.NoError(err, "switch was created")

		r.True(testTime.Equal(got.CreatedAt))
		gotID := got.ReplicationID()
		r.NoError(err)
		r.Equal(replID, gotID)
		r.Equal(entity.StatusInProgress, got.LastStatus)
		r.EqualValues(*validSwitch, got.ReplicationSwitchZeroDowntimeOpts)
		r.EqualValues(entity.ReplicationSwitchDowntimeOpts{}, got.ReplicationSwitchDowntimeOpts, "zero downtime switch should not have downtime switch")
		r.True(got.IsZeroDowntime())
		r.NotNil(got.LastStartedAt)
		r.True(testTime.Equal(*got.LastStartedAt))

		// check that routing policy was changed
		routeToStorage, err := svc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket))
		r.NoError(err)
		r.Equal(replID.ToStorage, routeToStorage, "routing policy was changed")
		// check that replication policy was archived
		_, err = svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(replID.User, replID.FromBucket))
		r.ErrorIs(err, dom.ErrNotFound)

		// but repliction metadata should be still there
		repl, err := svc.GetReplicationPolicyInfo(ctx, replID)
		r.NoError(err)
		r.True(repl.IsArchived)
		r.NotNil(repl.ArchivedAt)
		r.True(repl.ListingStarted)
		//delete metadata
		r.NoError(svc.DeleteReplication(ctx, replID))
		_, err = svc.GetReplicationPolicyInfo(ctx, replID)
		r.ErrorIs(err, dom.ErrNotFound)

		// delete switch
		r.NoError(svc.DeleteReplicationSwitch(ctx, replID))

		_, err = svc.GetInProgressZeroDowntimeSwitchInfo(ctx, entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket))
		r.ErrorIs(err, dom.ErrNotFound, "no in progress switch")
		_, err = svc.GetReplicationSwitchInfo(ctx, replID)
		r.ErrorIs(err, dom.ErrNotFound, "switch was deleted")
		// check that routing policy was changed back
		routeToStorage, err = svc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket))
		r.NoError(err)
		r.Equal(replID.FromStorage, routeToStorage, "routing policy was changed back")
	})
	t.Run("complete", func(t *testing.T) {
		r := require.New(t)
		// cleanup redis
		r.NoError(c.FlushAll(ctx).Err())
		// reset queues mock
		tasks.Reset(queuesMock)

		// create replication
		r.NoError(svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket), replID.FromStorage, true))
		r.NoError(svc.AddBucketReplicationPolicy(ctx, replID, nil))
		queuesMock.InitReplicationInProgress(replID)
		// finish init replication
		r.NoError(svc.ObjListStarted(ctx, replID))
		queuesMock.InitReplicationDone(replID)

		// create switch
		err := svc.AddZeroDowntimeReplicationSwitch(ctx, replID, validSwitch)
		r.NoError(err)
		info, err := svc.GetInProgressZeroDowntimeSwitchInfo(ctx, entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket))
		r.NoError(err)
		r.EqualValues(validSwitch.MultipartTTL, info.MultipartTTL)

		got, err := svc.GetReplicationSwitchInfo(ctx, replID)
		r.NoError(err, "switch was created")
		r.True(got.IsZeroDowntime())
		r.Equal(entity.StatusInProgress, got.LastStatus)
		r.NotNil(got.LastStartedAt)
		r.Empty(got.History)

		// check that routing policy was changed
		routeToStorage, err := svc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket))
		r.NoError(err)
		r.Equal(replID.ToStorage, routeToStorage, "routing policy was changed")
		// check that replication policy was archived
		_, err = svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(replID.User, replID.FromBucket))
		r.ErrorIs(err, dom.ErrNotFound)

		// but repliction metadata should be still there
		repl, err := svc.GetReplicationPolicyInfo(ctx, replID)
		r.NoError(err)
		r.True(repl.IsArchived)

		// complete switch
		r.NoError(svc.CompleteZeroDowntimeReplicationSwitch(ctx, replID))

		_, err = svc.GetInProgressZeroDowntimeSwitchInfo(ctx, entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket))
		r.ErrorIs(err, dom.ErrNotFound, "no in progress switch")

		got, err = svc.GetReplicationSwitchInfo(ctx, replID)
		r.NoError(err, "switch was completed")
		r.True(got.IsZeroDowntime())
		r.Equal(entity.StatusDone, got.LastStatus)
		r.NotNil(got.LastStartedAt)
		r.NotNil(got.DoneAt)
		r.Len(got.History, 1)

		// delete switch
		r.NoError(svc.DeleteReplicationSwitch(ctx, replID))

		_, err = svc.GetInProgressZeroDowntimeSwitchInfo(ctx, entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket))
		r.ErrorIs(err, dom.ErrNotFound, "no in progress switch")
		_, err = svc.GetReplicationSwitchInfo(ctx, replID)
		r.ErrorIs(err, dom.ErrNotFound, "switch was deleted")
		// check that routing policy was not changed back
		routeToStorage, err = svc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket))
		r.NoError(err)
		r.Equal(replID.ToStorage, routeToStorage, "routing policy was not changed back")
	})
}

func Test_policySvc_UpdateDowntimeSwitchStatus(t *testing.T) {
	c := testutil.SetupRedis(t)
	ctx := context.TODO()

	queuesMock := &tasks.QueueServiceMock{}
	tasks.Reset(queuesMock)
	svc := NewService(c, queuesMock)
	replID := entity.ReplicationStatusID{
		User:        "u",
		FromBucket:  "b",
		FromStorage: "f",
		ToStorage:   "t",
		ToBucket:    "b",
	}
	validSwitch := &entity.ReplicationSwitchDowntimeOpts{
		StartOnInitDone:     false,
		Cron:                stringPtr("0 0 * * *"),
		StartAt:             nil,
		MaxDuration:         (3 * time.Hour),
		MaxEventLag:         uint32Ptr(100),
		SkipBucketCheck:     true,
		ContinueReplication: true,
	}
	//setup time mock
	testTime := time.Now()
	entity.TimeNow = func() time.Time {
		return testTime
	}
	defer func() {
		entity.TimeNow = time.Now
	}()
	statuses := []entity.ReplicationSwitchStatus{
		entity.StatusNotStarted,
		entity.StatusInProgress,
		entity.StatusCheckInProgress,
		entity.StatusDone,
		entity.StatusSkipped,
		entity.StatusError,
	}
	t.Run("to not_started", func(t *testing.T) {
		// check all switch transition to not started
		for _, status := range statuses {
			t.Run("from "+string(status), func(t *testing.T) {
				r := require.New(t)
				// cleanup redis
				r.NoError(c.FlushAll(ctx).Err())
				// reset queues mock
				tasks.Reset(queuesMock)
				// create switch in status
				setupDowntimeSwitchState(t, svc, queuesMock, replID, validSwitch, status)
				// transition to not started not allowed for any status
				r.ErrorIs(svc.UpdateDowntimeSwitchStatus(ctx, replID, entity.StatusNotStarted, "test", nil, nil), dom.ErrInvalidArg, "transition not allowed")
			})
		}
	})
	t.Run("to in_progress", func(t *testing.T) {
		for _, status := range statuses {
			t.Run("from "+string(status), func(t *testing.T) {
				r := require.New(t)
				// cleanup redis
				r.NoError(c.FlushAll(ctx).Err())
				// reset queues mock
				tasks.Reset(queuesMock)
				// create switch in status
				setupDowntimeSwitchState(t, svc, queuesMock, replID, validSwitch, status)
				now := entity.TimeNow()

				err := svc.UpdateDowntimeSwitchStatus(ctx, replID, entity.StatusInProgress, "test", &now, nil)
				if status == entity.StatusNotStarted || status == entity.StatusSkipped || status == entity.StatusError {
					r.NoError(err, "transition is allowed")

					// validate switch state
					info, err := svc.GetReplicationSwitchInfo(ctx, replID)
					r.NoError(err)
					r.Equal(entity.StatusInProgress, info.LastStatus)
					r.NotNil(info.LastStartedAt)
					r.Nil(info.DoneAt)
					r.True(now.Equal(*info.LastStartedAt))
					r.NotEmpty(info.History)
					_, err = svc.GetInProgressZeroDowntimeSwitchInfo(ctx, entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket))
					r.ErrorIs(err, dom.ErrNotFound, "no zero downtime switch in progress")
					// check that routing is blocked
					_, err = svc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket))
					r.ErrorIs(err, dom.ErrRoutingBlock, "routing is blocked")
					// check that replication remains the same
					replications, err := svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(replID.User, replID.FromBucket))
					r.NoError(err)
					r.Len(replications.Destinations, 1)
					r.Equal(entity.NewBucketReplicationPolicyDestination(replID.ToStorage, replID.ToBucket), replications.Destinations[0])

				} else {
					r.ErrorIs(err, dom.ErrInvalidArg, "transition not allowed")
				}
			})
		}
	})
	t.Run("to check_in_progress", func(t *testing.T) {
		for _, status := range statuses {
			t.Run("from "+string(status), func(t *testing.T) {
				r := require.New(t)
				// cleanup redis
				r.NoError(c.FlushAll(ctx).Err())
				// reset queues mock
				tasks.Reset(queuesMock)
				// create switch in status
				setupDowntimeSwitchState(t, svc, queuesMock, replID, validSwitch, status)

				err := svc.UpdateDowntimeSwitchStatus(ctx, replID, entity.StatusCheckInProgress, "test", nil, nil)
				if status == entity.StatusInProgress {
					r.NoError(err, "transition is allowed")

					// validate switch state
					info, err := svc.GetReplicationSwitchInfo(ctx, replID)
					r.NoError(err)
					r.Equal(entity.StatusCheckInProgress, info.LastStatus)
					r.Nil(info.DoneAt)
					r.NotNil(info.LastStartedAt)
					r.NotEmpty(info.History)
					_, err = svc.GetInProgressZeroDowntimeSwitchInfo(ctx, entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket))
					r.ErrorIs(err, dom.ErrNotFound, "no zero downtime switch in progress")
					// check that routing is blocked
					_, err = svc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket))
					r.ErrorIs(err, dom.ErrRoutingBlock, "routing is blocked")
					// check that replication remains the same
					replications, err := svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(replID.User, replID.FromBucket))
					r.NoError(err)
					r.Len(replications.Destinations, 1)
					r.Equal(entity.NewBucketReplicationPolicyDestination(replID.ToStorage, replID.ToBucket), replications.Destinations[0])
				} else {
					r.ErrorIs(err, dom.ErrInvalidArg, "transition not allowed")
				}
			})
		}
	})
	t.Run("to done", func(t *testing.T) {
		for _, status := range statuses {
			t.Run("from "+string(status), func(t *testing.T) {
				r := require.New(t)
				// cleanup redis
				r.NoError(c.FlushAll(ctx).Err())
				// reset queues mock
				tasks.Reset(queuesMock)
				// create switch in status
				setupDowntimeSwitchState(t, svc, queuesMock, replID, validSwitch, status)
				now := entity.TimeNow()

				err := svc.UpdateDowntimeSwitchStatus(ctx, replID, entity.StatusDone, "test", nil, &now)
				if status == entity.StatusCheckInProgress || status == entity.StatusDone {
					r.NoError(err, "transition is allowed")

					// validate switch state
					info, err := svc.GetReplicationSwitchInfo(ctx, replID)
					r.NoError(err)
					r.Equal(entity.StatusDone, info.LastStatus)
					r.NotNil(info.DoneAt)
					r.NotNil(info.LastStartedAt)
					r.NotEmpty(info.History)
					_, err = svc.GetInProgressZeroDowntimeSwitchInfo(ctx, entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket))
					r.ErrorIs(err, dom.ErrNotFound, "no zero downtime switch in progress")
					// check that routing is switched
					routeToStorage, err := svc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket))
					r.NoError(err)
					r.Equal(replID.ToStorage, routeToStorage, "routing is switched")
				} else {
					r.ErrorIs(err, dom.ErrInvalidArg, "transition not allowed")
				}
			})
		}
	})
	t.Run("to skipped", func(t *testing.T) {
		for _, status := range statuses {
			t.Run("from "+string(status), func(t *testing.T) {
				r := require.New(t)
				// cleanup redis
				r.NoError(c.FlushAll(ctx).Err())
				// reset queues mock
				tasks.Reset(queuesMock)
				// create switch in status
				setupDowntimeSwitchState(t, svc, queuesMock, replID, validSwitch, status)

				err := svc.UpdateDowntimeSwitchStatus(ctx, replID, entity.StatusSkipped, "test", nil, nil)
				if status == entity.StatusNotStarted || status == entity.StatusSkipped || status == entity.StatusError {
					r.NoError(err, "transition is allowed")
					// validate switch state
					info, err := svc.GetReplicationSwitchInfo(ctx, replID)
					r.NoError(err)
					r.Equal(entity.StatusSkipped, info.LastStatus)
					r.Nil(info.DoneAt)
					r.NotEmpty(info.History)
					_, err = svc.GetInProgressZeroDowntimeSwitchInfo(ctx, entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket))
					r.ErrorIs(err, dom.ErrNotFound, "no zero downtime switch in progress")
					// check that routing to old bucket
					routeToStorage, err := svc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket))
					r.NoError(err)
					r.Equal(replID.FromStorage, routeToStorage, "routing is to old bucket")
					// check that replication remains the same
					replications, err := svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(replID.User, replID.FromBucket))
					r.NoError(err)
					r.Len(replications.Destinations, 1)
					r.Equal(entity.NewBucketReplicationPolicyDestination(replID.ToStorage, replID.ToBucket), replications.Destinations[0])
				} else {
					r.ErrorIs(err, dom.ErrInvalidArg, "transition not allowed")
				}
			})
		}
	})
	t.Run("to error", func(t *testing.T) {
		for _, status := range statuses {
			t.Run("from "+string(status), func(t *testing.T) {
				r := require.New(t)
				// cleanup redis
				r.NoError(c.FlushAll(ctx).Err())
				// reset queues mock
				tasks.Reset(queuesMock)
				// create switch in status
				setupDowntimeSwitchState(t, svc, queuesMock, replID, validSwitch, status)

				err := svc.UpdateDowntimeSwitchStatus(ctx, replID, entity.StatusError, "test", nil, nil)
				// transition to error allowed from all except done
				if status == entity.StatusDone {
					r.ErrorIs(err, dom.ErrInvalidArg, "transition not allowed")
				} else {
					r.NoError(err, "transition is allowed")
					// validate switch state
					info, err := svc.GetReplicationSwitchInfo(ctx, replID)
					r.NoError(err)
					r.Equal(entity.StatusError, info.LastStatus)
					r.Nil(info.DoneAt)
					r.NotEmpty(info.History)
					_, err = svc.GetInProgressZeroDowntimeSwitchInfo(ctx, entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket))
					r.ErrorIs(err, dom.ErrNotFound, "no zero downtime switch in progress")
					// check that routing to old bucket
					routeToStorage, err := svc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket))
					r.NoError(err)
					r.Equal(replID.FromStorage, routeToStorage, "routing is to old bucket")
					// check that replication remains the same
					replications, err := svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(replID.User, replID.FromBucket))
					r.NoError(err)
					r.Len(replications.Destinations, 1)
					r.Equal(entity.NewBucketReplicationPolicyDestination(replID.ToStorage, replID.ToBucket), replications.Destinations[0])
				}
			})
		}
	})

}

func setupDowntimeSwitchState(t *testing.T, svc Service, queuesMock *tasks.QueueServiceMock, replID entity.ReplicationStatusID, opts *entity.ReplicationSwitchDowntimeOpts, status entity.ReplicationSwitchStatus) {
	t.Helper()
	ctx := context.TODO()
	r := require.New(t)

	// create routing and replication
	r.NoError(svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket), replID.FromStorage, true))
	r.NoError(svc.AddBucketReplicationPolicy(ctx, replID, nil))
	queuesMock.InitReplicationInProgress(replID)
	// create switch
	err := svc.SetDowntimeReplicationSwitch(ctx, replID, opts)
	r.NoError(err)
	// check that switch was created in NOT_STARTED status
	got, err := svc.GetReplicationSwitchInfo(ctx, replID)
	r.NoError(err)
	r.False(got.IsZeroDowntime())
	r.Equal(replID, got.ReplID)
	r.Equal(entity.StatusNotStarted, got.LastStatus)
	r.Nil(got.LastStartedAt)
	// check that routing policy was not changed
	routeToStorage, err := svc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket))
	r.NoError(err)
	r.Equal(replID.FromStorage, routeToStorage, "routing policy was not changed")
	// check that replication policy was not changed
	replications, err := svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(replID.User, replID.FromBucket))
	r.NoError(err)
	r.Len(replications.Destinations, 1)
	r.Equal(entity.NewBucketReplicationPolicyDestination(replID.ToStorage, replID.ToBucket), replications.Destinations[0])

	if status == entity.StatusNotStarted {
		return
	}
	if status == entity.StatusSkipped || status == entity.StatusError {
		r.NoError(svc.UpdateDowntimeSwitchStatus(ctx, replID, status, "test", nil, nil))
		// check that switch was changed
		got, err := svc.GetReplicationSwitchInfo(ctx, replID)
		r.NoError(err)
		r.False(got.IsZeroDowntime())
		r.Equal(replID, got.ReplID)
		r.Equal(status, got.LastStatus)
		r.Nil(got.LastStartedAt)
		// check that routing policy was not changed
		routeToStorage, err := svc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket))
		r.NoError(err)
		r.Equal(replID.FromStorage, routeToStorage, "routing policy was not changed")
		// check that replication policy was not changed
		replications, err := svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(replID.User, replID.FromBucket))
		r.NoError(err)
		r.Len(replications.Destinations, 1)
		r.Equal(entity.NewBucketReplicationPolicyDestination(replID.ToStorage, replID.ToBucket), replications.Destinations[0])
		return
	}

	// move to in progress
	now := entity.TimeNow()
	r.NoError(svc.UpdateDowntimeSwitchStatus(ctx, replID, entity.StatusInProgress, "test", &now, nil))
	// check that switch was changed
	got, err = svc.GetReplicationSwitchInfo(ctx, replID)
	r.NoError(err)
	r.False(got.IsZeroDowntime())
	r.Equal(replID, got.ReplID)
	r.Equal(entity.StatusInProgress, got.LastStatus)
	r.NotNil(got.LastStartedAt)
	// check that routing was blocked
	_, err = svc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket))
	r.ErrorIs(err, dom.ErrRoutingBlock)
	// check that replication policy was not changed
	replications, err = svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(replID.User, replID.FromBucket))
	r.NoError(err)
	r.Len(replications.Destinations, 1)
	r.Equal(entity.NewBucketReplicationPolicyDestination(replID.ToStorage, replID.ToBucket), replications.Destinations[0])
	if status == entity.StatusInProgress {
		return
	}
	// move to check in progress
	r.NoError(svc.UpdateDowntimeSwitchStatus(ctx, replID, entity.StatusCheckInProgress, "test", nil, nil))
	// check that switch was changed
	got, err = svc.GetReplicationSwitchInfo(ctx, replID)
	r.NoError(err)
	r.False(got.IsZeroDowntime())
	r.Equal(replID, got.ReplID)
	r.Equal(entity.StatusCheckInProgress, got.LastStatus)
	r.NotNil(got.LastStartedAt)
	r.Nil(got.DoneAt)
	// check that routing was blocked
	_, err = svc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket))
	r.ErrorIs(err, dom.ErrRoutingBlock)
	// check that replication policy was not changed
	replications, err = svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(replID.User, replID.FromBucket))
	r.NoError(err)
	r.Len(replications.Destinations, 1)
	r.Equal(entity.NewBucketReplicationPolicyDestination(replID.ToStorage, replID.ToBucket), replications.Destinations[0])
	if status == entity.StatusCheckInProgress {
		return
	}
	// move to done
	r.NoError(svc.UpdateDowntimeSwitchStatus(ctx, replID, entity.StatusDone, "test", nil, &now))
	// check that switch was changed
	got, err = svc.GetReplicationSwitchInfo(ctx, replID)
	r.NoError(err)
	r.False(got.IsZeroDowntime())
	r.Equal(replID, got.ReplID)
	r.Equal(entity.StatusDone, got.LastStatus)
	r.NotNil(got.LastStartedAt)
	r.NotNil(got.DoneAt)
	// check that routing switched
	routeToStorage, err = svc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket))
	r.NoError(err)
	r.Equal(replID.ToStorage, routeToStorage, "routing policy was changed")
	// check that replication policy was archived
	repl, err := svc.GetReplicationPolicyInfo(ctx, replID)
	r.NoError(err)
	r.True(repl.IsArchived)
	r.NotNil(repl.ArchivedAt)
	replications, err = svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(replID.User, replID.FromBucket))
	if opts != nil && opts.ContinueReplication {
		r.NoError(err)
		r.Equal(replID.ToStorage, replications.FromStorage)
		r.Len(replications.Destinations, 1)
		r.EqualValues(entity.NewBucketReplicationPolicyDestination(replID.FromStorage, replID.FromBucket), replications.Destinations[0])

		replBackID := replID
		replBackID.FromStorage, replBackID.ToStorage = replBackID.ToStorage, replBackID.FromStorage
		repl, err := svc.GetReplicationPolicyInfo(ctx, replBackID)
		r.NoError(err)
		r.False(repl.IsArchived)
	} else {
		r.ErrorIs(err, dom.ErrNotFound)
	}
	// we covered all status. return for status done
	r.Equal(entity.StatusDone, status)
}

func Test_policySvc_ListReplicationSwitchInfo(t *testing.T) {
	c := testutil.SetupRedis(t)
	ctx := context.TODO()

	queuesMock := &tasks.QueueServiceMock{}
	tasks.Reset(queuesMock)
	svc := NewService(c, queuesMock)

	r := require.New(t)
	list, err := svc.ListReplicationSwitchInfo(ctx)
	r.NoError(err)
	r.Empty(list)
	// create downtime switch
	replID := entity.ReplicationStatusID{
		User:        "u",
		FromBucket:  "b",
		FromStorage: "f",
		ToStorage:   "t",
		ToBucket:    "b",
	}
	validSwitch := &entity.ReplicationSwitchDowntimeOpts{
		StartOnInitDone:     false,
		Cron:                stringPtr("0 0 * * *"),
		StartAt:             nil,
		MaxDuration:         (3 * time.Hour),
		MaxEventLag:         uint32Ptr(100),
		SkipBucketCheck:     true,
		ContinueReplication: true,
	}
	setupDowntimeSwitchState(t, svc, queuesMock, replID, validSwitch, entity.StatusDone)

	list, err = svc.ListReplicationSwitchInfo(ctx)
	r.NoError(err)
	r.Len(list, 1)
	r.Equal(replID, list[0].ReplID)
	r.False(list[0].IsZeroDowntime())
	info, err := svc.GetReplicationSwitchInfo(ctx, replID)
	r.NoError(err)
	r.EqualValues(info, list[0])

	// create zero downtime switch
	replIDZero := entity.ReplicationStatusID{
		User:        "zu",
		FromBucket:  "zb",
		FromStorage: "zf",
		ToStorage:   "zt",
		ToBucket:    "zb",
	}
	validSwitchZero := &entity.ReplicationSwitchZeroDowntimeOpts{
		MultipartTTL: 15*time.Minute + 30*time.Second,
	}
	// create routing and replication for zero downtime switch
	r.NoError(svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(replIDZero.User, replIDZero.FromBucket), replIDZero.FromStorage, true))
	r.NoError(svc.AddBucketReplicationPolicy(ctx, replIDZero, nil))
	queuesMock.InitReplicationInProgress(replIDZero)
	// finish init replication
	r.NoError(svc.ObjListStarted(ctx, replIDZero))
	queuesMock.InitReplicationDone(replIDZero)
	// create switch
	err = svc.AddZeroDowntimeReplicationSwitch(ctx, replIDZero, validSwitchZero)
	r.NoError(err)

	list, err = svc.ListReplicationSwitchInfo(ctx)
	r.NoError(err)
	r.Len(list, 2)
	dwt, zeroDwt := list[0], list[1]
	if dwt.IsZeroDowntime() {
		dwt, zeroDwt = zeroDwt, dwt
	}
	r.Equal(replID, dwt.ReplID)
	r.Equal(replIDZero, zeroDwt.ReplID)
	r.NotEqual(dwt.ReplID, zeroDwt.ReplID)
	r.False(dwt.IsZeroDowntime())
	r.True(zeroDwt.IsZeroDowntime())
	info, err = svc.GetReplicationSwitchInfo(ctx, replID)
	r.NoError(err)
	r.EqualValues(info, dwt)
	info, err = svc.GetReplicationSwitchInfo(ctx, replIDZero)
	r.NoError(err)
	r.EqualValues(info, zeroDwt)
}
