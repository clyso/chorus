package policy

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/tasks"
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
		Window        SwitchDowntimeOpts
		LastStatus    SwitchStatus
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
				Window: SwitchDowntimeOpts{
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
				Window: SwitchDowntimeOpts{
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
				Window: SwitchDowntimeOpts{
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
				Window: SwitchDowntimeOpts{
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
				Window: SwitchDowntimeOpts{
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
				Window: SwitchDowntimeOpts{
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
				Window: SwitchDowntimeOpts{
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
				Window: SwitchDowntimeOpts{
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
				Window: SwitchDowntimeOpts{
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
				Window: SwitchDowntimeOpts{
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
				timeNow = func() time.Time {
					return tt.currentTime
				}
				defer func() {
					timeNow = func() time.Time {
						return time.Now()
					}
				}()
			}
			s := &SwitchInfo{
				SwitchDowntimeOpts: tt.fields.Window,
				LastStatus:         tt.fields.LastStatus,
				LastStartedAt:      tt.fields.LastStartedAt,
				CreatedAt:          tt.fields.CreatedAt,
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
	db := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: db.Addr()})
	ctx := context.TODO()
	svc := &policySvc{client: client}
	replID := ReplicationID{
		User:   "u",
		Bucket: "b",
		From:   "f",
		To:     "t",
	}

	tests := []struct {
		name    string
		before  *SwitchDowntimeOpts
		opts    *SwitchDowntimeOpts
		wantErr bool
	}{
		{
			name: "set switch with optional values",
			opts: &SwitchDowntimeOpts{
				StartOnInitDone: true,
				Cron:            stringPtr("0 0 * * *"),
				MaxDuration:     (10 * time.Second),
				MaxEventLag:     nil, // Should trigger HDEL
			},
			wantErr: false,
		},
		{
			name: "set switch with all values",
			opts: &SwitchDowntimeOpts{
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
			before: &SwitchDowntimeOpts{
				StartOnInitDone:     true,
				Cron:                stringPtr("0 0 * * *"),
				StartAt:             timePtr(time.Now()),
				MaxDuration:         (10 * time.Second),
				MaxEventLag:         uint32Ptr(100),
				SkipBucketCheck:     true,
				ContinueReplication: true,
			},
			opts: &SwitchDowntimeOpts{
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
			before: &SwitchDowntimeOpts{
				StartOnInitDone:     true,
				Cron:                stringPtr("0 0 * * *"),
				StartAt:             timePtr(time.Now()),
				MaxDuration:         (10 * time.Second),
				MaxEventLag:         uint32Ptr(100),
				SkipBucketCheck:     true,
				ContinueReplication: true,
			},
			opts:    &SwitchDowntimeOpts{}, // All pointers nil, bools false
			wantErr: false,
		},
		{
			name: "delete existing data with nil opts",
			before: &SwitchDowntimeOpts{
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
			before: &SwitchDowntimeOpts{
				StartOnInitDone: true,
				Cron:            stringPtr("0 0 * * *"),
				MaxDuration:     (10 * time.Second),
				MaxEventLag:     uint32Ptr(100),
			},
			opts: &SwitchDowntimeOpts{
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

			if tt.opts == nil {
				// Check if all fields were deleted
				res, err := client.HGetAll(ctx, replID.SwitchKey()).Result()
				assert.NoError(t, err)
				assert.Empty(t, res)
			} else {
				// Check if all fields were set correctly
				var got SwitchDowntimeOpts
				err = client.HGetAll(ctx, replID.SwitchKey()).Scan(&got)
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
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	ctx := context.TODO()

	svc := NewService(c)
	replID := ReplicationID{
		User:   "u",
		Bucket: "b",
		From:   "f",
		To:     "t",
	}
	validSwitch := &SwitchDowntimeOpts{
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
	timeNow = func() time.Time {
		return testTime
	}
	defer func() {
		timeNow = time.Now
	}()

	t.Run("create new", func(t *testing.T) {
		t.Run("validate against replication policy", func(t *testing.T) {
			r := require.New(t)
			// cleanup redis
			r.NoError(c.FlushAll(ctx).Err())

			// canot create switch for non-existing replication
			err := svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.Error(err, "replication not exists")

			// create replication but to other destination
			r.NoError(svc.addBucketRoutingPolicy(ctx, replID.User, replID.Bucket, replID.From, true))
			r.NoError(svc.AddBucketReplicationPolicy(ctx, replID.User, replID.Bucket, replID.From, "asdf", replID.ToBucket, tasks.Priority2, nil))
			// try again and get error
			err = svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.Error(err, "replication not exists")
			_, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.Error(err, "switch was not created")

			//create correct replication but now there are 2 destinations which is not allowed
			r.NoError(svc.AddBucketReplicationPolicy(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, tasks.Priority2, nil))
			// try again and get error
			err = svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.Error(err, "only one destination allowed")
			_, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.Error(err, "switch was not created")

			// delete first replication and check that it works
			r.NoError(svc.DeleteReplication(ctx, replID.User, replID.Bucket, replID.From, "asdf", replID.ToBucket))
			err = svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.NoError(err, "success")
			_, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.NoError(err, "switch was created")
		})
		t.Run("replication using agent not allowed", func(t *testing.T) {
			r := require.New(t)
			// cleanup redis
			r.NoError(c.FlushAll(ctx).Err())

			//create replication with agent which is not allowed
			r.NoError(svc.AddBucketReplicationPolicy(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, tasks.Priority2, stringPtr("http://example.com")))
			err := svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.Error(err, "replication using agent")
			_, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.Error(err, "switch was not created")

			// check that similar replication without agent works
			r.NoError(svc.DeleteReplication(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket))
			r.NoError(svc.AddBucketReplicationPolicy(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, tasks.Priority2, nil))
			err = svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.NoError(err, "success")
		})
		t.Run("err: init replication not done for immediate switch", func(t *testing.T) {
			r := require.New(t)
			// cleanup redis
			r.NoError(c.FlushAll(ctx).Err())
			err := svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.Error(err, "replication not exists")

			// create replication with init not done
			r.NoError(svc.addBucketRoutingPolicy(ctx, replID.User, replID.Bucket, replID.From, true))
			r.NoError(svc.AddBucketReplicationPolicy(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, tasks.Priority2, nil))

			// create switch with immediate start
			immediateSwitch := &SwitchDowntimeOpts{}
			err = svc.SetDowntimeReplicationSwitch(ctx, replID, immediateSwitch)
			r.Error(err, "init replication not done")
		})
		t.Run("success crud", func(t *testing.T) {
			r := require.New(t)
			// cleanup redis
			r.NoError(c.FlushAll(ctx).Err())

			// create replication
			r.NoError(svc.addBucketRoutingPolicy(ctx, replID.User, replID.Bucket, replID.From, true))
			r.NoError(svc.AddBucketReplicationPolicy(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, tasks.Priority2, nil))

			// create switch
			err := svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.NoError(err)
			// check that switch was created
			got, err := svc.GetReplicationSwitchInfo(ctx, replID)
			r.NoError(err)
			r.False(got.IsZeroDowntime())
			r.True(testTime.Equal(got.CreatedAt))
			gotID, err := got.ReplicationID()
			r.NoError(err)
			r.Equal(replID.String(), gotID.String())
			r.Equal(StatusNotStarted, got.LastStatus)
			r.EqualValues(*validSwitch, got.SwitchDowntimeOpts)
			_, err = svc.GetInProgressZeroDowntimeSwitchInfo(ctx, replID.User, replID.Bucket)
			r.ErrorIs(err, dom.ErrNotFound, "no in progress zero downtime switch")
			// check that routing policy was not changed
			routeToStorage, err := svc.GetRoutingPolicy(ctx, replID.User, replID.Bucket)
			r.NoError(err)
			r.Equal(replID.From, routeToStorage, "routing policy was not changed")
			// check that replication policy was not changed
			replications, err := svc.GetBucketReplicationPolicies(ctx, replID.User, replID.Bucket)
			r.NoError(err)
			r.Len(replications.To, 1)
			r.Equal(tasks.Priority2, replications.To[ReplicationPolicyDest(replID.To)])
			_, err = svc.GetInProgressZeroDowntimeSwitchInfo(ctx, replID.User, replID.Bucket)
			r.ErrorIs(err, dom.ErrNotFound, "no in progress switch zero downtime")

			//delete switch
			r.NoError(svc.DeleteReplicationSwitch(ctx, replID))
			_, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.ErrorIs(err, dom.ErrNotFound, "switch was deleted")
			// check that routing policy was not changed
			routeToStorage, err = svc.GetRoutingPolicy(ctx, replID.User, replID.Bucket)
			r.NoError(err)
			r.Equal(replID.From, routeToStorage, "routing policy was not changed")
			// check that replication policy was not changed
			replications, err = svc.GetBucketReplicationPolicies(ctx, replID.User, replID.Bucket)
			r.NoError(err)
			r.Len(replications.To, 1)
			r.Equal(tasks.Priority2, replications.To[ReplicationPolicyDest(replID.To)])
		})
	})
	t.Run("update existing", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			r := require.New(t)
			// cleanup redis
			r.NoError(c.FlushAll(ctx).Err())

			// create replication
			r.NoError(svc.addBucketRoutingPolicy(ctx, replID.User, replID.Bucket, replID.From, true))
			r.NoError(svc.AddBucketReplicationPolicy(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, tasks.Priority2, nil))

			// create existing switch
			err := svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.NoError(err)
			existing, err := svc.GetReplicationSwitchInfo(ctx, replID)
			r.NoError(err)

			// update switch
			inHour := time.Now().Add(1 * time.Hour)
			updated := &SwitchDowntimeOpts{
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
			gotID, err := got.ReplicationID()
			r.NoError(err)
			r.Equal(replID.String(), gotID.String())
			r.Equal(existing.ReplicationIDStr, got.ReplicationIDStr)
			r.Equal(StatusNotStarted, got.LastStatus)
			r.Equal(existing.LastStatus, got.LastStatus)
			// compare StartAt time separately:
			r.True(inHour.Equal(*got.StartAt))
			r.True(updated.StartAt.Equal(*got.StartAt))
			got.StartAt = nil
			updated.StartAt = nil
			r.EqualValues(*updated, got.SwitchDowntimeOpts)

			// update with empty opts
			err = svc.SetDowntimeReplicationSwitch(ctx, replID, &SwitchDowntimeOpts{})
			r.NoError(err)
			got, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.NoError(err)
			r.False(got.IsZeroDowntime())
			r.True(existing.CreatedAt.Equal(got.CreatedAt), "created at should not change")
			r.EqualValues(SwitchDowntimeOpts{}, got.SwitchDowntimeOpts)
		})
		t.Run("err: existing switch is zero downtime", func(t *testing.T) {
			r := require.New(t)
			// cleanup redis
			r.NoError(c.FlushAll(ctx).Err())
			// create replication
			r.NoError(svc.addBucketRoutingPolicy(ctx, replID.User, replID.Bucket, replID.From, true))
			r.NoError(svc.AddBucketReplicationPolicy(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, tasks.Priority2, nil))
			// finish init replication
			r.NoError(svc.ObjListStarted(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket))
			r.NoError(svc.IncReplInitObjListed(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, 0, time.Now()))
			r.NoError(svc.IncReplInitObjDone(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, 0, time.Now()))

			// create zero downtime switch
			err := svc.AddZeroDowntimeReplicationSwitch(ctx, replID, &SwitchZeroDowntimeOpts{MultipartTTL: 15*time.Minute + 30*time.Second})
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
			allStatuses := []SwitchStatus{
				StatusNotStarted,
				StatusInProgress,
				StatusCheckInProgress,
				StatusDone,
				StatusSkipped,
				StatusError,
			}
			inHour := time.Now().Add(1 * time.Hour)
			updated := &SwitchDowntimeOpts{
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
					setupDiwntimeSwitchState(t, svc, replID, validSwitch, status)
					if status == StatusInProgress || status == StatusCheckInProgress || status == StatusDone {
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
						r.EqualValues(updCopy, got.SwitchDowntimeOpts)
					}
				})
			}
		})
	})
}

func Test_policySvc_AddZeroDowntimeSwitch(t *testing.T) {
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	ctx := context.TODO()

	svc := NewService(c)
	replID := ReplicationID{
		User:   "u",
		Bucket: "b",
		From:   "f",
		To:     "t",
	}
	validSwitch := &SwitchZeroDowntimeOpts{
		MultipartTTL: 15*time.Minute + 30*time.Second,
	}
	//setup time mock
	testTime := time.Now()
	timeNow = func() time.Time {
		return testTime
	}
	defer func() {
		timeNow = time.Now
	}()

	t.Run("error cases", func(t *testing.T) {
		r := require.New(t)
		// cleanup redis
		r.NoError(c.FlushAll(ctx).Err())

		// canot create switch for non-existing replication
		err := svc.AddZeroDowntimeReplicationSwitch(ctx, replID, validSwitch)
		r.Error(err, "replication not exists")

		// create replication but to wrong destination
		r.NoError(svc.addBucketRoutingPolicy(ctx, replID.User, replID.Bucket, replID.From, true))
		r.NoError(svc.AddBucketReplicationPolicy(ctx, replID.User, replID.Bucket, replID.From, "asdf", replID.ToBucket, tasks.Priority2, nil))
		// try again
		err = svc.AddZeroDowntimeReplicationSwitch(ctx, replID, validSwitch)
		r.Error(err, "replication not exists")
		_, err = svc.GetReplicationSwitchInfo(ctx, replID)
		r.Error(err, "switch was not created")

		//create replication but now there are 2 destinations which is not allowed
		r.NoError(svc.AddBucketReplicationPolicy(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, tasks.Priority2, nil))
		// try again
		err = svc.AddZeroDowntimeReplicationSwitch(ctx, replID, validSwitch)
		r.Error(err, "only one destination allowed")
		_, err = svc.GetReplicationSwitchInfo(ctx, replID)
		r.Error(err, "switch was not created")

		// delete first replication and check that it works
		r.NoError(svc.DeleteReplication(ctx, replID.User, replID.Bucket, replID.From, "asdf", replID.ToBucket))
		err = svc.AddZeroDowntimeReplicationSwitch(ctx, replID, validSwitch)
		r.Error(err, "replication not done")

		// finish init replication
		r.NoError(svc.ObjListStarted(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket))
		r.NoError(svc.IncReplInitObjListed(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, 0, time.Now()))
		r.NoError(svc.IncReplInitObjDone(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, 0, time.Now()))

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

		_, err := svc.GetInProgressZeroDowntimeSwitchInfo(ctx, replID.User, replID.Bucket)
		r.ErrorIs(err, dom.ErrNotFound, "no in progress switch")

		// create replication
		r.NoError(svc.addBucketRoutingPolicy(ctx, replID.User, replID.Bucket, replID.From, true))
		r.NoError(svc.AddBucketReplicationPolicy(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, tasks.Priority2, nil))
		// finish init replication
		r.NoError(svc.ObjListStarted(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket))
		r.NoError(svc.IncReplInitObjListed(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, 0, time.Now()))
		r.NoError(svc.IncReplInitObjDone(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, 0, time.Now()))

		// create switch
		err = svc.AddZeroDowntimeReplicationSwitch(ctx, replID, validSwitch)
		r.NoError(err)
		info, err := svc.GetInProgressZeroDowntimeSwitchInfo(ctx, replID.User, replID.Bucket)
		r.NoError(err)
		r.EqualValues(validSwitch.MultipartTTL, info.MultipartTTL)
		r.EqualValues(replID.String(), info.ReplicationIDStr)
		r.EqualValues(StatusInProgress, info.Status)
		r.EqualValues(tasks.Priority2, info.ReplicationPriority)

		got, err := svc.GetReplicationSwitchInfo(ctx, replID)
		r.NoError(err, "switch was created")

		r.True(testTime.Equal(got.CreatedAt))
		gotID, err := got.ReplicationID()
		r.NoError(err)
		r.Equal(replID.String(), gotID.String())
		r.Equal(StatusInProgress, got.LastStatus)
		r.EqualValues(*validSwitch, got.SwitchZeroDowntimeOpts)
		r.EqualValues(SwitchDowntimeOpts{}, got.SwitchDowntimeOpts, "zero downtime switch should not have downtime switch")
		r.True(got.IsZeroDowntime())
		r.NotNil(got.LastStartedAt)
		r.True(testTime.Equal(*got.LastStartedAt))
		r.EqualValues(tasks.Priority2, got.ReplicationPriority)

		// check that routing policy was changed
		routeToStorage, err := svc.GetRoutingPolicy(ctx, replID.User, replID.Bucket)
		r.NoError(err)
		r.Equal(replID.To, routeToStorage, "routing policy was changed")
		// check that replication policy was archived
		_, err = svc.GetBucketReplicationPolicies(ctx, replID.User, replID.Bucket)
		r.ErrorIs(err, dom.ErrNotFound)

		// but repliction metadata should be still there
		repl, err := svc.GetReplicationPolicyInfo(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket)
		r.NoError(err)
		r.True(repl.IsArchived)
		r.NotNil(repl.ArchivedAt)
		r.True(repl.ListingStarted)
		r.True(repl.InitDone())
		r.EqualValues(1, repl.InitObjListed)
		r.EqualValues(1, repl.InitObjDone)
		//delete metadata
		r.NoError(svc.DeleteReplication(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket))
		_, err = svc.GetReplicationPolicyInfo(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket)
		r.ErrorIs(err, dom.ErrNotFound)

		// delete switch
		r.NoError(svc.DeleteReplicationSwitch(ctx, replID))

		_, err = svc.GetInProgressZeroDowntimeSwitchInfo(ctx, replID.User, replID.Bucket)
		r.ErrorIs(err, dom.ErrNotFound, "no in progress switch")
		_, err = svc.GetReplicationSwitchInfo(ctx, replID)
		r.ErrorIs(err, dom.ErrNotFound, "switch was deleted")
		// check that routing policy was changed back
		routeToStorage, err = svc.GetRoutingPolicy(ctx, replID.User, replID.Bucket)
		r.NoError(err)
		r.Equal(replID.From, routeToStorage, "routing policy was changed back")
	})
	t.Run("complete", func(t *testing.T) {
		r := require.New(t)
		// cleanup redis
		r.NoError(c.FlushAll(ctx).Err())

		// create replication
		r.NoError(svc.addBucketRoutingPolicy(ctx, replID.User, replID.Bucket, replID.From, true))
		r.NoError(svc.AddBucketReplicationPolicy(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, tasks.Priority2, nil))
		// finish init replication
		r.NoError(svc.ObjListStarted(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket))
		r.NoError(svc.IncReplInitObjListed(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, 0, time.Now()))
		r.NoError(svc.IncReplInitObjDone(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, 0, time.Now()))

		// create switch
		err := svc.AddZeroDowntimeReplicationSwitch(ctx, replID, validSwitch)
		r.NoError(err)
		info, err := svc.GetInProgressZeroDowntimeSwitchInfo(ctx, replID.User, replID.Bucket)
		r.NoError(err)
		r.EqualValues(validSwitch.MultipartTTL, info.MultipartTTL)

		got, err := svc.GetReplicationSwitchInfo(ctx, replID)
		r.NoError(err, "switch was created")
		r.True(got.IsZeroDowntime())
		r.Equal(StatusInProgress, got.LastStatus)
		r.NotNil(got.LastStartedAt)
		r.Empty(got.History)

		// check that routing policy was changed
		routeToStorage, err := svc.GetRoutingPolicy(ctx, replID.User, replID.Bucket)
		r.NoError(err)
		r.Equal(replID.To, routeToStorage, "routing policy was changed")
		// check that replication policy was archived
		_, err = svc.GetBucketReplicationPolicies(ctx, replID.User, replID.Bucket)
		r.ErrorIs(err, dom.ErrNotFound)

		// but repliction metadata should be still there
		repl, err := svc.GetReplicationPolicyInfo(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket)
		r.NoError(err)
		r.True(repl.IsArchived)

		// complete switch
		r.NoError(svc.CompleteZeroDowntimeReplicationSwitch(ctx, replID))

		_, err = svc.GetInProgressZeroDowntimeSwitchInfo(ctx, replID.User, replID.Bucket)
		r.ErrorIs(err, dom.ErrNotFound, "no in progress switch")

		got, err = svc.GetReplicationSwitchInfo(ctx, replID)
		r.NoError(err, "switch was completed")
		r.True(got.IsZeroDowntime())
		r.Equal(StatusDone, got.LastStatus)
		r.NotNil(got.LastStartedAt)
		r.NotNil(got.DoneAt)
		r.Len(got.History, 1)

		// delete switch
		r.NoError(svc.DeleteReplicationSwitch(ctx, replID))

		_, err = svc.GetInProgressZeroDowntimeSwitchInfo(ctx, replID.User, replID.Bucket)
		r.ErrorIs(err, dom.ErrNotFound, "no in progress switch")
		_, err = svc.GetReplicationSwitchInfo(ctx, replID)
		r.ErrorIs(err, dom.ErrNotFound, "switch was deleted")
		// check that routing policy was not changed back
		routeToStorage, err = svc.GetRoutingPolicy(ctx, replID.User, replID.Bucket)
		r.NoError(err)
		r.Equal(replID.To, routeToStorage, "routing policy was not changed back")
	})
}

func Test_policySvc_UpdateDowntimeSwitchStatus(t *testing.T) {
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	ctx := context.TODO()

	svc := NewService(c)
	replID := ReplicationID{
		User:   "u",
		Bucket: "b",
		From:   "f",
		To:     "t",
	}
	validSwitch := &SwitchDowntimeOpts{
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
	timeNow = func() time.Time {
		return testTime
	}
	defer func() {
		timeNow = time.Now
	}()
	statuses := []SwitchStatus{
		StatusNotStarted,
		StatusInProgress,
		StatusCheckInProgress,
		StatusDone,
		StatusSkipped,
		StatusError,
	}
	t.Run("to not_started", func(t *testing.T) {
		// check all switch transition to not started
		for _, status := range statuses {
			t.Run("from "+string(status), func(t *testing.T) {
				r := require.New(t)
				// cleanup redis
				r.NoError(c.FlushAll(ctx).Err())
				// create switch in status
				setupDiwntimeSwitchState(t, svc, replID, validSwitch, status)
				// transition to not started not allowed for any status
				r.ErrorIs(svc.UpdateDowntimeSwitchStatus(ctx, replID, StatusNotStarted, "test", nil, nil), dom.ErrInvalidArg, "transition not allowed")
			})
		}
	})
	t.Run("to in_progress", func(t *testing.T) {
		for _, status := range statuses {
			t.Run("from "+string(status), func(t *testing.T) {
				r := require.New(t)
				// cleanup redis
				r.NoError(c.FlushAll(ctx).Err())
				// create switch in status
				setupDiwntimeSwitchState(t, svc, replID, validSwitch, status)
				now := timeNow()

				err := svc.UpdateDowntimeSwitchStatus(ctx, replID, StatusInProgress, "test", &now, nil)
				if status == StatusNotStarted || status == StatusSkipped || status == StatusError {
					r.NoError(err, "transition is allowed")

					// validate switch state
					info, err := svc.GetReplicationSwitchInfo(ctx, replID)
					r.NoError(err)
					r.Equal(StatusInProgress, info.LastStatus)
					r.NotNil(info.LastStartedAt)
					r.Nil(info.DoneAt)
					r.True(now.Equal(*info.LastStartedAt))
					r.NotEmpty(info.History)
					_, err = svc.GetInProgressZeroDowntimeSwitchInfo(ctx, replID.User, replID.Bucket)
					r.ErrorIs(err, dom.ErrNotFound, "no zero downtime switch in progress")
					// check that routing is blocked
					_, err = svc.GetRoutingPolicy(ctx, replID.User, replID.Bucket)
					r.ErrorIs(err, dom.ErrRoutingBlock, "routing is blocked")
					// check that replication remains the same
					replications, err := svc.GetBucketReplicationPolicies(ctx, replID.User, replID.Bucket)
					r.NoError(err)
					r.Len(replications.To, 1)
					r.Equal(tasks.Priority2, replications.To[ReplicationPolicyDest(replID.To)])

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
				// create switch in status
				setupDiwntimeSwitchState(t, svc, replID, validSwitch, status)

				err := svc.UpdateDowntimeSwitchStatus(ctx, replID, StatusCheckInProgress, "test", nil, nil)
				if status == StatusInProgress {
					r.NoError(err, "transition is allowed")

					// validate switch state
					info, err := svc.GetReplicationSwitchInfo(ctx, replID)
					r.NoError(err)
					r.Equal(StatusCheckInProgress, info.LastStatus)
					r.Nil(info.DoneAt)
					r.NotNil(info.LastStartedAt)
					r.NotEmpty(info.History)
					_, err = svc.GetInProgressZeroDowntimeSwitchInfo(ctx, replID.User, replID.Bucket)
					r.ErrorIs(err, dom.ErrNotFound, "no zero downtime switch in progress")
					// check that routing is blocked
					_, err = svc.GetRoutingPolicy(ctx, replID.User, replID.Bucket)
					r.ErrorIs(err, dom.ErrRoutingBlock, "routing is blocked")
					// check that replication remains the same
					replications, err := svc.GetBucketReplicationPolicies(ctx, replID.User, replID.Bucket)
					r.NoError(err)
					r.Len(replications.To, 1)
					r.Equal(tasks.Priority2, replications.To[ReplicationPolicyDest(replID.To)])
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
				// create switch in status
				setupDiwntimeSwitchState(t, svc, replID, validSwitch, status)
				now := timeNow()

				err := svc.UpdateDowntimeSwitchStatus(ctx, replID, StatusDone, "test", nil, &now)
				if status == StatusCheckInProgress || status == StatusDone {
					r.NoError(err, "transition is allowed")

					// validate switch state
					info, err := svc.GetReplicationSwitchInfo(ctx, replID)
					r.NoError(err)
					r.Equal(StatusDone, info.LastStatus)
					r.NotNil(info.DoneAt)
					r.NotNil(info.LastStartedAt)
					r.NotEmpty(info.History)
					_, err = svc.GetInProgressZeroDowntimeSwitchInfo(ctx, replID.User, replID.Bucket)
					r.ErrorIs(err, dom.ErrNotFound, "no zero downtime switch in progress")
					// check that routing is switched
					routeToStorage, err := svc.GetRoutingPolicy(ctx, replID.User, replID.Bucket)
					r.NoError(err)
					r.Equal(replID.To, routeToStorage, "routing is switched")
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
				// create switch in status
				setupDiwntimeSwitchState(t, svc, replID, validSwitch, status)

				err := svc.UpdateDowntimeSwitchStatus(ctx, replID, StatusSkipped, "test", nil, nil)
				if status == StatusNotStarted || status == StatusSkipped || status == StatusError {
					r.NoError(err, "transition is allowed")
					// validate switch state
					info, err := svc.GetReplicationSwitchInfo(ctx, replID)
					r.NoError(err)
					r.Equal(StatusSkipped, info.LastStatus)
					r.Nil(info.DoneAt)
					r.NotEmpty(info.History)
					_, err = svc.GetInProgressZeroDowntimeSwitchInfo(ctx, replID.User, replID.Bucket)
					r.ErrorIs(err, dom.ErrNotFound, "no zero downtime switch in progress")
					// check that routing to old bucket
					routeToStorage, err := svc.GetRoutingPolicy(ctx, replID.User, replID.Bucket)
					r.NoError(err)
					r.Equal(replID.From, routeToStorage, "routing is to old bucket")
					// check that replication remains the same
					replications, err := svc.GetBucketReplicationPolicies(ctx, replID.User, replID.Bucket)
					r.NoError(err)
					r.Len(replications.To, 1)
					r.Equal(tasks.Priority2, replications.To[ReplicationPolicyDest(replID.To)])
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
				// create switch in status
				setupDiwntimeSwitchState(t, svc, replID, validSwitch, status)

				err := svc.UpdateDowntimeSwitchStatus(ctx, replID, StatusError, "test", nil, nil)
				// transition to error allowed from all except done
				if status == StatusDone {
					r.ErrorIs(err, dom.ErrInvalidArg, "transition not allowed")
				} else {
					r.NoError(err, "transition is allowed")
					// validate switch state
					info, err := svc.GetReplicationSwitchInfo(ctx, replID)
					r.NoError(err)
					r.Equal(StatusError, info.LastStatus)
					r.Nil(info.DoneAt)
					r.NotEmpty(info.History)
					_, err = svc.GetInProgressZeroDowntimeSwitchInfo(ctx, replID.User, replID.Bucket)
					r.ErrorIs(err, dom.ErrNotFound, "no zero downtime switch in progress")
					// check that routing to old bucket
					routeToStorage, err := svc.GetRoutingPolicy(ctx, replID.User, replID.Bucket)
					r.NoError(err)
					r.Equal(replID.From, routeToStorage, "routing is to old bucket")
					// check that replication remains the same
					replications, err := svc.GetBucketReplicationPolicies(ctx, replID.User, replID.Bucket)
					r.NoError(err)
					r.Len(replications.To, 1)
					r.Equal(tasks.Priority2, replications.To[ReplicationPolicyDest(replID.To)])
				}
			})
		}
	})

}

func setupDiwntimeSwitchState(t *testing.T, svc Service, replID ReplicationID, opts *SwitchDowntimeOpts, status SwitchStatus) {
	t.Helper()
	ctx := context.TODO()
	r := require.New(t)

	// create routing and replication
	r.NoError(svc.addBucketRoutingPolicy(ctx, replID.User, replID.Bucket, replID.From, true))
	r.NoError(svc.AddBucketReplicationPolicy(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, tasks.Priority2, nil))
	// create switch
	err := svc.SetDowntimeReplicationSwitch(ctx, replID, opts)
	r.NoError(err)
	// check that switch was created in NOT_STARTED status
	got, err := svc.GetReplicationSwitchInfo(ctx, replID)
	r.NoError(err)
	r.False(got.IsZeroDowntime())
	r.Equal(replID.String(), got.ReplicationIDStr)
	r.Equal(StatusNotStarted, got.LastStatus)
	r.Nil(got.LastStartedAt)
	// check that routing policy was not changed
	routeToStorage, err := svc.GetRoutingPolicy(ctx, replID.User, replID.Bucket)
	r.NoError(err)
	r.Equal(replID.From, routeToStorage, "routing policy was not changed")
	// check that replication policy was not changed
	replications, err := svc.GetBucketReplicationPolicies(ctx, replID.User, replID.Bucket)
	r.NoError(err)
	r.Len(replications.To, 1)
	r.Equal(tasks.Priority2, replications.To[ReplicationPolicyDest(replID.To)])

	if status == StatusNotStarted {
		return
	}
	if status == StatusSkipped || status == StatusError {
		r.NoError(svc.UpdateDowntimeSwitchStatus(ctx, replID, status, "test", nil, nil))
		// check that switch was changed
		got, err := svc.GetReplicationSwitchInfo(ctx, replID)
		r.NoError(err)
		r.False(got.IsZeroDowntime())
		r.Equal(replID.String(), got.ReplicationIDStr)
		r.Equal(status, got.LastStatus)
		r.Nil(got.LastStartedAt)
		// check that routing policy was not changed
		routeToStorage, err := svc.GetRoutingPolicy(ctx, replID.User, replID.Bucket)
		r.NoError(err)
		r.Equal(replID.From, routeToStorage, "routing policy was not changed")
		// check that replication policy was not changed
		replications, err := svc.GetBucketReplicationPolicies(ctx, replID.User, replID.Bucket)
		r.NoError(err)
		r.Len(replications.To, 1)
		r.Equal(tasks.Priority2, replications.To[ReplicationPolicyDest(replID.To)])
		return
	}

	// move to in progress
	now := timeNow()
	r.NoError(svc.UpdateDowntimeSwitchStatus(ctx, replID, StatusInProgress, "test", &now, nil))
	// check that switch was changed
	got, err = svc.GetReplicationSwitchInfo(ctx, replID)
	r.NoError(err)
	r.False(got.IsZeroDowntime())
	r.Equal(replID.String(), got.ReplicationIDStr)
	r.Equal(StatusInProgress, got.LastStatus)
	r.NotNil(got.LastStartedAt)
	// check that routing was blocked
	_, err = svc.GetRoutingPolicy(ctx, replID.User, replID.Bucket)
	r.ErrorIs(err, dom.ErrRoutingBlock)
	// check that replication policy was not changed
	replications, err = svc.GetBucketReplicationPolicies(ctx, replID.User, replID.Bucket)
	r.NoError(err)
	r.Len(replications.To, 1)
	r.Equal(tasks.Priority2, replications.To[ReplicationPolicyDest(replID.To)])
	if status == StatusInProgress {
		return
	}
	// move to check in progress
	r.NoError(svc.UpdateDowntimeSwitchStatus(ctx, replID, StatusCheckInProgress, "test", nil, nil))
	// check that switch was changed
	got, err = svc.GetReplicationSwitchInfo(ctx, replID)
	r.NoError(err)
	r.False(got.IsZeroDowntime())
	r.Equal(replID.String(), got.ReplicationIDStr)
	r.Equal(StatusCheckInProgress, got.LastStatus)
	r.NotNil(got.LastStartedAt)
	r.Nil(got.DoneAt)
	r.EqualValues(tasks.Priority2, got.ReplicationPriority)
	// check that routing was blocked
	_, err = svc.GetRoutingPolicy(ctx, replID.User, replID.Bucket)
	r.ErrorIs(err, dom.ErrRoutingBlock)
	// check that replication policy was not changed
	replications, err = svc.GetBucketReplicationPolicies(ctx, replID.User, replID.Bucket)
	r.NoError(err)
	r.Len(replications.To, 1)
	r.Equal(tasks.Priority2, replications.To[ReplicationPolicyDest(replID.To)])
	if status == StatusCheckInProgress {
		return
	}
	// move to done
	r.NoError(svc.UpdateDowntimeSwitchStatus(ctx, replID, StatusDone, "test", nil, &now))
	// check that switch was changed
	got, err = svc.GetReplicationSwitchInfo(ctx, replID)
	r.NoError(err)
	r.False(got.IsZeroDowntime())
	r.Equal(replID.String(), got.ReplicationIDStr)
	r.Equal(StatusDone, got.LastStatus)
	r.NotNil(got.LastStartedAt)
	r.NotNil(got.DoneAt)
	// check that routing switched
	routeToStorage, err = svc.GetRoutingPolicy(ctx, replID.User, replID.Bucket)
	r.NoError(err)
	r.Equal(replID.To, routeToStorage, "routing policy was changed")
	// check that replication policy was archived
	repl, err := svc.GetReplicationPolicyInfo(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket)
	r.NoError(err)
	r.True(repl.IsArchived)
	r.NotNil(repl.ArchivedAt)
	replications, err = svc.GetBucketReplicationPolicies(ctx, replID.User, replID.Bucket)
	if opts != nil && opts.ContinueReplication {
		r.NoError(err)
		r.Equal(replID.To, replications.From)
		r.EqualValues(tasks.Priority2, replications.To[ReplicationPolicyDest(replID.From)])

		repl, err := svc.GetReplicationPolicyInfo(ctx, replID.User, replID.Bucket, replID.To, replID.From, replID.ToBucket)
		r.NoError(err)
		r.False(repl.IsArchived)
		r.True(repl.InitDone())
	} else {
		r.ErrorIs(err, dom.ErrNotFound)
	}
	// we covered all status. return for status done
	r.Equal(StatusDone, status)
}

func Test_policySvc_ListReplicationSwitchInfo(t *testing.T) {
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	ctx := context.TODO()

	svc := NewService(c)

	r := require.New(t)
	list, err := svc.ListReplicationSwitchInfo(ctx)
	r.NoError(err)
	r.Empty(list)
	// create downtime switch
	replID := ReplicationID{
		User:   "u",
		Bucket: "b",
		From:   "f",
		To:     "t",
	}
	validSwitch := &SwitchDowntimeOpts{
		StartOnInitDone:     false,
		Cron:                stringPtr("0 0 * * *"),
		StartAt:             nil,
		MaxDuration:         (3 * time.Hour),
		MaxEventLag:         uint32Ptr(100),
		SkipBucketCheck:     true,
		ContinueReplication: true,
	}
	setupDiwntimeSwitchState(t, svc, replID, validSwitch, StatusDone)

	list, err = svc.ListReplicationSwitchInfo(ctx)
	r.NoError(err)
	r.Len(list, 1)
	r.Equal(replID.String(), list[0].ReplicationIDStr)
	r.False(list[0].IsZeroDowntime())
	info, err := svc.GetReplicationSwitchInfo(ctx, replID)
	r.NoError(err)
	r.EqualValues(info, list[0])

	// create zero downtime switch
	replIDZero := ReplicationID{
		User:   "zu",
		Bucket: "zb",
		From:   "zf",
		To:     "zt",
	}
	validSwitchZero := &SwitchZeroDowntimeOpts{
		MultipartTTL: 15*time.Minute + 30*time.Second,
	}
	// create routing and replication for zero downtime switch
	r.NoError(svc.addBucketRoutingPolicy(ctx, replIDZero.User, replIDZero.Bucket, replIDZero.From, true))
	r.NoError(svc.AddBucketReplicationPolicy(ctx, replIDZero.User, replIDZero.Bucket, replIDZero.From, replIDZero.To, replIDZero.ToBucket, tasks.Priority2, nil))
	// finish init replication
	r.NoError(svc.ObjListStarted(ctx, replIDZero.User, replIDZero.Bucket, replIDZero.From, replIDZero.To, replIDZero.ToBucket))
	r.NoError(svc.IncReplInitObjListed(ctx, replIDZero.User, replIDZero.Bucket, replIDZero.From, replIDZero.To, replIDZero.ToBucket, 0, time.Now()))
	r.NoError(svc.IncReplInitObjDone(ctx, replIDZero.User, replIDZero.Bucket, replIDZero.From, replIDZero.To, replIDZero.ToBucket, 0, time.Now()))
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
	r.Equal(replID.String(), dwt.ReplicationIDStr)
	r.Equal(replIDZero.String(), zeroDwt.ReplicationIDStr)
	r.NotEqual(dwt.ReplicationIDStr, zeroDwt.ReplicationIDStr)
	r.False(dwt.IsZeroDowntime())
	r.True(zeroDwt.IsZeroDowntime())
	info, err = svc.GetReplicationSwitchInfo(ctx, replID)
	r.NoError(err)
	r.EqualValues(info, dwt)
	info, err = svc.GetReplicationSwitchInfo(ctx, replIDZero)
	r.NoError(err)
	r.EqualValues(info, zeroDwt)
}
