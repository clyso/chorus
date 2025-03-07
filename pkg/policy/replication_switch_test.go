package policy

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func timePtr(t time.Time) *time.Time {
	return &t
}

func stringPtr(s string) *string {
	return &s
}

func durationPtr(d time.Duration) *time.Duration {
	return &d
}

func uint32Ptr(u uint32) *uint32 {
	return &u
}

func TestSwitchWithDowntime_IsTimeToStart(t *testing.T) {
	type fields struct {
		Window        SwitchDowntimeOpts
		LastStatus    SwitchWithDowntimeStatus
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
				MaxDuration:     durationPtr(10 * time.Second),
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
				MaxDuration:         durationPtr(10 * time.Second),
				MaxEventLag:         uint32Ptr(100),
				SkipBucketCheck:     true,
				ContinueReplication: true,
			},
			wantErr: false,
		},
		{
			name: "update existing data with empty opts",
			before: &SwitchDowntimeOpts{
				StartOnInitDone:     true,
				Cron:                stringPtr("0 0 * * *"),
				StartAt:             timePtr(time.Now()),
				MaxDuration:         durationPtr(10 * time.Second),
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
				MaxDuration:         durationPtr(10 * time.Second),
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
				MaxDuration:     durationPtr(10 * time.Second),
				MaxEventLag:     uint32Ptr(100),
			},
			opts: &SwitchDowntimeOpts{
				StartOnInitDone: false,
				Cron:            stringPtr("1 1 * * *"),
				MaxDuration:     durationPtr(20 * time.Second),
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
		MaxDuration:         durationPtr(3 * time.Hour),
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

			// create replication but to wrong destination
			r.NoError(svc.addBucketRoutingPolicy(ctx, replID.User, replID.Bucket, replID.From, true))
			r.NoError(svc.AddBucketReplicationPolicy(ctx, replID.User, replID.Bucket, replID.From, "asdf", replID.ToBucket, tasks.Priority2, nil))
			// try again
			err = svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.Error(err, "replication not exists")
			_, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.Error(err, "switch was not created")

			//create replication but now there are 2 destinations which is not allowed
			r.NoError(svc.AddBucketReplicationPolicy(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, tasks.Priority2, nil))
			// try again
			err = svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.Error(err, "only one destination allowed")
			_, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.Error(err, "switch was not created")

			// delete first replication and check that it works
			r.NoError(svc.DeleteReplication(ctx, replID.User, replID.Bucket, replID.From, "asdf", replID.ToBucket))
			err = svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.NoError(err, "success")
		})
		t.Run("err: replication using agent", func(t *testing.T) {
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
		t.Run("success", func(t *testing.T) {
			r := require.New(t)
			// cleanup redis
			r.NoError(c.FlushAll(ctx).Err())

			// create replication
			r.NoError(svc.addBucketRoutingPolicy(ctx, replID.User, replID.Bucket, replID.From, true))
			r.NoError(svc.AddBucketReplicationPolicy(ctx, replID.User, replID.Bucket, replID.From, replID.To, replID.ToBucket, tasks.Priority2, nil))

			// create switch
			err := svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.NoError(err)
			got, err := svc.GetReplicationSwitchInfo(ctx, replID)
			r.NoError(err)
			r.True(testTime.Equal(got.CreatedAt))
			gotID, err := got.ReplicationID()
			r.NoError(err)
			r.Equal(replID.String(), gotID.String())
			r.Equal(StatusNotStarted, got.LastStatus)
			r.EqualValues(validSwitch, got.SwitchDowntimeOpts)
		})

	})

}
