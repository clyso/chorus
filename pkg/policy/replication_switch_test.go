package policy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
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
	ctx := t.Context()
	queuesMock := &tasks.QueueServiceMock{}
	tasks.Reset(queuesMock)
	svc := NewService(client, queuesMock, "f")

	bucketRepl := entity.BucketReplicationPolicy{
		User:        "u",
		FromBucket:  "b",
		FromStorage: "f",
		ToStorage:   "t",
		ToBucket:    "b",
	}
	userRepl := entity.UserReplicationPolicy{
		User:        "u",
		FromStorage: "f",
		ToStorage:   "t",
	}
	replications := map[string]entity.UniversalReplicationID{
		"bucket-replication": entity.UniversalFromBucketReplication(bucketRepl),
		"user-replication":   entity.UniversalFromUserReplication(userRepl),
	}

	tests := []struct {
		name     string
		existing entity.ReplicationSwitchInfo
		opts     *entity.ReplicationSwitchDowntimeOpts
		wantErr  bool
	}{
		{
			name: "set switch with optional values",
			existing: entity.ReplicationSwitchInfo{
				LastStatus: entity.StatusNotStarted,
			},
			opts: &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone: true,
				Cron:            stringPtr("0 0 * * *"),
				MaxDuration:     (10 * time.Second),
				MaxEventLag:     nil, // Should trigger HDEL
			},
			wantErr: false,
		},
		{
			name: "err: existing switch is zero downtime",
			existing: entity.ReplicationSwitchInfo{
				ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: 15 * time.Minute},
				LastStatus:                        entity.StatusNotStarted,
			},
			opts: &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone: true,
			},
			wantErr: true,
		},
		{
			name: "err: existing switch is in progress",
			existing: entity.ReplicationSwitchInfo{
				LastStatus: entity.StatusInProgress,
			},
			opts: &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone: true,
			},
			wantErr: true,
		},
		{
			name: "err: existing switch is check in progress",
			existing: entity.ReplicationSwitchInfo{
				LastStatus: entity.StatusCheckInProgress,
			},
			opts: &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone: true,
			},
			wantErr: true,
		},
		{
			name: "err: existing switch is done",
			existing: entity.ReplicationSwitchInfo{
				LastStatus: entity.StatusDone,
			},
			opts: &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone: true,
			},
			wantErr: true,
		},
		{
			name: "ok: existing switch is error",
			existing: entity.ReplicationSwitchInfo{
				LastStatus: entity.StatusError,
			},
			opts: &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone: true,
			},
			wantErr: false,
		},
		{
			name: "ok: existing switch is skipped",
			existing: entity.ReplicationSwitchInfo{
				LastStatus: entity.StatusSkipped,
			},
			opts: &entity.ReplicationSwitchDowntimeOpts{
				StartOnInitDone: true,
			},
			wantErr: false,
		},
	}

	for replName, replID := range replications {
		for _, tt := range tests {
			t.Run(replName+" "+tt.name, func(t *testing.T) {
				// Clear Redis state
				if err := client.FlushAll(ctx).Err(); err != nil {
					t.Fatalf("failed to flush Redis: %v", err)
				}
				// reset queues mock
				tasks.Reset(queuesMock)

				// Run the update
				err := svc.updateDowntimeSwitchOpts(ctx, tt.existing, replID, tt.opts)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
			})
		}
	}
}

func userRepl(user, from, to string) entity.UniversalReplicationID {
	return entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
		User:        user,
		FromStorage: from,
		ToStorage:   to,
	})
}

func bucketRepl(user, fromStor, toStor, fromBuck, toBuck string) entity.UniversalReplicationID {
	return entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User:        user,
		FromStorage: fromStor,
		ToStorage:   toStor,
		FromBucket:  fromBuck,
		ToBucket:    toBuck,
	})
}

func Test_policySvc_SetDowntimeReplicationSwitch(t *testing.T) {
	c := testutil.SetupRedis(t)
	queuesMock := &tasks.QueueServiceMock{}
	tasks.Reset(queuesMock)
	validSwitch := &entity.ReplicationSwitchDowntimeOpts{
		StartOnInitDone:     false,
		Cron:                stringPtr("0 0 * * *"),
		StartAt:             nil,
		MaxDuration:         (3 * time.Hour),
		MaxEventLag:         uint32Ptr(100),
		SkipBucketCheck:     true,
		ContinueReplication: true,
	}

	type before struct {
		userRepl            *entity.UserReplicationPolicy
		userReplHasSwitch   bool
		userReplInitDone    bool
		bucketRepl          *entity.BucketReplicationPolicy
		bucketReplInitDone  bool
		bucketReplHasSwitch bool
		bucketReplAgent     string
	}
	tests := []struct {
		name    string
		before  before
		policy  entity.UniversalReplicationID
		opts    *entity.ReplicationSwitchDowntimeOpts
		wantErr error
	}{
		{
			name: "ok: bucket switch",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "",
			},
			policy:  bucketRepl("user", "main", "follower", "bucket", "bucket"),
			opts:    validSwitch,
			wantErr: nil,
		},
		{
			name: "err: bucket replication has agent",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "agent",
			},
			policy:  bucketRepl("user", "main", "follower", "bucket", "bucket"),
			opts:    validSwitch,
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "err: bucket replication has custom dest bucket",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket-2",
				},
				bucketReplAgent: "agent",
			},
			policy:  bucketRepl("user", "main", "follower", "bucket", "bucket-2"),
			opts:    validSwitch,
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "err: start on init but init not done",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "",
			},
			policy:  bucketRepl("user", "main", "follower", "bucket", "bucket"),
			opts:    nil,
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "ok: start on init done",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent:    "",
				bucketReplInitDone: true,
			},
			policy:  bucketRepl("user", "main", "follower", "bucket", "bucket"),
			opts:    nil,
			wantErr: nil,
		},
		{
			name: "err: bucket switch for user replication",
			before: before{
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				},
			},
			policy:  bucketRepl("user", "main", "follower", "bucket", "bucket"),
			opts:    validSwitch,
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: user switch for bucket replication",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
			},
			policy:  userRepl("user", "main", "follower"),
			opts:    validSwitch,
			wantErr: dom.ErrNotFound,
		},
		{
			name: "ok: user switch match",
			before: before{
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				},
			},
			policy:  userRepl("user", "main", "follower"),
			opts:    validSwitch,
			wantErr: nil,
		},
		{
			name: "err: user switch user not match",
			before: before{
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				},
			},
			policy:  userRepl("user-2", "main", "follower"),
			opts:    validSwitch,
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: user switch from storage not match",
			before: before{
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				},
			},
			policy:  userRepl("user", "main-2", "follower"),
			opts:    validSwitch,
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: user switch to storage not match",
			before: before{
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				},
			},
			policy:  userRepl("user", "main", "follower-2"),
			opts:    validSwitch,
			wantErr: dom.ErrNotFound,
		},
		{
			name: "ok: bucket switch  match",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "",
			},
			policy:  bucketRepl("user", "main", "follower", "bucket", "bucket"),
			opts:    validSwitch,
			wantErr: nil,
		},
		{
			name: "err: bucket switch user not match",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "",
			},
			policy:  bucketRepl("user-2", "main", "follower", "bucket", "bucket"),
			opts:    validSwitch,
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: bucket switch from stor not  match",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "",
			},
			policy:  bucketRepl("user", "main-2", "follower", "bucket", "bucket"),
			opts:    validSwitch,
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: bucket switch to stor not match",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "",
			},
			policy:  bucketRepl("user", "main", "follower-2", "bucket", "bucket"),
			opts:    validSwitch,
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: bucket switch from bucket not match",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "",
			},
			policy:  bucketRepl("user", "main", "follower", "bucket-2", "bucket"),
			opts:    validSwitch,
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: bucket switch to bucket not  match",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "",
			},
			policy:  bucketRepl("user", "main", "follower", "bucket", "bucket-2"),
			opts:    validSwitch,
			wantErr: dom.ErrNotFound,
		},
		{
			name: "ok: bucket switch updated",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplHasSwitch: true,
				bucketReplAgent:     "",
			},
			policy:  bucketRepl("user", "main", "follower", "bucket", "bucket"),
			opts:    validSwitch,
			wantErr: nil,
		},
		{
			name: "ok: user switch updated",
			before: before{
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				},
				userReplHasSwitch: true,
			},
			policy:  userRepl("user", "main", "follower"),
			opts:    validSwitch,
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := c.FlushAll(t.Context()).Err(); err != nil {
				t.Fatal(err)
			}
			tasks.Reset(queuesMock)
			testTime := time.Now().UTC()
			entity.TimeNow = func() time.Time {
				return testTime
			}
			defer func() {
				entity.TimeNow = time.Now
			}()
			r := require.New(t)
			svc := NewService(c, queuesMock, "main")

			ctx := t.Context()

			if tt.before.userRepl != nil {
				err := svc.AddUserReplicationPolicy(ctx, *tt.before.userRepl, entity.ReplicationOptions{})
				r.NoError(err)
				if tt.before.userReplHasSwitch {
					uid := entity.UniversalFromUserReplication(*tt.before.userRepl)
					err = svc.SetDowntimeReplicationSwitch(ctx, uid, nil)
					r.NoError(err)
				}
				if tt.before.userReplInitDone {
					queuesMock.InitReplicationDone(entity.UniversalFromUserReplication(*tt.before.userRepl))
				} else {
					queuesMock.InitReplicationInProgress(entity.UniversalFromUserReplication(*tt.before.userRepl))
				}
			}
			if tt.before.bucketRepl != nil {
				err := svc.AddBucketReplicationPolicy(ctx, *tt.before.bucketRepl, entity.ReplicationOptions{AgentURL: tt.before.bucketReplAgent})
				r.NoError(err)
				if tt.before.bucketReplHasSwitch {
					bid := entity.UniversalFromBucketReplication(*tt.before.bucketRepl)
					err = svc.SetDowntimeReplicationSwitch(ctx, bid, nil)
					r.NoError(err)
				}
				if tt.before.bucketReplInitDone {
					queuesMock.InitReplicationDone(entity.UniversalFromBucketReplication(*tt.before.bucketRepl))
				} else {
					queuesMock.InitReplicationInProgress(entity.UniversalFromBucketReplication(*tt.before.bucketRepl))
				}
			}
			routeBefore, err := svc.getRoutingForReplication(ctx, tt.policy)
			r.NoError(err)

			err = svc.SetDowntimeReplicationSwitch(ctx, tt.policy, tt.opts)
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				routeAfter, err := svc.getRoutingForReplication(ctx, tt.policy)
				r.NoError(err)
				r.EqualValues(routeBefore, routeAfter)
				return
			}
			r.NoError(err)
			routeAfter, err := svc.getRoutingForReplication(ctx, tt.policy)
			r.NoError(err)
			r.EqualValues(routeBefore, routeAfter)

			info, err := svc.GetReplicationPolicyInfoExtended(ctx, tt.policy)
			r.NoError(err)
			r.NotNil(info.Switch)
			r.Equal(entity.StatusNotStarted, info.Switch.LastStatus)
			if tt.opts != nil {
				r.EqualValues(*tt.opts, info.Switch.ReplicationSwitchDowntimeOpts)
			} else {
				r.EqualValues(entity.ReplicationSwitchDowntimeOpts{}, info.Switch.ReplicationSwitchDowntimeOpts)
			}
			r.EqualValues(info.Switch.CreatedAt, testTime)
			r.EqualValues(info.Switch.ReplicationIDStr, tt.policy.AsString())

			switchInfo, err := svc.GetReplicationSwitchInfo(ctx, tt.policy)
			r.NoError(err)
			r.EqualValues(*info.Switch, switchInfo)
		})
	}
}

func Test_policySvc_AddZeroDowntimeSwitch(t *testing.T) {
	c := testutil.SetupRedis(t)
	queuesMock := &tasks.QueueServiceMock{}
	tasks.Reset(queuesMock)

	validSwitch := &entity.ReplicationSwitchZeroDowntimeOpts{
		MultipartTTL: 15 * time.Minute,
	}

	type before struct {
		userRepl            *entity.UserReplicationPolicy
		userReplHasSwitch   bool
		bucketRepl          *entity.BucketReplicationPolicy
		bucketReplHasSwitch bool
		bucketReplAgent     string
		replPaused          bool
		replInitDone        bool
	}
	tests := []struct {
		name    string
		before  before
		policy  entity.UniversalReplicationID
		opts    *entity.ReplicationSwitchZeroDowntimeOpts
		wantErr error
	}{
		{
			name: "ok: bucket switch",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "",
				replInitDone:    true,
			},
			policy:  bucketRepl("user", "main", "follower", "bucket", "bucket"),
			opts:    validSwitch,
			wantErr: nil,
		},
		{
			name: "err: empty opts",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "",
				replInitDone:    true,
			},
			policy:  bucketRepl("user", "main", "follower", "bucket", "bucket"),
			opts:    nil,
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "err: multipart ttl zero",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "",
				replInitDone:    true,
			},
			policy:  bucketRepl("user", "main", "follower", "bucket", "bucket"),
			opts:    &entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: 0},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "err: bucket replication has agent",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "agent",
				replInitDone:    true,
			},
			policy:  bucketRepl("user", "main", "follower", "bucket", "bucket"),
			opts:    validSwitch,
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "err: bucket replication has custom dest bucket",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket-2",
				},
				bucketReplAgent: "agent",
				replInitDone:    true,
			},
			policy:  bucketRepl("user", "main", "follower", "bucket", "bucket-2"),
			opts:    validSwitch,
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "err: init not done",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "",
			},
			policy:  bucketRepl("user", "main", "follower", "bucket", "bucket"),
			opts:    validSwitch,
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "err: bucket repl paused",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "",
				replInitDone:    true,
				replPaused:      true,
			},
			policy:  bucketRepl("user", "main", "follower", "bucket", "bucket"),
			opts:    validSwitch,
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "err: bucket switch for user replication",
			before: before{
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				},
				replInitDone: true,
			},
			policy:  bucketRepl("user", "main", "follower", "bucket", "bucket"),
			opts:    validSwitch,
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: user switch for bucket replication",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				replInitDone: true,
			},
			policy:  userRepl("user", "main", "follower"),
			opts:    validSwitch,
			wantErr: dom.ErrNotFound,
		},
		{
			name: "ok: user switch match",
			before: before{
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				},
				replInitDone: true,
			},
			policy:  userRepl("user", "main", "follower"),
			opts:    validSwitch,
			wantErr: nil,
		},
		{
			name: "err: user switch user not match",
			before: before{
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				},
				replInitDone: true,
			},
			policy:  userRepl("user-2", "main", "follower"),
			opts:    validSwitch,
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: user switch from storage not match",
			before: before{
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				},
				replInitDone: true,
			},
			policy:  userRepl("user", "main-2", "follower"),
			opts:    validSwitch,
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: user switch to storage not match",
			before: before{
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				},
				replInitDone: true,
			},
			policy:  userRepl("user", "main", "follower-2"),
			opts:    validSwitch,
			wantErr: dom.ErrNotFound,
		},
		{
			name: "ok: bucket switch  match",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "",
				replInitDone:    true,
			},
			policy:  bucketRepl("user", "main", "follower", "bucket", "bucket"),
			opts:    validSwitch,
			wantErr: nil,
		},
		{
			name: "err: bucket switch user not match",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "",
				replInitDone:    true,
			},
			policy:  bucketRepl("user-2", "main", "follower", "bucket", "bucket"),
			opts:    validSwitch,
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: bucket switch from stor not  match",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "",
				replInitDone:    true,
			},
			policy:  bucketRepl("user", "main-2", "follower", "bucket", "bucket"),
			opts:    validSwitch,
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: bucket switch to stor not match",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "",
				replInitDone:    true,
			},
			policy:  bucketRepl("user", "main", "follower-2", "bucket", "bucket"),
			opts:    validSwitch,
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: bucket switch from bucket not match",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "",
				replInitDone:    true,
			},
			policy:  bucketRepl("user", "main", "follower", "bucket-2", "bucket"),
			opts:    validSwitch,
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: bucket switch to bucket not  match",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplAgent: "",
				replInitDone:    true,
			},
			policy:  bucketRepl("user", "main", "follower", "bucket", "bucket-2"),
			opts:    validSwitch,
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: bucket has switch",
			before: before{
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					FromBucket:  "bucket",
					ToStorage:   "follower",
					ToBucket:    "bucket",
				},
				bucketReplHasSwitch: true,
				replInitDone:        true,
				bucketReplAgent:     "",
			},
			policy:  bucketRepl("user", "main", "follower", "bucket", "bucket"),
			opts:    validSwitch,
			wantErr: dom.ErrAlreadyExists,
		},
		{
			name: "err: user has switch",
			before: before{
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				},
				userReplHasSwitch: true,
				replInitDone:      true,
			},
			policy:  userRepl("user", "main", "follower"),
			opts:    validSwitch,
			wantErr: dom.ErrAlreadyExists,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup
			if err := c.FlushAll(t.Context()).Err(); err != nil {
				t.Fatal(err)
			}
			tasks.Reset(queuesMock)
			testTime := time.Now().UTC()
			entity.TimeNow = func() time.Time {
				return testTime
			}
			defer func() {
				entity.TimeNow = time.Now
			}()
			r := require.New(t)
			svc := NewService(c, queuesMock, "main")

			ctx := t.Context()

			if tt.before.userRepl != nil {
				err := svc.AddUserReplicationPolicy(ctx, *tt.before.userRepl, entity.ReplicationOptions{})
				r.NoError(err)
				if tt.before.userReplHasSwitch {
					uid := entity.UniversalFromUserReplication(*tt.before.userRepl)
					err = svc.SetDowntimeReplicationSwitch(ctx, uid, nil)
					r.NoError(err)
				}
				if tt.before.replInitDone {
					queuesMock.InitReplicationDone(entity.UniversalFromUserReplication(*tt.before.userRepl))
				} else {
					queuesMock.InitReplicationInProgress(entity.UniversalFromUserReplication(*tt.before.userRepl))
				}
				if tt.before.replPaused {
					queuesMock.Pause(ctx, tasks.InitMigrationListQueue(entity.UniversalFromUserReplication(*tt.before.userRepl)))
				}
			}
			if tt.before.bucketRepl != nil {
				err := svc.AddBucketReplicationPolicy(ctx, *tt.before.bucketRepl, entity.ReplicationOptions{AgentURL: tt.before.bucketReplAgent})
				r.NoError(err)
				if tt.before.bucketReplHasSwitch {
					bid := entity.UniversalFromBucketReplication(*tt.before.bucketRepl)
					err = svc.SetDowntimeReplicationSwitch(ctx, bid, nil)
					r.NoError(err)
				}
				if tt.before.replInitDone {
					queuesMock.InitReplicationDone(entity.UniversalFromBucketReplication(*tt.before.bucketRepl))
				} else {
					queuesMock.InitReplicationInProgress(entity.UniversalFromBucketReplication(*tt.before.bucketRepl))
				}
				if tt.before.replPaused {
					queuesMock.Pause(ctx, tasks.InitMigrationListQueue(entity.UniversalFromBucketReplication(*tt.before.bucketRepl)))
				}
			}
			// setup done
			// test

			routeBefore, err := svc.getRoutingForReplication(ctx, tt.policy)
			r.NoError(err)

			err = svc.AddZeroDowntimeReplicationSwitch(ctx, tt.policy, tt.opts)
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				return
			}
			r.NoError(err)

			// verify switch status
			info, err := svc.GetReplicationPolicyInfoExtended(ctx, tt.policy)
			r.NoError(err)
			r.NotNil(info.Switch)
			r.Equal(entity.StatusInProgress, info.Switch.LastStatus)
			r.NotNil(info.Switch.LastStartedAt)
			r.EqualValues(testTime, *info.Switch.LastStartedAt)
			if tt.opts != nil {
				r.EqualValues(*tt.opts, info.Switch.ReplicationSwitchZeroDowntimeOpts)
			} else {
				r.EqualValues(entity.ReplicationSwitchZeroDowntimeOpts{}, info.Switch.ReplicationSwitchZeroDowntimeOpts)
			}
			r.EqualValues(info.Switch.CreatedAt, testTime)
			r.EqualValues(info.Switch.ReplicationIDStr, tt.policy.AsString())
			// replication is archived now
			r.True(info.IsArchived)
			r.NotNil(info.ArchivedAt)
			r.EqualValues(testTime, *info.ArchivedAt)

			// replication policy is removed
			replicationPolicyExists(t, svc, tt.policy, false)

			switchInfo, err := svc.GetReplicationSwitchInfo(ctx, tt.policy)
			r.NoError(err)
			r.EqualValues(*info.Switch, switchInfo)

			// verify routing
			routeAfter, err := svc.getRoutingForReplication(ctx, tt.policy)
			r.NoError(err)
			r.NotEqualValues(routeBefore, routeAfter, "routing should change")
			r.EqualValues(tt.policy.FromStorage(), routeBefore)
			r.EqualValues(tt.policy.ToStorage(), routeAfter)
		})
	}
}

func replicationPolicyExists(t *testing.T, s *policySvc, policy entity.UniversalReplicationID, exists bool) {
	t.Helper()
	r := require.New(t)
	if bucketPolicy, ok := policy.AsBucketID(); ok {
		got, err := s.bucketReplicationPolicyStore.Get(t.Context(), bucketPolicy.LookupID())
		if exists {
			r.NoError(err)
			r.Contains(got, bucketPolicy)
		} else {
			r.NotContains(got, bucketPolicy)
		}
	} else if userPolicy, ok := policy.AsUserID(); ok {
		got, err := s.userReplicationPolicyStore.Get(t.Context(), userPolicy.LookupID())
		if exists {
			r.NoError(err)
			r.Contains(got, userPolicy)
		} else {
			r.NotContains(got, userPolicy)
		}
	} else {
		// should never happen
		t.Fatal("invalid replication policy type")
	}
}

func Test_policySvc_UpdateDowntimeSwitchStatus(t *testing.T) {
	c := testutil.SetupRedis(t)
	ctx := context.TODO()

	queuesMock := &tasks.QueueServiceMock{}
	tasks.Reset(queuesMock)
	svc := NewService(c, queuesMock, "f")
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

	rplications := map[string]entity.UniversalReplicationID{
		"user": entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
			User:        "u",
			FromStorage: "f",
			ToStorage:   "t",
		}),
		"bucket": entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
			User:        "u",
			FromStorage: "f",
			FromBucket:  "b",
			ToStorage:   "t",
			ToBucket:    "b",
		}),
	}

	statuses := []entity.ReplicationSwitchStatus{
		entity.StatusNotStarted,
		entity.StatusInProgress,
		entity.StatusCheckInProgress,
		entity.StatusDone,
		entity.StatusSkipped,
		entity.StatusError,
	}
	for replType, replID := range rplications {
		t.Run(replType+"to not_started", func(t *testing.T) {
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
					r.ErrorIs(svc.UpdateDowntimeSwitchStatus(ctx, replID, entity.StatusNotStarted, "test"), dom.ErrInvalidArg, "transition not allowed")
				})
			}
		})
		t.Run(replType+"to in_progress", func(t *testing.T) {
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

					err := svc.UpdateDowntimeSwitchStatus(ctx, replID, entity.StatusInProgress, "test")
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
						// check that routing is blocked
						_, err = svc.getRoutingForReplication(ctx, replID)
						r.ErrorIs(err, dom.ErrRoutingBlock, "routing is blocked")
						// check that replication remains the same
						replicationPolicyExists(t, svc, replID, true)
					} else {
						r.ErrorIs(err, dom.ErrInvalidArg, "transition not allowed")
					}
				})
			}
		})
		t.Run(replType+"to check_in_progress", func(t *testing.T) {
			for _, status := range statuses {
				t.Run("from "+string(status), func(t *testing.T) {
					r := require.New(t)
					// cleanup redis
					r.NoError(c.FlushAll(ctx).Err())
					// reset queues mock
					tasks.Reset(queuesMock)
					// create switch in status
					setupDowntimeSwitchState(t, svc, queuesMock, replID, validSwitch, status)

					err := svc.UpdateDowntimeSwitchStatus(ctx, replID, entity.StatusCheckInProgress, "test")
					if status == entity.StatusInProgress {
						r.NoError(err, "transition is allowed")

						// validate switch state
						info, err := svc.GetReplicationSwitchInfo(ctx, replID)
						r.NoError(err)
						r.Equal(entity.StatusCheckInProgress, info.LastStatus)
						r.Nil(info.DoneAt)
						r.NotNil(info.LastStartedAt)
						r.NotEmpty(info.History)
						// check that routing is blocked
						_, err = svc.getRoutingForReplication(ctx, replID)
						r.ErrorIs(err, dom.ErrRoutingBlock, "routing is blocked")
						// check that replication remains the same
						replicationPolicyExists(t, svc, replID, true)
					} else {
						r.ErrorIs(err, dom.ErrInvalidArg, "transition not allowed")
					}
				})
			}
		})
		t.Run(replType+"to done", func(t *testing.T) {
			for _, status := range statuses {
				t.Run("from "+string(status), func(t *testing.T) {
					r := require.New(t)
					// cleanup redis
					r.NoError(c.FlushAll(ctx).Err())
					// reset queues mock
					tasks.Reset(queuesMock)
					// create switch in status
					setupDowntimeSwitchState(t, svc, queuesMock, replID, validSwitch, status)

					err := svc.UpdateDowntimeSwitchStatus(ctx, replID, entity.StatusDone, "test")
					if status == entity.StatusCheckInProgress || status == entity.StatusDone {
						r.NoError(err, "transition is allowed")

						// validate switch state
						info, err := svc.GetReplicationSwitchInfo(ctx, replID)
						r.NoError(err)
						r.Equal(entity.StatusDone, info.LastStatus)
						r.NotNil(info.DoneAt)
						r.NotNil(info.LastStartedAt)
						r.NotEmpty(info.History)
						// check that routing is switched
						routeToStorage, err := svc.getRoutingForReplication(ctx, replID)
						r.NoError(err)
						r.Equal(replID.ToStorage(), routeToStorage, "routing is switched")
					} else {
						r.ErrorIs(err, dom.ErrInvalidArg, "transition not allowed")
					}
				})
			}
		})
		t.Run(replType+"to skipped", func(t *testing.T) {
			for _, status := range statuses {
				t.Run("from "+string(status), func(t *testing.T) {
					r := require.New(t)
					// cleanup redis
					r.NoError(c.FlushAll(ctx).Err())
					// reset queues mock
					tasks.Reset(queuesMock)
					// create switch in status
					setupDowntimeSwitchState(t, svc, queuesMock, replID, validSwitch, status)

					err := svc.UpdateDowntimeSwitchStatus(ctx, replID, entity.StatusSkipped, "test")
					if status == entity.StatusNotStarted || status == entity.StatusSkipped || status == entity.StatusError {
						r.NoError(err, "transition is allowed")
						// validate switch state
						info, err := svc.GetReplicationSwitchInfo(ctx, replID)
						r.NoError(err)
						r.Equal(entity.StatusSkipped, info.LastStatus)
						r.Nil(info.DoneAt)
						r.NotEmpty(info.History)
						// check that routing to old bucket
						routeToStorage, err := svc.getRoutingForReplication(ctx, replID)
						r.NoError(err)
						r.Equal(replID.FromStorage(), routeToStorage, "routing is to old bucket")
						// check that replication remains the same
						replicationPolicyExists(t, svc, replID, true)
					} else {
						r.ErrorIs(err, dom.ErrInvalidArg, "transition not allowed")
					}
				})
			}
		})
		t.Run(replType+"to error", func(t *testing.T) {
			for _, status := range statuses {
				t.Run("from "+string(status), func(t *testing.T) {
					r := require.New(t)
					// cleanup redis
					r.NoError(c.FlushAll(ctx).Err())
					// reset queues mock
					tasks.Reset(queuesMock)
					// create switch in status
					setupDowntimeSwitchState(t, svc, queuesMock, replID, validSwitch, status)

					err := svc.UpdateDowntimeSwitchStatus(ctx, replID, entity.StatusError, "test")
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
						// check that routing to old bucket
						routeToStorage, err := svc.getRoutingForReplication(ctx, replID)
						r.NoError(err)
						r.Equal(replID.FromStorage(), routeToStorage, "routing is to old bucket")
						// check that replication remains the same
						replicationPolicyExists(t, svc, replID, true)
					}
				})
			}
		})
	}

}

func setupDowntimeSwitchState(t *testing.T, svc *policySvc, queuesMock *tasks.QueueServiceMock, replID entity.UniversalReplicationID, opts *entity.ReplicationSwitchDowntimeOpts, status entity.ReplicationSwitchStatus) {
	t.Helper()
	ctx := context.TODO()
	r := require.New(t)

	// create routing and replication
	if bucketID, ok := replID.AsBucketID(); ok {
		r.NoError(svc.AddBucketReplicationPolicy(ctx, bucketID, entity.ReplicationOptions{}))
	} else if userID, ok := replID.AsUserID(); ok {
		r.NoError(svc.AddUserReplicationPolicy(ctx, userID, entity.ReplicationOptions{}))
	} else {
		t.Fatal("invalid replication ID type")
	}
	queuesMock.InitReplicationInProgress(replID)
	// create switch
	err := svc.SetDowntimeReplicationSwitch(ctx, replID, opts)
	r.NoError(err)
	// check that switch was created in NOT_STARTED status
	got, err := svc.GetReplicationSwitchInfo(ctx, replID)
	r.NoError(err)
	r.False(got.IsZeroDowntime())
	r.Equal(replID.AsString(), got.ReplicationIDStr)
	r.Equal(entity.StatusNotStarted, got.LastStatus)
	r.Nil(got.LastStartedAt)
	// check that routing policy was not changed
	routeToStorage, err := svc.getRoutingForReplication(ctx, replID)
	r.NoError(err)
	r.Equal(replID.FromStorage(), routeToStorage, "routing policy was not changed")
	// check that replication policy was not changed
	replicationPolicyExists(t, svc, replID, true)

	if status == entity.StatusNotStarted {
		return
	}
	if status == entity.StatusSkipped || status == entity.StatusError {
		r.NoError(svc.UpdateDowntimeSwitchStatus(ctx, replID, status, "test"))
		// check that switch was changed
		got, err := svc.GetReplicationSwitchInfo(ctx, replID)
		r.NoError(err)
		r.False(got.IsZeroDowntime())
		r.Equal(replID.AsString(), got.ReplicationIDStr)
		r.Equal(status, got.LastStatus)
		r.Nil(got.LastStartedAt)
		// check that routing policy was not changed
		routeToStorage, err := svc.getRoutingForReplication(ctx, replID)
		r.NoError(err)
		r.Equal(replID.FromStorage(), routeToStorage, "routing policy was not changed")
		// check that replication policy was not changed
		replicationPolicyExists(t, svc, replID, true)
		return
	}

	// move to in progress
	r.NoError(svc.UpdateDowntimeSwitchStatus(ctx, replID, entity.StatusInProgress, "test"))
	// check that switch was changed
	got, err = svc.GetReplicationSwitchInfo(ctx, replID)
	r.NoError(err)
	r.False(got.IsZeroDowntime())
	r.Equal(replID.AsString(), got.ReplicationIDStr)
	r.Equal(entity.StatusInProgress, got.LastStatus)
	r.NotNil(got.LastStartedAt)
	// check that routing was blocked
	_, err = svc.getRoutingForReplication(ctx, replID)
	r.ErrorIs(err, dom.ErrRoutingBlock)
	// check that replication policy was not changed
	replicationPolicyExists(t, svc, replID, true)
	if status == entity.StatusInProgress {
		return
	}
	// move to check in progress
	r.NoError(svc.UpdateDowntimeSwitchStatus(ctx, replID, entity.StatusCheckInProgress, "test"))
	// check that switch was changed
	got, err = svc.GetReplicationSwitchInfo(ctx, replID)
	r.NoError(err)
	r.False(got.IsZeroDowntime())
	r.Equal(replID.AsString(), got.ReplicationIDStr)
	r.Equal(entity.StatusCheckInProgress, got.LastStatus)
	r.NotNil(got.LastStartedAt)
	r.Nil(got.DoneAt)
	// check that routing was blocked
	_, err = svc.getRoutingForReplication(ctx, replID)
	r.ErrorIs(err, dom.ErrRoutingBlock)
	// check that replication policy was not changed
	replicationPolicyExists(t, svc, replID, true)
	if status == entity.StatusCheckInProgress {
		return
	}
	// move to done
	r.NoError(svc.UpdateDowntimeSwitchStatus(ctx, replID, entity.StatusDone, "test"))
	// check that switch was changed
	got, err = svc.GetReplicationSwitchInfo(ctx, replID)
	r.NoError(err)
	r.False(got.IsZeroDowntime())
	r.Equal(replID.AsString(), got.ReplicationIDStr)
	r.Equal(entity.StatusDone, got.LastStatus)
	r.NotNil(got.LastStartedAt)
	r.NotNil(got.DoneAt)
	// check that routing switched
	routeToStorage, err = svc.getRoutingForReplication(ctx, replID)
	r.NoError(err)
	r.Equal(replID.ToStorage(), routeToStorage, "routing policy was changed")
	// check that replication policy was archived
	repl, err := svc.GetReplicationPolicyInfoExtended(ctx, replID)
	r.NoError(err)
	r.True(repl.IsArchived)
	r.NotNil(repl.ArchivedAt)
	replicationPolicyExists(t, svc, replID, false)
	if opts != nil && opts.ContinueReplication {
		backward := replID.Swap()
		replicationPolicyExists(t, svc, backward, true)

		repl, err := svc.GetReplicationPolicyInfoExtended(ctx, backward)
		r.NoError(err)
		r.False(repl.IsArchived)
	}
	// we covered all status. return for status done
	r.Equal(entity.StatusDone, status)
}

func Test_ZeroDowntimeSwitch_E2E(t *testing.T) {
	c := testutil.SetupRedis(t)
	queuesMock := &tasks.QueueServiceMock{}
	tasks.Reset(queuesMock)

	validSwitch := &entity.ReplicationSwitchZeroDowntimeOpts{
		MultipartTTL: 15 * time.Minute,
	}

	replications := map[string]entity.UniversalReplicationID{
		"user": entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
			User:        "user",
			FromStorage: "main",
			ToStorage:   "follower",
		}),
		"bucket": entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
			User:        "user",
			FromStorage: "main",
			FromBucket:  "bucket",
			ToStorage:   "follower",
			ToBucket:    "bucket",
		}),
	}

	for replType, replID := range replications {
		t.Run(replType, func(t *testing.T) {
			// setup
			if err := c.FlushAll(t.Context()).Err(); err != nil {
				t.Fatal(err)
			}
			tasks.Reset(queuesMock)
			r := require.New(t)
			svc := NewService(c, queuesMock, "main")
			ctx := t.Context()

			// proxy ctx empty
			proxyCtx, err := svc.BuildProxyContext(ctx, "user", "bucket")
			r.NoError(err)
			r.Empty(xctx.GetReplications(proxyCtx), "no replications in proxy ctx")
			r.Equal("main", xctx.GetRoutingPolicy(proxyCtx), "current storage is main")
			r.Nil(xctx.GetInProgressZeroDowntime(proxyCtx), "no zero downtime in progress")

			// create routing and replication
			if bucketID, ok := replID.AsBucketID(); ok {
				r.NoError(svc.AddBucketReplicationPolicy(ctx, bucketID, entity.ReplicationOptions{}))
			} else if userID, ok := replID.AsUserID(); ok {
				r.NoError(svc.AddUserReplicationPolicy(ctx, userID, entity.ReplicationOptions{}))
			} else {
				t.Fatal("invalid replication ID type")
			}

			// proxy ctx has replications
			proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
			r.NoError(err)
			r.Len(xctx.GetReplications(proxyCtx), 1, "one replication in proxy ctx")
			r.Equal(replID.AsString(), xctx.GetReplications(proxyCtx)[0].AsString())
			r.Equal("main", xctx.GetRoutingPolicy(proxyCtx), "current storage is main")
			r.Nil(xctx.GetInProgressZeroDowntime(proxyCtx), "no zero downtime in progress")

			queuesMock.InitReplicationInProgress(replID)
			// check that replication policy was created
			replicationPolicyExists(t, svc, replID, true)
			info, err := svc.GetReplicationPolicyInfoExtended(ctx, replID)
			r.NoError(err)
			r.False(info.IsArchived)
			r.Nil(info.Switch)
			r.False(info.InitDone())
			// check routing
			routeTo, err := svc.getRoutingForReplication(ctx, replID)
			r.NoError(err)
			r.Equal(replID.FromStorage(), routeTo)
			// check that switch does not exist
			_, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.ErrorIs(err, dom.ErrNotFound)

			// cannot complete non existing switch
			err = svc.CompleteZeroDowntimeReplicationSwitch(ctx, replID)
			r.Error(err)

			// create zero downtime switch
			queuesMock.InitReplicationDone(replID)
			err = svc.AddZeroDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.NoError(err)

			// proxy ctx has replications and switch in progress
			proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
			r.NoError(err)
			r.Empty(xctx.GetReplications(proxyCtx), "no replications in proxy ctx when zero downtime in progress")
			r.Equal("follower", xctx.GetRoutingPolicy(proxyCtx), "current storage is follower when zero downtime in progress")
			r.NotNil(xctx.GetInProgressZeroDowntime(proxyCtx), "no zero downtime in progress")

			// verify switch and replication status
			info, err = svc.GetReplicationPolicyInfoExtended(ctx, replID)
			r.NoError(err)
			r.NotNil(info.Switch)
			r.Equal(entity.StatusInProgress, info.Switch.LastStatus)
			r.NotNil(info.Switch.LastStartedAt)
			r.EqualValues(*validSwitch, info.Switch.ReplicationSwitchZeroDowntimeOpts)
			r.EqualValues(info.IsArchived, true)
			r.NotNil(info.ArchivedAt)
			r.EqualValues(info.InitDone(), true)

			switchInfo, err := svc.GetReplicationSwitchInfo(ctx, replID)
			r.NoError(err)
			r.EqualValues(*info.Switch, switchInfo)
			proxySwitchInfo := xctx.GetInProgressZeroDowntime(proxyCtx)
			r.EqualValues(*proxySwitchInfo, switchInfo)

			// replication policy deleted
			replicationPolicyExists(t, svc, replID, false)
			// routing switched
			routeTo, err = svc.getRoutingForReplication(ctx, replID)
			r.NoError(err)
			r.Equal(replID.ToStorage(), routeTo)

			// delete switch
			err = svc.DeleteReplicationSwitch(ctx, replID)
			r.NoError(err)

			_, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.ErrorIs(err, dom.ErrNotFound)

			info, err = svc.GetReplicationPolicyInfoExtended(ctx, replID)
			r.NoError(err)
			r.Nil(info.Switch)
			r.True(info.IsArchived, "replication remains archived after switch deletion")
			replicationPolicyExists(t, svc, replID, false)

			// routing reverted
			routeTo, err = svc.getRoutingForReplication(ctx, replID)
			r.NoError(err)
			r.Equal(replID.FromStorage(), routeTo)

			// proxy ctx has no replications and switch in progress
			proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
			r.NoError(err)
			r.Empty(xctx.GetReplications(proxyCtx), "no replications in proxy ctx when zero downtime in progress")
			r.Equal("main", xctx.GetRoutingPolicy(proxyCtx), "current storage is main when no zero downtime in progress")
			r.Nil(xctx.GetInProgressZeroDowntime(proxyCtx), "no zero downtime in progress")

			// remove replication
			if bucketID, ok := replID.AsBucketID(); ok {
				r.NoError(svc.DeleteBucketReplication(ctx, bucketID))
			} else if userID, ok := replID.AsUserID(); ok {
				r.NoError(svc.DeleteUserReplication(ctx, userID))
			} else {
				t.Fatal("invalid replication ID type")
			}

			replicationPolicyExists(t, svc, replID, false)

			// recreate replication and switch again
			if bucketID, ok := replID.AsBucketID(); ok {
				r.NoError(svc.AddBucketReplicationPolicy(ctx, bucketID, entity.ReplicationOptions{}))
			} else if userID, ok := replID.AsUserID(); ok {
				r.NoError(svc.AddUserReplicationPolicy(ctx, userID, entity.ReplicationOptions{}))
			} else {
				t.Fatal("invalid replication ID type")
			}
			queuesMock.InitReplicationInProgress(replID)

			// proxy ctx has replications but no switch in progress
			proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
			r.NoError(err)
			r.Len(xctx.GetReplications(proxyCtx), 1, "one replication in proxy ctx")
			r.Equal(replID.AsString(), xctx.GetReplications(proxyCtx)[0].AsString())
			r.Equal("main", xctx.GetRoutingPolicy(proxyCtx), "current storage is main when no zero downtime in progress")
			r.Nil(xctx.GetInProgressZeroDowntime(proxyCtx), "no zero downtime in progress")

			info, err = svc.GetReplicationPolicyInfoExtended(ctx, replID)
			r.NoError(err)
			r.False(info.IsArchived)
			r.Nil(info.Switch)
			r.False(info.InitDone())

			routeTo, err = svc.getRoutingForReplication(ctx, replID)
			r.NoError(err)
			r.Equal(replID.FromStorage(), routeTo)

			queuesMock.InitReplicationDone(replID)
			err = svc.AddZeroDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.NoError(err)
			// verify switch and replication status
			info, err = svc.GetReplicationPolicyInfoExtended(ctx, replID)
			r.NoError(err)
			r.NotNil(info.Switch)
			r.Equal(entity.StatusInProgress, info.Switch.LastStatus)
			r.NotNil(info.Switch.LastStartedAt)
			r.Nil(info.Switch.DoneAt)
			r.EqualValues(*validSwitch, info.Switch.ReplicationSwitchZeroDowntimeOpts)
			r.EqualValues(info.IsArchived, true)
			r.NotNil(info.ArchivedAt)

			switchInfo, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.NoError(err)
			r.EqualValues(*info.Switch, switchInfo)

			// replication policy deleted
			replicationPolicyExists(t, svc, replID, false)
			// routing switched
			routeTo, err = svc.getRoutingForReplication(ctx, replID)
			r.NoError(err)
			r.Equal(replID.ToStorage(), routeTo)

			// proxy ctx has no replications and switch in progress
			proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
			r.NoError(err)
			r.Empty(xctx.GetReplications(proxyCtx))
			r.Equal("follower", xctx.GetRoutingPolicy(proxyCtx))
			r.NotNil(xctx.GetInProgressZeroDowntime(proxyCtx))

			// complete switch
			err = svc.CompleteZeroDowntimeReplicationSwitch(ctx, replID)
			r.NoError(err)

			info, err = svc.GetReplicationPolicyInfoExtended(ctx, replID)
			r.NoError(err)
			r.NotNil(info.Switch)
			r.Equal(entity.StatusDone, info.Switch.LastStatus)
			r.NotNil(info.Switch.LastStartedAt)
			r.NotNil(info.Switch.DoneAt)
			r.EqualValues(*validSwitch, info.Switch.ReplicationSwitchZeroDowntimeOpts)

			switchInfo, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.NoError(err)
			r.EqualValues(*info.Switch, switchInfo)

			// replication remains deleted
			replicationPolicyExists(t, svc, replID, false)
			// routing remains switched
			routeTo, err = svc.getRoutingForReplication(ctx, replID)
			r.NoError(err)
			r.Equal(replID.ToStorage(), routeTo)

			// proxy ctx has no replications and no switch in progress
			proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
			r.NoError(err)
			r.Empty(xctx.GetReplications(proxyCtx))
			r.Equal("follower", xctx.GetRoutingPolicy(proxyCtx), "current storage is main when no zero downtime in progress")
			r.Nil(xctx.GetInProgressZeroDowntime(proxyCtx), "no zero downtime in progress")

			// cleanup
			err = svc.DeleteReplicationSwitch(ctx, replID)
			r.NoError(err)

			_, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.ErrorIs(err, dom.ErrNotFound)

			info, err = svc.GetReplicationPolicyInfoExtended(ctx, replID)
			r.NoError(err)
			r.Nil(info.Switch)
			r.True(info.IsArchived, "replication remains archived after switch deletion")

			// routing not reverted
			routeTo, err = svc.getRoutingForReplication(ctx, replID)
			r.NoError(err)
			r.Equal(replID.ToStorage(), routeTo)

			proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
			r.NoError(err)
			r.Empty(xctx.GetReplications(proxyCtx))
			r.Equal("follower", xctx.GetRoutingPolicy(proxyCtx))
			r.Nil(xctx.GetInProgressZeroDowntime(proxyCtx))
		})
	}
}

func Test_DowntimeSwitch_E2E(t *testing.T) {
	c := testutil.SetupRedis(t)
	queuesMock := &tasks.QueueServiceMock{}
	tasks.Reset(queuesMock)

	validSwitch := &entity.ReplicationSwitchDowntimeOpts{
		StartOnInitDone:     true,
		Cron:                stringPtr("0 0 * * *"),
		StartAt:             nil,
		MaxDuration:         (3 * time.Hour),
		MaxEventLag:         uint32Ptr(100),
		SkipBucketCheck:     true,
		ContinueReplication: true,
	}

	replications := map[string]entity.UniversalReplicationID{
		"user": entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
			User:        "user",
			FromStorage: "main",
			ToStorage:   "follower",
		}),
		"bucket": entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
			User:        "user",
			FromStorage: "main",
			FromBucket:  "bucket",
			ToStorage:   "follower",
			ToBucket:    "bucket",
		}),
	}

	for replType, replID := range replications {
		t.Run(replType, func(t *testing.T) {
			// setup
			if err := c.FlushAll(t.Context()).Err(); err != nil {
				t.Fatal(err)
			}
			tasks.Reset(queuesMock)
			r := require.New(t)
			svc := NewService(c, queuesMock, "main")
			ctx := t.Context()
			replicationPolicyExists(t, svc, replID, false)

			// check proxy ctx empty
			proxyCtx, err := svc.BuildProxyContext(ctx, "user", "bucket")
			r.NoError(err)
			r.Empty(xctx.GetReplications(proxyCtx))
			r.Equal("main", xctx.GetRoutingPolicy(proxyCtx))
			r.Nil(xctx.GetInProgressZeroDowntime(proxyCtx))

			// cannot create switch for non existing replication
			err = svc.SetDowntimeReplicationSwitch(ctx, replID, validSwitch)
			r.Error(err)

			// setup not started
			setupDowntimeSwitchState(t, svc, queuesMock, replID, validSwitch, entity.StatusNotStarted)

			// proxy ctx has replications
			proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
			r.NoError(err)
			r.Len(xctx.GetReplications(proxyCtx), 1)
			r.Equal(replID.AsString(), xctx.GetReplications(proxyCtx)[0].AsString())
			r.Equal("main", xctx.GetRoutingPolicy(proxyCtx))
			r.Nil(xctx.GetInProgressZeroDowntime(proxyCtx))

			// verify switch status
			info, err := svc.GetReplicationPolicyInfoExtended(ctx, replID)
			r.NoError(err)
			r.False(info.IsArchived)
			r.NotNil(info.Switch)
			r.Equal(entity.StatusNotStarted, info.Switch.LastStatus)
			replicationPolicyExists(t, svc, replID, true)

			// delete switch
			err = svc.DeleteReplicationSwitch(ctx, replID)
			r.NoError(err)

			// switch deleted, replication remains
			_, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.ErrorIs(err, dom.ErrNotFound)
			info, err = svc.GetReplicationPolicyInfoExtended(ctx, replID)
			r.NoError(err)
			r.Nil(info.Switch)
			r.False(info.IsArchived)
			replicationPolicyExists(t, svc, replID, true)
			// routing remains
			routeTo, err := svc.getRoutingForReplication(ctx, replID)
			r.NoError(err)
			r.Equal(replID.FromStorage(), routeTo)

			// proxy ctx has replications
			proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
			r.NoError(err)
			r.Len(xctx.GetReplications(proxyCtx), 1)
			r.Equal(replID.AsString(), xctx.GetReplications(proxyCtx)[0].AsString())
			r.Equal("main", xctx.GetRoutingPolicy(proxyCtx))
			r.Nil(xctx.GetInProgressZeroDowntime(proxyCtx))

			// delete replication
			if bucketID, ok := replID.AsBucketID(); ok {
				r.NoError(svc.DeleteBucketReplication(ctx, bucketID))
			} else if userID, ok := replID.AsUserID(); ok {
				r.NoError(svc.DeleteUserReplication(ctx, userID))
			}

			replicationPolicyExists(t, svc, replID, false)
			_, err = svc.GetReplicationPolicyInfoExtended(ctx, replID)
			r.ErrorIs(err, dom.ErrNotFound)
			// routing remains
			routeTo, err = svc.getRoutingForReplication(ctx, replID)
			r.NoError(err)
			r.Equal(replID.FromStorage(), routeTo)

			// proxy ctx has no replications
			proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
			r.NoError(err)
			r.Empty(xctx.GetReplications(proxyCtx))
			r.Equal("main", xctx.GetRoutingPolicy(proxyCtx))
			r.Nil(xctx.GetInProgressZeroDowntime(proxyCtx))

			//--------------------- setup in progress ---------------------
			setupDowntimeSwitchState(t, svc, queuesMock, replID, validSwitch, entity.StatusInProgress)

			// proxy routing blocked
			_, err = svc.BuildProxyContext(ctx, "user", "bucket")
			r.ErrorIs(err, dom.ErrRoutingBlock)

			// policy exists
			info, err = svc.GetReplicationPolicyInfoExtended(ctx, replID)
			r.NoError(err)
			r.NotNil(info.Switch)
			r.Equal(entity.StatusInProgress, info.Switch.LastStatus)
			r.False(info.IsArchived)
			replicationPolicyExists(t, svc, replID, true)

			// routing blocked
			_, err = svc.getRoutingForReplication(ctx, replID)
			r.ErrorIs(err, dom.ErrRoutingBlock)
			// no bucket routung blocked
			if _, ok := replID.AsUserID(); ok {
				_, err = svc.BuildProxyNoBucketContext(ctx, "user")
				r.ErrorIs(err, dom.ErrRoutingBlock)
			}

			// delete in progress switch
			err = svc.DeleteReplicationSwitch(ctx, replID)
			r.NoError(err)
			// switch deleted, replication remains
			_, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.ErrorIs(err, dom.ErrNotFound)

			info, err = svc.GetReplicationPolicyInfoExtended(ctx, replID)
			r.NoError(err)
			r.Nil(info.Switch)
			r.False(info.IsArchived)
			replicationPolicyExists(t, svc, replID, true)
			// routing unblocked
			routeTo, err = svc.getRoutingForReplication(ctx, replID)
			r.NoError(err)
			r.Equal(replID.FromStorage(), routeTo)

			// proxy ctx has replications
			proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
			r.NoError(err)
			r.Len(xctx.GetReplications(proxyCtx), 1)
			r.Equal(replID.AsString(), xctx.GetReplications(proxyCtx)[0].AsString())
			r.Equal("main", xctx.GetRoutingPolicy(proxyCtx))
			r.Nil(xctx.GetInProgressZeroDowntime(proxyCtx))

			// delete replication
			if bucketID, ok := replID.AsBucketID(); ok {
				r.NoError(svc.DeleteBucketReplication(ctx, bucketID))
			} else if userID, ok := replID.AsUserID(); ok {
				r.NoError(svc.DeleteUserReplication(ctx, userID))
			}

			replicationPolicyExists(t, svc, replID, false)
			_, err = svc.GetReplicationPolicyInfoExtended(ctx, replID)
			r.ErrorIs(err, dom.ErrNotFound)

			// proxy ctx has no replications
			proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
			r.NoError(err)
			r.Empty(xctx.GetReplications(proxyCtx))
			r.Equal("main", xctx.GetRoutingPolicy(proxyCtx))
			r.Nil(xctx.GetInProgressZeroDowntime(proxyCtx))

			//--------------------- setup switch done ---------------------
			setupDowntimeSwitchState(t, svc, queuesMock, replID, validSwitch, entity.StatusDone)
			// proxy route backwards and has backwards replication
			proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
			r.NoError(err)
			r.Len(xctx.GetReplications(proxyCtx), 1)
			backward := replID.Swap()
			r.Equal(backward.AsString(), xctx.GetReplications(proxyCtx)[0].AsString())
			r.Equal("follower", xctx.GetRoutingPolicy(proxyCtx))
			r.Nil(xctx.GetInProgressZeroDowntime(proxyCtx))

			// verify switch status
			info, err = svc.GetReplicationPolicyInfoExtended(ctx, replID)
			r.NoError(err)
			r.True(info.IsArchived)
			r.NotNil(info.Switch)
			r.Equal(entity.StatusDone, info.Switch.LastStatus)
			replicationPolicyExists(t, svc, replID, false)

			info, err = svc.GetReplicationPolicyInfoExtended(ctx, backward)
			r.NoError(err)
			r.False(info.IsArchived)
			r.NotNil(info.Switch)
			replicationPolicyExists(t, svc, backward, true)

			// delete done switch
			err = svc.DeleteReplicationSwitch(ctx, replID)
			r.NoError(err)

			// backwards replication and backwards routing remains
			proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
			r.NoError(err)
			r.Len(xctx.GetReplications(proxyCtx), 1)
			r.Equal(backward.AsString(), xctx.GetReplications(proxyCtx)[0].AsString())
			r.Equal("follower", xctx.GetRoutingPolicy(proxyCtx))
			r.Nil(xctx.GetInProgressZeroDowntime(proxyCtx))

			info, err = svc.GetReplicationPolicyInfoExtended(ctx, replID)
			r.NoError(err)
			r.Nil(info.Switch)
			r.True(info.IsArchived)
			replicationPolicyExists(t, svc, replID, false)

			info, err = svc.GetReplicationPolicyInfoExtended(ctx, backward)
			r.NoError(err)
			r.False(info.IsArchived)
			r.Nil(info.Switch)
			replicationPolicyExists(t, svc, backward, true)

			// cleanup
			if bucketID, ok := backward.AsBucketID(); ok {
				r.NoError(svc.DeleteBucketReplication(ctx, bucketID))

			} else if userID, ok := backward.AsUserID(); ok {
				r.NoError(svc.DeleteUserReplication(ctx, userID))
			}
			// delete replication
			if bucketID, ok := replID.AsBucketID(); ok {
				r.NoError(svc.DeleteBucketReplication(ctx, bucketID))
			} else if userID, ok := replID.AsUserID(); ok {
				r.NoError(svc.DeleteUserReplication(ctx, userID))
			}

			// routing remains
			routeTo, err = svc.getRoutingForReplication(ctx, replID)
			r.NoError(err)
			r.Equal(replID.ToStorage(), routeTo)
			// revert routing to main
			tx := svc.bucketReplicationPolicyStore.TxExecutor()
			svc.routeToOldInTx(ctx, tx, replID)
			err = tx.Exec(ctx)
			r.NoError(err)
			routeTo, err = svc.getRoutingForReplication(ctx, replID)
			r.NoError(err)
			r.Equal(replID.FromStorage(), routeTo)

			// create failed switch
			setupDowntimeSwitchState(t, svc, queuesMock, replID, validSwitch, entity.StatusError)

			// proxy ctx has replications
			proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
			r.NoError(err)
			r.Len(xctx.GetReplications(proxyCtx), 1)
			r.Equal(replID.AsString(), xctx.GetReplications(proxyCtx)[0].AsString())
			r.Equal("main", xctx.GetRoutingPolicy(proxyCtx))
			r.Nil(xctx.GetInProgressZeroDowntime(proxyCtx))

			// verify switch status
			info, err = svc.GetReplicationPolicyInfoExtended(ctx, replID)
			r.NoError(err)
			r.False(info.IsArchived)
			r.NotNil(info.Switch)
			r.Equal(entity.StatusError, info.Switch.LastStatus)
			replicationPolicyExists(t, svc, replID, true)

			// delete failed switch
			err = svc.DeleteReplicationSwitch(ctx, replID)
			r.NoError(err)
			// switch deleted, replication remains
			_, err = svc.GetReplicationSwitchInfo(ctx, replID)
			r.ErrorIs(err, dom.ErrNotFound)
			info, err = svc.GetReplicationPolicyInfoExtended(ctx, replID)
			r.NoError(err)
			r.Nil(info.Switch)
			r.False(info.IsArchived)
			replicationPolicyExists(t, svc, replID, true)
			// routing remains
			routeTo, err = svc.getRoutingForReplication(ctx, replID)
			r.NoError(err)
			r.Equal(replID.FromStorage(), routeTo)
			proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
			r.NoError(err)
			r.Len(xctx.GetReplications(proxyCtx), 1)
			r.Equal(replID.AsString(), xctx.GetReplications(proxyCtx)[0].AsString())
			r.Equal("main", xctx.GetRoutingPolicy(proxyCtx))
			r.Nil(xctx.GetInProgressZeroDowntime(proxyCtx))

			// delete replication
			if bucketID, ok := replID.AsBucketID(); ok {
				r.NoError(svc.DeleteBucketReplication(ctx, bucketID))
			} else if userID, ok := replID.AsUserID(); ok {
				r.NoError(svc.DeleteUserReplication(ctx, userID))
			}

			// check that can recreate switch after deletion
			setupDowntimeSwitchState(t, svc, queuesMock, replID, validSwitch, entity.StatusNotStarted)

			info, err = svc.GetReplicationPolicyInfoExtended(ctx, replID)
			r.NoError(err)
			r.False(info.IsArchived)
			r.NotNil(info.Switch)
			r.Equal(entity.StatusNotStarted, info.Switch.LastStatus)
			replicationPolicyExists(t, svc, replID, true)
			routeTo, err = svc.getRoutingForReplication(ctx, replID)
			r.NoError(err)
		})
	}
}
