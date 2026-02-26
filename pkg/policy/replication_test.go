package policy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/testutil"
)

func Test_AddUserReplicationPolicy(t *testing.T) {
	c := testutil.SetupRedis(t)
	type before struct {
		mainStorage         string
		routingBlock        *entity.UniversalReplicationID
		userRepl            *entity.UserReplicationPolicy
		userReplHasSwitch   bool
		bucketRepl          *entity.BucketReplicationPolicy
		bucketReplHasSwitch bool
	}
	tests := []struct {
		name    string
		before  before
		policy  entity.UserReplicationPolicy
		wantErr error
	}{
		{
			name: "ok",
			before: before{
				mainStorage: "main",
			},
			policy: entity.UserReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
			},
			wantErr: nil,
		},
		{
			name: "ok - same source but other destination",
			before: before{
				mainStorage: "main",
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				}},
			policy: entity.UserReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower2",
			},
			wantErr: nil,
		},
		{
			name: "err: source not main",
			before: before{
				mainStorage: "main",
			},
			policy: entity.UserReplicationPolicy{
				User:        "user",
				FromStorage: "follower",
				ToStorage:   "main",
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "err: user routing blocked",
			before: before{
				mainStorage: "main",
				routingBlock: uidPtr(entity.UniversalFromUserReplication(
					entity.UserReplicationPolicy{
						User:        "user",
						FromStorage: "main",
						ToStorage:   "follower",
					})),
			},
			policy: entity.UserReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
			},
			wantErr: dom.ErrRoutingBlock,
		},
		{
			name: "err: already exists",
			before: before{
				mainStorage: "main",
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				}},
			policy: entity.UserReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
			},
			wantErr: dom.ErrAlreadyExists,
		},
		{
			name: "err: bucket replication exists for user",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
					FromBucket:  "any",
					ToBucket:    "any",
				}},
			policy: entity.UserReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "ok: bucket replication exists for different user",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user-2",
					FromStorage: "main",
					ToStorage:   "follower",
					FromBucket:  "any",
					ToBucket:    "any",
				}},
			policy: entity.UserReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
			},
			wantErr: nil,
		},
		{
			name: "ok: user replication exists for different user",
			before: before{
				mainStorage: "main",
				userRepl: &entity.UserReplicationPolicy{
					User:        "user-2",
					FromStorage: "main",
					ToStorage:   "follower",
				}},
			policy: entity.UserReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
			},
			wantErr: nil,
		},
		{
			name: "err: switch exists for user",
			before: before{
				mainStorage: "main",
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				},
				userReplHasSwitch: true,
			},
			policy: entity.UserReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower2",
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "ok: switch exists for different user",
			before: before{
				mainStorage: "main",
				userRepl: &entity.UserReplicationPolicy{
					User:        "user-2",
					FromStorage: "main",
					ToStorage:   "follower",
				},
				userReplHasSwitch: true,
			},
			policy: entity.UserReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower2",
			},
			wantErr: nil,
		},
		{
			name: "err: same from/to storage",
			before: before{
				mainStorage: "main",
			},
			policy: entity.UserReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "main",
			},
			wantErr: dom.ErrInvalidArg,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := c.FlushAll(t.Context()).Err(); err != nil {
				t.Fatal(err)
			}
			qs := tasks.QueueServiceMock{
				Queues: map[string]int{},
				Paused: map[string]bool{},
			}
			r := require.New(t)
			svc := NewService(c, &qs, tt.before.mainStorage)

			ctx := t.Context()

			if tt.before.routingBlock != nil {
				tx := svc.userReplicationPolicyStore.TxExecutor()
				svc.blockRouteInTx(ctx, tx, *tt.before.routingBlock)
				err := tx.Exec(ctx)
				r.NoError(err)
			}

			if tt.before.userRepl != nil {
				err := svc.AddUserReplicationPolicy(ctx, *tt.before.userRepl, entity.ReplicationOptions{})
				r.NoError(err)
				if tt.before.userReplHasSwitch {
					uid := entity.UniversalFromUserReplication(*tt.before.userRepl)
					err = svc.SetDowntimeReplicationSwitch(ctx, uid, nil)
					r.NoError(err)
				}
			}
			if tt.before.bucketRepl != nil {
				err := svc.AddBucketReplicationPolicy(ctx, *tt.before.bucketRepl, entity.ReplicationOptions{})
				r.NoError(err)
				if tt.before.bucketReplHasSwitch {
					bid := entity.UniversalFromBucketReplication(*tt.before.bucketRepl)
					err = svc.SetDowntimeReplicationSwitch(ctx, bid, nil)
					r.NoError(err)
				}
			}

			err := svc.AddUserReplicationPolicy(ctx, tt.policy, entity.ReplicationOptions{})
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				return
			}
			r.NoError(err)
			// check if added
			uid := entity.UniversalFromUserReplication(tt.policy)
			info, err := svc.GetReplicationPolicyInfoExtended(ctx, uid)
			r.NoError(err)
			r.WithinDuration(time.Now(), info.CreatedAt, time.Second)

			// list
			policies, err := svc.ListUserReplicationsInfo(ctx)
			r.NoError(err)
			infoFromList, ok := policies[tt.policy]
			r.True(ok)
			r.WithinDuration(info.CreatedAt, infoFromList.CreatedAt, time.Millisecond)
		})
	}
}

func Test_AddBucketReplicationPolicy(t *testing.T) {
	c := testutil.SetupRedis(t)
	type before struct {
		mainStorage         string
		routingBlock        *entity.UniversalReplicationID
		userRepl            *entity.UserReplicationPolicy
		userReplHasSwitch   bool
		bucketRepl          *entity.BucketReplicationPolicy
		bucketReplHasSwitch bool
	}
	tests := []struct {
		name    string
		before  before
		policy  entity.BucketReplicationPolicy
		wantErr error
	}{
		{
			name: "ok",
			before: before{
				mainStorage: "main",
			},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
				FromBucket:  "bucket",
				ToBucket:    "bucket",
			},
			wantErr: nil,
		},
		{
			name: "ok - same source but other destination",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
					FromBucket:  "bucket",
					ToBucket:    "bucket",
				}},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower2",
				FromBucket:  "bucket",
				ToBucket:    "bucket",
			},
			wantErr: nil,
		},
		{
			name: "err: source not main",
			before: before{
				mainStorage: "main",
			},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "follower",
				ToStorage:   "main",
				FromBucket:  "bucket",
				ToBucket:    "bucket",
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "err: user routing blocked",
			before: before{
				mainStorage: "main",
				routingBlock: uidPtr(entity.UniversalFromUserReplication(
					entity.UserReplicationPolicy{
						User:        "user",
						FromStorage: "main",
						ToStorage:   "follower",
					})),
			},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
				FromBucket:  "bucket",
				ToBucket:    "bucket",
			},
			wantErr: dom.ErrRoutingBlock,
		},
		{
			name: "err: already exists",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
					FromBucket:  "bucket",
					ToBucket:    "bucket",
				}},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
				FromBucket:  "bucket",
				ToBucket:    "bucket",
			},
			wantErr: dom.ErrAlreadyExists,
		},
		{
			name: "err: user replication exists",
			before: before{
				mainStorage: "main",
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				}},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
				FromBucket:  "bucket",
				ToBucket:    "bucket",
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "ok: user replication exists for different user",
			before: before{
				mainStorage: "main",
				userRepl: &entity.UserReplicationPolicy{
					User:        "user-2",
					FromStorage: "main",
					ToStorage:   "follower",
				}},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
				FromBucket:  "bucket",
				ToBucket:    "bucket",
			},
			wantErr: nil,
		},
		{
			name: "ok: bucket replication exists for different bucket",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
					FromBucket:  "bucket-2",
					ToBucket:    "bucket-2",
				}},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
				FromBucket:  "bucket",
				ToBucket:    "bucket",
			},
			wantErr: nil,
		},
		{
			name: "ok: bucket replication exists for different to bucket",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
					FromBucket:  "bucket",
					ToBucket:    "bucket-2",
				}},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
				FromBucket:  "bucket",
				ToBucket:    "bucket",
			},
			wantErr: nil,
		},
		{
			name: "ok: bucket replication exists for different user",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user-2",
					FromStorage: "main",
					ToStorage:   "follower",
					FromBucket:  "bucket",
					ToBucket:    "bucket",
				}},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
				FromBucket:  "bucket",
				ToBucket:    "bucket",
			},
			wantErr: nil,
		},
		{
			name: "err: switch exists for bucket",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
					FromBucket:  "bucket",
					ToBucket:    "bucket",
				},
				bucketReplHasSwitch: true,
			},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower-2",
				FromBucket:  "bucket",
				ToBucket:    "bucket",
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "ok: switch exists for different bucket",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
					FromBucket:  "bucket",
					ToBucket:    "bucket",
				},
				userReplHasSwitch: true,
			},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
				FromBucket:  "bucket-2",
				ToBucket:    "bucket-2",
			},
			wantErr: nil,
		},
		{
			name: "err: destination is already in use",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
					FromBucket:  "bucket",
					ToBucket:    "bucket-2",
				}},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
				FromBucket:  "bucket-3",
				ToBucket:    "bucket-2",
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "err: same from/to storage",
			before: before{
				mainStorage: "main",
			},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "main",
				FromBucket:  "bucket",
				ToBucket:    "bucket",
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "ok: same from/to storage but different buckets",
			before: before{
				mainStorage: "main",
			},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "main",
				FromBucket:  "bucket",
				ToBucket:    "bucket-2",
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := c.FlushAll(t.Context()).Err(); err != nil {
				t.Fatal(err)
			}
			qs := tasks.QueueServiceMock{
				Queues: map[string]int{},
				Paused: map[string]bool{},
			}
			r := require.New(t)
			svc := NewService(c, &qs, tt.before.mainStorage)

			ctx := t.Context()

			if tt.before.routingBlock != nil {
				tx := svc.userReplicationPolicyStore.TxExecutor()
				svc.blockRouteInTx(ctx, tx, *tt.before.routingBlock)
				err := tx.Exec(ctx)
				r.NoError(err)
			}

			if tt.before.userRepl != nil {
				err := svc.AddUserReplicationPolicy(ctx, *tt.before.userRepl, entity.ReplicationOptions{})
				r.NoError(err)
				if tt.before.userReplHasSwitch {
					uid := entity.UniversalFromUserReplication(*tt.before.userRepl)
					err = svc.SetDowntimeReplicationSwitch(ctx, uid, nil)
					r.NoError(err)
				}
			}
			if tt.before.bucketRepl != nil {
				err := svc.AddBucketReplicationPolicy(ctx, *tt.before.bucketRepl, entity.ReplicationOptions{})
				r.NoError(err)
				if tt.before.bucketReplHasSwitch {
					bid := entity.UniversalFromBucketReplication(*tt.before.bucketRepl)
					err = svc.SetDowntimeReplicationSwitch(ctx, bid, nil)
					r.NoError(err)
				}
			}

			err := svc.AddBucketReplicationPolicy(ctx, tt.policy, entity.ReplicationOptions{AgentURL: tt.name, EventSource: dom.EventSourceS3Notification})
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				return
			}
			r.NoError(err)
			// check if added
			uid := entity.UniversalFromBucketReplication(tt.policy)
			info, err := svc.GetReplicationPolicyInfoExtended(ctx, uid)
			r.NoError(err)
			r.WithinDuration(time.Now(), info.CreatedAt, time.Second)
			r.Equal(tt.name, info.AgentURL)
			r.Equal(string(dom.EventSourceS3Notification), info.EventSource)

			// list
			policies, err := svc.ListBucketReplicationsInfo(ctx, tt.policy.User)
			r.NoError(err)
			infoFromList, ok := policies[tt.policy]
			r.True(ok)
			r.WithinDuration(info.CreatedAt, infoFromList.CreatedAt, time.Millisecond)
			r.Equal(tt.name, infoFromList.AgentURL)
			r.Equal(string(dom.EventSourceS3Notification), infoFromList.EventSource)
			_, err = svc.ListBucketReplicationsInfo(ctx, tt.policy.User+"-qwerasdfasdf")
			r.ErrorIs(err, dom.ErrNotFound, "other user not affected")
			// custom bucket blocked
			if tt.policy.FromBucket != tt.policy.ToBucket {
				destPolicy := tt.policy
				destPolicy.FromBucket, destPolicy.ToBucket = destPolicy.ToBucket, destPolicy.FromBucket

				_, err = svc.getRoutingForReplication(ctx, entity.UniversalFromBucketReplication(destPolicy))
				r.ErrorIs(err, dom.ErrRoutingBlock, "custom bucket should be blocked")
			}
		})
	}
}

func Test_DeleteBucketReplicationPolicy(t *testing.T) {
	c := testutil.SetupRedis(t)
	type before struct {
		mainStorage         string
		userRepl            *entity.UserReplicationPolicy
		userReplHasSwitch   bool
		bucketRepl          *entity.BucketReplicationPolicy
		bucketReplHasSwitch bool
	}
	tests := []struct {
		name    string
		before  before
		policy  entity.BucketReplicationPolicy
		wantErr error
	}{
		{
			name: "ok",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
					FromBucket:  "bucket",
					ToBucket:    "bucket",
				}},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
				FromBucket:  "bucket",
				ToBucket:    "bucket",
			},
			wantErr: nil,
		},
		{
			name: "err: not exists-different user",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
					FromBucket:  "bucket",
					ToBucket:    "bucket",
				}},
			policy: entity.BucketReplicationPolicy{
				User:        "user-2",
				FromStorage: "main",
				ToStorage:   "follower",
				FromBucket:  "bucket",
				ToBucket:    "bucket",
			},
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: not exists-different from storage",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
					FromBucket:  "bucket",
					ToBucket:    "bucket",
				}},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main-2",
				ToStorage:   "follower",
				FromBucket:  "bucket",
				ToBucket:    "bucket",
			},
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: not exists-different to storage",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
					FromBucket:  "bucket",
					ToBucket:    "bucket",
				}},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower-2",
				FromBucket:  "bucket",
				ToBucket:    "bucket",
			},
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: not exists-different from bucket",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
					FromBucket:  "bucket",
					ToBucket:    "bucket",
				}},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
				FromBucket:  "bucket-2",
				ToBucket:    "bucket",
			},
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: not exists-different to bucket",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
					FromBucket:  "bucket",
					ToBucket:    "bucket",
				}},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
				FromBucket:  "bucket",
				ToBucket:    "bucket-2",
			},
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: has switch",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
					FromBucket:  "bucket",
					ToBucket:    "bucket",
				},
				bucketReplHasSwitch: true,
			},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
				FromBucket:  "bucket",
				ToBucket:    "bucket",
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "ok: user replication not affected",
			before: before{
				mainStorage: "main",
				userRepl: &entity.UserReplicationPolicy{
					User:        "user-2",
					FromStorage: "main",
					ToStorage:   "follower",
				},
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
					FromBucket:  "bucket",
					ToBucket:    "bucket",
				},
			},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
				FromBucket:  "bucket",
				ToBucket:    "bucket",
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := c.FlushAll(t.Context()).Err(); err != nil {
				t.Fatal(err)
			}
			qs := tasks.QueueServiceMock{
				Queues: map[string]int{},
				Paused: map[string]bool{},
			}
			r := require.New(t)
			svc := NewService(c, &qs, tt.before.mainStorage)

			ctx := t.Context()

			if tt.before.userRepl != nil {
				err := svc.AddUserReplicationPolicy(ctx, *tt.before.userRepl, entity.ReplicationOptions{})
				r.NoError(err)
				if tt.before.userReplHasSwitch {
					uid := entity.UniversalFromUserReplication(*tt.before.userRepl)
					err = svc.SetDowntimeReplicationSwitch(ctx, uid, nil)
					r.NoError(err)
				}
			}
			if tt.before.bucketRepl != nil {
				err := svc.AddBucketReplicationPolicy(ctx, *tt.before.bucketRepl, entity.ReplicationOptions{})
				r.NoError(err)
				if tt.before.bucketReplHasSwitch {
					bid := entity.UniversalFromBucketReplication(*tt.before.bucketRepl)
					err = svc.SetDowntimeReplicationSwitch(ctx, bid, nil)
					r.NoError(err)
				}
			}

			err := svc.DeleteBucketReplication(ctx, tt.policy)
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				return
			}
			r.NoError(err)

			// check if deleted
			uid := entity.UniversalFromBucketReplication(tt.policy)
			_, err = svc.GetReplicationPolicyInfoExtended(ctx, uid)
			r.ErrorIs(err, dom.ErrNotFound)

			if tt.before.userRepl != nil {
				// user replication should not be affected
				_, err = svc.GetReplicationPolicyInfoExtended(ctx, entity.UniversalFromUserReplication(*tt.before.userRepl))
				r.NoError(err)
			}
			// can recreate
			err = svc.AddBucketReplicationPolicy(ctx, tt.policy, entity.ReplicationOptions{})
			r.NoError(err)
			// check if added
			uid = entity.UniversalFromBucketReplication(tt.policy)
			_, err = svc.GetReplicationPolicyInfoExtended(ctx, uid)
			r.NoError(err)
		})
	}
}

func Test_DeleteUserReplicationPolicy(t *testing.T) {
	c := testutil.SetupRedis(t)
	type before struct {
		mainStorage         string
		userRepl            *entity.UserReplicationPolicy
		userReplHasSwitch   bool
		bucketRepl          *entity.BucketReplicationPolicy
		bucketReplHasSwitch bool
	}
	tests := []struct {
		name    string
		before  before
		policy  entity.UserReplicationPolicy
		wantErr error
	}{
		{
			name: "ok",
			before: before{
				mainStorage: "main",
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				}},
			policy: entity.UserReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
			},
			wantErr: nil,
		},
		{
			name: "err: not exists-different user",
			before: before{
				mainStorage: "main",
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				}},
			policy: entity.UserReplicationPolicy{
				User:        "user-2",
				FromStorage: "main",
				ToStorage:   "follower",
			},
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: not exists-different from storage",
			before: before{
				mainStorage: "main",
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				}},
			policy: entity.UserReplicationPolicy{
				User:        "user",
				FromStorage: "main-2",
				ToStorage:   "follower",
			},
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: not exists-different to storage",
			before: before{
				mainStorage: "main",
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				}},
			policy: entity.UserReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower-2",
			},
			wantErr: dom.ErrNotFound,
		},
		{
			name: "err: has switch",
			before: before{
				mainStorage: "main",
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				},
				userReplHasSwitch: true,
			},
			policy: entity.UserReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "ok: user replication not affected",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user-2",
					FromStorage: "main",
					ToStorage:   "follower",
					FromBucket:  "any",
					ToBucket:    "any",
				},
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				},
			},
			policy: entity.UserReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := c.FlushAll(t.Context()).Err(); err != nil {
				t.Fatal(err)
			}
			qs := tasks.QueueServiceMock{
				Queues: map[string]int{},
				Paused: map[string]bool{},
			}
			r := require.New(t)
			svc := NewService(c, &qs, tt.before.mainStorage)

			ctx := t.Context()

			if tt.before.userRepl != nil {
				err := svc.AddUserReplicationPolicy(ctx, *tt.before.userRepl, entity.ReplicationOptions{})
				r.NoError(err)
				if tt.before.userReplHasSwitch {
					uid := entity.UniversalFromUserReplication(*tt.before.userRepl)
					err = svc.SetDowntimeReplicationSwitch(ctx, uid, nil)
					r.NoError(err)
				}
			}
			if tt.before.bucketRepl != nil {
				err := svc.AddBucketReplicationPolicy(ctx, *tt.before.bucketRepl, entity.ReplicationOptions{})
				r.NoError(err)
				if tt.before.bucketReplHasSwitch {
					bid := entity.UniversalFromBucketReplication(*tt.before.bucketRepl)
					err = svc.SetDowntimeReplicationSwitch(ctx, bid, nil)
					r.NoError(err)
				}
			}

			err := svc.DeleteUserReplication(ctx, tt.policy)
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				return
			}
			r.NoError(err)

			// check if deleted
			uid := entity.UniversalFromUserReplication(tt.policy)
			_, err = svc.GetReplicationPolicyInfoExtended(ctx, uid)
			r.ErrorIs(err, dom.ErrNotFound)

			if tt.before.bucketRepl != nil {
				// user replication should not be affected
				_, err = svc.GetReplicationPolicyInfoExtended(ctx, entity.UniversalFromBucketReplication(*tt.before.bucketRepl))
				r.NoError(err)
			}
			// can recreate
			err = svc.AddUserReplicationPolicy(ctx, tt.policy, entity.ReplicationOptions{})
			r.NoError(err)
			// check if added
			uid = entity.UniversalFromUserReplication(tt.policy)
			_, err = svc.GetReplicationPolicyInfoExtended(ctx, uid)
			r.NoError(err)
		})
	}
}

func uidPtr(u entity.UniversalReplicationID) *entity.UniversalReplicationID {
	return &u
}
