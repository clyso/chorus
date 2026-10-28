package policy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/testutil"
)

func Test_AddUserReplicationPolicy(t *testing.T) {
	c := testutil.SetupRedis(t)
	type before struct {
		mainStorage                     string
		routingBlock                    *entity.UniversalReplicationID
		userRepl                        *entity.UserReplicationPolicy
		userReplHasSwitch               bool
		bucketRepl                      *entity.BucketReplicationPolicy
		bucketReplHasSwitch             bool
		bucketReplHasZeroDowntimeSwitch bool
		bucketRoute                     *entity.BucketRoutingPolicyID
		bucketRouteToStorage            string
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
			name: "err: bucket replication in zero-downtime switch for user",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
					FromBucket:  "any",
					ToBucket:    "any",
				},
				bucketReplHasZeroDowntimeSwitch: true,
			},
			policy: entity.UserReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower2",
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "err: user bucket routed to other storage",
			before: before{
				mainStorage:          "main",
				bucketRoute:          &entity.BucketRoutingPolicyID{User: "user", Bucket: "any"},
				bucketRouteToStorage: "follower2",
			},
			policy: entity.UserReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
			},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "ok: user bucket routed to source storage",
			before: before{
				mainStorage:          "main",
				bucketRoute:          &entity.BucketRoutingPolicyID{User: "user", Bucket: "any"},
				bucketRouteToStorage: "main",
			},
			policy: entity.UserReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
			},
			wantErr: nil,
		},
		{
			name: "ok: bucket routed to other storage for different user",
			before: before{
				mainStorage:          "main",
				bucketRoute:          &entity.BucketRoutingPolicyID{User: "user-2", Bucket: "any"},
				bucketRouteToStorage: "follower2",
			},
			policy: entity.UserReplicationPolicy{
				User:        "user",
				FromStorage: "main",
				ToStorage:   "follower",
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
				_, err := svc.AddUserReplicationPolicy(ctx, *tt.before.userRepl, entity.ReplicationOptions{})
				r.NoError(err)
				if tt.before.userReplHasSwitch {
					uid := entity.UniversalFromUserReplication(*tt.before.userRepl)
					err = svc.SetDowntimeReplicationSwitch(ctx, uid, nil)
					r.NoError(err)
				}
			}
			if tt.before.bucketRepl != nil {
				_, err := svc.AddBucketReplicationPolicy(ctx, *tt.before.bucketRepl, entity.ReplicationOptions{})
				r.NoError(err)
				if tt.before.bucketReplHasSwitch {
					bid := entity.UniversalFromBucketReplication(*tt.before.bucketRepl)
					err = svc.SetDowntimeReplicationSwitch(ctx, bid, nil)
					r.NoError(err)
				}
				if tt.before.bucketReplHasZeroDowntimeSwitch {
					startZeroDowntimeSwitch(t, svc, &qs, entity.UniversalFromBucketReplication(*tt.before.bucketRepl))
				}
			}

			if tt.before.bucketRoute != nil {
				err := svc.SetBucketRouting(ctx, *tt.before.bucketRoute, tt.before.bucketRouteToStorage)
				r.NoError(err)
			}

			_, err := svc.AddUserReplicationPolicy(ctx, tt.policy, entity.ReplicationOptions{})
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
		mainStorage                   string
		routingBlock                  *entity.UniversalReplicationID
		userRepl                      *entity.UserReplicationPolicy
		userReplHasSwitch             bool
		bucketRepl                    *entity.BucketReplicationPolicy
		bucketReplHasSwitch           bool
		userReplHasZeroDowntimeSwitch bool
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
			name: "err: user replication in zero-downtime switch",
			before: before{
				mainStorage: "main",
				userRepl: &entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "main",
					ToStorage:   "follower",
				},
				userReplHasZeroDowntimeSwitch: true,
			},
			policy: entity.BucketReplicationPolicy{
				User:        "user",
				FromStorage: "follower",
				ToStorage:   "follower2",
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
				_, err := svc.AddUserReplicationPolicy(ctx, *tt.before.userRepl, entity.ReplicationOptions{})
				r.NoError(err)
				if tt.before.userReplHasSwitch {
					uid := entity.UniversalFromUserReplication(*tt.before.userRepl)
					err = svc.SetDowntimeReplicationSwitch(ctx, uid, nil)
					r.NoError(err)
				}
				if tt.before.userReplHasZeroDowntimeSwitch {
					startZeroDowntimeSwitch(t, svc, &qs, entity.UniversalFromUserReplication(*tt.before.userRepl))
				}
			}
			if tt.before.bucketRepl != nil {
				_, err := svc.AddBucketReplicationPolicy(ctx, *tt.before.bucketRepl, entity.ReplicationOptions{})
				r.NoError(err)
				if tt.before.bucketReplHasSwitch {
					bid := entity.UniversalFromBucketReplication(*tt.before.bucketRepl)
					err = svc.SetDowntimeReplicationSwitch(ctx, bid, nil)
					r.NoError(err)
				}
			}

			_, err := svc.AddBucketReplicationPolicy(ctx, tt.policy, entity.ReplicationOptions{AgentURL: tt.name, EventSource: dom.EventSourceS3Notification})
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
		missing bool
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
			name: "ok: not exists-different user",
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
			missing: true,
		},
		{
			name: "ok: not exists-different from storage",
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
			missing: true,
		},
		{
			name: "ok: not exists-different to storage",
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
			missing: true,
		},
		{
			name: "ok: not exists-different from bucket",
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
			missing: true,
		},
		{
			name: "ok: not exists-different to bucket",
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
			missing: true,
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
				_, err := svc.AddUserReplicationPolicy(ctx, *tt.before.userRepl, entity.ReplicationOptions{})
				r.NoError(err)
				if tt.before.userReplHasSwitch {
					uid := entity.UniversalFromUserReplication(*tt.before.userRepl)
					err = svc.SetDowntimeReplicationSwitch(ctx, uid, nil)
					r.NoError(err)
				}
			}
			if tt.before.bucketRepl != nil {
				_, err := svc.AddBucketReplicationPolicy(ctx, *tt.before.bucketRepl, entity.ReplicationOptions{})
				r.NoError(err)
				if tt.before.bucketReplHasSwitch {
					bid := entity.UniversalFromBucketReplication(*tt.before.bucketRepl)
					err = svc.SetDowntimeReplicationSwitch(ctx, bid, nil)
					r.NoError(err)
				}
			}

			uid := entity.UniversalFromBucketReplication(tt.policy)
			if tt.missing {
				for _, q := range tasks.AllReplicationQueues(uid) {
					qs.Queues[q] = 1
				}
			}
			err := svc.DeleteBucketReplication(ctx, tt.policy)
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				return
			}
			r.NoError(err)
			if tt.missing {
				r.Empty(qs.Queues, "delete of missing replication removes its leftover queues")
				_, err = svc.GetReplicationPolicyInfoExtended(ctx, entity.UniversalFromBucketReplication(*tt.before.bucketRepl))
				r.NoError(err, "existing replication is not affected")
				return
			}

			// check if deleted
			_, err = svc.GetReplicationPolicyInfoExtended(ctx, uid)
			r.ErrorIs(err, dom.ErrNotFound)

			if tt.before.userRepl != nil {
				// user replication should not be affected
				_, err = svc.GetReplicationPolicyInfoExtended(ctx, entity.UniversalFromUserReplication(*tt.before.userRepl))
				r.NoError(err)
			}
			// can recreate
			_, err = svc.AddBucketReplicationPolicy(ctx, tt.policy, entity.ReplicationOptions{})
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
		missing bool
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
			name: "ok: not exists-different user",
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
			missing: true,
		},
		{
			name: "ok: not exists-different from storage",
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
			missing: true,
		},
		{
			name: "ok: not exists-different to storage",
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
			missing: true,
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
				_, err := svc.AddUserReplicationPolicy(ctx, *tt.before.userRepl, entity.ReplicationOptions{})
				r.NoError(err)
				if tt.before.userReplHasSwitch {
					uid := entity.UniversalFromUserReplication(*tt.before.userRepl)
					err = svc.SetDowntimeReplicationSwitch(ctx, uid, nil)
					r.NoError(err)
				}
			}
			if tt.before.bucketRepl != nil {
				_, err := svc.AddBucketReplicationPolicy(ctx, *tt.before.bucketRepl, entity.ReplicationOptions{})
				r.NoError(err)
				if tt.before.bucketReplHasSwitch {
					bid := entity.UniversalFromBucketReplication(*tt.before.bucketRepl)
					err = svc.SetDowntimeReplicationSwitch(ctx, bid, nil)
					r.NoError(err)
				}
			}

			uid := entity.UniversalFromUserReplication(tt.policy)
			if tt.missing {
				for _, q := range tasks.AllReplicationQueues(uid) {
					qs.Queues[q] = 1
				}
			}
			err := svc.DeleteUserReplication(ctx, tt.policy)
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
				return
			}
			r.NoError(err)
			if tt.missing {
				r.Empty(qs.Queues, "delete of missing replication removes its leftover queues")
				_, err = svc.GetReplicationPolicyInfoExtended(ctx, entity.UniversalFromUserReplication(*tt.before.userRepl))
				r.NoError(err, "existing replication is not affected")
				return
			}

			// check if deleted
			_, err = svc.GetReplicationPolicyInfoExtended(ctx, uid)
			r.ErrorIs(err, dom.ErrNotFound)

			if tt.before.bucketRepl != nil {
				// user replication should not be affected
				_, err = svc.GetReplicationPolicyInfoExtended(ctx, entity.UniversalFromBucketReplication(*tt.before.bucketRepl))
				r.NoError(err)
			}
			// can recreate
			_, err = svc.AddUserReplicationPolicy(ctx, tt.policy, entity.ReplicationOptions{})
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

func Test_ReplicationGeneration(t *testing.T) {
	c := testutil.SetupRedis(t)
	userPolicy := entity.UserReplicationPolicy{
		User:        "user",
		FromStorage: "main",
		ToStorage:   "follower",
	}
	bucketPolicy := entity.BucketReplicationPolicy{
		User:        "user",
		FromStorage: "main",
		ToStorage:   "follower",
		FromBucket:  "bucket",
		ToBucket:    "bucket",
	}
	tests := []struct {
		name    string
		id      entity.UniversalReplicationID
		otherID entity.UniversalReplicationID
		add     func(svc *policySvc) (int64, error)
		delete  func(svc *policySvc) error
	}{
		{
			name: "user replication",
			add: func(svc *policySvc) (int64, error) {
				return svc.AddUserReplicationPolicy(t.Context(), userPolicy, entity.ReplicationOptions{})
			},
			delete: func(svc *policySvc) error {
				return svc.DeleteUserReplication(t.Context(), userPolicy)
			},
			id:      entity.UniversalFromUserReplication(userPolicy),
			otherID: entity.UniversalFromBucketReplication(bucketPolicy),
		},
		{
			name: "bucket replication",
			add: func(svc *policySvc) (int64, error) {
				return svc.AddBucketReplicationPolicy(t.Context(), bucketPolicy, entity.ReplicationOptions{})
			},
			delete: func(svc *policySvc) error {
				return svc.DeleteBucketReplication(t.Context(), bucketPolicy)
			},
			id:      entity.UniversalFromBucketReplication(bucketPolicy),
			otherID: entity.UniversalFromUserReplication(userPolicy),
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
			svc := NewService(c, &qs, "main")
			ctx := t.Context()

			gen, err := svc.GetReplicationGeneration(ctx, tt.id)
			r.NoError(err)
			r.Zero(gen, "generation of never created replication")

			created, err := tt.add(svc)
			r.NoError(err)
			r.EqualValues(1, created)
			gen, err = svc.GetReplicationGeneration(ctx, tt.id)
			r.NoError(err)
			r.Equal(created, gen)

			proxyCtx, err := svc.BuildProxyContext(ctx, "user", "bucket")
			r.NoError(err)
			r.Equal(created, xctx.GetReplicationGeneration(proxyCtx, tt.id), "proxy uses generation of active replication")

			_, err = tt.add(svc)
			r.ErrorIs(err, dom.ErrAlreadyExists)
			gen, err = svc.GetReplicationGeneration(ctx, tt.id)
			r.NoError(err)
			r.Equal(created, gen, "failed add does not change generation")

			for _, q := range tasks.AllReplicationQueues(tt.id) {
				qs.Queues[q] = 1
			}
			r.NoError(tt.delete(svc))
			gen, err = svc.GetReplicationGeneration(ctx, tt.id)
			r.NoError(err)
			r.Greater(gen, created, "delete starts new generation")
			r.Empty(qs.Queues, "delete removes replication queues")

			deleted := gen
			for _, q := range tasks.AllReplicationQueues(tt.id) {
				qs.Queues[q] = 1
			}
			r.NoError(tt.delete(svc), "delete of missing replication succeeds")
			gen, err = svc.GetReplicationGeneration(ctx, tt.id)
			r.NoError(err)
			r.Equal(deleted, gen, "delete of missing replication does not change generation")
			r.Empty(qs.Queues, "delete of missing replication removes leftover queues")

			recreated, err := tt.add(svc)
			r.NoError(err)
			r.Greater(recreated, deleted, "recreated replication gets new generation")

			gen, err = svc.GetReplicationGeneration(ctx, tt.otherID)
			r.NoError(err)
			r.Zero(gen, "other replication of the same user and storages is not affected")
		})
	}
}

func Test_DeleteReplication_ZeroDowntimeSwitchInProgress(t *testing.T) {
	userPolicy := entity.UserReplicationPolicy{User: "user", FromStorage: "main", ToStorage: "follower"}
	bucketPolicy := entity.BucketReplicationPolicy{User: "user", FromStorage: "main", ToStorage: "follower", FromBucket: "bucket", ToBucket: "bucket"}
	tests := []struct {
		add    func(svc *policySvc) error
		delete func(svc *policySvc) error
		name   string
		id     entity.UniversalReplicationID
	}{
		{
			name: "user replication",
			id:   entity.UniversalFromUserReplication(userPolicy),
			add: func(svc *policySvc) error {
				_, err := svc.AddUserReplicationPolicy(t.Context(), userPolicy, entity.ReplicationOptions{})
				return err
			},
			delete: func(svc *policySvc) error {
				return svc.DeleteUserReplication(t.Context(), userPolicy)
			},
		},
		{
			name: "bucket replication",
			id:   entity.UniversalFromBucketReplication(bucketPolicy),
			add: func(svc *policySvc) error {
				_, err := svc.AddBucketReplicationPolicy(t.Context(), bucketPolicy, entity.ReplicationOptions{})
				return err
			},
			delete: func(svc *policySvc) error {
				return svc.DeleteBucketReplication(t.Context(), bucketPolicy)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			c := testutil.SetupRedis(t)
			qs := &tasks.QueueServiceMock{
				Queues: map[string]int{},
				Paused: map[string]bool{},
			}
			svc := NewService(c, qs, "main")
			r.NoError(tt.add(svc))
			startZeroDowntimeSwitch(t, svc, qs, tt.id)
			for _, q := range tasks.AllReplicationQueues(tt.id) {
				qs.Queues[q] = 1
			}

			r.ErrorIs(tt.delete(svc), dom.ErrInvalidArg)
			for _, q := range tasks.AllReplicationQueues(tt.id) {
				r.Contains(qs.Queues, q, "queues of replication in switch are not purged")
			}
		})
	}
}
