package policy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/testutil"
)

func boolResult(v bool, err error) store.OperationResult[bool] {
	return store.NewRedisOperationResult(func() (bool, error) {
		return v, err
	})
}

func Test_determineActiveRouting(t *testing.T) {
	var stringResult = func(v string, err error) store.OperationResult[string] {
		return store.NewRedisOperationResult(func() (string, error) {
			return v, err
		})
	}
	type args struct {
		mainStorage   string
		userRouting   store.OperationResult[string]
		bucketRouting store.OperationResult[string]
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr error
	}{
		{
			name: "route to main storage if no routing policies",
			args: args{
				mainStorage:   "main",
				userRouting:   stringResult("", dom.ErrNotFound),
				bucketRouting: stringResult("", dom.ErrNotFound),
			},
			want:    "main",
			wantErr: nil,
		},
		{
			name: "route to user storage if no bucket policy",
			args: args{
				mainStorage:   "main",
				userRouting:   stringResult("user", nil),
				bucketRouting: stringResult("", dom.ErrNotFound),
			},
			want:    "user",
			wantErr: nil,
		},
		{
			name: "route to bucket storage if bucket policy",
			args: args{
				mainStorage:   "main",
				userRouting:   stringResult("", dom.ErrNotFound),
				bucketRouting: stringResult("bucket", nil),
			},
			want:    "bucket",
			wantErr: nil,
		},
		{
			name: "error if both policies set",
			args: args{
				mainStorage:   "main",
				userRouting:   stringResult("user", nil),
				bucketRouting: stringResult("bucket", nil),
			},
			want:    "",
			wantErr: dom.ErrInternal,
		},
		{
			name: "error if bucket policy errors",
			args: args{
				mainStorage:   "main",
				userRouting:   stringResult("user", nil),
				bucketRouting: stringResult("", dom.ErrInternal),
			},
			wantErr: dom.ErrInternal,
		},
		{
			name: "error if user policy errors",
			args: args{
				mainStorage:   "main",
				userRouting:   stringResult("", dom.ErrInternal),
				bucketRouting: stringResult("", dom.ErrNotFound),
			},
			wantErr: dom.ErrInternal,
		},
		{
			name: "user blocked",
			args: args{
				mainStorage:   "main",
				userRouting:   stringResult("", dom.ErrRoutingBlock),
				bucketRouting: stringResult("bucket", nil),
			},
			wantErr: dom.ErrRoutingBlock,
		},
		{
			name: "bucket blocked",
			args: args{
				mainStorage:   "main",
				userRouting:   stringResult("user", nil),
				bucketRouting: stringResult("", dom.ErrRoutingBlock),
			},
			wantErr: dom.ErrRoutingBlock,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			got, err := determineActiveRouting(tt.args.mainStorage, tt.args.userRouting, tt.args.bucketRouting)
			if tt.wantErr == nil {
				r.NoError(err)
				r.Equal(tt.want, got)
			} else {
				r.ErrorIs(err, tt.wantErr)
			}
		})
	}
}

func Test_getReplications(t *testing.T) {
	bucketPoliciesResult := func(v []entity.BucketReplicationPolicy, err error) store.OperationResult[[]entity.BucketReplicationPolicy] {
		return store.NewRedisOperationResult(func() ([]entity.BucketReplicationPolicy, error) {
			return v, err
		})
	}
	userPoliciesResult := func(v []entity.UserReplicationPolicy, err error) store.OperationResult[[]entity.UserReplicationPolicy] {
		return store.NewRedisOperationResult(func() ([]entity.UserReplicationPolicy, error) {
			return v, err
		})
	}
	type args struct {
		bucketPolicies store.OperationResult[[]entity.BucketReplicationPolicy]
		userPolicies   store.OperationResult[[]entity.UserReplicationPolicy]
	}
	tests := []struct {
		name    string
		args    args
		want    []entity.UniversalReplicationID
		wantErr error
	}{
		{
			name: "missing user",
			args: args{
				bucketPolicies: bucketPoliciesResult([]entity.BucketReplicationPolicy{
					{FromStorage: "s1", ToStorage: "s2", ToBucket: "b2"},
				}, nil),
				userPolicies: userPoliciesResult([]entity.UserReplicationPolicy{
					{FromStorage: "s1", ToStorage: "s2"},
				}, nil),
			},
			want:    []entity.UniversalReplicationID{},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "missing bucket",
			args: args{
				bucketPolicies: bucketPoliciesResult([]entity.BucketReplicationPolicy{
					{User: "user", FromStorage: "s1", ToStorage: "s2", ToBucket: "b2"},
				}, nil),
				userPolicies: userPoliciesResult([]entity.UserReplicationPolicy{
					{User: "user", FromStorage: "s1", ToStorage: "s2"},
				}, nil),
			},
			want:    []entity.UniversalReplicationID{},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "bucket level replications",
			args: args{
				bucketPolicies: bucketPoliciesResult([]entity.BucketReplicationPolicy{
					{User: "user", FromStorage: "s1", ToStorage: "s2", FromBucket: "bucket1", ToBucket: "b2"},
					{User: "user", FromStorage: "s1", ToStorage: "s3", FromBucket: "bucket1", ToBucket: "b3"},
				}, nil),
				userPolicies: userPoliciesResult([]entity.UserReplicationPolicy{ // ignored in favor of bucket level
					{User: "user", FromStorage: "x3", ToStorage: "x2"},
					{User: "user", FromStorage: "x1", ToStorage: "x3"},
				}, nil),
			},
			want: []entity.UniversalReplicationID{
				entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "s1",
					FromBucket:  "bucket1",
					ToStorage:   "s2",
					ToBucket:    "b2",
				}),
				entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
					User:        "user",
					FromStorage: "s1",
					FromBucket:  "bucket1",
					ToStorage:   "s3",
					ToBucket:    "b3",
				}),
			},
			wantErr: nil,
		},

		{
			name: "bucket level not found, user level replications",
			args: args{
				bucketPolicies: bucketPoliciesResult(nil, dom.ErrNotFound),
				userPolicies: userPoliciesResult([]entity.UserReplicationPolicy{
					{User: "user", FromStorage: "x3", ToStorage: "x2"},
					{User: "user", FromStorage: "x1", ToStorage: "x3"},
				}, nil),
			},
			want: []entity.UniversalReplicationID{
				entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "x3",
					ToStorage:   "x2",
				}),
				entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "x1",
					ToStorage:   "x3",
				}),
			},
			wantErr: nil,
		},
		{
			name: "bucket level empty, user level replications",
			args: args{
				bucketPolicies: bucketPoliciesResult(nil, nil),
				userPolicies: userPoliciesResult([]entity.UserReplicationPolicy{
					{User: "user", FromStorage: "x3", ToStorage: "x2"},
					{User: "user", FromStorage: "x1", ToStorage: "x3"},
				}, nil),
			},
			want: []entity.UniversalReplicationID{
				entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "x3",
					ToStorage:   "x2",
				}),
				entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
					User:        "user",
					FromStorage: "x1",
					ToStorage:   "x3",
				}),
			},
			wantErr: nil,
		},
		{
			name: "both not found",
			args: args{
				bucketPolicies: bucketPoliciesResult(nil, dom.ErrNotFound),
				userPolicies:   userPoliciesResult(nil, dom.ErrNotFound),
			},
			want:    []entity.UniversalReplicationID{},
			wantErr: nil,
		},
		{
			name: "both empty",
			args: args{
				bucketPolicies: bucketPoliciesResult(nil, nil),
				userPolicies:   userPoliciesResult(nil, nil),
			},
			want:    []entity.UniversalReplicationID{},
			wantErr: nil,
		},
		{
			name: "bucket level error",
			args: args{
				bucketPolicies: bucketPoliciesResult(nil, dom.ErrInternal),
				userPolicies: userPoliciesResult([]entity.UserReplicationPolicy{
					{FromStorage: "x3", ToStorage: "x2"},
					{FromStorage: "x1", ToStorage: "x3"},
				}, nil),
			},
			want:    nil,
			wantErr: dom.ErrInternal,
		},
		{
			name: "user level error",
			args: args{
				bucketPolicies: bucketPoliciesResult(nil, nil),
				userPolicies:   userPoliciesResult(nil, dom.ErrInternal),
			},
			want:    nil,
			wantErr: dom.ErrInternal,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			got, err := getReplications(tt.args.bucketPolicies, tt.args.userPolicies)
			if tt.wantErr == nil {
				r.NoError(err)
				r.Equal(len(tt.want), len(got))
				for i := range tt.want {
					r.Equal(tt.want[i].AsString(), got[i].AsString())
				}
			} else {
				r.ErrorIs(err, tt.wantErr)
			}
		})
	}
}

func Test_getActiveZeroDowntimeSwitch(t *testing.T) {
	c := testutil.SetupRedis(t)
	qs := &tasks.QueueServiceMock{}
	tasks.Reset(qs)
	svc := NewService(c, qs, "main")
	ctx := t.Context()

	t.Run("active res errors", func(t *testing.T) {
		r := require.New(t)
		someID := entity.NewBucketReplicationPolicyID("user", "bucket")
		_, err := svc.getActiveZeroDowntimeSwitch(ctx,
			boolResult(false, dom.ErrInternal),
			boolResult(true, nil),
			someID)
		r.ErrorIs(err, dom.ErrInternal, "should return error if userActiveRes errors")

		_, err = svc.getActiveZeroDowntimeSwitch(ctx,
			boolResult(true, nil),
			boolResult(false, dom.ErrInternal),
			someID)
		r.ErrorIs(err, dom.ErrInternal, "should return error if bucketActiveRes errors")

		_, err = svc.getActiveZeroDowntimeSwitch(ctx,
			boolResult(false, dom.ErrInternal),
			boolResult(false, dom.ErrInternal),
			someID)
		r.ErrorIs(err, dom.ErrInternal, "should return error if both userActiveRes and bucketActiveRes error")

		res, err := svc.getActiveZeroDowntimeSwitch(ctx,
			boolResult(false, dom.ErrNotFound),
			boolResult(false, dom.ErrNotFound),
			someID)
		r.NoError(err, "should not return error if both userActiveRes and bucketActiveRes are not found")
		r.Nil(res, "should return nil switch if both userActiveRes and bucketActiveRes are not found")
	})

	t.Run("both active", func(t *testing.T) {
		r := require.New(t)
		someID := entity.NewBucketReplicationPolicyID("user", "bucket")
		_, err := svc.getActiveZeroDowntimeSwitch(ctx,
			boolResult(true, nil),
			boolResult(true, nil),
			someID)
		r.ErrorIs(err, dom.ErrInternal, "should return error if both userActiveRes and bucketActiveRes are true")
	})

	t.Run("user active", func(t *testing.T) {
		r := require.New(t)
		// create user replication policy
		policy := entity.UserReplicationPolicy{
			User:        "user",
			FromStorage: "main",
			ToStorage:   "follower",
		}
		uid := entity.UniversalFromUserReplication(policy)
		err := svc.AddUserReplicationPolicy(ctx, policy, entity.ReplicationOptions{})
		r.NoError(err)
		// create zero downtime switch
		err = svc.AddZeroDowntimeReplicationSwitch(ctx, uid, &entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: time.Minute})
		r.NoError(err)

		id := policy.LookupID()
		res, err := svc.getActiveZeroDowntimeSwitch(ctx,
			boolResult(true, nil),
			boolResult(false, nil),
			entity.NewBucketReplicationPolicyID(id, "somebucket"))
		r.NoError(err)
		r.NotNil(res)

		gotRepl := res.ReplicationID()
		r.Equal(uid.AsString(), gotRepl.AsString(), "should return correct replication ID")

		// complete the switch
		err = svc.CompleteZeroDowntimeReplicationSwitch(ctx, uid)
		r.NoError(err)
		// should return nil now
		res, err = svc.getActiveZeroDowntimeSwitch(ctx,
			boolResult(true, nil),
			boolResult(false, nil),
			entity.NewBucketReplicationPolicyID(id, "somebucket"))
		r.NoError(err)
		r.Nil(res)
	})

	t.Run("bucket active", func(t *testing.T) {
		r := require.New(t)
		// create bucket replication policy
		policy := entity.BucketReplicationPolicy{
			User:        "user2",
			FromStorage: "main",
			FromBucket:  "bucket2",
			ToStorage:   "follower",
			ToBucket:    "bucket2",
		}
		uid := entity.UniversalFromBucketReplication(policy)
		err := svc.AddBucketReplicationPolicy(ctx, policy, entity.ReplicationOptions{})
		r.NoError(err)
		// create zero downtime switch
		err = svc.AddZeroDowntimeReplicationSwitch(ctx, uid, &entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: time.Minute})
		r.NoError(err)
		id := policy.LookupID()
		res, err := svc.getActiveZeroDowntimeSwitch(ctx,
			boolResult(false, nil),
			boolResult(true, nil),
			id)
		r.NoError(err)
		r.NotNil(res)
		gotRepl := res.ReplicationID()
		r.Equal(uid.AsString(), gotRepl.AsString(), "should return correct replication ID")
	})

}

func Test_BuildProxyContext(t *testing.T) {

	t.Run("empty", func(t *testing.T) {
		c := testutil.SetupRedis(t)
		r := require.New(t)
		svc := NewService(c, nil, "main")
		ctx := t.Context()

		proxyCtx, err := svc.BuildProxyContext(ctx, "user", "bucket")
		r.NoError(err)

		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls:  nil,
		})

		poxyNoBucketCtx, err := svc.BuildProxyNoBucketContext(ctx, "user")
		r.NoError(err)
		validateCtx(t, poxyNoBucketCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls:  nil,
		})
	})
	t.Run("user replications and switch", func(t *testing.T) {
		c := testutil.SetupRedis(t)
		qs := &tasks.QueueServiceMock{}
		tasks.Reset(qs)
		r := require.New(t)
		svc := NewService(c, qs, "main")
		ctx := t.Context()

		//setup: add user replication
		userPolicy := entity.UserReplicationPolicy{
			User:        "user",
			FromStorage: "main",
			ToStorage:   "follower",
		}
		err := svc.AddUserReplicationPolicy(ctx, userPolicy, entity.ReplicationOptions{})
		r.NoError(err)

		// other user returns no policies
		proxyCtx, err := svc.BuildProxyContext(ctx, "other-user", "bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls:  nil,
		})
		proxyNoBucketCtx, err := svc.BuildProxyNoBucketContext(ctx, "other-user")
		r.NoError(err)
		validateCtx(t, proxyNoBucketCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls:  nil,
		})

		// user with replication
		proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls: []entity.UniversalReplicationID{
				entity.UniversalFromUserReplication(userPolicy),
			},
		})
		proxyNoBucketCtx, err = svc.BuildProxyNoBucketContext(ctx, "user")
		r.NoError(err)
		validateCtx(t, proxyNoBucketCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls: []entity.UniversalReplicationID{
				entity.UniversalFromUserReplication(userPolicy),
			},
		})

		// add downtime switch - should not affect context
		uid := entity.UniversalFromUserReplication(userPolicy)
		err = svc.SetDowntimeReplicationSwitch(ctx, uid, nil)
		r.NoError(err)

		proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls: []entity.UniversalReplicationID{
				entity.UniversalFromUserReplication(userPolicy),
			}})
		proxyNoBucketCtx, err = svc.BuildProxyNoBucketContext(ctx, "user")
		r.NoError(err)
		validateCtx(t, proxyNoBucketCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls: []entity.UniversalReplicationID{
				entity.UniversalFromUserReplication(userPolicy),
			}})

		// move downtime switch to in-progress
		err = svc.UpdateDowntimeSwitchStatus(ctx, uid, entity.StatusInProgress, "asdf")
		r.NoError(err)

		// should receive routing block
		proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
		r.ErrorIs(err, dom.ErrRoutingBlock)
		proxyNoBucketCtx, err = svc.BuildProxyNoBucketContext(ctx, "user")
		r.ErrorIs(err, dom.ErrRoutingBlock)

		// complete downtime switch
		err = svc.UpdateDowntimeSwitchStatus(ctx, uid, entity.StatusCheckInProgress, "check")
		r.NoError(err)
		err = svc.UpdateDowntimeSwitchStatus(ctx, uid, entity.StatusDone, "done")
		r.NoError(err)

		// routed to follower now
		proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "follower",
			expectedSwitch: nil,
			expectedRepls:  nil, // no replication after switch done
		})
		proxyNoBucketCtx, err = svc.BuildProxyNoBucketContext(ctx, "user")
		r.NoError(err)
		validateCtx(t, proxyNoBucketCtx, validateCtxParams{
			expectedRoute:  "follower",
			expectedSwitch: nil,
			expectedRepls:  nil, // no replication after switch done
		})

		// delete switch
		err = svc.DeleteReplicationSwitch(ctx, uid)
		r.NoError(err)

		// routed to follower still
		proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "follower",
			expectedSwitch: nil,
			expectedRepls:  nil, // no replication after switch done
		})
		proxyNoBucketCtx, err = svc.BuildProxyNoBucketContext(ctx, "user")
		r.NoError(err)
		validateCtx(t, proxyNoBucketCtx, validateCtxParams{
			expectedRoute:  "follower",
			expectedSwitch: nil,
			expectedRepls:  nil, // no replication after switch done
		})

		// do zero downtime switch now
		// add policy for different user
		otherUserPolicy := entity.UserReplicationPolicy{
			User:        "other-user",
			FromStorage: "main",
			ToStorage:   "follower",
		}

		err = svc.AddUserReplicationPolicy(ctx, otherUserPolicy, entity.ReplicationOptions{})
		r.NoError(err)

		proxyCtx, err = svc.BuildProxyContext(ctx, "other-user", "bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls: []entity.UniversalReplicationID{
				entity.UniversalFromUserReplication(otherUserPolicy),
			}})
		proxyNoBucketCtx, err = svc.BuildProxyNoBucketContext(ctx, "other-user")
		r.NoError(err)
		validateCtx(t, proxyNoBucketCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls: []entity.UniversalReplicationID{
				entity.UniversalFromUserReplication(otherUserPolicy),
			}})

		// add zero downtime switch
		otherUID := entity.UniversalFromUserReplication(otherUserPolicy)
		err = svc.AddZeroDowntimeReplicationSwitch(ctx, otherUID, &entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: time.Minute})
		r.NoError(err)

		// should have active switch and no replications
		proxyCtx, err = svc.BuildProxyContext(ctx, "other-user", "bucket")
		r.NoError(err)
		expectedSwitch := &entity.ReplicationSwitchInfo{}
		expectedSwitch.SetReplicationID(otherUID)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "follower",
			expectedSwitch: expectedSwitch,
			expectedRepls:  nil,
		})
		proxyNoBucketCtx, err = svc.BuildProxyNoBucketContext(ctx, "other-user")
		r.NoError(err)
		validateCtx(t, proxyNoBucketCtx, validateCtxParams{
			expectedRoute:  "follower",
			expectedSwitch: expectedSwitch,
			expectedRepls:  nil,
		})

		// complete zero downtime switch
		err = svc.CompleteZeroDowntimeReplicationSwitch(ctx, otherUID)
		r.NoError(err)

		// should be routed to follower and no replications
		proxyCtx, err = svc.BuildProxyContext(ctx, "other-user", "bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "follower",
			expectedSwitch: nil,
			expectedRepls:  nil,
		})
		proxyNoBucketCtx, err = svc.BuildProxyNoBucketContext(ctx, "other-user")
		r.NoError(err)
		validateCtx(t, proxyNoBucketCtx, validateCtxParams{
			expectedRoute:  "follower",
			expectedSwitch: nil,
			expectedRepls:  nil,
		})
	})

	t.Run("multiple user replications", func(t *testing.T) {
		c := testutil.SetupRedis(t)
		r := require.New(t)
		svc := NewService(c, nil, "main")
		ctx := t.Context()

		//setup: add user replication
		userPolicy1 := entity.UserReplicationPolicy{
			User:        "user",
			FromStorage: "main",
			ToStorage:   "follower",
		}
		err := svc.AddUserReplicationPolicy(ctx, userPolicy1, entity.ReplicationOptions{})
		r.NoError(err)

		proxyCtx, err := svc.BuildProxyContext(ctx, "user", "bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute: "main",
			expectedRepls: []entity.UniversalReplicationID{
				entity.UniversalFromUserReplication(userPolicy1),
			},
			expectedSwitch: nil,
		})
		proxyNoBucketCtx, err := svc.BuildProxyNoBucketContext(ctx, "user")
		r.NoError(err)
		validateCtx(t, proxyNoBucketCtx, validateCtxParams{
			expectedRoute: "main",
			expectedRepls: []entity.UniversalReplicationID{
				entity.UniversalFromUserReplication(userPolicy1),
			},
			expectedSwitch: nil,
		})

		// add another replication
		userPolicy2 := entity.UserReplicationPolicy{
			User:        "user",
			FromStorage: "main",
			ToStorage:   "follower2",
		}
		err = svc.AddUserReplicationPolicy(ctx, userPolicy2, entity.ReplicationOptions{})
		r.NoError(err)

		proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute: "main",
			expectedRepls: []entity.UniversalReplicationID{
				entity.UniversalFromUserReplication(userPolicy1),
				entity.UniversalFromUserReplication(userPolicy2),
			},
			expectedSwitch: nil,
		})
		proxyNoBucketCtx, err = svc.BuildProxyNoBucketContext(ctx, "user")
		r.NoError(err)
		validateCtx(t, proxyNoBucketCtx, validateCtxParams{
			expectedRoute: "main",
			expectedRepls: []entity.UniversalReplicationID{
				entity.UniversalFromUserReplication(userPolicy1),
				entity.UniversalFromUserReplication(userPolicy2),
			},
			expectedSwitch: nil,
		})
	})

	t.Run("bucket replications and switch", func(t *testing.T) {
		c := testutil.SetupRedis(t)
		qs := &tasks.QueueServiceMock{}
		tasks.Reset(qs)
		r := require.New(t)
		svc := NewService(c, qs, "main")
		ctx := t.Context()

		//setup: add bucket replication
		bucketPolicy := entity.BucketReplicationPolicy{
			User:        "user",
			FromStorage: "main",
			ToStorage:   "follower",
			FromBucket:  "bucket",
			ToBucket:    "bucket",
		}
		err := svc.AddBucketReplicationPolicy(ctx, bucketPolicy, entity.ReplicationOptions{})
		r.NoError(err)

		// other user returns no policies
		proxyCtx, err := svc.BuildProxyContext(ctx, "other-user", "bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls:  nil,
		})
		proxyNoBucketCtx, err := svc.BuildProxyNoBucketContext(ctx, "other-user")
		r.NoError(err)
		validateCtx(t, proxyNoBucketCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls:  nil,
		})

		// other bucket returns no policies
		proxyCtx, err = svc.BuildProxyContext(ctx, "user", "other-bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls:  nil,
		})
		proxyNoBucketCtx, err = svc.BuildProxyNoBucketContext(ctx, "user")
		r.NoError(err)
		validateCtx(t, proxyNoBucketCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls:  nil,
		})

		// bucket with replication
		proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls: []entity.UniversalReplicationID{
				entity.UniversalFromBucketReplication(bucketPolicy),
			},
		})
		proxyNoBucketCtx, err = svc.BuildProxyNoBucketContext(ctx, "user")
		r.NoError(err)
		validateCtx(t, proxyNoBucketCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls:  nil,
		})

		// add downtime switch - should not affect context
		uid := entity.UniversalFromBucketReplication(bucketPolicy)
		err = svc.SetDowntimeReplicationSwitch(ctx, uid, nil)
		r.NoError(err)

		proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls: []entity.UniversalReplicationID{
				entity.UniversalFromBucketReplication(bucketPolicy),
			}})
		// other bucket - should not be affected
		proxyCtx, err = svc.BuildProxyContext(ctx, "user", "other-bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls:  nil,
		})
		proxyNoBucketCtx, err = svc.BuildProxyNoBucketContext(ctx, "user")
		r.NoError(err)
		validateCtx(t, proxyNoBucketCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls:  nil,
		})

		// move downtime switch to in-progress
		err = svc.UpdateDowntimeSwitchStatus(ctx, uid, entity.StatusInProgress, "asdf")
		r.NoError(err)

		// should receive routing block
		proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
		r.ErrorIs(err, dom.ErrRoutingBlock)
		// other bucket - should not be affected
		proxyCtx, err = svc.BuildProxyContext(ctx, "user", "other-bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls:  nil,
		})
		proxyNoBucketCtx, err = svc.BuildProxyNoBucketContext(ctx, "user")
		r.NoError(err)
		validateCtx(t, proxyNoBucketCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls:  nil,
		})

		// complete downtime switch
		err = svc.UpdateDowntimeSwitchStatus(ctx, uid, entity.StatusCheckInProgress, "check")
		r.NoError(err)
		err = svc.UpdateDowntimeSwitchStatus(ctx, uid, entity.StatusDone, "done")
		r.NoError(err)

		// routed to follower now
		proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "follower",
			expectedSwitch: nil,
			expectedRepls:  nil, // no replication after switch done
		})
		// other bucket - should not be affected
		proxyCtx, err = svc.BuildProxyContext(ctx, "user", "other-bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls:  nil,
		})
		proxyNoBucketCtx, err = svc.BuildProxyNoBucketContext(ctx, "user")
		r.NoError(err)
		validateCtx(t, proxyNoBucketCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls:  nil, // no replication after switch done
		})

		// delete switch
		err = svc.DeleteReplicationSwitch(ctx, uid)
		r.NoError(err)

		// routed to follower still
		proxyCtx, err = svc.BuildProxyContext(ctx, "user", "bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "follower",
			expectedSwitch: nil,
			expectedRepls:  nil, // no replication after switch done
		})
		proxyNoBucketCtx, err = svc.BuildProxyNoBucketContext(ctx, "user")
		r.NoError(err)
		validateCtx(t, proxyNoBucketCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls:  nil, // no replication after switch done
		})

		// do zero downtime switch now
		// add policy for different user
		otherBucketPolicy := entity.BucketReplicationPolicy{
			User:        "user",
			FromStorage: "main",
			ToStorage:   "follower",
			FromBucket:  "other-bucket",
			ToBucket:    "other-bucket",
		}

		err = svc.AddBucketReplicationPolicy(ctx, otherBucketPolicy, entity.ReplicationOptions{})
		r.NoError(err)

		proxyCtx, err = svc.BuildProxyContext(ctx, "user", "other-bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls: []entity.UniversalReplicationID{
				entity.UniversalFromBucketReplication(otherBucketPolicy),
			}})

		// add zero downtime switch
		otherUID := entity.UniversalFromBucketReplication(otherBucketPolicy)
		err = svc.AddZeroDowntimeReplicationSwitch(ctx, otherUID, &entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: time.Minute})
		r.NoError(err)

		// should have active switch and no replications
		proxyCtx, err = svc.BuildProxyContext(ctx, "user", "other-bucket")
		r.NoError(err)
		expectedSwitch := &entity.ReplicationSwitchInfo{}
		expectedSwitch.SetReplicationID(otherUID)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "follower",
			expectedSwitch: expectedSwitch,
			expectedRepls:  nil,
		})
		// some other bucket - should not be affected
		proxyCtx, err = svc.BuildProxyContext(ctx, "user", "some-bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "main",
			expectedSwitch: nil,
			expectedRepls:  nil,
		})

		// complete zero downtime switch
		err = svc.CompleteZeroDowntimeReplicationSwitch(ctx, otherUID)
		r.NoError(err)

		// should be routed to follower and no replications
		proxyCtx, err = svc.BuildProxyContext(ctx, "user", "other-bucket")
		r.NoError(err)
		validateCtx(t, proxyCtx, validateCtxParams{
			expectedRoute:  "follower",
			expectedSwitch: nil,
			expectedRepls:  nil,
		})
	})

}

type validateCtxParams struct {
	expectedRoute  string
	expectedSwitch *entity.ReplicationSwitchInfo
	expectedRepls  []entity.UniversalReplicationID
}

func validateCtx(t *testing.T, ctx context.Context, expected validateCtxParams) {
	t.Helper()
	r := require.New(t)
	route := xctx.GetRoutingPolicy(ctx)
	r.Equal(expected.expectedRoute, route, "routing policy should match")

	got := xctx.GetInProgressZeroDowntime(ctx)
	if expected.expectedSwitch == nil {
		r.Nil(got)
	} else {
		r.NotNil(got)
		expectedRepl := expected.expectedSwitch.ReplicationID()
		gotRepl := got.ReplicationID()
		r.Equal(expectedRepl.AsString(), gotRepl.AsString(), "in-progress switch replication ID should match")
	}

	gotReplications := xctx.GetReplications(ctx)
	if len(expected.expectedRepls) == 0 {
		r.Empty(gotReplications)
	} else {
		r.Equal(len(expected.expectedRepls), len(gotReplications), "number of replications should match")
		for i := range expected.expectedRepls {
			r.Equal(expected.expectedRepls[i].AsString(), gotReplications[i].AsString(), "replication ID should match")
		}
	}
}
