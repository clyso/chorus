// Copyright 2025 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package policy

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/testutil"
)

func Test_policySvc_SetBucketRouting(t *testing.T) {
	c := testutil.SetupRedis(t)
	type before struct {
		mainStorage string
		userRepl    *entity.UserReplicationPolicy
		bucketRepl  *entity.BucketReplicationPolicy
		id          *entity.BucketRoutingPolicyID
		toStorage   string
	}
	tests := []struct {
		name      string
		before    before
		id        entity.BucketRoutingPolicyID
		toStorage string
		wantErr   error
	}{
		{
			name: "set routing without replications",
			before: before{
				mainStorage: "main",
			},
			id:        entity.BucketRoutingPolicyID{User: "user1", Bucket: "bucket1"},
			toStorage: "storageA",
			wantErr:   nil,
		},
		{
			name: "fail when user-level replication exists",
			before: before{
				mainStorage: "main",
				userRepl: &entity.UserReplicationPolicy{
					User:        "user1",
					FromStorage: "main",
					ToStorage:   "storageB",
				},
			},
			id:        entity.BucketRoutingPolicyID{User: "user1", Bucket: "bucket1"},
			toStorage: "main",
			wantErr:   dom.ErrInvalidArg,
		},
		{
			name: "fail when bucket-level replication exists",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user1",
					FromStorage: "main",
					FromBucket:  "bucket1",
					ToStorage:   "storageC",
					ToBucket:    "bucket1",
				},
			},
			id:        entity.BucketRoutingPolicyID{User: "user1", Bucket: "bucket1"},
			toStorage: "main",
			wantErr:   dom.ErrInvalidArg,
		},
		{
			name: "update routing",
			before: before{
				mainStorage: "main",
				id: &entity.BucketRoutingPolicyID{
					User:   "user1",
					Bucket: "bucket1",
				},
				toStorage: "storageB",
			},
			id:        entity.BucketRoutingPolicyID{User: "user1", Bucket: "bucket1"},
			toStorage: "storageA",
			wantErr:   nil,
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
			}
			if tt.before.bucketRepl != nil {
				err := svc.AddBucketReplicationPolicy(ctx, *tt.before.bucketRepl, entity.ReplicationOptions{})
				r.NoError(err)
			}
			if tt.before.id != nil {
				err := svc.SetBucketRouting(ctx, *tt.before.id, tt.before.toStorage)
				r.NoError(err)
			}

			err := svc.SetBucketRouting(ctx, tt.id, tt.toStorage)
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
			} else {
				r.NoError(err)

				res, err := svc.GetBucketRoutings(ctx, tt.id.User)
				r.NoError(err)
				r.Equal(tt.toStorage, res[tt.id.Bucket])
				if tt.before.id != nil && tt.before.toStorage != tt.toStorage {
					// check update
					r.NotEqual(tt.before.toStorage, res[tt.id.Bucket])
				}
			}
		})
	}
}
func Test_policySvc_DeleteBucketRouting(t *testing.T) {
	c := testutil.SetupRedis(t)
	type before struct {
		mainStorage         string
		userRepl            *entity.UserReplicationPolicy
		bucketRepl          *entity.BucketReplicationPolicy
		bucketRoute         *entity.BucketRoutingPolicyID
		buckeRouteToStorage string
	}
	tests := []struct {
		name    string
		before  before
		id      entity.BucketRoutingPolicyID
		wantErr error
	}{
		{
			name: "delete routing without dependent replications",
			before: before{
				mainStorage: "main",
				bucketRoute: &entity.BucketRoutingPolicyID{
					User:   "user1",
					Bucket: "bucket1",
				},
				buckeRouteToStorage: "storageA",
			},
			id:      entity.BucketRoutingPolicyID{User: "user1", Bucket: "bucket1"},
			wantErr: nil,
		},
		{
			name: "fail when dependent bucket-level replication exists",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user1",
					FromStorage: "main",
					FromBucket:  "bucket1",
					ToStorage:   "storageC",
					ToBucket:    "bucket1-replica",
				},
			},
			id:      entity.BucketRoutingPolicyID{User: "user1", Bucket: "bucket1"},
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "delete idempotent",
			before: before{
				mainStorage: "main",
			},
			id:      entity.BucketRoutingPolicyID{User: "user1", Bucket: "bucket1"},
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
			}
			if tt.before.bucketRepl != nil {
				err := svc.AddBucketReplicationPolicy(ctx, *tt.before.bucketRepl, entity.ReplicationOptions{})
				r.NoError(err)
			}
			if tt.before.bucketRoute != nil {
				err := svc.SetBucketRouting(ctx, *tt.before.bucketRoute, tt.before.buckeRouteToStorage)
				r.NoError(err)
			}

			err := svc.DeleteBucketRouting(ctx, tt.id)
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
			} else {
				r.NoError(err)

				res, err := svc.GetBucketRoutings(ctx, tt.id.User)
				r.NoError(err)
				_, exists := res[tt.id.Bucket]
				r.False(exists)
			}
		})
	}
}

func Test_policySvc_SetUserRouting(t *testing.T) {
	c := testutil.SetupRedis(t)
	type before struct {
		mainStorage string
		userRepl    *entity.UserReplicationPolicy
		bucketRepl  *entity.BucketReplicationPolicy
		userRoute   *string
		toStorage   string
	}
	tests := []struct {
		name      string
		before    before
		user      string
		toStorage string
		wantErr   error
	}{
		{
			name: "set routing without replications",
			before: before{
				mainStorage: "main",
			},
			user:      "user1",
			toStorage: "storageA",
			wantErr:   nil,
		},
		{
			name: "fail when user-level replication exists",
			before: before{
				mainStorage: "main",
				userRepl: &entity.UserReplicationPolicy{
					User:        "user1",
					FromStorage: "main",
					ToStorage:   "storageB",
				},
			},
			user:      "user1",
			toStorage: "storageA",
			wantErr:   dom.ErrInvalidArg,
		},
		{
			name: "succeed when bucket-level replication exists",
			before: before{
				mainStorage: "main",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user1",
					FromStorage: "main",
					FromBucket:  "bucket1",
					ToStorage:   "storageC",
					ToBucket:    "bucket1-replica",
				},
			},
			user:      "user1",
			toStorage: "storageA",
			wantErr:   nil,
		},
		{
			name: "update routing",
			before: before{
				mainStorage: "main",
				userRoute:   stringPtr("user1"),
				toStorage:   "storageB",
			},
			user:      "user1",
			toStorage: "storageA",
			wantErr:   nil,
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
			}
			if tt.before.bucketRepl != nil {
				err := svc.AddBucketReplicationPolicy(ctx, *tt.before.bucketRepl, entity.ReplicationOptions{})
				r.NoError(err)
			}
			if tt.before.userRoute != nil {
				err := svc.SetUserRouting(ctx, *tt.before.userRoute, tt.before.toStorage)
				r.NoError(err)
			}

			err := svc.SetUserRouting(ctx, tt.user, tt.toStorage)
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
			} else {
				r.NoError(err)

				res, err := svc.GetUserRoutings(ctx)
				r.NoError(err)
				r.Equal(tt.toStorage, res[tt.user])
				if tt.before.userRoute != nil && *tt.before.userRoute == tt.user && tt.before.toStorage != tt.toStorage {
					// check update
					r.NotEqual(tt.before.toStorage, res[tt.user])
				}
			}
		})
	}
}

func Test_policySvc_DeleteUserRouting(t *testing.T) {
	c := testutil.SetupRedis(t)
	type before struct {
		mainStorage string
		userRepl    *entity.UserReplicationPolicy
		bucketRepl  *entity.BucketReplicationPolicy
		userRoute   *string
		routeTo     string
	}
	tests := []struct {
		name    string
		before  before
		user    string
		wantErr error
	}{
		{
			name: "delete routing without dependent replications",
			before: before{
				mainStorage: "main",
				userRoute:   stringPtr("user1"),
				routeTo:     "storageA",
			},
			user:    "user1",
			wantErr: nil,
		},
		{
			name: "fail when dependent user-level replication exists",
			before: before{
				mainStorage: "main",
				userRepl: &entity.UserReplicationPolicy{
					User:        "user1",
					FromStorage: "main",
					ToStorage:   "storageB",
				},
			},
			user:    "user1",
			wantErr: dom.ErrInvalidArg,
		},
		{
			name: "succeed when bucket-level replication exists",
			before: before{
				mainStorage: "main",
				userRoute:   stringPtr("user1"),
				routeTo:     "storageA",
				bucketRepl: &entity.BucketReplicationPolicy{
					User:        "user1",
					FromStorage: "main",
					FromBucket:  "bucket1",
					ToStorage:   "storageC",
					ToBucket:    "bucket1-replica",
				},
			},
			user:    "user1",
			wantErr: nil,
		},
		{
			name: "delete idempotent",
			before: before{
				mainStorage: "main",
			},
			user:    "user1",
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
			}
			if tt.before.bucketRepl != nil {
				err := svc.AddBucketReplicationPolicy(ctx, *tt.before.bucketRepl, entity.ReplicationOptions{})
				r.NoError(err)
			}
			if tt.before.userRoute != nil {
				err := svc.SetUserRouting(ctx, *tt.before.userRoute, tt.before.routeTo)
				r.NoError(err)
			}

			err := svc.DeleteUserRouting(ctx, tt.user)
			if tt.wantErr != nil {
				r.ErrorIs(err, tt.wantErr)
			} else {
				r.NoError(err)

				res, err := svc.GetUserRoutings(ctx)
				r.NoError(err)
				_, exists := res[tt.user]
				r.False(exists)
			}
		})
	}
}
