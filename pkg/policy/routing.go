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
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/store"
)

type RoutingSvc interface {
	SetUserRouting(ctx context.Context, user string, toStorage string) error
	GetUserRoutings(ctx context.Context) (map[string]string, error)
	GetUserRoutingBlocks(ctx context.Context) (map[string]bool, error)
	DeleteUserRouting(ctx context.Context, user string) error
	BlockUserRouting(ctx context.Context, user string) error
	UnblockUserRouting(ctx context.Context, user string) error

	SetBucketRouting(ctx context.Context, id entity.BucketRoutingPolicyID, toStorage string) error
	GetBucketRoutings(ctx context.Context, user string) (map[string]string, error)
	GetBucketRoutingBlocks(ctx context.Context, user string) (map[string]bool, error)
	DeleteBucketRouting(ctx context.Context, id entity.BucketRoutingPolicyID) error
	BlockBucketRouting(ctx context.Context, id entity.BucketRoutingPolicyID) error
	UnblockBucketRouting(ctx context.Context, id entity.BucketRoutingPolicyID) error
}

var _ RoutingSvc = (*policySvc)(nil)

func (r *policySvc) SetBucketRouting(ctx context.Context, id entity.BucketRoutingPolicyID, toStorage string) error {
	//check if there are no active user-level replications for the user
	userRepls, err := r.userReplicationPolicyStore.Get(ctx, id.User)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("unable to get user-level replications for user %s: %w", id.User, err)
	}
	if len(userRepls) > 0 {
		return fmt.Errorf("%w: cannot set bucket-level routing for user %s and bucket %s because user-level replications exist", dom.ErrInvalidArg, id.User, id.Bucket)
	}
	// check if there are no active bucket-level replications
	bucketRepls, err := r.bucketReplicationPolicyStore.Get(ctx, entity.BucketReplicationPolicyID{
		User:       id.User,
		FromBucket: id.Bucket,
	})
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("unable to get bucket-level replications for user %s and bucket %s: %w", id.User, id.Bucket, err)
	}
	if len(bucketRepls) > 0 {
		return fmt.Errorf("%w: cannot set bucket-level routing for user %s and bucket %s because active bucket-level replications exist", dom.ErrInvalidArg, id.User, id.Bucket)
	}
	// add the routing
	return r.bucketRoutingStore.SetOp(ctx, id, toStorage).Get()
}

func (r *policySvc) SetUserRouting(ctx context.Context, user string, toStorage string) error {
	//check if there are no active user-level replications
	userRepls, err := r.userReplicationPolicyStore.Get(ctx, user)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("unable to get user-level replications for user %s: %w", user, err)
	}
	if len(userRepls) > 0 {
		return fmt.Errorf("%w: cannot set user-level routing for user %s because active user-level replications exist", dom.ErrInvalidArg, user)
	}
	// add the routing
	return r.userRoutingStore.SetOp(ctx, user, toStorage).Get()
}

func (r *policySvc) DeleteBucketRouting(ctx context.Context, id entity.BucketRoutingPolicyID) error {
	// check if there are no dependent bucket-level replications and it is safe to delete
	bucketRepl, err := r.bucketReplicationPolicyStore.Get(ctx, entity.BucketReplicationPolicyID{
		User:       id.User,
		FromBucket: id.Bucket,
	})
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("unable to get bucket-level replications for user %s and bucket %s: %w", id.User, id.Bucket, err)
	}
	if len(bucketRepl) > 0 {
		return fmt.Errorf("%w: cannot delete bucket-level routing for user %s and bucket %s because dependent bucket-level replications exist", dom.ErrInvalidArg, id.User, id.Bucket)
	}
	return r.bucketRoutingStore.DeleteRouteOp(ctx, id).Get()
}

func (r *policySvc) DeleteUserRouting(ctx context.Context, user string) error {
	// check if there are no active user-level replications and it is safe to delete
	userRepls, err := r.userReplicationPolicyStore.Get(ctx, user)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("unable to get user-level replications for user %s: %w", user, err)
	}
	if len(userRepls) > 0 {
		return fmt.Errorf("%w: cannot delete user-level routing for user %s because dependent user-level replications exist", dom.ErrInvalidArg, user)
	}
	return r.userRoutingStore.DeleteRouteOp(ctx, user).Get()
}

func (r *policySvc) GetBucketRoutings(ctx context.Context, user string) (map[string]string, error) {
	return r.bucketRoutingStore.ListRoutesOp(ctx, user).Get()
}

func (r *policySvc) GetUserRoutings(ctx context.Context) (map[string]string, error) {
	return r.userRoutingStore.ListRoutesOp(ctx).Get()
}

func (r *policySvc) BlockBucketRouting(ctx context.Context, id entity.BucketRoutingPolicyID) error {
	return r.bucketRoutingStore.BlockOp(ctx, id).Get()
}

func (r *policySvc) BlockUserRouting(ctx context.Context, user string) error {
	return r.userRoutingStore.BlockOp(ctx, user).Get()
}

func (r *policySvc) UnblockBucketRouting(ctx context.Context, id entity.BucketRoutingPolicyID) error {
	return r.bucketRoutingStore.UnblockOp(ctx, id).Get()
}

func (r *policySvc) UnblockUserRouting(ctx context.Context, user string) error {
	return r.userRoutingStore.UnblockOp(ctx, user).Get()
}

func (r *policySvc) GetBucketRoutingBlocks(ctx context.Context, user string) (map[string]bool, error) {
	return r.bucketRoutingStore.ListBlocksOp(ctx, user).Get()
}

func (r *policySvc) GetUserRoutingBlocks(ctx context.Context) (map[string]bool, error) {
	return r.userRoutingStore.ListBlocksOp(ctx).Get()
}

func (r *policySvc) unblockRouteInTx(ctx context.Context, tx store.Executor[redis.Pipeliner], policy entity.UniversalReplicationID) {
	if bucketPolicy, ok := policy.AsBucketID(); ok {
		_ = r.bucketRoutingStore.WithExecutor(tx).UnblockOp(ctx, bucketPolicy.RoutingID())
	} else if userPolicy, ok := policy.AsUserID(); ok {
		_ = r.userRoutingStore.WithExecutor(tx).UnblockOp(ctx, userPolicy.RoutingID())
	} else {
		// should never happen
		panic(fmt.Sprintf("unknown policy type %s", policy.AsString()))
	}
}

func (r *policySvc) blockRouteInTx(ctx context.Context, tx store.Executor[redis.Pipeliner], policy entity.UniversalReplicationID) {
	if bucketPolicy, ok := policy.AsBucketID(); ok {
		_ = r.bucketRoutingStore.WithExecutor(tx).BlockOp(ctx, bucketPolicy.RoutingID())
	} else if userPolicy, ok := policy.AsUserID(); ok {
		_ = r.userRoutingStore.WithExecutor(tx).BlockOp(ctx, userPolicy.RoutingID())
	} else {
		// should never happen
		panic(fmt.Sprintf("unknown policy type %s", policy.AsString()))
	}
}

func (r *policySvc) routeToNewInTx(ctx context.Context, tx store.Executor[redis.Pipeliner], policy entity.UniversalReplicationID) {
	if bucketPolicy, ok := policy.AsBucketID(); ok {
		_ = r.bucketRoutingStore.WithExecutor(tx).SetOp(ctx, bucketPolicy.RoutingID(), bucketPolicy.ToStorage)
	} else if userPolicy, ok := policy.AsUserID(); ok {
		_ = r.userRoutingStore.WithExecutor(tx).SetOp(ctx, userPolicy.RoutingID(), userPolicy.ToStorage)
	} else {
		// should never happen
		panic(fmt.Sprintf("unknown policy type %s", policy.AsString()))
	}
}

func (r *policySvc) routeToOldInTx(ctx context.Context, tx store.Executor[redis.Pipeliner], policy entity.UniversalReplicationID) {
	if bucketPolicy, ok := policy.AsBucketID(); ok {
		_ = r.bucketRoutingStore.WithExecutor(tx).SetOp(ctx, bucketPolicy.RoutingID(), bucketPolicy.FromStorage)
	} else if userPolicy, ok := policy.AsUserID(); ok {
		_ = r.userRoutingStore.WithExecutor(tx).SetOp(ctx, userPolicy.RoutingID(), userPolicy.FromStorage)
	} else {
		// should never happen
		panic(fmt.Sprintf("unknown policy type %s", policy.AsString()))
	}
}

func (r *policySvc) getRoutingForReplication(ctx context.Context, repliction entity.UniversalReplicationID) (string, error) {
	if bucketPolicy, ok := repliction.AsBucketID(); ok {
		routeTo, err := r.bucketRoutingStore.GetOp(ctx, bucketPolicy.RoutingID()).Get()
		switch {
		case errors.Is(err, dom.ErrNotFound):
			// try user level routing
			break
		default:
			return routeTo, err
		}
	}
	userRouteTo, err := r.userRoutingStore.GetOp(ctx, repliction.User()).Get()
	switch {
	case errors.Is(err, dom.ErrNotFound):
		// no user level routing, return main
		return r.mainStorage, nil
	default:
		return userRouteTo, err
	}
}
