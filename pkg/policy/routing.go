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
	// TODO: discuss routing management API before implementing. Should we allow to set it explicitly???
	/*
		// SetUserLevelRouting - allows chorus user to configure custom routing per user on chorus proxy.
		SetUserLevelRouting(ctx context.Context, user string, toStorage string) error
		// SetBucketLevelRouting - allows chorus user to configure custom routing per bucket on chorus proxy.
		SetBucketLevelRouting(ctx context.Context, id entity.BucketRoutingPolicyID, toStorage string) error
		// routing visualisation API. Return as graph?
		GetRoutings(ctx context.Context) (Routings, error)

		type Routings struct {
			Main  string                  // main storage - default routing
			Users map[string]UserRoutings // user -> storage
		}

		type UserRoutings struct {
			Storage string            // route to storage if User level routing is set
			Buckets map[string]string // bucket level routings if set
		}
	*/
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
