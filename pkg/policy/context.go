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

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/validate"
)

type ContextSvc interface {
	// Sets active policies to proxy request context.
	BuildProxyContext(ctx context.Context, user, bucket string) (context.Context, error)
	// Same as BuildProxyContext but for requests where bucket is not known (e.g. ListBuckets).
	BuildProxyNoBucketContext(ctx context.Context, user string) (context.Context, error)
	// Sets active policies to agent bucket notification request context.
	BuildAgentContext(ctx context.Context, user, bucket string) (context.Context, error)
}

var _ ContextSvc = (*policySvc)(nil)

func (r *policySvc) BuildProxyContext(ctx context.Context, user, bucket string) (context.Context, error) {
	bucketRouteID := entity.NewBucketRoutingPolicyID(user, bucket)
	if err := validate.BucketRoutingPolicyID(bucketRouteID); err != nil {
		return ctx, fmt.Errorf("unable to validate bucket routing policy id: %w", err)
	}
	bucketReplID := entity.NewBucketReplicationPolicyID(user, bucket)
	if err := validate.BucketReplicationPolicyID(bucketReplID); err != nil {
		return ctx, fmt.Errorf("unable to validate bucket replication policy id: %w", err)
	}

	// in a single batch get:
	//  1. user level routing
	//  2. bucket level routing
	//  3. user level active zero downtime switch
	//  4. bucket level active zero downtime switch
	//  5. user replication policies
	//  6. bucket replication policies
	tx := r.userRoutingStore.TxExecutor()
	userRoutingResult := r.userRoutingStore.WithExecutor(tx).GetOp(ctx, user)
	bucketRoutingResult := r.bucketRoutingStore.WithExecutor(tx).GetOp(ctx, bucketRouteID)
	userActiveSwitchResult := r.userReplicationSwitchStore.WithExecutor(tx).IsZeroDowntimeActiveOp(ctx, user)
	bucketActiveSwitchResult := r.bucketReplicationSwitchStore.WithExecutor(tx).IsZeroDowntimeActiveOp(ctx, bucketReplID)
	userReplications := r.userReplicationPolicyStore.WithExecutor(tx).GetOp(ctx, user)
	bucketReplications := r.bucketReplicationPolicyStore.WithExecutor(tx).GetOp(ctx, entity.NewBucketReplicationPolicyID(user, bucket))
	_ = tx.Exec(ctx) // ignore error - we will catch it on individual results

	// get active routing policy
	rotuteTo, err := determineActiveRouting(r.mainStorage, userRoutingResult, bucketRoutingResult)
	if err != nil {
		return ctx, err
	}
	ctx = xctx.SetRoutingPolicy(ctx, rotuteTo)

	// check in_progress zero downtime switch if present
	activeSwitch, err := r.getActiveZeroDowntimeSwitch(ctx, userActiveSwitchResult, bucketActiveSwitchResult, bucketReplID)
	if err != nil {
		return ctx, err
	}
	if activeSwitch != nil {
		ctx = xctx.SetInProgressZeroDowntime(ctx, *activeSwitch)
	}

	// set replication policies
	relications, err := getReplications(bucketReplications, userReplications)
	if err != nil {
		return ctx, err
	}
	if len(relications) != 0 {
		ctx = xctx.SetReplications(ctx, relications)
	}

	return ctx, nil
}

func determineActiveRouting(mainStorage string, userRouting, bucketRouting store.OperationResult[string]) (string, error) {
	userRoute, userErr := userRouting.Get()
	bucketRoute, bucketErr := bucketRouting.Get()

	// check for routing blocks
	if errors.Is(userErr, dom.ErrRoutingBlock) || errors.Is(bucketErr, dom.ErrRoutingBlock) {
		return "", dom.ErrRoutingBlock
	}

	// fist check bucket routing
	switch {
	case bucketErr == nil:
		return bucketRoute, nil
	case errors.Is(bucketErr, dom.ErrNotFound):
		// bucket routing not found, try user routing
		break
	default:
		return "", fmt.Errorf("unable to get bucket routing policy: %w", bucketErr)
	}
	// try user routing
	switch {
	case userErr == nil:
		return userRoute, nil
	case errors.Is(userErr, dom.ErrNotFound):
		// user routing not found, use main mainStorage
		return mainStorage, nil
	default:
		return "", fmt.Errorf("unable to get user routing policy: %w", userErr)
	}
}

func (r *policySvc) getActiveZeroDowntimeSwitch(ctx context.Context, userActiveRes, bucketActiveRes store.OperationResult[bool], id entity.BucketReplicationPolicyID) (*entity.ReplicationSwitchInfo, error) {
	// check errors first
	userSwitchActive, err := userActiveRes.Get()
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return nil, fmt.Errorf("unable to get user zero downtime switch info: %w", err)
	}
	bucketSwitchActive, err := bucketActiveRes.Get()
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return nil, fmt.Errorf("unable to get user zero downtime switch info: %w", err)
	}
	// sanity check:
	if bucketSwitchActive && userSwitchActive {
		// should never happen - only one level of zero downtime switch can be active at a time
		return nil, fmt.Errorf("%w: both user and bucket zero downtime switch are active", dom.ErrInternal)
	}
	// lookup user switch if active
	if userSwitchActive {
		userSwitch, err := r.userReplicationSwitchStore.GetOp(ctx, id.User).Get()
		if err != nil {
			return nil, fmt.Errorf("unable to get user zero downtime switch info: %w", err)
		}
		if userSwitch.MultipartTTL == 0 {
			// should never happen
			return nil, fmt.Errorf("%w: user zero downtime switch info is invalid: %+v", dom.ErrInternal, userSwitch)
		}
		if userSwitch.LastStatus == entity.StatusInProgress {
			return &userSwitch, nil
		}
		// switch is not in progress anymore
		return nil, nil
	}
	//otherwise lookup bucket switch if active
	if bucketSwitchActive {
		bucketSwitch, err := r.bucketReplicationSwitchStore.GetOp(ctx, id).Get()
		if err != nil {
			return nil, fmt.Errorf("unable to get bucket zero downtime switch info: %w", err)
		}
		if bucketSwitch.MultipartTTL == 0 {
			// should never happen
			return nil, fmt.Errorf("%w: bucket zero downtime switch info is invalid: %+v", dom.ErrInternal, bucketSwitch)
		}
		if bucketSwitch.LastStatus == entity.StatusInProgress {
			return &bucketSwitch, nil
		}
	}
	// no active switch found
	return nil, nil
}

func getReplications(bucketPolicies store.OperationResult[[]entity.BucketReplicationPolicy], userPolicies store.OperationResult[[]entity.UserReplicationPolicy]) ([]entity.UniversalReplicationID, error) {
	bucketRepls, err := bucketPolicies.Get()
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return nil, fmt.Errorf("unable to get bucket replication policies: %w", err)
	}
	result := make([]entity.UniversalReplicationID, 0, len(bucketRepls))
	for _, br := range bucketRepls {
		uid := entity.UniversalFromBucketReplication(br)
		if err := uid.Validate(); err != nil {
			return nil, fmt.Errorf("invalid bucket replication policy from store: %w", err)
		}
		result = append(result, uid)
	}
	if len(result) != 0 {
		// bucket level replications take precedence over user level replications
		return result, nil
	}
	// try user level replications
	userRepls, err := userPolicies.Get()
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return nil, fmt.Errorf("unable to get user replication policies: %w", err)
	}
	for _, ur := range userRepls {
		uid := entity.UniversalFromUserReplication(ur)
		if err := uid.Validate(); err != nil {
			return nil, fmt.Errorf("invalid user replication policy from store: %w", err)
		}
		result = append(result, uid)
	}
	return result, nil
}

func (r *policySvc) BuildProxyNoBucketContext(ctx context.Context, user string) (context.Context, error) {
	tx := r.userRoutingStore.TxExecutor()
	userRoutingResult := r.userRoutingStore.WithExecutor(tx).GetOp(ctx, user)
	userActiveSwitchResult := r.userReplicationSwitchStore.WithExecutor(tx).IsZeroDowntimeActiveOp(ctx, user)
	userReplications := r.userReplicationPolicyStore.WithExecutor(tx).GetOp(ctx, user)
	_ = tx.Exec(ctx) // ignore error - we will catch it on individual results

	bucketRoutingResult := store.NewRedisFailedOperationResult[string](dom.ErrNotFound)                          // no bucket, so no bucket routing
	bucketActiveSwitchResult := store.NewRedisFailedOperationResult[bool](dom.ErrNotFound)                       // no bucket, so no bucket switch
	bucketReplications := store.NewRedisFailedOperationResult[[]entity.BucketReplicationPolicy](dom.ErrNotFound) // no bucket, so no bucket replications

	// get active routing policy
	rotuteTo, err := determineActiveRouting(r.mainStorage, userRoutingResult, bucketRoutingResult)
	if err != nil {
		return ctx, err
	}
	ctx = xctx.SetRoutingPolicy(ctx, rotuteTo)

	// check in_progress zero downtime switch if present
	activeSwitch, err := r.getActiveZeroDowntimeSwitch(ctx, userActiveSwitchResult, bucketActiveSwitchResult, entity.BucketReplicationPolicyID{User: user})
	if err != nil {
		return ctx, err
	}
	if activeSwitch != nil {
		ctx = xctx.SetInProgressZeroDowntime(ctx, *activeSwitch)
	}

	// set replication policies
	relications, err := getReplications(bucketReplications, userReplications)
	if err != nil {
		return ctx, err
	}
	if len(relications) != 0 {
		ctx = xctx.SetReplications(ctx, relications)
	}
	return ctx, nil
}

func (r *policySvc) BuildAgentContext(ctx context.Context, user, bucket string) (context.Context, error) {
	// in a single batch:
	tx := r.userReplicationPolicyStore.TxExecutor()
	userReplications := r.userReplicationPolicyStore.WithExecutor(tx).GetOp(ctx, user)
	bucketReplications := r.bucketReplicationPolicyStore.WithExecutor(tx).GetOp(ctx, entity.NewBucketReplicationPolicyID(user, bucket))
	_ = tx.Exec(ctx) // ignore error - we will catch it on individual results

	// set replication policies
	relications, err := getReplications(bucketReplications, userReplications)
	if err != nil {
		return ctx, err
	}
	if len(relications) != 0 {
		ctx = xctx.SetReplications(ctx, relications)
	}

	return ctx, nil
}
