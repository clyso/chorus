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
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
)

type ReplicationSvc interface {
	// common methods for user and bucket replication policies
	PauseReplication(ctx context.Context, id entity.UniversalReplicationID) error
	ResumeReplication(ctx context.Context, id entity.UniversalReplicationID) error
	GetReplicationPolicyInfoExtended(ctx context.Context, id entity.UniversalReplicationID) (entity.ReplicationStatusExtended, error)

	// user replication policies
	AddUserReplicationPolicy(ctx context.Context, policy entity.UserReplicationPolicy, opts entity.ReplicationOptions) error
	DeleteUserReplication(ctx context.Context, policy entity.UserReplicationPolicy) error
	ListUserReplicationsInfo(ctx context.Context) (map[entity.UserReplicationPolicy]entity.ReplicationStatusExtended, error)
	IsUserReplicationExists(ctx context.Context, user string) (bool, error)

	// bucket replication policies
	AddBucketReplicationPolicy(ctx context.Context, id entity.BucketReplicationPolicy, opts entity.ReplicationOptions) error
	ListBucketReplicationsInfo(ctx context.Context, user string) (map[entity.BucketReplicationPolicy]entity.ReplicationStatusExtended, error)
	DeleteBucketReplication(ctx context.Context, id entity.BucketReplicationPolicy) error
}

var _ ReplicationSvc = (*policySvc)(nil)

func (r *policySvc) PauseReplication(ctx context.Context, id entity.UniversalReplicationID) error {
	if err := id.Validate(); err != nil {
		return fmt.Errorf("pause replication: unable to validate replication status id: %w", err)
	}
	for _, queue := range tasks.AllReplicationQueues(id) {
		err := r.queueSvc.Pause(ctx, queue)
		if err != nil {
			if errors.Is(err, dom.ErrNotFound) {
				// since queues created on demand, it is possible that some queues are not created yet
				// for example, event replication queue will be created on the first write request to chorus proxy
				continue
			}
			return fmt.Errorf("pause replication: unable to pause queue %s: %w", queue, err)
		}
	}
	return nil
}

func (r *policySvc) ResumeReplication(ctx context.Context, id entity.UniversalReplicationID) error {
	if err := id.Validate(); err != nil {
		return fmt.Errorf("resume replication: unable to validate replication status id: %w", err)
	}
	for _, queue := range tasks.AllReplicationQueues(id) {
		err := r.queueSvc.Resume(ctx, queue)
		if err != nil {
			if errors.Is(err, dom.ErrNotFound) {
				// since queues created on demand, it is possible that some queues are not created yet
				// for example, event replication queue will be created on the first write request to chorus proxy
				continue
			}
			return fmt.Errorf("resume replication: unable to resume queue %s: %w", queue, err)
		}
	}
	return nil
}

func (r *policySvc) GetReplicationPolicyInfoExtended(ctx context.Context, id entity.UniversalReplicationID) (entity.ReplicationStatusExtended, error) {
	if err := id.Validate(); err != nil {
		return entity.ReplicationStatusExtended{}, fmt.Errorf("get replication status: unable to validate replication status id: %w", err)
	}
	tx := r.bucketReplicationStatusStore.TxExecutor()
	statusRes := r.getReplicationStatusInTx(ctx, tx, id)
	switchRes := r.getSwitchInTx(ctx, tx, id)
	_ = tx.Exec(ctx)
	status, err := statusRes.Get()
	if err != nil {
		return entity.ReplicationStatusExtended{}, err
	}
	switchInfo, err := switchRes.Get()
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return entity.ReplicationStatusExtended{}, err
	}
	var switchInfoPtr *entity.ReplicationSwitchInfo
	if switchInfo.ReplicationIDStr != "" {
		switchInfoPtr = &switchInfo
	}

	return r.fillExtendedReplicationStatus(ctx, id, &status, switchInfoPtr)

}

func (r *policySvc) getReplicationStatusInTx(ctx context.Context, tx store.Executor[redis.Pipeliner], policy entity.UniversalReplicationID) store.OperationResult[entity.ReplicationStatus] {
	if bucketPolicy, ok := policy.AsBucketID(); ok {
		return r.bucketReplicationStatusStore.WithExecutor(tx).GetOp(ctx, bucketPolicy)
	} else if userPolicy, ok := policy.AsUserID(); ok {
		return r.userReplicationStatusStore.WithExecutor(tx).GetOp(ctx, userPolicy)
	} else {
		// should never happen
		panic(fmt.Sprintf("unknown policy type %s", policy.AsString()))
	}
}

func (r *policySvc) fillExtendedReplicationStatus(ctx context.Context, id entity.UniversalReplicationID, status *entity.ReplicationStatus, switchInfo *entity.ReplicationSwitchInfo) (entity.ReplicationStatusExtended, error) {
	result := entity.ReplicationStatusExtended{
		ReplicationStatus:    status,
		IsPaused:             false,
		Switch:               switchInfo,
		InitMigration:        entity.QueueStats{},
		EventMigration:       entity.QueueStats{},
		InitMigrationListing: entity.QueueStats{},
	}
	// replication is paused if at least one of the queues is paused
	paused := false
	// get initial migration listing queues stats
	var err error
	paused, result.InitMigrationListing, err = r.buildQueueStats(ctx, tasks.InitMigrationListQueue(id))
	if err != nil {
		return entity.ReplicationStatusExtended{}, fmt.Errorf("unable to get init migration queue stats: %w", err)
	}
	result.IsPaused = result.IsPaused || paused

	// get initial migration queues stats
	paused, result.InitMigration, err = r.buildQueueStats(ctx, tasks.InitMigrationCopyQueue(id))
	if err != nil {
		return entity.ReplicationStatusExtended{}, fmt.Errorf("unable to get init migration queue stats: %w", err)
	}
	result.IsPaused = result.IsPaused || paused

	// get event migration queues stats
	paused, result.EventMigration, err = r.buildQueueStats(ctx, tasks.EventMigrationQueue(id))
	if err != nil {
		return entity.ReplicationStatusExtended{}, fmt.Errorf("unable to get event migration queue stats: %w", err)
	}
	result.IsPaused = result.IsPaused || paused

	return result, nil
}

func (r *policySvc) buildQueueStats(ctx context.Context, queue string) (bool, entity.QueueStats, error) {
	stats, err := r.queueSvc.Stats(ctx, queue)
	if errors.Is(err, dom.ErrNotFound) {
		// queue does not exist, so no stats available
		return false, entity.QueueStats{}, nil
	}
	if err != nil {
		return false, entity.QueueStats{}, fmt.Errorf("unable to get queue stats for %s: %w", queue, err)
	}
	return stats.Paused, entity.QueueStats{
		Unprocessed: stats.Unprocessed,
		Done:        stats.ProcessedTotal,
		Latency:     stats.Latency,
		MemoryUsage: stats.MemoryUsage,
	}, nil
}

func (r *policySvc) AddUserReplicationPolicy(ctx context.Context, policy entity.UserReplicationPolicy, opts entity.ReplicationOptions) error {
	if err := policy.Validate(); err != nil {
		return fmt.Errorf("unable to validate user replication policy: %w", err)
	}
	// check if already exists
	_, err := r.userReplicationStatusStore.GetOp(ctx, policy).Get()
	switch {
	case err == nil:
		return fmt.Errorf("%w: replication already exists, delete it first", dom.ErrAlreadyExists)
	case errors.Is(err, dom.ErrNotFound):
		// not found, good
		break
	default:
		return fmt.Errorf("unable to check existing replication status: %w", err)
	}
	// check if routing match
	uid := entity.UniversalFromUserReplication(policy)
	routeTo, err := r.getRoutingForReplication(ctx, uid)
	if err != nil {
		return fmt.Errorf("unable to get routing for user replication: %w", err)
	}
	if routeTo != policy.FromStorage {
		return fmt.Errorf("%w: unable to create user %s replication from %s because it is different from routing %s", dom.ErrInvalidArg, policy.User, policy.FromStorage, routeTo)
	}
	// check if there are existing bucket replications for this user
	bucketPolicyExists, err := r.bucketReplicationPolicyStore.ExistsForUser(ctx, policy.User)
	if err != nil {
		return fmt.Errorf("unable to check existing bucket replications: %w", err)
	}
	if bucketPolicyExists {
		return fmt.Errorf("%w: unable to create user replication when there are existing bucket replications", dom.ErrInvalidArg)
	}

	// check if replication switch already exists for this user
	userSwitch, err := r.userReplicationSwitchStore.Get(ctx, policy.User)
	switch {
	case err == nil:
		return fmt.Errorf("%w: cannot have multiple user replications when user replication switch exists: %s", dom.ErrInvalidArg, userSwitch.ReplicationIDStr)
	case errors.Is(err, dom.ErrNotFound):
		// not found, good
		break
	default:
		return fmt.Errorf("unable to check existing user replication switch: %w", err)
	}
	// all good, create policy
	tx := r.userReplicationPolicyStore.TxExecutor()
	// 1. set explicit user-level routing
	_ = r.userRoutingStore.WithExecutor(tx).SetOp(ctx, policy.User, policy.FromStorage)
	// 2. add replication policy
	_ = r.userReplicationPolicyStore.WithExecutor(tx).AddOp(ctx, policy)
	// 3. add replication status
	_ = r.userReplicationStatusStore.WithExecutor(tx).AddOp(ctx, policy, entity.StatusFromOptions(opts))

	if err := tx.Exec(ctx); err != nil {
		return fmt.Errorf("unable to execute transaction: %w", err)
	}
	return nil
}

func (r *policySvc) DeleteUserReplication(ctx context.Context, policy entity.UserReplicationPolicy) error {
	if err := policy.Validate(); err != nil {
		return fmt.Errorf("unable to validate user replication policy: %w", err)
	}
	// cannot delete if switch exists
	_, err := r.userReplicationSwitchStore.Get(ctx, policy.User)
	switch {
	case err == nil:
		return fmt.Errorf("%w: cannot delete user replication with replication switch. Delete the switch first", dom.ErrInvalidArg)
	case errors.Is(err, dom.ErrNotFound):
		// not found, good
		break
	default:
		return fmt.Errorf("unable to check existing user replication switch: %w", err)
	}
	// check if policy exists
	_, err = r.userReplicationStatusStore.GetOp(ctx, policy).Get()
	if err != nil {
		return fmt.Errorf("unable to get existing replication status: %w", err)
	}

	// delete
	tx := r.userReplicationPolicyStore.TxExecutor()
	// 1. delete replication policy
	_ = r.userReplicationPolicyStore.WithExecutor(tx).RemoveOp(ctx, policy)
	// 2. delete replication status
	_ = r.userReplicationStatusStore.WithExecutor(tx).DeleteOp(ctx, policy)
	// we do NOT delete user-level routing.
	// user may be deleting replication after successful switch.
	// or in case if routing was set manually.

	if err := tx.Exec(ctx); err != nil {
		return fmt.Errorf("unable to execute transaction: %w", err)
	}
	// delete queues
	queues := tasks.AllReplicationQueues(entity.UniversalFromUserReplication(policy))
	for _, queue := range queues {
		err := r.queueSvc.Delete(ctx, queue, true)
		if err != nil && !errors.Is(err, dom.ErrNotFound) {
			zerolog.Ctx(ctx).Err(err).Msgf("unable to delete queue %s", queue)
			continue
		}
	}
	return nil
}

func (r *policySvc) AddBucketReplicationPolicy(ctx context.Context, policy entity.BucketReplicationPolicy, opts entity.ReplicationOptions) error {
	if err := policy.Validate(); err != nil {
		return fmt.Errorf("unable to validate bucket replication policy: %w", err)
	}
	// check if already exists
	_, err := r.bucketReplicationStatusStore.GetOp(ctx, policy).Get()
	switch {
	case err == nil:
		return fmt.Errorf("%w: replication already exists, delete it first", dom.ErrAlreadyExists)
	case errors.Is(err, dom.ErrNotFound):
		// not found, good
		break
	default:
		return fmt.Errorf("unable to check existing replication status: %w", err)
	}
	// check if routing match
	uid := entity.UniversalFromBucketReplication(policy)
	routeTo, err := r.getRoutingForReplication(ctx, uid)
	if err != nil {
		return fmt.Errorf("unable to get routing for bucket replication: %w", err)
	}
	if routeTo != policy.FromStorage {
		return fmt.Errorf("%w: unable to create bucket %s replication from %s because it is different from routing %s", dom.ErrInvalidArg, policy.User, policy.FromStorage, routeTo)
	}
	// check if there are existing user replications for this user
	userReplications, err := r.userReplicationPolicyStore.Get(ctx, policy.User)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("unable to check existing user replications: %w", err)
	}
	if len(userReplications) != 0 {
		return fmt.Errorf("%w: unable to create bucket replication when there are existing user replications", dom.ErrInvalidArg)
	}

	// check if replication switch already exists
	userSwitch, err := r.bucketReplicationSwitchStore.Get(ctx, policy.LookupID())
	switch {
	case err == nil:
		return fmt.Errorf("%w: cannot have multiple bucket replications when bucket replication switch exists: %s", dom.ErrInvalidArg, userSwitch.ReplicationIDStr)
	case errors.Is(err, dom.ErrNotFound):
		// not found, good
		break
	default:
		return fmt.Errorf("unable to check existing bucket replication switch: %w", err)
	}
	// check if bucket already used as destination to avoid many-to-one writes from other replications
	// we check it only for bucket-level replications because because of custom destination bucket name
	inUse, err := r.bucketReplicationPolicyStore.IsDestinationInUse(ctx, policy)
	if err != nil {
		return fmt.Errorf("unable to check existing replications for destination bucket: %w", err)
	}
	if inUse {
		return fmt.Errorf("%w: cannot use bucket %s as destination because it is already used in another replication", dom.ErrInvalidArg, policy.ToBucket)
	}

	// all good, create policy
	tx := r.bucketReplicationPolicyStore.TxExecutor()
	// 1. set explicit bucket-level routing
	_ = r.bucketRoutingStore.WithExecutor(tx).SetOp(ctx, policy.RoutingID(), policy.FromStorage)
	// 2. add replication policy
	_ = r.bucketReplicationPolicyStore.WithExecutor(tx).AddOp(ctx, policy)
	// 3. add replication status
	_ = r.bucketReplicationStatusStore.WithExecutor(tx).AddOp(ctx, policy, entity.StatusFromOptions(opts))
	// 4. block routing for custom destination bucket to avoid many-to-one writes from proxy
	if policy.ToBucket != policy.FromBucket {
		_ = r.bucketRoutingStore.WithExecutor(tx).BlockOp(ctx, entity.NewBucketRoutingPolicyID(policy.User, policy.ToBucket))
	}

	if err := tx.Exec(ctx); err != nil {
		return fmt.Errorf("unable to execute transaction: %w", err)
	}
	return nil
}

func (r *policySvc) DeleteBucketReplication(ctx context.Context, policy entity.BucketReplicationPolicy) error {
	if err := policy.Validate(); err != nil {
		return fmt.Errorf("unable to validate bucket replication policy: %w", err)
	}
	// cannot delete if switch exists
	_, err := r.bucketReplicationSwitchStore.Get(ctx, policy.LookupID())
	switch {
	case err == nil:
		return fmt.Errorf("%w: cannot delete bucket replication with replication switch. Delete the switch first", dom.ErrInvalidArg)
	case errors.Is(err, dom.ErrNotFound):
		// not found, good
		break
	default:
		return fmt.Errorf("unable to check existing bucket replication switch: %w", err)
	}
	// check if policy exists
	_, err = r.bucketReplicationStatusStore.GetOp(ctx, policy).Get()
	if err != nil {
		return fmt.Errorf("unable to get existing replication status: %w", err)
	}

	// delete
	tx := r.bucketReplicationPolicyStore.TxExecutor()
	// 1. delete replication policy
	_ = r.bucketReplicationPolicyStore.WithExecutor(tx).RemoveOp(ctx, policy)
	// 2. delete replication status
	_ = r.bucketReplicationStatusStore.WithExecutor(tx).DeleteOp(ctx, policy)
	// 3. unbock routing for custom destination bucket
	if policy.ToBucket != policy.FromBucket {
		_ = r.bucketRoutingStore.WithExecutor(tx).UnblockOp(ctx, entity.NewBucketRoutingPolicyID(policy.User, policy.ToBucket))
	}

	if err := tx.Exec(ctx); err != nil {
		return fmt.Errorf("unable to execute transaction: %w", err)
	}
	// delete queues
	queues := tasks.AllReplicationQueues(entity.UniversalFromBucketReplication(policy))
	for _, queue := range queues {
		err := r.queueSvc.Delete(ctx, queue, true)
		if err != nil && !errors.Is(err, dom.ErrNotFound) {
			zerolog.Ctx(ctx).Err(err).Msgf("unable to delete queue %s", queue)
			continue
		}
	}
	return nil
}

func (r *policySvc) ListBucketReplicationsInfo(ctx context.Context, user string) (map[entity.BucketReplicationPolicy]entity.ReplicationStatusExtended, error) {
	// get all statuses
	statuses, err := r.bucketReplicationStatusStore.List(ctx, user)
	if err != nil {
		return nil, err
	}
	// fetch switches in batch
	batch := r.bucketReplicationSwitchStore.TxExecutor()
	inBatch := r.bucketReplicationSwitchStore.WithExecutor(batch)
	switches := make(map[entity.BucketReplicationPolicy]store.OperationResult[entity.ReplicationSwitchInfo], len(statuses))
	for policy := range statuses {
		switches[policy] = inBatch.GetOp(ctx, policy.LookupID())
	}
	_ = batch.Exec(ctx)

	// fill results
	result := make(map[entity.BucketReplicationPolicy]entity.ReplicationStatusExtended, len(statuses))
	for policy, status := range statuses {
		switchInfo, err := switches[policy].Get()
		if err != nil && !errors.Is(err, dom.ErrNotFound) {
			return nil, fmt.Errorf("unable to get existing bucket replication switch: %w", err)
		}
		var switchInfoPtr *entity.ReplicationSwitchInfo
		if err == nil {
			switchInfoPtr = &switchInfo
		}
		extendedStatus, err := r.fillExtendedReplicationStatus(ctx, entity.UniversalFromBucketReplication(policy), &status, switchInfoPtr)
		if err != nil {
			return nil, fmt.Errorf("unable to fill extended status for policy %#v: %w", policy, err)
		}
		result[policy] = extendedStatus
	}
	return result, nil
}

func (r *policySvc) ListUserReplicationsInfo(ctx context.Context) (map[entity.UserReplicationPolicy]entity.ReplicationStatusExtended, error) {
	// get all statuses
	statuses, err := r.userReplicationStatusStore.List(ctx)
	if err != nil {
		return nil, err
	}
	// fetch switches in batch
	batch := r.userReplicationSwitchStore.TxExecutor()
	inBatch := r.userReplicationSwitchStore.WithExecutor(batch)
	switches := make(map[entity.UserReplicationPolicy]store.OperationResult[entity.ReplicationSwitchInfo], len(statuses))
	for policy := range statuses {
		switches[policy] = inBatch.GetOp(ctx, policy.User)
	}
	_ = batch.Exec(ctx)

	// fill results
	result := make(map[entity.UserReplicationPolicy]entity.ReplicationStatusExtended, len(statuses))
	for policy, status := range statuses {
		switchInfo, err := switches[policy].Get()
		if err != nil && !errors.Is(err, dom.ErrNotFound) {
			return nil, fmt.Errorf("unable to get existing user replication switch: %w", err)
		}
		var switchInfoPtr *entity.ReplicationSwitchInfo
		if err == nil {
			switchInfoPtr = &switchInfo
		}
		extendedStatus, err := r.fillExtendedReplicationStatus(ctx, entity.UniversalFromUserReplication(policy), &status, switchInfoPtr)
		if err != nil {
			return nil, fmt.Errorf("unable to fill extended status for policy %#v: %w", policy, err)
		}
		result[policy] = extendedStatus
	}
	return result, nil
}

func (r *policySvc) IsUserReplicationExists(ctx context.Context, user string) (bool, error) {
	return r.userReplicationStatusStore.Exists(ctx, user)
}

func (r *policySvc) archieveReplicationStatusInTx(ctx context.Context, tx store.Executor[redis.Pipeliner], policy entity.UniversalReplicationID) {
	if bucketPolicy, ok := policy.AsBucketID(); ok {
		_ = r.bucketReplicationStatusStore.WithExecutor(tx).ArchieveOp(ctx, bucketPolicy)
	} else if userPolicy, ok := policy.AsUserID(); ok {
		_ = r.userReplicationStatusStore.WithExecutor(tx).ArchieveOp(ctx, userPolicy)
	} else {
		// should never happen
		panic(fmt.Sprintf("unknown policy type %s", policy.AsString()))
	}
}

func (r *policySvc) removeReplicationPolicyInTx(ctx context.Context, tx store.Executor[redis.Pipeliner], policy entity.UniversalReplicationID) {
	if bucketPolicy, ok := policy.AsBucketID(); ok {
		_ = r.bucketReplicationPolicyStore.WithExecutor(tx).RemoveOp(ctx, bucketPolicy)
	} else if userPolicy, ok := policy.AsUserID(); ok {
		_ = r.userReplicationPolicyStore.WithExecutor(tx).RemoveOp(ctx, userPolicy)
	} else {
		// should never happen
		panic(fmt.Sprintf("unknown policy type %s", policy.AsString()))
	}
}

func (r *policySvc) createReplicationPolicyInTx(ctx context.Context, tx store.Executor[redis.Pipeliner], policy entity.UniversalReplicationID, status entity.ReplicationStatus) {
	if bucketPolicy, ok := policy.AsBucketID(); ok {
		_ = r.bucketReplicationPolicyStore.WithExecutor(tx).AddOp(ctx, bucketPolicy)
		_ = r.bucketReplicationStatusStore.WithExecutor(tx).AddOp(ctx, bucketPolicy, status)
	} else if userPolicy, ok := policy.AsUserID(); ok {
		_ = r.userReplicationPolicyStore.WithExecutor(tx).AddOp(ctx, userPolicy)
		_ = r.userReplicationStatusStore.WithExecutor(tx).AddOp(ctx, userPolicy, status)
	} else {
		// should never happen
		panic(fmt.Sprintf("unknown policy type %s", policy.AsString()))
	}
}
