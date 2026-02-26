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

type ReplicationSwitchSvc interface {
	// Upsert downtime replication switch. If switch already exists and not in progress, it will be updated.
	SetDowntimeReplicationSwitch(ctx context.Context, replID entity.UniversalReplicationID, opts *entity.ReplicationSwitchDowntimeOpts) error
	// Change downtime replication switch status. Makes required adjustments to routing and replication policies.
	// According to switch status and configured options.
	UpdateDowntimeSwitchStatus(ctx context.Context, replID entity.UniversalReplicationID, newStatus entity.ReplicationSwitchStatus, description string) error
	// Creates new zero downtime replication switch.
	AddZeroDowntimeReplicationSwitch(ctx context.Context, replID entity.UniversalReplicationID, opts *entity.ReplicationSwitchZeroDowntimeOpts) error
	// Completes zero downtime replication switch.
	CompleteZeroDowntimeReplicationSwitch(ctx context.Context, replID entity.UniversalReplicationID) error
	// Deletes any replication switch if exists and reverts routing policy if switch was not done.
	DeleteReplicationSwitch(ctx context.Context, replID entity.UniversalReplicationID) error
	// Returns replication switch config and status information.
	GetReplicationSwitchInfo(ctx context.Context, replID entity.UniversalReplicationID) (entity.ReplicationSwitchInfo, error)
}

var _ ReplicationSwitchSvc = (*policySvc)(nil)

func (r *policySvc) SetDowntimeReplicationSwitch(ctx context.Context, policy entity.UniversalReplicationID, opts *entity.ReplicationSwitchDowntimeOpts) error {
	if err := policy.Validate(); err != nil {
		return fmt.Errorf("unable to validate replication id: %w", err)
	}
	// check if switch already exists
	existing, err := r.GetReplicationSwitchInfo(ctx, policy)
	switch {
	case err == nil:
		// already exists:
		return r.updateDowntimeSwitchOpts(ctx, existing, policy, opts)
	case errors.Is(err, dom.ErrNotFound):
		// not exists:
		break
	default:
		return fmt.Errorf("unable to get replication switch info: %w", err)
	}

	// validate corresponding replication state:
	if err := r.validateDowntimeSwitchCreation(ctx, policy, opts); err != nil {
		return err
	}
	// create switch
	info := entity.ReplicationSwitchInfo{
		LastStatus: entity.StatusNotStarted,
	}
	if opts != nil {
		info.ReplicationSwitchDowntimeOpts = *opts
	}

	tx := r.bucketReplicationSwitchStore.TxExecutor()
	r.createSwitchInTx(ctx, tx, policy, info)
	err = tx.Exec(ctx)
	if err != nil {
		return fmt.Errorf("unable to create replication switch: %w", err)
	}
	return nil
}

func (r *policySvc) validateDowntimeSwitchCreation(ctx context.Context, requestedPolicy entity.UniversalReplicationID, opts *entity.ReplicationSwitchDowntimeOpts) error {
	if err := requestedPolicy.Validate(); err != nil {
		return fmt.Errorf("unable to validate replication id: %w", err)
	}
	policyInfo, err := r.GetReplicationPolicyInfoExtended(ctx, requestedPolicy)
	if err != nil {
		return fmt.Errorf("unable to get replication policy: %w", err)
	}
	if policyInfo.AgentURL != "" {
		return fmt.Errorf("%w: cannot create downtime switch: given replication is agent based", dom.ErrInvalidArg)
	}
	forceStartNow := opts == nil || (opts.StartAt == nil && opts.Cron == nil && !opts.StartOnInitDone)
	if forceStartNow && !policyInfo.InitDone() {
		return fmt.Errorf("%w: cannot create downtime switch: init replication is not done", dom.ErrInvalidArg)
	}
	if err := r.switchAndStoredReplicationPolicyMatch(ctx, requestedPolicy); err != nil {
		return err
	}
	return nil
}

func (r *policySvc) switchAndStoredReplicationPolicyMatch(ctx context.Context, request entity.UniversalReplicationID) error {
	// switch policy must match stored replication policy
	// replication policy must exist and have a single destination
	// bucket replication must not have custom destination bucket name
	var storedPolicy entity.UniversalReplicationID
	if bucketPolicy, ok := request.AsBucketID(); ok {
		policies, err := r.bucketReplicationPolicyStore.Get(ctx, bucketPolicy.LookupID())
		if err != nil {
			return fmt.Errorf("unable to get replication policies: %w", err)
		}
		if len(policies) != 1 {
			return fmt.Errorf("%w: cannot create switch: existing bucket replication should have a single destination", dom.ErrInvalidArg)
		}
		if policies[0].ToBucket != policies[0].FromBucket {
			return fmt.Errorf("%w: cannot create replication switch: bucket replication has custom destination bucket name", dom.ErrInvalidArg)
		}
		storedPolicy = entity.UniversalFromBucketReplication(policies[0])
	} else if userPolicy, ok := request.AsUserID(); ok {
		policies, err := r.userReplicationPolicyStore.Get(ctx, userPolicy.LookupID())
		if err != nil {
			return fmt.Errorf("unable to get replication policies: %w", err)
		}
		if len(policies) != 1 {
			return fmt.Errorf("%w: cannot create switch: existing user replication should have a single destination", dom.ErrInvalidArg)
		}
		storedPolicy = entity.UniversalFromUserReplication(policies[0])
	} else {
		return fmt.Errorf("%w: unknown replication id type: %#v", dom.ErrInvalidArg, request)
	}
	if request.AsString() != storedPolicy.AsString() {
		return fmt.Errorf("%w: cannot create replication switch: request policy not matching stored policy: req %s, stored %s", dom.ErrInvalidArg, request.AsString(), storedPolicy.AsString())
	}
	return nil
}

// updateDowntimeSwitchOpts goes through the opts struct and updates the corresponding fields in redis hash
// if a field is nil, it will be deleted from hash
// if a field is not nil, it will be set in hash
func (r *policySvc) updateDowntimeSwitchOpts(ctx context.Context, existing entity.ReplicationSwitchInfo, replID entity.UniversalReplicationID, opts *entity.ReplicationSwitchDowntimeOpts) error {
	if existing.IsZeroDowntime() {
		return fmt.Errorf("%w: cannot update donwntime switch: there is existing zero downtime switch for given replication", dom.ErrAlreadyExists)
	}
	if existing.LastStatus == entity.StatusInProgress || existing.LastStatus == entity.StatusCheckInProgress {
		return fmt.Errorf("%w: cannot update downtime switch: switch is already in progress", dom.ErrInvalidArg)
	}
	if existing.LastStatus == entity.StatusDone {
		return fmt.Errorf("%w: cannot update downtime switch: switch is already completed", dom.ErrInvalidArg)
	}
	if bucketPolicy, ok := replID.AsBucketID(); ok {
		return r.bucketReplicationSwitchStore.UpdateDowntimeOpts(ctx, bucketPolicy, opts)
	}
	if userPolicy, ok := replID.AsUserID(); ok {
		return r.userReplicationSwitchStore.UpdateDowntimeOpts(ctx, userPolicy, opts)
	}
	return fmt.Errorf("%w: unknown replication id type: %#v", dom.ErrInvalidArg, replID)
}

func (r *policySvc) AddZeroDowntimeReplicationSwitch(ctx context.Context, policy entity.UniversalReplicationID, opts *entity.ReplicationSwitchZeroDowntimeOpts) error {
	if opts == nil || opts.MultipartTTL <= 0 {
		return fmt.Errorf("%w: multipartTTL must be set to zero downtime switch options", dom.ErrInvalidArg)
	}
	if err := policy.Validate(); err != nil {
		return fmt.Errorf("unable to validate replication id: %w", err)
	}
	_, err := r.GetReplicationSwitchInfo(ctx, policy)
	switch {
	case err == nil:
		// already exists:
		return dom.ErrAlreadyExists
	case errors.Is(err, dom.ErrNotFound):
		// not exists:
		break
	default:
		return fmt.Errorf("unable to get replication switch info: %w", err)
	}
	// validate corresponding replication state:
	if err := r.validateZeroDowntimeSwitchCreation(ctx, policy, opts); err != nil {
		return err
	}

	// start transaction:
	tx := r.bucketReplicationSwitchStore.TxExecutor()

	// 1. create switch info
	r.createSwitchInTx(ctx, tx, policy, entity.ReplicationSwitchInfo{
		ReplicationSwitchZeroDowntimeOpts: *opts,
		LastStatus:                        entity.StatusInProgress,
	})
	// 2. change routing to new storage
	r.routeToNewInTx(ctx, tx, policy)
	// 3. delete replication policy
	r.removeReplicationPolicyInTx(ctx, tx, policy)
	// 4. archive replication status
	// - keep replication status HSET for history, but mark it as archieved
	// - archived replication may be used to resolve CompleteMultipartUpload, when parts were uploaded to old storage
	//    before switch started. Now if proxy will receive CompleteMultipartUpload request, it will be able to
	//   create replication task to sync this object to new storage after switch was completed.
	r.archieveReplicationStatusInTx(ctx, tx, policy)

	// commit
	err = tx.Exec(ctx)
	if err != nil {
		return fmt.Errorf("unable to create zero-downtime replication switch: %w", err)
	}
	return nil
}

func (r *policySvc) validateZeroDowntimeSwitchCreation(ctx context.Context, requestedPolicy entity.UniversalReplicationID, opts *entity.ReplicationSwitchZeroDowntimeOpts) error {
	if err := requestedPolicy.Validate(); err != nil {
		return fmt.Errorf("unable to validate replication id: %w", err)
	}
	policyInfo, err := r.GetReplicationPolicyInfoExtended(ctx, requestedPolicy)
	if err != nil {
		return fmt.Errorf("unable to get replication policy: %w", err)
	}
	if policyInfo.AgentURL != "" {
		return fmt.Errorf("%w: cannot create zero downtime switch: given replication is agent based", dom.ErrInvalidArg)
	}
	if !policyInfo.InitDone() {
		return fmt.Errorf("%w: cannot create zero-downtime switch: init replication is not done", dom.ErrInvalidArg)
	}
	if policyInfo.IsPaused {
		return fmt.Errorf("%w: cannot create zero-downtime switch: replication is paused", dom.ErrInvalidArg)
	}
	if err := r.switchAndStoredReplicationPolicyMatch(ctx, requestedPolicy); err != nil {
		return err
	}
	return nil
}

func (r *policySvc) DeleteReplicationSwitch(ctx context.Context, replID entity.UniversalReplicationID) error {
	existing, err := r.GetReplicationSwitchInfo(ctx, replID)
	if err != nil {
		return err
	}
	// start transaction:
	tx := r.bucketReplicationSwitchStore.TxExecutor()
	if !existing.IsZeroDowntime() {
		r.unblockRouteInTx(ctx, tx, replID)
	}
	if existing.IsZeroDowntime() && existing.LastStatus != entity.StatusDone {
		//revert routing idempotently
		r.routeToOldInTx(ctx, tx, replID)
	}
	//delete switch metadata
	r.removeSwitchInTx(ctx, tx, replID)
	// commit
	err = tx.Exec(ctx)
	if err != nil {
		return fmt.Errorf("unable to delete replication switch: %w", err)
	}

	return nil
}

func (r *policySvc) GetReplicationSwitchInfo(ctx context.Context, replID entity.UniversalReplicationID) (info entity.ReplicationSwitchInfo, err error) {
	if err := replID.Validate(); err != nil {
		return entity.ReplicationSwitchInfo{}, fmt.Errorf("unable to validate replication id: %w", err)
	}

	if bucketPolicy, ok := replID.AsBucketID(); ok {
		info, err = r.bucketReplicationSwitchStore.Get(ctx, bucketPolicy.LookupID())
	} else if userPolicy, ok := replID.AsUserID(); ok {
		info, err = r.userReplicationSwitchStore.Get(ctx, userPolicy.LookupID())
	} else {
		// should never happen
		return entity.ReplicationSwitchInfo{}, fmt.Errorf("%w: unknown replication id type: %#v", dom.ErrInvalidArg, replID)
	}
	if err != nil {
		return entity.ReplicationSwitchInfo{}, fmt.Errorf("unable to get replication switch info from store: %w", err)
	}
	gotReplicationID := info.ReplicationID()
	if replID.AsString() != gotReplicationID.AsString() {
		return entity.ReplicationSwitchInfo{}, fmt.Errorf("%w: replication ID mismatch: requested %s, got %s", dom.ErrInternal, replID.AsString(), gotReplicationID.AsString())
	}

	return info, nil
}

// UpdateDowntimeSwitchStatus handles downtime switch status transitions
// and performs corresponding idempotent operations with replication and routing policies
func (r *policySvc) UpdateDowntimeSwitchStatus(ctx context.Context, replID entity.UniversalReplicationID, newStatus entity.ReplicationSwitchStatus, description string) error {
	// validate input
	if newStatus == "" || newStatus == entity.StatusNotStarted {
		return fmt.Errorf("status cannot be %s: %w", newStatus, dom.ErrInvalidArg)
	}
	// validate status transition
	existing, err := r.GetReplicationSwitchInfo(ctx, replID)
	if err != nil {
		return err
	}
	switch existing.LastStatus {
	case "", entity.StatusNotStarted, entity.StatusError, entity.StatusSkipped:
		// from starting status, only allowed transitions are to in progress, error or skipped
		if newStatus != entity.StatusInProgress && newStatus != entity.StatusError && newStatus != entity.StatusSkipped {
			return fmt.Errorf("invalid status transition %s->%s: %w", existing.LastStatus, newStatus, dom.ErrInvalidArg)
		}
	case entity.StatusInProgress:
		if newStatus != entity.StatusCheckInProgress && newStatus != entity.StatusError {
			return fmt.Errorf("invalid status transition %s->%s: %w", existing.LastStatus, newStatus, dom.ErrInvalidArg)
		}
	case entity.StatusCheckInProgress:
		if newStatus != entity.StatusDone && newStatus != entity.StatusError {
			return fmt.Errorf("invalid status transition %s->%s: %w", existing.LastStatus, newStatus, dom.ErrInvalidArg)
		}
	case entity.StatusDone:
		if newStatus != entity.StatusDone {
			return fmt.Errorf("invalid status transition %s->%s: %w", existing.LastStatus, newStatus, dom.ErrInvalidArg)
		}
	}

	// update downtime switch status along with routing and replication policies in one transaction:
	tx := r.bucketReplicationSwitchStore.TxExecutor()
	switch newStatus {
	// if switch in progress set routing block idempotently
	case entity.StatusInProgress, entity.StatusCheckInProgress:
		r.blockRouteInTx(ctx, tx, replID)
	// if error, delete routing block idempotently
	case entity.StatusError:
		r.unblockRouteInTx(ctx, tx, replID)
	// if done, switch routing and replication to new bucket idempotently
	case entity.StatusDone:
		r.unblockRouteInTx(ctx, tx, replID)
		r.routeToNewInTx(ctx, tx, replID)
		r.archieveReplicationStatusInTx(ctx, tx, replID)
		r.removeReplicationPolicyInTx(ctx, tx, replID)

		if existing.ContinueReplication {
			backwards := replID.Swap()
			r.createReplicationPolicyInTx(ctx, tx, backwards, entity.ReplicationStatus{})
		}
	}
	r.updateSwitchStatusInTx(ctx, tx, replID, existing.LastStatus, newStatus, description)

	//commit
	if err := tx.Exec(ctx); err != nil {
		return fmt.Errorf("unable to update downtime switch status: %w", err)
	}

	return nil
}

func (r *policySvc) CompleteZeroDowntimeReplicationSwitch(ctx context.Context, replID entity.UniversalReplicationID) error {
	info, err := r.GetReplicationSwitchInfo(ctx, replID)
	if err != nil {
		return err
	}
	if !info.IsZeroDowntime() {
		return fmt.Errorf("%w: cannot complete zero downtime switch: switch is not zero downtime", dom.ErrInvalidArg)
	}
	if info.LastStatus != entity.StatusInProgress {
		return fmt.Errorf("%w: cannot complete zero downtime switch: switch is not in progress", dom.ErrInvalidArg)
	}
	tx := r.bucketReplicationSwitchStore.TxExecutor()
	r.updateSwitchStatusInTx(ctx, tx, replID, info.LastStatus, entity.StatusDone, "complete zero downtime switch")
	if err := tx.Exec(ctx); err != nil {
		return fmt.Errorf("unable to update zero downtime switch status: %w", err)
	}
	return nil
}

func (r *policySvc) createSwitchInTx(ctx context.Context, tx store.Executor[redis.Pipeliner], policy entity.UniversalReplicationID, info entity.ReplicationSwitchInfo) {
	if bucketPolicy, ok := policy.AsBucketID(); ok {
		_ = r.bucketReplicationSwitchStore.WithExecutor(tx).CreateOp(ctx, bucketPolicy, info)
	} else if userPolicy, ok := policy.AsUserID(); ok {
		_ = r.userReplicationSwitchStore.WithExecutor(tx).CreateOp(ctx, userPolicy, info)
	} else {
		// should never happen
		panic(fmt.Sprintf("unknown policy type %s", policy.AsString()))
	}
}

func (r *policySvc) getSwitchInTx(ctx context.Context, tx store.Executor[redis.Pipeliner], policy entity.UniversalReplicationID) store.OperationResult[entity.ReplicationSwitchInfo] {
	if bucketPolicy, ok := policy.AsBucketID(); ok {
		return r.bucketReplicationSwitchStore.WithExecutor(tx).GetOp(ctx, bucketPolicy.LookupID())
	} else if userPolicy, ok := policy.AsUserID(); ok {
		return r.userReplicationSwitchStore.WithExecutor(tx).GetOp(ctx, userPolicy.LookupID())
	} else {
		// should never happen
		panic(fmt.Sprintf("unknown policy type %s", policy.AsString()))
	}
}

func (r *policySvc) removeSwitchInTx(ctx context.Context, tx store.Executor[redis.Pipeliner], policy entity.UniversalReplicationID) {
	if bucketPolicy, ok := policy.AsBucketID(); ok {
		_ = r.bucketReplicationSwitchStore.WithExecutor(tx).DeleteOp(ctx, bucketPolicy.LookupID())
	} else if userPolicy, ok := policy.AsUserID(); ok {
		_ = r.userReplicationSwitchStore.WithExecutor(tx).DeleteOp(ctx, userPolicy.LookupID())
	} else {
		// should never happen
		panic(fmt.Sprintf("unknown policy type %s", policy.AsString()))
	}
}

func (r *policySvc) updateSwitchStatusInTx(ctx context.Context, tx store.Executor[redis.Pipeliner], policy entity.UniversalReplicationID, prevStatus, newStatus entity.ReplicationSwitchStatus, description string) {
	if bucketPolicy, ok := policy.AsBucketID(); ok {
		_ = r.bucketReplicationSwitchStore.WithExecutor(tx).UpdateStatusOp(ctx, bucketPolicy, prevStatus, newStatus, description)
	} else if userPolicy, ok := policy.AsUserID(); ok {
		_ = r.userReplicationSwitchStore.WithExecutor(tx).UpdateStatusOp(ctx, userPolicy, prevStatus, newStatus, description)
	} else {
		// should never happen
		panic(fmt.Sprintf("unknown policy type %s", policy.AsString()))
	}
}
