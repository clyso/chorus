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
	"time"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/store"
)

func (r *policySvc) SetDowntimeReplicationSwitch(ctx context.Context, replID entity.ReplicationStatusID, opts *entity.ReplicationSwitchDowntimeOpts) error {
	// check if switch already exists
	existing, err := r.GetReplicationSwitchInfo(ctx, replID)
	if err != nil {
		if !errors.Is(err, dom.ErrNotFound) {
			return fmt.Errorf("unable to get replication switch info: %w", err)
		}
		// not exists: fall through
	} else {
		// already exists:
		if existing.IsZeroDowntime() {
			return fmt.Errorf("%w: cannot update donwntime switch: there is existing zero downtime switch for given replication", dom.ErrAlreadyExists)
		}
		if existing.LastStatus == entity.StatusInProgress || existing.LastStatus == entity.StatusCheckInProgress {
			return fmt.Errorf("%w: cannot update downtime switch: switch is already in progress", dom.ErrInvalidArg)
		}
		if existing.LastStatus == entity.StatusDone {
			return fmt.Errorf("%w: cannot update downtime switch: switch is already completed", dom.ErrInvalidArg)
		}
		// all good, update existing switch options:
		return r.updateDowntimeSwitchOpts(ctx, replID, opts)
	}

	// add new downtime switch
	// validate corresponding replication state:
	policy, err := r.GetReplicationPolicyInfoExtended(ctx, replID)
	if err != nil {
		return fmt.Errorf("unable to get replication policy: %w", err)
	}
	if policy.AgentURL != "" {
		return fmt.Errorf("%w: cannot create downtime switch: given replication is agent based", dom.ErrInvalidArg)
	}
	forceStartNow := opts == nil || (opts.StartAt == nil && opts.Cron == nil && !opts.StartOnInitDone)
	if forceStartNow && !policy.InitDone() {
		return fmt.Errorf("%w: cannot create downtime switch: init replication is not done", dom.ErrInvalidArg)
	}

	bucketReplicationPolicyID := entity.NewBucketReplicationPolicyID(replID.User, replID.FromBucket)
	policies, err := r.GetBucketReplicationPolicies(ctx, bucketReplicationPolicyID)
	if err != nil {
		return fmt.Errorf("unable to get replication policies: %w", err)
	}
	if len(policies.Destinations) != 1 {
		return fmt.Errorf("%w: cannot create switch: existing bucket replication should have a single destination", dom.ErrInvalidArg)
	}
	dest := policies.Destinations[0]
	if dest.Storage != replID.ToStorage {
		return fmt.Errorf("%w: cannot create downtime switch: given replication is not routed to destination", dom.ErrInvalidArg)
	}
	replIDStr, err := r.replicationStatusStore.MakeKey(replID)
	if err != nil {
		return fmt.Errorf("unable to make replication status key: %w", err)
	}
	switchID := entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket)
	info := &entity.ReplicationSwitchInfo{
		CreatedAt:        entity.TimeNow(),
		ReplicationIDStr: replIDStr,
		LastStatus:       entity.StatusNotStarted,
	}
	if opts != nil {
		info.ReplicationSwitchDowntimeOpts = *opts
	}
	if err := r.replicationSwitchStore.SetWithDowntimeOpts(ctx, switchID, *info); err != nil {
		return fmt.Errorf("unable to set downtime switch: %w", err)
	}
	return nil
}

// updateDowntimeSwitchOpts goes through the opts struct and updates the corresponding fields in redis hash
// if a field is nil, it will be deleted from hash
// if a field is not nil, it will be set in hash
func (r *policySvc) updateDowntimeSwitchOpts(ctx context.Context, replID entity.ReplicationStatusID, opts *entity.ReplicationSwitchDowntimeOpts) error {
	switchInfoID := entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket)
	if err := r.replicationSwitchStore.UpdateDowntimeOpts(ctx, switchInfoID, opts); err != nil {
		return fmt.Errorf("unable to update downtime switch options: %w", err)
	}
	return nil
}

func (r *policySvc) AddZeroDowntimeReplicationSwitch(ctx context.Context, replID entity.ReplicationStatusID, opts *entity.ReplicationSwitchZeroDowntimeOpts) error {
	_, err := r.GetReplicationSwitchInfo(ctx, replID)
	if err != nil {
		if !errors.Is(err, dom.ErrNotFound) {
			return fmt.Errorf("unable to get replication switch info: %w", err)
		}
		// not exists: fall through
	} else {
		// already exists:
		return dom.ErrAlreadyExists
	}
	// validate corresponding replication state:
	policy, err := r.GetReplicationPolicyInfoExtended(ctx, replID)
	if err != nil {
		return fmt.Errorf("unable to get replication policy: %w", err)
	}
	if policy.AgentURL != "" {
		return fmt.Errorf("%w: cannot create zero-downtime switch: given replication is agent based", dom.ErrInvalidArg)
	}
	if !policy.InitDone() {
		return fmt.Errorf("%w: cannot create zero-downtime switch: init replication is not done", dom.ErrInvalidArg)
	}
	if policy.IsPaused {
		return fmt.Errorf("%w: cannot create zero-downtime switch: replication is paused", dom.ErrInvalidArg)
	}

	bucketReplicationPolicyID := entity.NewBucketReplicationPolicyID(replID.User, replID.FromBucket)
	policies, err := r.GetBucketReplicationPolicies(ctx, bucketReplicationPolicyID)
	if err != nil {
		return fmt.Errorf("unable to get replication policies: %w", err)
	}
	if len(policies.Destinations) != 1 {
		return fmt.Errorf("%w: cannot create switch: existing bucket replication should have a single destination", dom.ErrInvalidArg)
	}
	dest := policies.Destinations[0]
	if dest.Storage != replID.ToStorage {
		return fmt.Errorf("%w: cannot create zero-downtime switch: given replication is not routed to destination", dom.ErrInvalidArg)
	}

	// validate routing policy
	bucketRoutingPolicyID := entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket)
	toStorage, err := r.getRoutingPolicy(ctx, bucketRoutingPolicyID)
	if err != nil {
		return fmt.Errorf("unable to get routing policy: %w", err)
	}
	if toStorage != replID.FromStorage {
		return fmt.Errorf("%w: cannot create zero-downtime switch: given replication is not routed to destination", dom.ErrInvalidArg)
	}

	replicationKey, err := r.replicationStatusStore.MakeKey(replID)
	if err != nil {
		return fmt.Errorf("unable to make replication status key: %w", err)
	}
	now := entity.TimeNow()
	replicationSwitchID := entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket)
	exec := r.replicationSwitchStore.TxExecutor()
	txReplicationSwitchStore := r.replicationSwitchStore.WithExecutor(exec)
	_ = txReplicationSwitchStore.SetOp(ctx, replicationSwitchID, entity.ReplicationSwitchInfo{
		ReplicationIDStr: replicationKey,
		CreatedAt:        now,
		LastStatus:       entity.StatusInProgress,
	})
	_ = txReplicationSwitchStore.SetStartedAtOp(ctx, replicationSwitchID, now)
	_ = txReplicationSwitchStore.SetMultipartTTLOp(ctx, replicationSwitchID, opts.MultipartTTL)
	_ = r.bucketRoutingPolicyStore.WithExecutor(exec).SetOp(ctx, bucketRoutingPolicyID, replID.ToStorage)

	// archive replication:
	// - remove replication destination from sorted set: proxy will stop replicating new events to old destination
	bucketReplicationPolicy := entity.NewBucketReplicationPolicy(replID.FromStorage, replID.ToStorage, replID.ToBucket)
	_ = r.bucketReplicationPolicyStore.WithExecutor(exec).RemoveOp(ctx, bucketReplicationPolicyID, bucketReplicationPolicy)
	txReplicationStatusStore := r.replicationStatusStore.WithExecutor(exec)
	// - keep replication status HSET for history, but mark it as archieved
	// - archived replication may be used to resolve CompleteMultipartUpload, when parts were uploaded to old storage
	//    before switch started. Now if proxy will receive CompleteMultipartUpload request, it will be able to
	//   create replication tasky to sync this object to new storage after switch was completed.
	_ = txReplicationStatusStore.SetArchievedOp(ctx, replID)
	_ = txReplicationStatusStore.SetArchievedAtOp(ctx, replID, now)

	if err := exec.Exec(ctx); err != nil {
		return fmt.Errorf("unable to create zero downtime switch: %w", err)
	}

	return nil
}

func (r *policySvc) DeleteReplicationSwitch(ctx context.Context, replID entity.ReplicationStatusID) error {
	existing, err := r.GetReplicationSwitchInfo(ctx, replID)
	if err != nil {
		return err
	}
	switchID := entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket)
	exec := r.replicationSwitchStore.TxExecutor()

	if existing.IsZeroDowntime() && existing.LastStatus != entity.StatusDone {
		//revert routing idempotently
		bucketRoutingPolicyID := entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket)
		_ = r.bucketRoutingPolicyStore.WithExecutor(exec).SetOp(ctx, bucketRoutingPolicyID, replID.FromStorage)

	}
	if !existing.IsZeroDowntime() {
		// delete routing block idempotently
		_ = r.bucketRoutingBlockStore.WithExecutor(exec).RemoveOp(ctx, replID.FromStorage, replID.FromBucket)
	}
	//delete switch metadata
	_ = r.replicationSwitchStore.WithExecutor(exec).DropOp(ctx, switchID)
	// delete switch history
	_ = r.replicationSwitchHistoryStore.WithExecutor(exec).DropOp(ctx, switchID)
	if err := exec.Exec(ctx); err != nil {
		return fmt.Errorf("unable to delete replication switch: %w", err)
	}
	return nil
}

func (r *policySvc) GetReplicationSwitchInfo(ctx context.Context, replID entity.ReplicationStatusID) (entity.ReplicationSwitchInfo, error) {
	id := entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket)

	info, err := r.replicationSwitchStore.Get(ctx, id)
	if err != nil {
		return entity.ReplicationSwitchInfo{}, fmt.Errorf("unable to get replication swift info: %w", err)
	}

	switchReplID, err := r.replicationStatusStore.RestoreID(info.ReplicationIDStr)
	if err != nil {
		return entity.ReplicationSwitchInfo{}, fmt.Errorf("unable to restore replication id: %w", err)
	}
	info.ReplID = switchReplID

	if info.ReplID != replID {
		return entity.ReplicationSwitchInfo{}, fmt.Errorf("%w: replication ID mismatch: expected %s, got %s", dom.ErrInvalidArg, replID, info.ReplID)
	}

	history, err := r.replicationSwitchHistoryStore.GetAll(ctx, id)
	if err != nil {
		return info, fmt.Errorf("unable to get replication switch history: %w", err)
	}

	info.History = history
	return info, nil
}

func (r *policySvc) GetInProgressZeroDowntimeSwitchInfo(ctx context.Context, id entity.ReplicationSwitchInfoID) (entity.ZeroDowntimeSwitchInProgressInfo, error) {
	info, err := r.replicationSwitchStore.GetZeroDowntimeInfo(ctx, id)
	if err != nil {
		return entity.ZeroDowntimeSwitchInProgressInfo{}, fmt.Errorf("unable to get zero downtime info: %w", err)
	}
	if info.MultipartTTL == 0 {
		// no multipart TTL means it is not zero downtime switch
		return entity.ZeroDowntimeSwitchInProgressInfo{}, dom.ErrNotFound
	}
	if info.Status != entity.StatusInProgress {
		// only in progress switches are considered
		return entity.ZeroDowntimeSwitchInProgressInfo{}, dom.ErrNotFound
	}
	switchReplID, err := r.replicationStatusStore.RestoreID(info.ReplicationIDStr)
	if err != nil {
		return entity.ZeroDowntimeSwitchInProgressInfo{}, fmt.Errorf("unable to restore replication id: %w", err)
	}
	info.ReplID = switchReplID

	return info, nil
}

// UpdateDowntimeSwitchStatus handles downtime switch status transitions
// and performs corresponding idempotent operations with replication and routing policies
func (r *policySvc) UpdateDowntimeSwitchStatus(ctx context.Context, replID entity.ReplicationStatusID, newStatus entity.ReplicationSwitchStatus, description string, startedAt *time.Time, doneAt *time.Time) error {
	// validate input
	if newStatus == "" || newStatus == entity.StatusNotStarted {
		return fmt.Errorf("status cannot be %s: %w", newStatus, dom.ErrInvalidArg)
	}
	if startedAt != nil && doneAt != nil {
		return fmt.Errorf("cannot set both startedAt and doneAt: %w", dom.ErrInvalidArg)
	}
	switch newStatus {
	case entity.StatusError, entity.StatusSkipped, entity.StatusCheckInProgress:
		if startedAt != nil {
			return fmt.Errorf("cannot set startedAt for status %s: %w", newStatus, dom.ErrInvalidArg)
		}
		if doneAt != nil {
			return fmt.Errorf("cannot set doneAt for status %s: %w", newStatus, dom.ErrInvalidArg)
		}
	case entity.StatusInProgress:
		if startedAt == nil {
			return fmt.Errorf("startedAt is required for status in progress: %w", dom.ErrInvalidArg)
		}
		if doneAt != nil {
			return fmt.Errorf("cannot set doneAt for status in progress: %w", dom.ErrInvalidArg)
		}
	case entity.StatusDone:
		if doneAt == nil {
			return fmt.Errorf("doneAt is required for status done: %w", dom.ErrInvalidArg)
		}
		if startedAt != nil {
			return fmt.Errorf("cannot set startedAt for status done: %w", dom.ErrInvalidArg)
		}
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
	now := entity.TimeNow()
	exec := r.replicationSwitchStore.TxExecutor()
	switch newStatus {
	// if switch in progress set routing block idempotently
	case entity.StatusInProgress, entity.StatusCheckInProgress:
		_ = r.bucketRoutingBlockStore.WithExecutor(exec).AddOp(ctx, replID.FromStorage, replID.FromBucket)
	// if error, delete routing block idempotently
	case entity.StatusError:
		_ = r.bucketRoutingBlockStore.WithExecutor(exec).RemoveOp(ctx, replID.FromStorage, replID.FromBucket)
	// if done, switch routing and replication to new bucket idempotently
	case entity.StatusDone:
		_ = r.bucketRoutingBlockStore.WithExecutor(exec).RemoveOp(ctx, replID.FromStorage, replID.FromBucket)

		bucketRoutingPolicyID := entity.NewBucketRoutingPolicyID(replID.User, replID.FromBucket)
		_ = r.bucketRoutingPolicyStore.WithExecutor(exec).SetOp(ctx, bucketRoutingPolicyID, replID.ToStorage)

		txReplicationStatusStore := r.replicationStatusStore.WithExecutor(exec)
		_ = txReplicationStatusStore.SetArchievedOp(ctx, replID)
		_ = txReplicationStatusStore.SetArchievedAtOp(ctx, replID, now)

		bucketReplicationPolicyID := entity.NewBucketReplicationPolicyID(replID.User, replID.FromBucket)
		txBucketReplicationPolicyStore := r.bucketReplicationPolicyStore.WithExecutor(exec)
		_ = txBucketReplicationPolicyStore.RemoveOp(ctx, bucketReplicationPolicyID, entity.NewBucketReplicationPolicy(replID.FromStorage, replID.ToStorage, replID.ToBucket))

		if existing.ContinueReplication {
			// TODO: Here to storage and from storage are changing places
			// Why bucket was not present in policy?
			bucketReplicationPolicy := entity.NewBucketReplicationPolicy(replID.ToStorage, replID.FromStorage, replID.ToBucket)
			setEntry := *store.NewScoredSetEntry(bucketReplicationPolicy, uint8(1)) // scores are not used anymore, set to 1 to not change redis data structure
			_ = txBucketReplicationPolicyStore.AddIfNotExistsOp(ctx, bucketReplicationPolicyID, setEntry)

			replBackID := replID
			replBackID.FromStorage, replBackID.ToStorage = replID.ToStorage, replID.FromStorage
			txReplicationStatusStore := r.replicationStatusStore.WithExecutor(exec)
			txReplicationStatusStore.SetOp(ctx, replBackID, entity.ReplicationStatus{
				ListingStarted: true,
				CreatedAt:      now,
			})
			// set time separately because of go-redis bug with *time.Time
			_ = txReplicationStatusStore.SetInitDoneAtOp(ctx, replBackID, now)
		}
	}

	replicationSwitchID := entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket)
	txReplicationSwitchStore := r.replicationSwitchStore.WithExecutor(exec)

	_ = txReplicationSwitchStore.SetLastStatusOp(ctx, replicationSwitchID, newStatus)
	if startedAt != nil {
		_ = txReplicationSwitchStore.SetStartedAtOp(ctx, replicationSwitchID, *startedAt)
	}
	if doneAt != nil {
		_ = txReplicationSwitchStore.SetDoneAtOp(ctx, replicationSwitchID, *doneAt)
	}

	history := fmt.Sprintf("%s | %s -> %s: %s", entity.TimeNow().Format(time.RFC3339), existing.LastStatus, newStatus, description)
	_ = r.replicationSwitchHistoryStore.WithExecutor(exec).AddRightOp(ctx, replicationSwitchID, history)

	if err := exec.Exec(ctx); err != nil {
		return fmt.Errorf("unable to update downtime switch status: %w", err)
	}

	return nil
}

func (r *policySvc) CompleteZeroDowntimeReplicationSwitch(ctx context.Context, replID entity.ReplicationStatusID) error {
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
	now := entity.TimeNow()
	replicationSwitchID := entity.NewReplicationSwitchInfoID(replID.User, replID.FromBucket)
	history := fmt.Sprintf("%s | %s -> %s: complete zero downtime switch", now.Format(time.RFC3339), info.LastStatus, entity.StatusDone)
	exec := r.replicationSwitchStore.TxExecutor()
	txReplicationSwitchStore := r.replicationSwitchStore.WithExecutor(exec)
	_ = txReplicationSwitchStore.SetLastStatus(ctx, replicationSwitchID, entity.StatusDone)
	_ = txReplicationSwitchStore.SetDoneAt(ctx, replicationSwitchID, now)
	_ = r.replicationSwitchHistoryStore.WithExecutor(exec).AddRight(ctx, replicationSwitchID, history)
	if err := exec.Exec(ctx); err != nil {
		return fmt.Errorf("unable to update zero downtime switch status: %w", err)
	}
	return nil
}

func (r *policySvc) ListReplicationSwitchInfo(ctx context.Context) ([]entity.ReplicationSwitchInfo, error) {
	ids, err := r.replicationSwitchStore.GetAllIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to list replication switches: %w", err)
	}

	if len(ids) == 0 {
		return []entity.ReplicationSwitchInfo{}, nil
	}

	idCount := len(ids)
	infoResults := make([]store.OperationResult[entity.ReplicationSwitchInfo], 0, idCount)
	historyResults := make([]store.OperationResult[[]string], 0, idCount)

	exec := r.replicationSwitchStore.GroupExecutor()
	pipedReplicationSwitchStore := r.replicationSwitchStore.WithExecutor(exec)
	pipedReplicationSwitchHistoryStore := r.replicationSwitchHistoryStore.WithExecutor(exec)

	for _, id := range ids {
		infoResult := pipedReplicationSwitchStore.GetOp(ctx, id)
		infoResults = append(infoResults, infoResult)
		historyResult := pipedReplicationSwitchHistoryStore.GetOp(ctx, id)
		historyResults = append(historyResults, historyResult)
	}

	if err := exec.Exec(ctx); err != nil {
		return nil, fmt.Errorf("unable to execute pipeline: %w", err)
	}

	res := make([]entity.ReplicationSwitchInfo, 0, idCount)
	for i := 0; i < idCount; i++ {
		switchInfo, err := infoResults[i].Get()
		if err != nil {
			return nil, fmt.Errorf("unable to get replication switch info: %w", err)
		}
		switchReplID, err := r.replicationStatusStore.RestoreID(switchInfo.ReplicationIDStr)
		if err != nil {
			return nil, fmt.Errorf("unable to restore replication id: %w", err)
		}
		switchInfo.ReplID = switchReplID
		history, err := historyResults[i].Get()
		if err != nil {
			return nil, fmt.Errorf("unable to get replication switch history: %w", err)
		}
		switchInfo.History = history
		res = append(res, switchInfo)
	}

	return res, nil
}
