/*
 * Copyright © 2024 Clyso GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package policy

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/validate"
)

// // go:generate go tool mockery --name=Service --filename=service_mock.go --inpackage --structname=MockService
type Service interface {
	// -------------- Routing policy related methods: --------------

	// GetRoutingPolicy returns destination storage name.
	// Errors:
	//   dom.ErrRoutingBlock - if access to bucket should be blocked because bucket is used as replication destination.
	//   dom.ErrNotFound - if replication is not configured.
	GetRoutingPolicy(ctx context.Context, id entity.BucketRoutingPolicyID) (string, error)
	GetUserRoutingPolicy(ctx context.Context, user string) (string, error)
	AddUserRoutingPolicy(ctx context.Context, user, toStorage string) error
	AddBucketRoutingPolicy(ctx context.Context, id entity.BucketRoutingPolicyID, toStorage string, replace bool) error

	// -------------- Replication switch related methods: --------------

	// Upsert downtime replication switch. If switch already exists and not in progress, it will be updated.
	SetDowntimeReplicationSwitch(ctx context.Context, replID entity.ReplicationStatusID, opts *entity.ReplicationSwitchDowntimeOpts) error
	// Change downtime replication switch status. Makes required adjustments to routing and replication policies.
	// According to switch status and configured options.
	UpdateDowntimeSwitchStatus(ctx context.Context, replID entity.ReplicationStatusID, newStatus entity.ReplicationSwitchStatus, description string, startedAt, doneAt *time.Time) error
	// Creates new zero downtime replication switch.
	AddZeroDowntimeReplicationSwitch(ctx context.Context, replID entity.ReplicationStatusID, opts *entity.ReplicationSwitchZeroDowntimeOpts) error
	// Completes zero downtime replication switch.
	CompleteZeroDowntimeReplicationSwitch(ctx context.Context, replID entity.ReplicationStatusID) error
	// Deletes any replication switch if exists and reverts routing policy if switch was not done.
	DeleteReplicationSwitch(ctx context.Context, replID entity.ReplicationStatusID) error
	// Returns replication switch config and status information.
	GetReplicationSwitchInfo(ctx context.Context, replID entity.ReplicationStatusID) (entity.ReplicationSwitchInfo, error)
	ListReplicationSwitchInfo(ctx context.Context) ([]entity.ReplicationSwitchInfo, error)
	// GetInProgressZeroDowntimeSwitchInfo shortcut method for chorus proxy to get required information
	// to adjust route only when zero downtime switch is in progress.
	GetInProgressZeroDowntimeSwitchInfo(ctx context.Context, id entity.ReplicationSwitchInfoID) (entity.ZeroDowntimeSwitchInProgressInfo, error)

	// -------------- Replication policy related methods: --------------

	GetBucketReplicationPolicies(ctx context.Context, id entity.BucketReplicationPolicyID) (*entity.StorageReplicationPolicies, error)
	GetUserReplicationPolicies(ctx context.Context, user string) (*entity.StorageReplicationPolicies, error)
	AddUserReplicationPolicy(ctx context.Context, user string, policy entity.UserReplicationPolicy) error
	DeleteUserReplication(ctx context.Context, user string, policy entity.UserReplicationPolicy) error

	AddBucketReplicationPolicy(ctx context.Context, id entity.ReplicationStatusID, agentURL *string) error
	GetReplicationPolicyInfo(ctx context.Context, id entity.ReplicationStatusID) (entity.ReplicationStatus, error)
	GetReplicationPolicyInfoExtended(ctx context.Context, id entity.ReplicationStatusID) (entity.ReplicationStatusExtended, error)
	ListReplicationPolicyInfo(ctx context.Context) (map[entity.ReplicationStatusID]entity.ReplicationStatusExtended, error)
	IsReplicationPolicyExists(ctx context.Context, id entity.ReplicationStatusID) (bool, error)
	IncReplInitObjListed(ctx context.Context, id entity.ReplicationStatusID, bytes uint64, eventTime time.Time) error
	IncReplInitObjDone(ctx context.Context, id entity.ReplicationStatusID, bytes uint64, eventTime time.Time) error
	ObjListStarted(ctx context.Context, id entity.ReplicationStatusID) error
	IncReplEvents(ctx context.Context, id entity.ReplicationStatusID, eventTime time.Time) error
	IncReplEventsDone(ctx context.Context, id entity.ReplicationStatusID, eventTime time.Time) error

	PauseReplication(ctx context.Context, id entity.ReplicationStatusID) error
	ResumeReplication(ctx context.Context, id entity.ReplicationStatusID) error
	DeleteReplication(ctx context.Context, id entity.ReplicationStatusID) error
	// Archive replication. Will stop generating new events for this replication.
	// Existing events will be processed and replication status metadata will be kept.
	DeleteBucketReplicationsByUser(ctx context.Context, user, from string, to string) ([]string, error)
}

func NewService(client redis.UniversalClient, queueSVC tasks.QueueService) *policySvc {
	return &policySvc{
		userRoutingPolicyStore:        store.NewUserRoutingPolicyStore(client),
		bucketRoutingBlockStore:       store.NewRoutingBlockStore(client),
		bucketRoutingPolicyStore:      store.NewBucketRoutingPolicyStore(client),
		bucketReplicationPolicyStore:  store.NewBucketReplicationPolicyStore(client),
		userReplicationPolicyStore:    store.NewUserReplicationPolicyStore(client),
		replicationStatusStore:        store.NewReplicationStatusStore(client),
		replicationSwitchStore:        store.NewReplicationSwitchInfoStore(client),
		replicationSwitchHistoryStore: store.NewReplicationSwitchHistoryStore(client),
		queueSvc:                      queueSVC,
	}
}

type policySvc struct {
	userRoutingPolicyStore        *store.UserRoutingPolicyStore
	bucketRoutingBlockStore       *store.RoutingBlockStore
	bucketRoutingPolicyStore      *store.BucketRoutingPolicyStore
	bucketReplicationPolicyStore  *store.BucketReplicationPolicyStore
	userReplicationPolicyStore    *store.UserReplicationPolicyStore
	replicationStatusStore        *store.ReplicationStatusStore
	replicationSwitchStore        *store.ReplicationSwitchInfoStore
	replicationSwitchHistoryStore *store.ReplicationSwitchHistoryStore
	queueSvc                      tasks.QueueService
}

func (r *policySvc) GetReplicationPolicyInfoExtended(ctx context.Context, id entity.ReplicationStatusID) (entity.ReplicationStatusExtended, error) {
	replication, err := r.GetReplicationPolicyInfo(ctx, id)
	if err != nil {
		return entity.ReplicationStatusExtended{}, err
	}
	return r.fillExtendedReplicationStatus(ctx, id, &replication)
}

func (r *policySvc) fillExtendedReplicationStatus(ctx context.Context, id entity.ReplicationStatusID, status *entity.ReplicationStatus) (entity.ReplicationStatusExtended, error) {
	result := entity.ReplicationStatusExtended{
		ReplicationStatus: status,
		IsPaused:          false,
		InitMigration:     entity.QueueStats{},
		EventMigration:    entity.QueueStats{},
	}
	// get initial migration queues stats
	initQueues := tasks.InitMigrationQueues(id)
	paused, initStats, err := r.buildQueueStats(ctx, initQueues)
	if err != nil {
		return entity.ReplicationStatusExtended{}, fmt.Errorf("unable to get init migration queue stats: %w", err)
	}
	if paused {
		result.IsPaused = true
	}
	result.InitMigration = initStats

	// get event migration queues stats
	eventQueues := tasks.EventMigrationQueues(id)
	paused, eventStats, err := r.buildQueueStats(ctx, eventQueues)
	if err != nil {
		return entity.ReplicationStatusExtended{}, fmt.Errorf("unable to get event migration queue stats: %w", err)
	}
	if paused {
		result.IsPaused = true
	}
	result.EventMigration = eventStats

	return result, nil
}

func (r *policySvc) buildQueueStats(ctx context.Context, queues []string) (bool, entity.QueueStats, error) {
	isPaused := false
	result := entity.QueueStats{
		Unprocessed: 0,
		Done:        0,
		Failed:      0,
		Latency:     0,
		MemoryUsage: 0,
	}
	for _, queue := range queues {
		stats, err := r.queueSvc.Stats(ctx, queue)
		if errors.Is(err, dom.ErrNotFound) {
			// queue does not exist, so no stats available
			continue
		}
		if err != nil {
			return false, entity.QueueStats{}, fmt.Errorf("unable to get queue stats for %s: %w", queue, err)
		}
		if stats.Paused {
			// if at least one queue is paused, we consider the whole replication as paused
			isPaused = true
		}
		// sum up counters for all queues
		result.Unprocessed += stats.Unprocessed
		result.Done += stats.ProcessedTotal
		result.Failed += stats.FailedTotal
		result.MemoryUsage += stats.MemoryUsage
		if result.Latency < stats.Latency {
			// return the maximum latency across all queues
			result.Latency = stats.Latency
		}
	}
	return isPaused, result, nil
}

func (r *policySvc) ObjListStarted(ctx context.Context, id entity.ReplicationStatusID) error {
	if err := r.replicationStatusStore.SetListingStarted(ctx, id); err != nil {
		return fmt.Errorf("unable to set listing started: %w", err)
	}
	return nil
}

func (r *policySvc) getRoutingPolicy(ctx context.Context, id entity.BucketRoutingPolicyID) (string, error) {
	storage, err := r.getBucketRoutingPolicy(ctx, id)
	if err == nil {
		return storage, nil
	}
	if !errors.Is(err, dom.ErrNotFound) {
		return "", fmt.Errorf("unable to get bucket routing policy: %w", err)
	}
	// bucket policy not found, try user policy:
	return r.GetUserRoutingPolicy(ctx, id.User)
}

func (r *policySvc) GetRoutingPolicy(ctx context.Context, id entity.BucketRoutingPolicyID) (string, error) {
	storage, err := r.getRoutingPolicy(ctx, id)
	if err != nil {
		return "", fmt.Errorf("unable to get routing policy: %w", err)
	}
	blocked, err := r.isRoutingBlocked(ctx, storage, id.Bucket)
	if err != nil {
		return "", fmt.Errorf("unable to check if routing blocked: %w", err)
	}
	if blocked {
		return "", dom.ErrRoutingBlock
	}
	return storage, nil
}

func (r *policySvc) AddRoutingBlock(ctx context.Context, storage, bucket string) error {
	if _, err := r.bucketRoutingBlockStore.Add(ctx, storage, bucket); err != nil {
		return fmt.Errorf("unable to add routing block: %w", err)
	}
	return nil
}

func (r *policySvc) isRoutingBlocked(ctx context.Context, storage, bucket string) (bool, error) {
	blocked, err := r.bucketRoutingBlockStore.IsMember(ctx, storage, bucket)
	if err != nil {
		return false, fmt.Errorf("unable to check if routing blocked: %w", err)
	}
	return blocked, nil
}

func (r *policySvc) GetUserRoutingPolicy(ctx context.Context, user string) (string, error) {
	if user == "" {
		return "", fmt.Errorf("%w: user is required to get routing policy", dom.ErrInvalidArg)
	}

	toStorage, err := r.userRoutingPolicyStore.Get(ctx, user)
	if err != nil {
		return "", fmt.Errorf("unale to get user routing policy: %w", err)
	}
	return toStorage, nil
}

func (r *policySvc) getBucketRoutingPolicy(ctx context.Context, id entity.BucketRoutingPolicyID) (string, error) {
	if err := validate.BucketRoutingPolicyID(id); err != nil {
		return "", fmt.Errorf("unale to validate bucket routing policy id: %w", err)
	}
	toStorage, err := r.bucketRoutingPolicyStore.Get(ctx, id)
	if err != nil {
		return "", fmt.Errorf("unable to get bucket routing policy: %w", err)
	}
	return toStorage, nil
}

func (r *policySvc) AddUserRoutingPolicy(ctx context.Context, user, toStorage string) error {
	if user == "" {
		return fmt.Errorf("%w: user is required to add user routing policy", dom.ErrInvalidArg)
	}
	if toStorage == "" {
		return fmt.Errorf("%w: toStorage is required to add user routing policy", dom.ErrInvalidArg)
	}
	if err := r.userRoutingPolicyStore.SetIfNotExists(ctx, user, toStorage); err != nil {
		return fmt.Errorf("unable to set user routing policy: %w", err)
	}
	return nil
}

func (r *policySvc) AddBucketRoutingPolicy(ctx context.Context, id entity.BucketRoutingPolicyID, toStorage string, replace bool) error {
	if err := validate.BucketRoutingPolicyID(id); err != nil {
		return fmt.Errorf("unable to validate bucket routing policy id: %w", err)
	}
	if toStorage == "" {
		return fmt.Errorf("%w: toStorage is required to add user routing policy", dom.ErrInvalidArg)
	}
	if replace {
		if err := r.bucketRoutingPolicyStore.Set(ctx, id, toStorage); err != nil {
			return fmt.Errorf("unable to add bucket routing policy: %w", err)
		}
	} else {
		if err := r.bucketRoutingPolicyStore.SetIfNotExists(ctx, id, toStorage); err != nil {
			return fmt.Errorf("unable to add bucket routing policy: %w", err)
		}
	}
	return nil
}

func (r *policySvc) GetBucketReplicationPolicies(ctx context.Context, id entity.BucketReplicationPolicyID) (*entity.StorageReplicationPolicies, error) {
	if err := validate.BucketReplicationPolicyID(id); err != nil {
		return nil, fmt.Errorf("unable to validate bucket replication policy id: %w", err)
	}

	entries, err := r.bucketReplicationPolicyStore.GetAll(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("unable to get replication policies: %w", err)
	}
	if len(entries) == 0 {
		return nil, dom.ErrNotFound
	}

	var fromStorage string
	destinations := make([]entity.ReplicationPolicyDestination, 0, len(entries))
	for _, entry := range entries {
		if fromStorage == "" {
			fromStorage = entry.Value.FromStorage
		} else if fromStorage != entry.Value.FromStorage {
			return nil, fmt.Errorf("%w: invalid replication policy key: all keys should have same from: %+v", dom.ErrInternal, entries)
		}

		if fromStorage == entry.Value.ToStorage && id.FromBucket == entry.Value.ToBucket {
			return nil, fmt.Errorf("%w: invalid replication policy key: from and to should be different: %+v", dom.ErrInternal, entries)
		}
		destination := entity.NewBucketReplicationPolicyDestination(entry.Value.ToStorage, entry.Value.ToBucket)
		destinations = append(destinations, destination)
	}
	return &entity.StorageReplicationPolicies{
		FromStorage:  fromStorage,
		Destinations: destinations,
	}, nil
}

func (r *policySvc) GetReplicationPolicyInfo(ctx context.Context, id entity.ReplicationStatusID) (entity.ReplicationStatus, error) {
	if err := validate.ReplicationStatusID(id); err != nil {
		return entity.ReplicationStatus{}, fmt.Errorf("unable to validate replication status id: %w", err)
	}

	switchID := entity.NewReplicationSwitchInfoID(id.User, id.FromBucket)
	exec := r.replicationStatusStore.GroupExecutor()
	replicationResult := r.replicationStatusStore.WithExecutor(exec).GetOp(ctx, id)
	keyResult := r.replicationSwitchStore.WithExecutor(exec).GetReplicationKeyOp(ctx, switchID)
	_ = exec.Exec(ctx)
	replication, err := replicationResult.Get()
	if err != nil {
		return entity.ReplicationStatus{}, fmt.Errorf("unable to get replication: %w", err)
	}
	if replication.CreatedAt.IsZero() {
		return entity.ReplicationStatus{}, fmt.Errorf("%w: no replication policy status for user %q, from %q/%q, to %q/%q", dom.ErrNotFound, id.User, id.FromStorage, id.FromBucket, id.ToStorage, id.ToBucket)
	}

	key, err := keyResult.Get()
	if errors.Is(err, dom.ErrNotFound) {
		return replication, nil
	}
	if err != nil {
		return entity.ReplicationStatus{}, fmt.Errorf("unable to get replication key: %w", err)
	}

	switchReplicationID, err := r.replicationStatusStore.RestoreID(key)
	if err != nil {
		return entity.ReplicationStatus{}, fmt.Errorf("unable to restore switch replication id: %w", err)
	}

	if id == switchReplicationID {
		replication.HasSwitch = true
	}

	return replication, nil
}

func (r *policySvc) ListReplicationPolicyInfo(ctx context.Context) (map[entity.ReplicationStatusID]entity.ReplicationStatusExtended, error) {
	replicationIDs, err := r.replicationStatusStore.GetAllIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to get replication ids: %w", err)
	}

	idCount := len(replicationIDs)
	replicationStatusResults := make([]store.OperationResult[entity.ReplicationStatus], 0, idCount)
	replicationSwitchResults := make([]store.OperationResult[string], 0, idCount)
	exec := r.replicationStatusStore.GroupExecutor()
	groupReplicationStatusStore := r.replicationStatusStore.WithExecutor(exec)
	groupReplicationSwitchStore := r.replicationSwitchStore.WithExecutor(exec)
	for _, id := range replicationIDs {
		replicationStatusResult := groupReplicationStatusStore.GetOp(ctx, id)
		replicationSwitchID := entity.NewReplicationSwitchInfoID(id.User, id.FromBucket)
		replicationSwitchResult := groupReplicationSwitchStore.GetReplicationKeyOp(ctx, replicationSwitchID)
		replicationStatusResults = append(replicationStatusResults, replicationStatusResult)
		replicationSwitchResults = append(replicationSwitchResults, replicationSwitchResult)
	}

	_ = exec.Exec(ctx)

	replicationMap := make(map[entity.ReplicationStatusID]entity.ReplicationStatusExtended, idCount)
	for i := 0; i < idCount; i++ {
		replication, err := replicationStatusResults[i].Get()
		if err != nil {
			return nil, fmt.Errorf("unable to get result: %w", err)
		}
		result, err := r.fillExtendedReplicationStatus(ctx, replicationIDs[i], &replication)
		if err != nil {
			return nil, fmt.Errorf("unable to fill extended replication status: %w", err)
		}
		switchReplicationID, err := replicationSwitchResults[i].Get()
		if errors.Is(err, dom.ErrNotFound) {
			replicationMap[replicationIDs[i]] = result
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("unable to get result: %w", err)
		}
		replID, err := r.replicationStatusStore.RestoreID(switchReplicationID)
		if err != nil {
			return nil, fmt.Errorf("unable to restore id: %w", err)
		}
		if replID == replicationIDs[i] {
			replication.HasSwitch = true
		}
		replicationMap[replicationIDs[i]] = result
	}

	return replicationMap, nil
}

func (r *policySvc) IsReplicationPolicyExists(ctx context.Context, id entity.ReplicationStatusID) (bool, error) {
	bucketReplicationPolicyID := entity.NewBucketReplicationPolicyID(id.User, id.FromBucket)
	bucketReplicationPolicy := entity.NewBucketReplicationPolicy(id.FromStorage, id.ToStorage, id.ToBucket)
	exists, err := r.bucketReplicationPolicyStore.IsMember(ctx, bucketReplicationPolicyID, bucketReplicationPolicy)
	if err != nil {
		return false, fmt.Errorf("unable to check if bucket replication store exists: %w", err)
	}
	return exists, nil
}

// func (r *policySvc) IsReplicationPolicyPaused(ctx context.Context, id entity.ReplicationStatusID) (bool, error) {
// 	if err := validate.ReplicationStatusID(id); err != nil {
// 		return false, fmt.Errorf("unable to validate replication status id: %w", err)
// 	}
// 	// use InitMigrationListObjQueue to check if initial migration is paused
// 	// because it is the only queue which is always created even if bucket is empty
// 	paused, err := r.queueSVC.IsPaused(ctx, tasks.InitMigrationListObjQueue(id))
// 	if errors.Is(err, dom.ErrNotFound) {
// 		// not found errors can only appear for backwards replication after switch.
// 		// In this case there is not initial migration
// 		return false, nil // queue does not exist, so replication is not paused
// 	}
// 	return paused, err
// }

func (r *policySvc) IncReplInitObjListed(ctx context.Context, id entity.ReplicationStatusID, bytes uint64, eventTime time.Time) error {
	if err := validate.ReplicationStatusID(id); err != nil {
		return fmt.Errorf("unable to validate replication status id: %w", err)
	}
	if _, err := r.replicationStatusStore.IncrementObjectsListed(ctx, id); err != nil {
		return fmt.Errorf("unable to increment objects listed: %w", err)
	}
	if bytes == 0 {
		return nil
	}
	if _, err := r.replicationStatusStore.IncrementBytesListed(ctx, id, int64(bytes)); err != nil {
		return fmt.Errorf("unable to increment bytes listed: %w", err)
	}
	if err := r.replicationStatusStore.SetLastEmittedAt(ctx, id, eventTime); err != nil {
		return fmt.Errorf("unable to set last emitted: %w", err)
	}
	return nil
}

func (r *policySvc) IncReplInitObjDone(ctx context.Context, id entity.ReplicationStatusID, bytes uint64, eventTime time.Time) error {
	if err := validate.ReplicationStatusID(id); err != nil {
		return fmt.Errorf("unable to validate replication status id: %w", err)
	}
	if _, err := r.replicationStatusStore.IncrementObjectsDone(ctx, id); err != nil {
		return fmt.Errorf("unable to increment objects done: %w", err)
	}
	if _, err := r.replicationStatusStore.IncrementBytesDone(ctx, id, int64(bytes)); err != nil {
		return fmt.Errorf("unable to increment bytes done: %w", err)
	}
	r.updateProcessedAt(ctx, id, eventTime)

	listed, err := r.replicationStatusStore.GetObjectsListed(ctx, id)
	if err != nil {
		return fmt.Errorf("unable to get objects listed: %w", err)
	}
	done, err := r.replicationStatusStore.GetObjectsDone(ctx, id)
	if err != nil {
		return fmt.Errorf("unable to get objects done: %w", err)
	}

	if listed > done {
		return nil
	}

	if err := r.replicationStatusStore.SetInitDoneAt(ctx, id, time.Now()); err != nil {
		return fmt.Errorf("unable to set init done at: %w", err)
	}

	return nil
}

func (r *policySvc) IncReplEvents(ctx context.Context, id entity.ReplicationStatusID, eventTime time.Time) error {
	if err := validate.ReplicationStatusID(id); err != nil {
		return fmt.Errorf("unable to validate replication status id: %w", err)
	}
	if _, err := r.replicationStatusStore.IncrementEvents(ctx, id); err != nil {
		return fmt.Errorf("unable to increment events done: %w", err)
	}

	if err := r.replicationStatusStore.SetLastEmittedAt(ctx, id, eventTime); err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("unable to update last_emitted_at for event replication")
	}

	return nil
}

func (r *policySvc) IncReplEventsDone(ctx context.Context, id entity.ReplicationStatusID, eventTime time.Time) error {
	if err := validate.ReplicationStatusID(id); err != nil {
		return fmt.Errorf("unable to validate replication status id: %w", err)
	}
	if _, err := r.replicationStatusStore.IncrementEventsDone(ctx, id); err != nil {
		return fmt.Errorf("unable to increment events done: %w", err)
	}
	r.updateProcessedAt(ctx, id, eventTime)
	return nil
}

func (r *policySvc) updateProcessedAt(ctx context.Context, id entity.ReplicationStatusID, eventTime time.Time) {
	affected, err := r.replicationStatusStore.SetProcessedAtIfGreater(ctx, id, eventTime)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("unable to update policy last_processed_at")
		return
	}
	if affected == 0 {
		zerolog.Ctx(ctx).Info().Msg("policy last_processed_at is not updated")
	}
}

func (r *policySvc) GetUserReplicationPolicies(ctx context.Context, user string) (*entity.StorageReplicationPolicies, error) {
	if user == "" {
		return nil, fmt.Errorf("%w: user is required to get replication policy", dom.ErrInvalidArg)
	}

	entries, err := r.userReplicationPolicyStore.GetAll(ctx, user)
	if err != nil {
		return nil, fmt.Errorf("unable to get replication policies: %w", err)
	}
	if len(entries) == 0 {
		return nil, dom.ErrNotFound
	}

	var fromStorage string
	destinations := make([]entity.ReplicationPolicyDestination, 0, len(entries))
	for _, entry := range entries {
		if fromStorage == "" {
			fromStorage = entry.Value.FromStorage
		} else if fromStorage != entry.Value.FromStorage {
			return nil, fmt.Errorf("%w: invalid replication policy key: all keys should have same from: %+v", dom.ErrInternal, entries)
		}

		if fromStorage == entry.Value.ToStorage {
			return nil, fmt.Errorf("%w: invalid replication policy key: from and to should be different: %+v", dom.ErrInternal, entries)
		}
		destination := entity.NewUserReplicationPolicyDestination(entry.Value.ToStorage)
		destinations = append(destinations, destination)
	}
	return &entity.StorageReplicationPolicies{
		FromStorage:  fromStorage,
		Destinations: destinations,
	}, nil
}

func (r *policySvc) AddUserReplicationPolicy(ctx context.Context, user string, policy entity.UserReplicationPolicy) error {
	if user == "" {
		return fmt.Errorf("%w: user is required to add replication policy", dom.ErrInvalidArg)
	}
	if policy.FromStorage == "" {
		return fmt.Errorf("%w: from is required to add replication policy", dom.ErrInvalidArg)
	}
	if policy.ToStorage == "" {
		return fmt.Errorf("%w: to is required to add replication policy", dom.ErrInvalidArg)
	}
	if policy.FromStorage == policy.ToStorage {
		return fmt.Errorf("%w: invalid replication policy: from and to should be different", dom.ErrInvalidArg)
	}

	route, err := r.GetUserRoutingPolicy(ctx, user)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("%w: get routing error", err)
	}
	if err == nil && route != policy.FromStorage {
		return fmt.Errorf("%w: unable to create user %s replciation from %s because it is different from routing %s", dom.ErrInternal, user, policy.FromStorage, route)
	}

	prev, err := r.GetUserReplicationPolicies(ctx, user)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return err
	}
	if err == nil {
		if policy.FromStorage != prev.FromStorage {
			return fmt.Errorf("%w: all replication policies should have the same from value: got %s, current %s", dom.ErrInvalidArg, policy.FromStorage, prev.FromStorage)
		}
		alreadyExists := slices.Contains(prev.Destinations, entity.NewUserReplicationPolicyDestination(policy.ToStorage))
		if alreadyExists {
			return dom.ErrAlreadyExists
		}
	}

	entry := store.ScoredSetEntry[entity.UserReplicationPolicy, uint8]{
		Value: entity.NewUserReplicationPolicy(policy.FromStorage, policy.ToStorage),
		Score: uint8(1), // scores are not used anymore, set to 1 to not change redis data structure
	}
	affected, err := r.userReplicationPolicyStore.AddIfNotExists(ctx, user, entry)
	if err != nil {
		return fmt.Errorf("unable to add user replication policy: %w", err)
	}
	if affected == 0 {
		return dom.ErrAlreadyExists
	}
	return nil
}

func (r *policySvc) DeleteUserReplication(ctx context.Context, user string, policy entity.UserReplicationPolicy) error {
	affected, err := r.userReplicationPolicyStore.Remove(ctx, user, policy)
	if err != nil {
		return fmt.Errorf("unable to remove replication policy: %w", err)
	}
	if affected != 1 {
		return dom.ErrNotFound
	}
	size, err := r.userReplicationPolicyStore.Size(ctx, user)
	if err != nil {
		return fmt.Errorf("unable to get size: %w", err)
	}
	if size != 0 {
		return nil
	}
	if _, err := r.userReplicationPolicyStore.Drop(ctx, user); err != nil {
		return fmt.Errorf("unable to drop replication policy: %w", err)
	}
	return nil
}

func (r *policySvc) DeleteBucketReplicationsByUser(ctx context.Context, user, fromStorage string, toStorage string) ([]string, error) {
	ids, err := r.replicationStatusStore.GetAllIDs(ctx, user)
	if err != nil {
		return nil, fmt.Errorf("unable to get replication ids: %w", err)
	}

	deleted := []string{}
	for _, id := range ids {
		if id.FromStorage != fromStorage || id.ToStorage != toStorage {
			continue
		}
		if err := r.DeleteReplication(ctx, id); err != nil {
			zerolog.Ctx(ctx).Err(err).Msg("unable to delte replication")
			continue
		}
		deleted = append(deleted, id.FromBucket)
	}
	return deleted, nil
}

func (r *policySvc) AddBucketReplicationPolicy(ctx context.Context, id entity.ReplicationStatusID, agentURL *string) error {
	if err := validate.ReplicationStatusID(id); err != nil {
		return fmt.Errorf("unable to validate replication status id: %w", err)
	}

	bucketRoutingPolicyID := entity.NewBucketRoutingPolicyID(id.User, id.FromBucket)
	route, err := r.GetRoutingPolicy(ctx, bucketRoutingPolicyID)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("%w: get routing error", err)
	}
	if err == nil && route != id.FromStorage {
		return fmt.Errorf("%w: unable to create bucket %s replication from %s because it is different from routing %s", dom.ErrInternal, id.FromBucket, id.FromStorage, route)
	}

	bucketReplicationPolicyID := entity.NewBucketReplicationPolicyID(id.User, id.FromBucket)
	prev, err := r.GetBucketReplicationPolicies(ctx, bucketReplicationPolicyID)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("unable to get bucket replication policies: %w", err)
	}
	if err == nil {
		if id.FromStorage != prev.FromStorage {
			return fmt.Errorf("%w: all replication policies should have the same from value (u: %s b: %s): got %s, current %s", dom.ErrInvalidArg, id.User, id.FromBucket, id.FromStorage, prev.FromStorage)
		}
		alreadyExists := slices.Contains(prev.Destinations, entity.NewBucketReplicationPolicyDestination(id.ToStorage, id.ToBucket))
		if alreadyExists {
			return dom.ErrAlreadyExists
		}
	}

	entry := store.ScoredSetEntry[entity.BucketReplicationPolicy, uint8]{
		Value: entity.NewBucketReplicationPolicy(id.FromStorage, id.ToStorage, id.ToBucket),
		Score: uint8(1), // scores are not used anymore, set to 1 to not change redis data structure
	}
	affected, err := r.bucketReplicationPolicyStore.AddIfNotExists(ctx, bucketReplicationPolicyID, entry)
	if err != nil {
		return err
	}
	if affected == 0 {
		return dom.ErrAlreadyExists
	}

	status := entity.ReplicationStatus{
		CreatedAt: time.Now().UTC(),
		AgentURL:  fromStrPtr(agentURL),
	}
	if err := r.replicationStatusStore.Set(ctx, id, status); err != nil {
		return fmt.Errorf("unable to set replication status: %w", err)
	}
	if id.FromBucket == id.ToBucket {
		return nil
	}

	if err = r.AddRoutingBlock(ctx, id.ToStorage, id.ToBucket); err != nil {
		return fmt.Errorf("unable to add routing block: %w", err)
	}

	return nil
}

func fromStrPtr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func (r *policySvc) PauseReplication(ctx context.Context, id entity.ReplicationStatusID) error {
	if err := validate.ReplicationStatusID(id); err != nil {
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

func (r *policySvc) ResumeReplication(ctx context.Context, id entity.ReplicationStatusID) error {
	if err := validate.ReplicationStatusID(id); err != nil {
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

func (r *policySvc) DeleteReplication(ctx context.Context, id entity.ReplicationStatusID) error {
	bucketReplicationPolicyID := entity.NewBucketReplicationPolicyID(id.User, id.FromBucket)
	bucketReplicationPolicy := entity.NewBucketReplicationPolicy(id.FromStorage, id.ToStorage, id.ToBucket)
	exec := r.bucketReplicationPolicyStore.TxExecutor()
	_ = r.bucketReplicationPolicyStore.WithExecutor(exec).RemoveOp(ctx, bucketReplicationPolicyID, bucketReplicationPolicy)
	_ = r.replicationStatusStore.WithExecutor(exec).DropOp(ctx, id)
	_ = r.bucketRoutingBlockStore.WithExecutor(exec).RemoveOp(ctx, id.ToStorage, id.ToBucket)
	if err := exec.Exec(ctx); err != nil {
		return fmt.Errorf("unable to execute group: %w", err)
	}
	return nil
}
