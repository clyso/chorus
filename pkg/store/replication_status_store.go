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

package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
)

/*
Replication status stores historical information about replication jobs.
- Status is NOT USED by chorus business logic, it is only used for monitoring and reporting purposes.
- Status cannot be changed directly - it is updated by replication workers during replication process.
- Status cannot be deleted, it can only be overwritten if user recreates replication policy with the same parameters.
  When replication policy is deleted, status is kept, but marked as archived.

Because status cannot be deleted, we have to maintain index, to be able to list all statuses after replication is deleted.

Following Redis structures are used to store replication status:

1. User replication status index stored in a global Redis Set.
Key: p:repl_user_status_index
Value: { "<user>:<from_storage>:<to_storage>", ... }

2. User replication status stored in a Redis Hash.
Key: p:repl_status_user:<user>:<from_storage>:<to_storage>
Value: { key: value, ... }

3. Bucket replication status index stored in a per-user Redis Set.
Key: p:repl_bucket_status_index:<user>
Value: { "<user>:<from_storage>:<to_storage>:<from_bucket>:<to_bucket>", ... }

4. Bucket replication status stored in a Redis Hash.
Key: p:repl_status_bucket:<user>:<from_storage>:<to_storage>:<from_bucket>:<to_bucket>
Value: { key: value, ... }

*/

type BucketReplicationStatusStore struct {
	// Per-user bucket replication status index
	// Key: p:repl_bucket_status_index:<user>
	// Value: set of entity.BucketReplicationPolicy
	index *RedisIDKeySet[string, entity.BucketReplicationPolicy]
	// Hash with bucket replications status info
	// Key: p:repl_status_bucket:<user>:<from_storage>:<to_storage>:<from_bucket>:<to_bucket>
	// Value: hash of entity.ReplicationStatus fields
	status *RedisIDKeyHash[entity.BucketReplicationPolicy, entity.ReplicationStatus]
}

func NewBucketReplicationStatusStore(client redis.Cmdable) *BucketReplicationStatusStore {
	return &BucketReplicationStatusStore{
		index: NewRedisIDKeySet[string, entity.BucketReplicationPolicy](
			client, "p:repl_bucket_status_index",
			StringToSingleTokenConverter, SingleTokenToStringConverter,
			BucketReplicationPolicyToStringConverter, StringToBucketReplicationPolicyConverter),
		status: NewRedisIDKeyHash[entity.BucketReplicationPolicy, entity.ReplicationStatus](
			client, "p:repl_status_bucket",
			BucketReplicationPolicyToTokensConverter, TokensToBucketReplicationPolicyConverter),
	}
}

func (r *BucketReplicationStatusStore) WithExecutor(exec Executor[redis.Pipeliner]) *BucketReplicationStatusStore {
	return NewBucketReplicationStatusStore(exec.Get())
}

func (r *BucketReplicationStatusStore) TxExecutor() Executor[redis.Pipeliner] {
	return r.index.TxExecutor()
}

func (r *BucketReplicationStatusStore) GetOp(ctx context.Context, id entity.BucketReplicationPolicy) OperationResult[entity.ReplicationStatus] {
	return r.status.GetOp(ctx, id)
}

func (r *BucketReplicationStatusStore) List(ctx context.Context, user string) (map[entity.BucketReplicationPolicy]entity.ReplicationStatus, error) {
	policies, err := r.index.Get(ctx, user)
	if err != nil {
		return nil, fmt.Errorf("unable to list bucket replication policies from index: %w", err)
	}
	if len(policies) == 0 {
		return nil, dom.ErrNotFound
	}
	// fetch all statuses in a batch
	statuses := make(map[entity.BucketReplicationPolicy]OperationResult[entity.ReplicationStatus], len(policies))
	batch := r.status.GroupExecutor()
	inBatch := r.WithExecutor(batch)
	for _, p := range policies {
		statuses[p] = inBatch.status.GetOp(ctx, p)
	}
	_ = batch.Exec(ctx)
	// collect results
	result := make(map[entity.BucketReplicationPolicy]entity.ReplicationStatus, len(policies))
	for id, statusRes := range statuses {
		status, err := statusRes.Get()
		if err != nil {
			return nil, fmt.Errorf("unable to get bucket replication status for policy %+v: %w", id, err)
		}
		result[id] = status
	}
	return result, nil
}

func (r *BucketReplicationStatusStore) AddOp(ctx context.Context, id entity.BucketReplicationPolicy, status entity.ReplicationStatus) OperationStatus {
	status.CreatedAt = entity.TimeNow()
	res := map[string]OperationStatus{}
	res["add index"] = r.index.AddOp(ctx, id.User, id).Status()
	res["add info"] = r.status.SetOp(ctx, id, status)
	return JoinRedisOperationStatusWithMessages("add bucket replication status", res)
}

func (r *BucketReplicationStatusStore) ArchieveOp(ctx context.Context, id entity.BucketReplicationPolicy) OperationStatus {
	res := map[string]OperationStatus{}
	res["set archived"] = r.status.SetFieldOp(ctx, id, "archived", true).Status()
	res["set archivedAt"] = r.status.SetFieldOp(ctx, id, "archived_at", entity.TimeNow()).Status()
	return JoinRedisOperationStatusWithMessages("archive bucket replication status", res)
}
func (r *BucketReplicationStatusStore) DeleteOp(ctx context.Context, id entity.BucketReplicationPolicy) OperationStatus {
	res := map[string]OperationStatus{}
	res["remove index"] = r.index.RemoveOp(ctx, id.User, id).Status()
	res["remove info"] = r.status.DropOp(ctx, id).Status()
	return JoinRedisOperationStatusWithMessages("delete bucket replication status", res)
}

type UserReplicationStatusStore struct {
	// Global user replication status index
	// Key: p:repl_user_status_index
	// Value: set of entity.UserReplicationPolicy
	index *RedisIDKeySet[string, entity.UserReplicationPolicy]
	// Hash with user replications status info
	// Key: p:repl_status_user:<user>:<from_storage>:<to_storage>
	// Value: hash of entity.ReplicationStatus fields
	status *RedisIDKeyHash[entity.UserReplicationPolicy, entity.ReplicationStatus]
}

func NewUserReplicationStatusStore(client redis.Cmdable) *UserReplicationStatusStore {
	return &UserReplicationStatusStore{
		index: NewRedisIDKeySet[string, entity.UserReplicationPolicy](
			client, "p:repl_user_status_index",
			StringToSingleTokenConverter, SingleTokenToStringConverter,
			UserReplicationPolicyToStringConverter, StringToUserReplicationPolicyConverter),
		status: NewRedisIDKeyHash[entity.UserReplicationPolicy, entity.ReplicationStatus](
			client, "p:repl_status_user",
			UserReplicationPolicyToTokensConverter, TokensToUserReplicationPolicy),
	}
}

func (r *UserReplicationStatusStore) WithExecutor(exec Executor[redis.Pipeliner]) *UserReplicationStatusStore {
	return NewUserReplicationStatusStore(exec.Get())
}

func (r *UserReplicationStatusStore) TxExecutor() Executor[redis.Pipeliner] {
	return r.index.TxExecutor()
}

// -------------------------------- Methods --------------------------------

func (r *UserReplicationStatusStore) GetOp(ctx context.Context, id entity.UserReplicationPolicy) OperationResult[entity.ReplicationStatus] {
	return r.status.GetOp(ctx, id)
}

func (r *UserReplicationStatusStore) List(ctx context.Context) (map[entity.UserReplicationPolicy]entity.ReplicationStatus, error) {
	policies, err := r.index.Get(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("unable to list user replication policies from index: %w", err)
	}
	if len(policies) == 0 {
		return nil, dom.ErrNotFound
	}
	// fetch all statuses in a batch
	statuses := make(map[entity.UserReplicationPolicy]OperationResult[entity.ReplicationStatus], len(policies))
	batch := r.status.GroupExecutor()
	inBatch := r.WithExecutor(batch)
	for _, p := range policies {
		statuses[p] = inBatch.status.GetOp(ctx, p)
	}
	_ = batch.Exec(ctx)
	// collect results
	result := make(map[entity.UserReplicationPolicy]entity.ReplicationStatus, len(policies))
	for id, statusRes := range statuses {
		status, err := statusRes.Get()
		if err != nil {
			return nil, fmt.Errorf("unable to get user replication status for policy %+v: %w", id, err)
		}
		result[id] = status
	}
	return result, nil
}

// returns true if there is at least one user-replication for given user
func (r *UserReplicationStatusStore) Exists(ctx context.Context, user string) (bool, error) {
	policies, err := r.index.Get(ctx, "")
	if err != nil {
		return false, fmt.Errorf("unable to list user replication policies from index: %w", err)
	}
	for _, v := range policies {
		if v.User == user {
			return true, nil
		}
	}
	return false, nil
}

func (r *UserReplicationStatusStore) AddOp(ctx context.Context, id entity.UserReplicationPolicy, status entity.ReplicationStatus) OperationStatus {
	status.CreatedAt = entity.TimeNow()
	res := map[string]OperationStatus{}
	res["add index"] = r.index.AddOp(ctx, "", id).Status()
	res["add info"] = r.status.SetOp(ctx, id, status)
	return JoinRedisOperationStatusWithMessages("add user replication status", res)
}

func (r *UserReplicationStatusStore) ArchieveOp(ctx context.Context, id entity.UserReplicationPolicy) OperationStatus {
	res := map[string]OperationStatus{}
	res["set archived"] = r.status.SetFieldOp(ctx, id, "archived", true).Status()
	res["set archivedAt"] = r.status.SetFieldOp(ctx, id, "archived_at", entity.TimeNow()).Status()
	return JoinRedisOperationStatusWithMessages("archive user replication status", res)
}

func (r *UserReplicationStatusStore) DeleteOp(ctx context.Context, id entity.UserReplicationPolicy) OperationStatus {
	res := map[string]OperationStatus{}
	res["remove index"] = r.index.RemoveOp(ctx, "", id).Status()
	res["remove info"] = r.status.DropOp(ctx, id).Status()
	return JoinRedisOperationStatusWithMessages("delete user replication status", res)
}

// -------------------------------- ID converters --------------------------------
func UniversalReplicationIDToTokensConverter(value entity.UniversalReplicationID) ([]string, error) {
	if error := value.Validate(); error != nil {
		return nil, error
	}
	return strings.Split(value.AsString(), ":"), nil
}

func TokensToUniversalReplicationID(values []string) (entity.UniversalReplicationID, error) {
	return entity.UniversalIDFromString(strings.Join(values, ":"))
}

func BucketReplicationPolicyToTokensConverter(value entity.BucketReplicationPolicy) ([]string, error) {
	return UniversalReplicationIDToTokensConverter(entity.UniversalFromBucketReplication(value))
}

func TokensToBucketReplicationPolicyConverter(values []string) (entity.BucketReplicationPolicy, error) {
	uid, err := TokensToUniversalReplicationID(values)
	if err != nil {
		return entity.BucketReplicationPolicy{}, err
	}
	bucketID, ok := uid.AsBucketID()
	if !ok {
		return entity.BucketReplicationPolicy{}, fmt.Errorf("unable to convert tokens to BucketReplicationPolicy: %w", dom.ErrInternal)
	}
	return bucketID, nil
}

func UserReplicationPolicyToTokensConverter(value entity.UserReplicationPolicy) ([]string, error) {
	return UniversalReplicationIDToTokensConverter(entity.UniversalFromUserReplication(value))
}

func TokensToUserReplicationPolicy(values []string) (entity.UserReplicationPolicy, error) {
	uid, err := TokensToUniversalReplicationID(values)
	if err != nil {
		return entity.UserReplicationPolicy{}, err
	}
	userID, ok := uid.AsUserID()
	if !ok {
		return entity.UserReplicationPolicy{}, fmt.Errorf("unable to convert tokens to UserReplicationPolicy: %w", dom.ErrInternal)
	}
	return userID, nil
}

func UniversalReplicationIDToStringConverter(value entity.UniversalReplicationID) (string, error) {
	if error := value.Validate(); error != nil {
		return "", error
	}
	return value.AsString(), nil
}

func BucketReplicationPolicyToStringConverter(value entity.BucketReplicationPolicy) (string, error) {
	if error := value.Validate(); error != nil {
		return "", error
	}
	uid := entity.UniversalFromBucketReplication(value)
	return uid.AsString(), nil
}

func StringToBucketReplicationPolicyConverter(value string) (entity.BucketReplicationPolicy, error) {
	uid, err := entity.UniversalIDFromString(value)
	if err != nil {
		return entity.BucketReplicationPolicy{}, err
	}
	bucketID, ok := uid.AsBucketID()
	if !ok {
		return entity.BucketReplicationPolicy{}, fmt.Errorf("unable to convert string to BucketReplicationPolicy: %w", dom.ErrInternal)
	}
	return bucketID, nil
}
