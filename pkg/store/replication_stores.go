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
	"slices"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
)

/*
Replication policies tell chorus where to replicate data.

Main rules:
--------------------------------------------------------------------------
!!! Replication can be One-to-One or One-to-Many.
--------------------------------------------------------------------------
!!! Many-to-One or Many-to-Many leads to multiple writers and data corruption.
--------------------------------------------------------------------------
!!! Active replications must be unambiguously identified only by:
    - proxy request user
    - proxy request bucket (ignored for user-level replication)
--------------------------------------------------------------------------
! Try to avoid key range wildcard queries in Redis even for admin operations.
  Because range query can block Redis for a long time if there are many keys.
--------------------------------------------------------------------------
!!! Bucket-level replication can have CUSTOM destination bucket names.
    We must maintain a set of bucket names used as replication destinations.
    Otherwise, it will be possible to create Many-to-One replication.
--------------------------------------------------------------------------

Having this in mind, Redis keys and structures are designed to:
- enforce single replication destination
- fast lookup of active replications by user and bucket without LUA.

Since we optimize for fast lookups, we enforce other more complex rules on policy creation/update in Go service layer:
- user replication from/to storages must differ
- if bucket replication from/to storages are the same, then from/to buckets must differ
- if user replication exists, then no bucket replication can be created
- if at least one bucket replication exists for user, then no user replication can be created
- source storage must match routing storage
- if bucket is used as destination, then it cant be used as source in any other replication
- if destination bucket name is different from source, routing block is created for destination bucket.
  Because chorus worker must have exclusive write access to destination bucket during replication.


Replication policies are stored in Redis in the following structures:

1. User-level replication policies stored in 2 Redis structures:
a) Source storages for all user repliations stored in global Redis Hash
Key: p:repl_user_from
Value: {
   <user_1>: <from_storage_1>,
   <user_2>: <from_storage_2>,
   ...
}

b) Destination storages are stored in per-user as Redis Set:
Key: p:repl_user_to:<user>
Value: { <to_storage_1>, <to_storage_2>, ... }

User replications lookup by <user_id>:
> MULTI
> HGET p:repl_user_from <user>   # get a single from_storage
> SMEMBERS p:repl_user_to:<user> # get to_storages {<to_storage>, ...}
> EXEC


2. Bucket-level replication policies stored in 2 Redis structures:
a) Source storages for all user buckets are stored in per-user Redis Hash:
Key: p:repl_bucket_from:<user>
Value: {
   <from_bucket_1>: <from_storage_1>,
   <from_bucket_2>: <from_storage_2>,
   ...
}

b) Destination storages are stored in per-user per-bucket Redis Set:
Key: p:repl_bucket_to:<user>:<from_bucket>
Value: { "<to_storage_1>:<to_bucket1>", "<to_storage_2>:<to_bucket2>", ... }

c) User-level Set of all buckets used as replication destinations
Key: p:repl_bucket_to_dest_index:<user>
Value: { "<to_storage_1>:<to_bucket1>", "<to_storage_2>:<to_bucket2>", ... }

Bucket replications lookup by <user_id> and <bucket>:
> MULTI
> HGET p:repl_bucket_from:<user> <bucket>   # get a single from_storage
> SMEMBERS p:repl_bucket_to:<user>:<bucket> # get concatenated destinations {"<to_storage>:<to_bucket>", ...}
> EXEC

*/

type UserReplicationPolicyStore struct {
	// Source storages for all user replications
	// Key: p:repl_user_from
	// Value: { <user_1>: <from_storage_1>, ... }
	from *RedisIDKeyHash[string, map[string]string]
	// Destination storages
	// Key: p:repl_user_to:<user>
	// Value: { <to_storage_1>, <to_storage_2>, ... }
	to *RedisIDKeySet[string, string]
}

func NewUserReplicationPolicyStore(client redis.Cmdable) *UserReplicationPolicyStore {
	return &UserReplicationPolicyStore{
		from: NewRedisIDKeyHash[string, map[string]string](client, "p:repl_user_from",
			StringToSingleTokenConverter, SingleTokenToStringConverter),

		to: NewRedisIDKeySet(client, "p:repl_user_to",
			StringToSingleTokenConverter, SingleTokenToStringConverter,
			StringValueConverter, StringValueConverter),
	}
}

func (r *UserReplicationPolicyStore) TxExecutor() Executor[redis.Pipeliner] {
	return r.from.TxExecutor()
}

func (r *UserReplicationPolicyStore) WithExecutor(exec Executor[redis.Pipeliner]) *UserReplicationPolicyStore {
	return NewUserReplicationPolicyStore(exec.Get())
}

func (r *UserReplicationPolicyStore) Get(ctx context.Context, user string) ([]entity.UserReplicationPolicy, error) {
	tx := r.TxExecutor()
	result := r.WithExecutor(tx).GetOp(ctx, user)
	_ = tx.Exec(ctx)
	return result.Get()
}

func (r *UserReplicationPolicyStore) GetOp(ctx context.Context, user string) OperationResult[[]entity.UserReplicationPolicy] {
	fromStorageResult := r.from.GetFieldOp(ctx, "", user)
	toStoragesResult := r.to.GetOp(ctx, user)
	return NewRedisOperationResult(func() ([]entity.UserReplicationPolicy, error) {
		fromStorage, err := fromStorageResult.Get()
		if err != nil {
			return nil, fmt.Errorf("unable to get user replication source storage: %w", err)
		}
		toStorages, err := toStoragesResult.Get()
		if err != nil {
			return nil, fmt.Errorf("unable to get user replication destination storages: %w", err)
		}
		if len(toStorages) == 0 {
			// from exists but no to storages - should never happen
			return nil, fmt.Errorf("%w: user %q replication has no destination storages", dom.ErrInternal, user)
		}
		policies := make([]entity.UserReplicationPolicy, 0, len(toStorages))
		for _, toStorage := range toStorages {
			if toStorage == fromStorage {
				// this should never happen, because we validate this on creation
				return nil, fmt.Errorf("%w: user replication from and to storages must differ", dom.ErrInternal)
			}
			policies = append(policies, entity.NewUserReplicationPolicy(user, fromStorage, toStorage))
		}
		// sort by to storage for consistent order
		slices.SortFunc(policies, func(a, b entity.UserReplicationPolicy) int {
			return strings.Compare(a.ToStorage, b.ToStorage)
		})
		return policies, nil
	})
}

// Add adds a new user replication policy. Validation must be done before calling this method.
// Can be part of a transaction.
func (r *UserReplicationPolicyStore) AddOp(ctx context.Context, id entity.UserReplicationPolicy) OperationStatus {
	addFromResult := r.from.SetFieldOp(ctx, "", id.User, id.FromStorage)
	addToResult := r.to.AddOp(ctx, id.User, id.ToStorage)
	return NewRedisOperationStatus(func() error {
		if _, err := addFromResult.Get(); err != nil {
			return fmt.Errorf("unable to add user replication source storage: %w", err)
		}
		if _, err := addToResult.Get(); err != nil {
			return fmt.Errorf("unable to add user replication destination storage: %w", err)
		}
		return nil
	})
}

// removeReplicationPolicyScript is a LUA script that atomically removes replication policy
// it removes destination and if no destinations left, it removes source as well
//
// Input:
// KEYS[1] -> policy from Hash key (ex: "p:repl_user_from")
// KEYS[2] -> policy to Set key (ex: "p:repl_user_to:<user>")
// --
// ARGV[1] -> policy from Hash field (user)
// ARGV[2] -> policy to Set value (to_storage)
//
// Output:
// Returns 1 if policy was removed
// Returns 0 if was not found
var removeReplicationPolicyScript = redis.NewScript(`
local to_removed = redis.call("SREM", KEYS[2],ARGV[2])
if redis.call("SCARD", KEYS[2]) == 0 then
	return redis.call("HDEL", KEYS[1], ARGV[1])
end
return to_removed
`)

func (r *UserReplicationPolicyStore) RemoveOp(ctx context.Context, id entity.UserReplicationPolicy) OperationStatus {
	fromKey, err := r.from.MakeKey("")
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to make user replication source key: %w", err))
	}
	fromField := id.User
	toKey, err := r.to.MakeKey(id.User)
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to make user replication destination key: %w", err))
	}
	toValue := id.ToStorage
	cmdRes := removeReplicationPolicyScript.Eval(ctx, r.from.client, []string{fromKey, toKey}, fromField, toValue)
	return NewRedisOperationStatus(func() error {
		err := cmdRes.Err()
		if err != nil {
			return fmt.Errorf("unable to remove user replication policy: %w", err)
		}
		return nil
	})
}

// --------------------------------bucket replication policies--------------------------------
type BucketReplicationPolicyStore struct {
	// Source storages for all user buckets
	// Key: p:repl_bucket_from:<user>
	// Value: { <from_bucket_1>: <from_storage_1>, ... }
	from *RedisIDKeyHash[string, map[string]string]
	// Destination storages
	// Key: p:repl_bucket_to:<user>:<from_bucket>
	// Value: { "<to_storage_1>:<to_bucket1>", "<to_storage_2>:<to_bucket2>", ... }
	to *RedisIDKeySet[entity.BucketReplicationPolicyID, entity.BucketReplicationPolicyDestination]
	// Key: p:repl_bucket_to_dest_index:<user>
	// Value: { "<to_storage_1>:<to_bucket1>", "<to_storage_2>:<to_bucket2>", ... }
	bucketDestIndex *RedisIDKeySet[string, entity.BucketReplicationPolicyDestination]
}

func NewBucketReplicationPolicyStore(client redis.Cmdable) *BucketReplicationPolicyStore {
	return &BucketReplicationPolicyStore{
		from: NewRedisIDKeyHash[string, map[string]string](client, "p:repl_bucket_from",
			StringToSingleTokenConverter, SingleTokenToStringConverter),

		to: NewRedisIDKeySet(client, "p:repl_bucket_to",
			BucketReplicationPolicyIDToTokensConverter, TokensToBucketReplicationPolicyIDConverter,
			BucketReplicationPolicyDestinationToStringConverter, StringToBucketReplicationPolicyDestinationConverter),

		bucketDestIndex: NewRedisIDKeySet(client, "p:repl_bucket_to_dest_index",
			StringToSingleTokenConverter, SingleTokenToStringConverter,
			BucketReplicationPolicyDestinationToStringConverter, StringToBucketReplicationPolicyDestinationConverter),
	}
}

func (r *BucketReplicationPolicyStore) WithExecutor(exec Executor[redis.Pipeliner]) *BucketReplicationPolicyStore {
	return NewBucketReplicationPolicyStore(exec.Get())
}

func (r *BucketReplicationPolicyStore) TxExecutor() Executor[redis.Pipeliner] {
	return r.from.TxExecutor()
}

func (r *BucketReplicationPolicyStore) Get(ctx context.Context, id entity.BucketReplicationPolicyID) ([]entity.BucketReplicationPolicy, error) {
	tx := r.TxExecutor()
	result := r.WithExecutor(tx).GetOp(ctx, id)
	_ = tx.Exec(ctx)
	return result.Get()
}

func (r *BucketReplicationPolicyStore) GetOp(ctx context.Context, id entity.BucketReplicationPolicyID) OperationResult[[]entity.BucketReplicationPolicy] {
	fromStorageResult := r.from.GetFieldOp(ctx, id.User, id.FromBucket)
	toStoragesResult := r.to.GetOp(ctx, id)
	return NewRedisOperationResult(func() ([]entity.BucketReplicationPolicy, error) {
		fromStorage, err := fromStorageResult.Get()
		if err != nil {
			return nil, fmt.Errorf("unable to get bucket replication source storage: %w", err)
		}
		toStorages, err := toStoragesResult.Get()
		if err != nil {
			return nil, fmt.Errorf("unable to get bucket replication destination storages: %w", err)
		}
		if len(toStorages) == 0 {
			// from exists but no to storages - should never happen
			return nil, fmt.Errorf("%w: bucket %q replication for user %q has no destination storages", dom.ErrInternal, id.FromBucket, id.User)
		}
		policies := make([]entity.BucketReplicationPolicy, 0, len(toStorages))
		for _, toStorage := range toStorages {
			policy := entity.NewBucketRepliationPolicy(id.User, fromStorage, id.FromBucket, toStorage.ToStorage, toStorage.ToBucket)
			if err := policy.Validate(); err != nil {
				return nil, fmt.Errorf("invalid bucket replication policy found in store: %w", err)
			}
			policies = append(policies, policy)
		}
		// sort by to storage/bucket for consistent order
		slices.SortFunc(policies, func(a, b entity.BucketReplicationPolicy) int {
			return strings.Compare(a.ToStorage+a.ToBucket, b.ToStorage+b.ToBucket)
		})
		return policies, nil
	})
}

// Add adds a new bucket replication policy. Validation must be done before calling this method.
// Can be part of a transaction.
func (r *BucketReplicationPolicyStore) AddOp(ctx context.Context, id entity.BucketReplicationPolicy) OperationStatus {
	addFromResult := r.from.SetFieldOp(ctx, id.User, id.FromBucket, id.FromStorage)
	toID := id.LookupID()
	toDest := id.Destination()
	addToResult := r.to.AddOp(ctx, toID, toDest)
	addDestIndexResult := r.bucketDestIndex.AddOp(ctx, id.User, toDest)
	return NewRedisOperationStatus(func() error {
		if _, err := addFromResult.Get(); err != nil {
			return fmt.Errorf("unable to add bucket replication source storage: %w", err)
		}
		if _, err := addToResult.Get(); err != nil {
			return fmt.Errorf("unable to add bucket replication destination storage: %w", err)
		}
		if _, err := addDestIndexResult.Get(); err != nil {
			return fmt.Errorf("unable to add bucket replication destination bucket to index: %w", err)
		}
		return nil
	})
}

func (r *BucketReplicationPolicyStore) RemoveOp(ctx context.Context, id entity.BucketReplicationPolicy) OperationStatus {
	fromKey, err := r.from.MakeKey(id.User)
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to make user replication source key: %w", err))
	}
	fromField := id.FromBucket
	toKey, err := r.to.MakeKey(id.LookupID())
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to make user replication destination key: %w", err))
	}
	toValue, err := r.to.serializer.ConvertSingle(id.Destination())
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to serialize user replication destination value: %w", err))
	}
	cmdRes := removeReplicationPolicyScript.Eval(ctx, r.from.client, []string{fromKey, toKey}, fromField, toValue)
	destRemResult := r.bucketDestIndex.RemoveOp(ctx, id.User, id.Destination())
	return NewRedisOperationStatus(func() error {
		err := cmdRes.Err()
		if err != nil {
			return fmt.Errorf("unable to remove bucket replication policy: %w", err)
		}
		if _, err := destRemResult.Get(); err != nil {
			return fmt.Errorf("unable to remove bucket replication destination from index: %w", err)
		}
		return nil
	})
}

func (r *BucketReplicationPolicyStore) ExistsForUser(ctx context.Context, user string) (bool, error) {
	sourcesNum, err := r.from.Len(ctx, user)
	return sourcesNum > 0, err
}

func (r *BucketReplicationPolicyStore) IsDestinationInUse(ctx context.Context, policy entity.BucketReplicationPolicy) (bool, error) {
	return r.bucketDestIndex.IsMember(ctx, policy.User, policy.Destination())
}
