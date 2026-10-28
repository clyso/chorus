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
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
)

/*
Replication generation is a fencing token for replication tasks.
It is incremented on every replication create and delete. Each replication task carries
the generation of the replication it was created for, and the worker drops tasks
with a generation different from the current one.

Generations are never deleted, so a generation value is never reused for the same replication ID.
Missing generation equals 0, which is also the generation of tasks created before generations existed.

Generations are stored next to replication policy lookup keys, so proxy reads them in the same
transaction as replication policies (see replication_stores.go):

1. User replication generations stored in per-user Redis Hash:
Key: p:repl_user_gen:<user>
Value: { <to_storage>: <generation>, ... }

2. Bucket replication generations stored in per-user per-bucket Redis Hash:
Key: p:repl_bucket_gen:<user>:<from_bucket>
Value: { "<to_storage>:<to_bucket>": <generation>, ... }
*/

type ReplicationGenerationStore struct {
	// Key: p:repl_user_gen:<user>
	// Value: { <to_storage>: <generation>, ... }
	user *RedisIDKeyHash[string, map[string]string]
	// Key: p:repl_bucket_gen:<user>:<from_bucket>
	// Value: { "<to_storage>:<to_bucket>": <generation>, ... }
	bucket *RedisIDKeyHash[entity.BucketReplicationPolicyID, map[string]string]
}

func NewReplicationGenerationStore(client redis.Cmdable) *ReplicationGenerationStore {
	return &ReplicationGenerationStore{
		user: NewRedisIDKeyHash[string, map[string]string](client, "p:repl_user_gen",
			StringToSingleTokenConverter, SingleTokenToStringConverter),
		bucket: NewRedisIDKeyHash[entity.BucketReplicationPolicyID, map[string]string](client, "p:repl_bucket_gen",
			BucketReplicationPolicyIDToTokensConverter, TokensToBucketReplicationPolicyIDConverter),
	}
}

func (r *ReplicationGenerationStore) TxExecutor() Executor[redis.Pipeliner] {
	return r.user.TxExecutor()
}

func (r *ReplicationGenerationStore) WithExecutor(exec Executor[redis.Pipeliner]) *ReplicationGenerationStore {
	return NewReplicationGenerationStore(exec.Get())
}

func (r *ReplicationGenerationStore) IncrementOp(ctx context.Context, id entity.UniversalReplicationID) OperationResult[int64] {
	if bucketID, ok := id.AsBucketID(); ok {
		field, err := BucketReplicationPolicyDestinationToStringConverter(bucketID.Destination())
		if err != nil {
			return NewRedisFailedOperationResult[int64](fmt.Errorf("unable to make bucket replication generation field: %w", err))
		}
		return r.bucket.IncrementFieldOp(ctx, bucketID.LookupID(), field)
	}
	if userID, ok := id.AsUserID(); ok {
		return r.user.IncrementFieldOp(ctx, userID.User, userID.ToStorage)
	}
	return NewRedisFailedOperationResult[int64](fmt.Errorf("%w: invalid replication ID %#v", dom.ErrInvalidArg, id))
}

func (r *ReplicationGenerationStore) Get(ctx context.Context, id entity.UniversalReplicationID) (int64, error) {
	return r.GetOp(ctx, id).Get()
}

func (r *ReplicationGenerationStore) GetOp(ctx context.Context, id entity.UniversalReplicationID) OperationResult[int64] {
	var res OperationResult[string]
	if bucketID, ok := id.AsBucketID(); ok {
		field, err := BucketReplicationPolicyDestinationToStringConverter(bucketID.Destination())
		if err != nil {
			return NewRedisFailedOperationResult[int64](fmt.Errorf("unable to make bucket replication generation field: %w", err))
		}
		res = r.bucket.GetFieldOp(ctx, bucketID.LookupID(), field)
	} else if userID, ok := id.AsUserID(); ok {
		res = r.user.GetFieldOp(ctx, userID.User, userID.ToStorage)
	} else {
		return NewRedisFailedOperationResult[int64](fmt.Errorf("%w: invalid replication ID %#v", dom.ErrInvalidArg, id))
	}
	return NewRedisOperationResult(func() (int64, error) {
		value, err := res.Get()
		if errors.Is(err, dom.ErrNotFound) {
			return 0, nil
		}
		if err != nil {
			return 0, fmt.Errorf("unable to get replication generation: %w", err)
		}
		return strconv.ParseInt(value, 10, 64)
	})
}

func (r *ReplicationGenerationStore) GetUserOp(ctx context.Context, user string) OperationResult[map[string]int64] {
	res := r.user.GetOp(ctx, user)
	return NewRedisOperationResult(func() (map[string]int64, error) {
		return parseGenerations(res, StringValueConverter)
	})
}

func (r *ReplicationGenerationStore) GetBucketOp(ctx context.Context, id entity.BucketReplicationPolicyID) OperationResult[map[entity.BucketReplicationPolicyDestination]int64] {
	res := r.bucket.GetOp(ctx, id)
	return NewRedisOperationResult(func() (map[entity.BucketReplicationPolicyDestination]int64, error) {
		return parseGenerations(res, StringToBucketReplicationPolicyDestinationConverter)
	})
}

func parseGenerations[K comparable](res OperationResult[map[string]string], parseField func(string) (K, error)) (map[K]int64, error) {
	values, err := res.Get()
	if errors.Is(err, dom.ErrNotFound) {
		return map[K]int64{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("unable to get replication generations: %w", err)
	}
	result := make(map[K]int64, len(values))
	for field, value := range values {
		key, err := parseField(field)
		if err != nil {
			return nil, fmt.Errorf("unable to parse replication generation field %q: %w", field, err)
		}
		gen, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("unable to parse replication generation %q: %w", field, err)
		}
		result[key] = gen
	}
	return result, nil
}
