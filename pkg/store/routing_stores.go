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

	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
)

/*

File describes routing representation in Redis.

1. Default - main storage from chorus config.

2. User level routing - global Redis Hash. See: UserRoutingStore
Key: p:route_user
Values: {
   <user>: <to_storage>,
   <user>: <to_storage>,
   ...
}

3. Bucket level routing - per-user Redis Hash. See: BucketRoutingStore
Key: p:route_bucket:<user>
Values: {
   <bucket> : <to_storage>,
   <bucket> : <to_storage>,
   ...
}


To support downtime, routing can be blocked per user or per bucket.
Blocks are stored in a separate Redis Structs to revert the block easily.

1. User level routing block - global Redis Set. See: UserRoutingBlockStore
Key: p:route_block_user
Values: { <user>, <user>, ... }

2. Bucket level routing block - per-user Redis Set. See: BucketRoutingBlockStore
Key: p:route_block_bucket:<user>
Values: { <bucket>, <bucket>, ... }

*/

type UserRoutingStore struct {
	// User-level routing destinations.
	// Key: p:route_user
	// Values: { <user>: <to_storage>, ...}
	route *RedisIDKeyHash[string, map[string]string]
	// User-level routing blocks.
	// Key: p:route_block_user
	// Values: { <user>, ...}
	block *RedisIDKeySet[string, string]
}

func NewUserRoutingStore(client redis.Cmdable) *UserRoutingStore {
	return &UserRoutingStore{
		route: NewRedisIDKeyHash[string, map[string]string](client, "p:route_user",
			StringToSingleTokenConverter, SingleTokenToStringConverter),
		block: NewRedisIDKeySet(client, "p:route_block_user",
			StringToSingleTokenConverter, SingleTokenToStringConverter,
			StringValueConverter, StringValueConverter),
	}
}

func (r *UserRoutingStore) TxExecutor() Executor[redis.Pipeliner] {
	return r.route.TxExecutor()
}

func (r *UserRoutingStore) WithExecutor(exec Executor[redis.Pipeliner]) *UserRoutingStore {
	return NewUserRoutingStore(exec.Get())
}

func (r *UserRoutingStore) GetOp(ctx context.Context, user string) OperationResult[string] {
	routeRes := r.route.GetFieldOp(ctx, "", user)
	blockRes := r.block.IsMemberOp(ctx, "", user)
	return NewRedisOperationResult(func() (string, error) {
		blocked, err := blockRes.Get()
		if err != nil && !errors.Is(err, dom.ErrNotFound) {
			return "", err
		}
		if blocked {
			return "", dom.ErrRoutingBlock
		}
		return routeRes.Get()
	})
}

func (r *UserRoutingStore) SetOp(ctx context.Context, user, toStorage string) OperationStatus {
	result := r.route.SetFieldOp(ctx, "", user, toStorage)
	return NewRedisOperationStatus(func() error {
		_, err := result.Get()
		return err
	})
}

func (r *UserRoutingStore) BlockOp(ctx context.Context, user string) OperationStatus {
	result := r.block.AddOp(ctx, "", user)
	return NewRedisOperationStatus(func() error {
		_, err := result.Get()
		return err
	})
}

func (r *UserRoutingStore) UnblockOp(ctx context.Context, user string) OperationStatus {
	result := r.block.RemoveOp(ctx, "", user)
	return NewRedisOperationStatus(func() error {
		_, err := result.Get()
		return err
	})
}

func (r *UserRoutingStore) DeleteOp(ctx context.Context, user string) OperationStatus {
	blockRes := r.block.RemoveOp(ctx, "", user)
	routeRes := r.route.DelFieldOp(ctx, "", user)
	return NewRedisOperationStatus(func() error {
		if _, err := blockRes.Get(); err != nil {
			return err
		}
		if _, err := routeRes.Get(); err != nil {
			return err
		}
		return nil
	})
}

type BucketRoutingStore struct {
	// Bucket-level routing destinations as per-user Redis Hash.
	// Key: p:route_bucket:<user>
	// Values: { <bucket> : <to_storage>, ...}
	route *RedisIDKeyHash[string, map[string]string]
	// Bucket-level routing blocks as per-user Redis Set.
	// Key: p:route_block_bucket:<user>
	// Values: { <bucket>, ...}
	block *RedisIDKeySet[string, string]
}

func NewBucketRoutingStore(client redis.Cmdable) *BucketRoutingStore {
	return &BucketRoutingStore{
		route: NewRedisIDKeyHash[string, map[string]string](client, "p:route_bucket",
			StringToSingleTokenConverter, SingleTokenToStringConverter),
		block: NewRedisIDKeySet(client, "p:route_block_bucket",
			StringToSingleTokenConverter, SingleTokenToStringConverter,
			StringValueConverter, StringValueConverter),
	}
}

func (r *BucketRoutingStore) TxExecutor() Executor[redis.Pipeliner] {
	return r.route.TxExecutor()
}

func (r *BucketRoutingStore) WithExecutor(exec Executor[redis.Pipeliner]) *BucketRoutingStore {
	return NewBucketRoutingStore(exec.Get())
}

// Can be part of a transaction.
func (r *BucketRoutingStore) GetOp(ctx context.Context, id entity.BucketRoutingPolicyID) OperationResult[string] {
	routeRes := r.route.GetFieldOp(ctx, id.User, id.Bucket)
	blockRes := r.block.IsMemberOp(ctx, id.User, id.Bucket)
	return NewRedisOperationResult(func() (string, error) {
		blocked, err := blockRes.Get()
		if err != nil && !errors.Is(err, dom.ErrNotFound) {
			return "", err
		}
		if blocked {
			return "", dom.ErrRoutingBlock
		}
		return routeRes.Get()
	})
}

// Can be part of a transaction.
func (r *BucketRoutingStore) SetOp(ctx context.Context, id entity.BucketRoutingPolicyID, toStorage string) OperationStatus {
	result := r.route.SetFieldOp(ctx, id.User, id.Bucket, toStorage)
	return NewRedisOperationStatus(func() error {
		_, err := result.Get()
		return err
	})
}

func (r *BucketRoutingStore) BlockOp(ctx context.Context, id entity.BucketRoutingPolicyID) OperationStatus {
	result := r.block.AddOp(ctx, id.User, id.Bucket)
	return NewRedisOperationStatus(func() error {
		_, err := result.Get()
		return err
	})
}

func (r *BucketRoutingStore) UnblockOp(ctx context.Context, id entity.BucketRoutingPolicyID) OperationStatus {
	result := r.block.RemoveOp(ctx, id.User, id.Bucket)
	return NewRedisOperationStatus(func() error {
		_, err := result.Get()
		return err
	})
}

func (r *BucketRoutingStore) DeleteOp(ctx context.Context, id entity.BucketRoutingPolicyID) OperationStatus {
	blockRes := r.block.RemoveOp(ctx, id.User, id.Bucket)
	routeRes := r.route.DelFieldOp(ctx, id.User, id.Bucket)
	return NewRedisOperationStatus(func() error {
		if _, err := blockRes.Get(); err != nil {
			return err
		}
		if _, err := routeRes.Get(); err != nil {
			return err
		}
		return nil
	})
}

// ---------------- Converters ----------------
func TokensToBucketRoutingPolicyIDConverter(tokens []string) (entity.BucketRoutingPolicyID, error) {
	return entity.BucketRoutingPolicyID{
		User:   tokens[0],
		Bucket: tokens[1],
	}, nil
}

func BucketRoutingPolicyIDToTokensConverter(id entity.BucketRoutingPolicyID) ([]string, error) {
	return []string{id.User, id.Bucket}, nil
}
