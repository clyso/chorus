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
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
)

/*
Replication switch is the final step of storage migration.
It controls both routing and replication policies to switch from old to new storage.

Same as routing and replication policies, switch can be bucket-level or user-level.

All switches have the following restrictions:
 - User/bucket level switch can be created only if corresponding replication policy exists
 - Switch cannot be created if bucket replication policy has custom destination bucket name.
   Reason: not clear how to handle routing after switch.
 - Switch cannot be created if replication policy has multiple destination storages.
   Reason: it is tricky to continue replication after switch.

There are two types of switches:
1. Downtime switch:
  - it sets routing block
  - waits replication to finish
  - optionally checks data integrity
  - switches routing policy to replication destination and removes routing block.
2. Zero-downtime switch:
  - immediately switches routing policy to replication destination
  - resolves in-flight read requests to the storage with the latest data until replication is finished
  - when replication is finished, it stops in-flight resolving reads and simply routes all requests to the new storage.

--------------------------------------------------------------------------
The main difference is that Zero-downtime affects routing INDIRECTLY.
Proxy should know if zero-downtime switch is in progress to adjust read requests based on object versions in storages.
--------------------------------------------------------------------------
For quick proxy lookups Redis maintains a separate SET with active zero-downtime switches.
- User-level:
Key: p:repl_user_switch_zd_active
Value: { <user>, <user>, ... }
- Bucket-level:
Key: p:repl_bucket_switch_zd_active:<user>
Value: { <bucket>, <bucket>, ... }


Other than that, zero and non-zero downtime switches are stored the same way:

1. Redis stores switch status and configuration in Redis Hashes.
a) User-level switch:
Key: p:repl_switch_user:<user>
Value: { key: value, ... }

b) Bucket-level switch:
Key: p:repl_switch_bucket:<user>:<bucket>
Value: { key: value, ... }

NOTE: there is no index for switches. Because switch is always linked to replication policy,
      we can always find switch by looking up replication policy.

2. Switch status change history is stored in Redis lists.
a) User-level switch history:
Key: p:repl_switch_user_history:<user>
Value: [ "status transition message", ... ]

b) Bucket-level switch history:
Key: p:repl_switch_bucket_history:<user>:<bucket>
Value: [ "status transition message", ... ]

*/

type UserReplicationSwitchStore struct {
	// Active zero-downtime user-level switches.
	// Key: p:repl_user_switch_zd_active
	// Value: { <user>, ... }
	activeZeroDTIndex *RedisIDKeySet[string, string]
	// User-level switch config and status.
	// Key: p:repl_switch_user:<user>
	// Value: { key: value, ... }
	info *RedisIDKeyHash[string, entity.ReplicationSwitchInfo]
	// User-level switch status change history.
	// Key: p:repl_switch_user_history:<user>
	// Value: [ "status transition message", ... ]
	history *RedisIDKeyList[string, string]
}

func NewUserReplicationSwitchStore(client redis.Cmdable) *UserReplicationSwitchStore {
	return &UserReplicationSwitchStore{
		activeZeroDTIndex: NewRedisIDKeySet[string, string](
			client, "p:repl_user_switch_zd_active",
			StringToSingleTokenConverter, SingleTokenToStringConverter,
			StringValueConverter, StringValueConverter),
		info: NewRedisIDKeyHash[string, entity.ReplicationSwitchInfo](
			client, "p:repl_switch_user",
			StringToSingleTokenConverter, SingleTokenToStringConverter),
		history: NewRedisIDKeyList[string, string](
			client, "p:repl_switch_user_history",
			StringToSingleTokenConverter, SingleTokenToStringConverter,
			StringValueConverter, StringValueConverter),
	}
}

func (r *UserReplicationSwitchStore) TxExecutor() Executor[redis.Pipeliner] {
	return r.activeZeroDTIndex.TxExecutor()
}

func (r *UserReplicationSwitchStore) WithExecutor(exec Executor[redis.Pipeliner]) *UserReplicationSwitchStore {
	return NewUserReplicationSwitchStore(exec.Get())
}

func (r *UserReplicationSwitchStore) IsZeroDowntimeActiveOp(ctx context.Context, user string) OperationResult[bool] {
	return r.activeZeroDTIndex.IsMemberOp(ctx, "", user)
}

func (r *UserReplicationSwitchStore) DeleteOp(ctx context.Context, user string) OperationStatus {
	res := map[string]OperationStatus{}
	res["remove index"] = r.activeZeroDTIndex.RemoveOp(ctx, "", user).Status()
	res["remove info"] = r.info.DropOp(ctx, user).Status()
	res["drop history"] = r.history.DropOp(ctx, user).Status()
	return JoinRedisOperationStatusWithMessages("delete user replication switch", res)
}

func (r *UserReplicationSwitchStore) Get(ctx context.Context, user string) (entity.ReplicationSwitchInfo, error) {
	tx := r.info.TxExecutor()
	result := r.WithExecutor(tx).GetOp(ctx, user)
	_ = tx.Exec(ctx)
	return result.Get()
}

func (r *UserReplicationSwitchStore) GetOp(ctx context.Context, user string) OperationResult[entity.ReplicationSwitchInfo] {
	key, err := r.info.MakeKey(user)
	if err != nil {
		return NewRedisFailedOperationResult[entity.ReplicationSwitchInfo](fmt.Errorf("unable to make key: %w", err))
	}
	infoRes := r.info.client.HGetAll(ctx, key)
	histRes := r.history.GetAllOp(ctx, user)
	return mapToSwitchInfo(infoRes, histRes)
}

func mapToSwitchInfo(infoRes *redis.MapStringStringCmd, historyRes OperationResult[[]string]) OperationResult[entity.ReplicationSwitchInfo] {
	return NewRedisOperationResult(func() (entity.ReplicationSwitchInfo, error) {
		err := infoRes.Err()
		if errors.Is(err, redis.Nil) {
			return entity.ReplicationSwitchInfo{}, fmt.Errorf("%w: %w", dom.ErrNotFound, err)
		}
		if err != nil {
			return entity.ReplicationSwitchInfo{}, fmt.Errorf("unable to get hash map: %w", err)
		}
		if len(infoRes.Val()) == 0 {
			return entity.ReplicationSwitchInfo{}, dom.ErrNotFound
		}
		var info entity.ReplicationSwitchInfo
		if err := infoRes.Scan(&info); err != nil {
			return entity.ReplicationSwitchInfo{}, fmt.Errorf("unable to scan replication switch info: %w", err)
		}
		if err := infoRes.Scan(&info.ReplicationSwitchDowntimeOpts); err != nil {
			return entity.ReplicationSwitchInfo{}, fmt.Errorf("unable to scan replication switch info downtime opts: %w", err)
		}
		if err := infoRes.Scan(&info.ReplicationSwitchZeroDowntimeOpts); err != nil {
			return entity.ReplicationSwitchInfo{}, fmt.Errorf("unable to scan replication switch info zero downtime opts: %w", err)
		}
		history, err := historyRes.Get()
		if err != nil && !errors.Is(err, dom.ErrNotFound) {
			return entity.ReplicationSwitchInfo{}, fmt.Errorf("unable to get history: %w", err)
		}
		info.History = history
		return info, nil

	})
}

func (r *UserReplicationSwitchStore) Create(ctx context.Context, replicationID entity.UserReplicationPolicy, value entity.ReplicationSwitchInfo) error {
	tx := r.info.TxExecutor()
	result := r.WithExecutor(tx).CreateOp(ctx, replicationID, value)
	_ = tx.Exec(ctx)
	return result.Get()
}

func (r *UserReplicationSwitchStore) CreateOp(ctx context.Context, replicationID entity.UserReplicationPolicy, value entity.ReplicationSwitchInfo) OperationStatus {
	value.SetReplicationID(entity.UniversalFromUserReplication(replicationID))
	value.CreatedAt = entity.TimeNow()
	if err := value.Validate(); err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("invalid replication switch info: %w", err))
	}
	infoRes := r.info.SetOp(ctx, replicationID.User, value)

	key, err := r.info.MakeKey(replicationID.User)
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to make key: %w", err))
	}
	emptyDowntimeOpts := entity.ReplicationSwitchDowntimeOpts{}
	var indexRes OperationResult[uint64]
	var optsRes *redis.IntCmd
	if value.IsZeroDowntime() {
		if value.LastStatus != entity.StatusInProgress {
			return NewRedisFailedOperationStatus(fmt.Errorf("zero-downtime switch must start in InProgress status"))
		}
		indexRes = r.activeZeroDTIndex.AddOp(ctx, "", replicationID.User)
		optsRes = r.info.client.HSet(ctx, key, value.ReplicationSwitchZeroDowntimeOpts)
		_ = r.info.client.HSet(ctx, key, "startedAt", entity.TimeNow())
	} else if value.ReplicationSwitchDowntimeOpts != emptyDowntimeOpts {
		if value.LastStatus != entity.StatusNotStarted {
			return NewRedisFailedOperationStatus(fmt.Errorf("downtime switch must start in NotStarted status"))
		}
		optsRes = r.info.client.HSet(ctx, key, value.ReplicationSwitchDowntimeOpts)
	}
	return NewRedisOperationStatus(func() error {
		if err := infoRes.Get(); err != nil {
			return fmt.Errorf("unable to set switch info: %w", err)
		}
		if indexRes != nil {
			if _, err := indexRes.Get(); err != nil {
				return fmt.Errorf("unable to update active zero-downtime index: %w", err)
			}
		}
		if optsRes != nil {
			if err := optsRes.Err(); err != nil {
				return fmt.Errorf("unable to set switch options: %w", err)
			}
		}
		return nil
	})
}

func (r *UserReplicationSwitchStore) UpdateStatusOp(ctx context.Context, replicationID entity.UserReplicationPolicy, prevStatus, newStatus entity.ReplicationSwitchStatus, message string) OperationStatus {
	res := map[string]OperationStatus{}
	res["update last status"] = r.info.SetFieldOp(ctx, replicationID.LookupID(), "lastStatus", newStatus).Status()
	if newStatus == entity.StatusInProgress {
		res["set startedAt"] = r.info.SetFieldOp(ctx, replicationID.LookupID(), "startedAt", entity.TimeNow()).Status()
	}
	if newStatus == entity.StatusDone {
		res["set doneAt"] = r.info.SetFieldOp(ctx, replicationID.LookupID(), "doneAt", entity.TimeNow()).Status()
	}
	if newStatus != entity.StatusInProgress {
		res["remove index"] = r.activeZeroDTIndex.RemoveOp(ctx, "", replicationID.User).Status()
	}
	history := fmt.Sprintf("%s | %s -> %s: %s", entity.TimeNow().Format(time.RFC3339), prevStatus, newStatus, message)
	res["add history"] = r.history.AddRightOp(ctx, replicationID.LookupID(), history)

	return JoinRedisOperationStatusWithMessages("update switch status", res)
}

func (r *UserReplicationSwitchStore) UpdateDowntimeOpts(ctx context.Context, replication entity.UserReplicationPolicy, value *entity.ReplicationSwitchDowntimeOpts) error {
	tx := r.info.TxExecutor()
	result := r.WithExecutor(tx).UpdateDowntimeOptsOp(ctx, replication, value)
	_ = tx.Exec(ctx)
	return result.Get()
}

func (r *UserReplicationSwitchStore) UpdateDowntimeOptsOp(ctx context.Context, id entity.UserReplicationPolicy, value *entity.ReplicationSwitchDowntimeOpts) OperationStatus {
	var fieldMap map[string]any
	if value != nil {
		fieldMap = makeRedisFieldMap(*value)
	} else {
		fieldMap = makeRedisFieldMap(entity.ReplicationSwitchDowntimeOpts{})
	}
	res := make(map[string]OperationResult[uint64], len(fieldMap))
	for k, v := range fieldMap {
		if value == nil || v == nil {
			res[k] = r.info.DelFieldOp(ctx, id.User, k)
		} else {
			res[k] = r.info.SetFieldOp(ctx, id.User, k, v)
		}
	}

	collectFunc := func() error {
		for k, r := range res {
			if _, err := r.Get(); err != nil {
				return fmt.Errorf("unable to update downtime opts field %s: %w", k, err)
			}
		}
		return nil
	}

	return NewRedisOperationStatus(collectFunc)
}

type BucketReplicationSwitchStore struct {
	// Active zero-downtime bucket-level switches.
	// Key: p:repl_bucket_switch_zd_active:<user>
	// Value: { <bucket>, ... }
	activeZeroDTIndex *RedisIDKeySet[string, string]
	// Bucket-level switch config and status.
	// Key: p:repl_switch_bucket:<user>:<bucket>
	// Value: { key: value, ... }
	info *RedisIDKeyHash[entity.BucketReplicationPolicyID, entity.ReplicationSwitchInfo]
	// Bucket-level switch status change history.
	// Key: p:repl_switch_bucket_history:<user>:<bucket>
	// Value: [ "status transition message", ... ]
	history *RedisIDKeyList[entity.BucketReplicationPolicyID, string]
}

func NewBucketReplicationSwitchStore(client redis.Cmdable) *BucketReplicationSwitchStore {
	return &BucketReplicationSwitchStore{
		activeZeroDTIndex: NewRedisIDKeySet[string, string](
			client, "p:repl_bucket_switch_zd_active",
			StringToSingleTokenConverter, SingleTokenToStringConverter,
			StringValueConverter, StringValueConverter),
		info: NewRedisIDKeyHash[entity.BucketReplicationPolicyID, entity.ReplicationSwitchInfo](
			client, "p:repl_switch_bucket",
			BucketReplicationPolicyIDToTokensConverter, TokensToBucketReplicationPolicyIDConverter),
		history: NewRedisIDKeyList[entity.BucketReplicationPolicyID, string](
			client, "p:repl_switch_bucket_history",
			BucketReplicationPolicyIDToTokensConverter, TokensToBucketReplicationPolicyIDConverter,
			StringValueConverter, StringValueConverter),
	}
}

func (r *BucketReplicationSwitchStore) TxExecutor() Executor[redis.Pipeliner] {
	return r.activeZeroDTIndex.TxExecutor()
}

func (r *BucketReplicationSwitchStore) WithExecutor(exec Executor[redis.Pipeliner]) *BucketReplicationSwitchStore {
	return NewBucketReplicationSwitchStore(exec.Get())
}

func (r *BucketReplicationSwitchStore) IsZeroDowntimeActiveOp(ctx context.Context, id entity.BucketReplicationPolicyID) OperationResult[bool] {
	return r.activeZeroDTIndex.IsMemberOp(ctx, id.User, id.FromBucket)
}

func (r *BucketReplicationSwitchStore) DeleteOp(ctx context.Context, id entity.BucketReplicationPolicyID) OperationStatus {
	res := map[string]OperationStatus{}
	res["delete zero downtime index"] = r.activeZeroDTIndex.RemoveOp(ctx, id.User, id.FromBucket).Status()
	res["delete info"] = r.info.DropOp(ctx, id).Status()
	res["drop history"] = r.history.DropOp(ctx, id).Status()
	return JoinRedisOperationStatusWithMessages("delete bucket replication switch", res)
}

func (r *BucketReplicationSwitchStore) Get(ctx context.Context, id entity.BucketReplicationPolicyID) (entity.ReplicationSwitchInfo, error) {
	tx := r.info.TxExecutor()
	result := r.WithExecutor(tx).GetOp(ctx, id)
	_ = tx.Exec(ctx)
	return result.Get()
}

func (r *BucketReplicationSwitchStore) GetOp(ctx context.Context, id entity.BucketReplicationPolicyID) OperationResult[entity.ReplicationSwitchInfo] {
	key, err := r.info.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationResult[entity.ReplicationSwitchInfo](fmt.Errorf("unable to make key: %w", err))
	}
	infoRes := r.info.client.HGetAll(ctx, key)
	histRes := r.history.GetAllOp(ctx, id)
	return mapToSwitchInfo(infoRes, histRes)
}

func (r *BucketReplicationSwitchStore) Create(ctx context.Context, replicationID entity.BucketReplicationPolicy, value entity.ReplicationSwitchInfo) error {
	tx := r.info.TxExecutor()
	result := r.WithExecutor(tx).CreateOp(ctx, replicationID, value)
	_ = tx.Exec(ctx)
	return result.Get()
}

func (r *BucketReplicationSwitchStore) CreateOp(ctx context.Context, replicationID entity.BucketReplicationPolicy, value entity.ReplicationSwitchInfo) OperationStatus {
	value.SetReplicationID(entity.UniversalFromBucketReplication(replicationID))
	value.CreatedAt = entity.TimeNow()
	if err := value.Validate(); err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("invalid replication switch info: %w", err))
	}
	id := replicationID.LookupID()
	infoRes := r.info.SetOp(ctx, id, value)

	key, err := r.info.MakeKey(id)
	if err != nil {
		return NewRedisFailedOperationStatus(fmt.Errorf("unable to make key: %w", err))
	}
	emptyDowntimeOpts := entity.ReplicationSwitchDowntimeOpts{}
	var indexRes OperationResult[uint64]
	var optsRes *redis.IntCmd
	if value.IsZeroDowntime() {
		if value.LastStatus != entity.StatusInProgress {
			return NewRedisFailedOperationStatus(fmt.Errorf("zero-downtime switch must start in InProgress status"))
		}
		indexRes = r.activeZeroDTIndex.AddOp(ctx, id.User, id.FromBucket)
		optsRes = r.info.client.HSet(ctx, key, value.ReplicationSwitchZeroDowntimeOpts)
		_ = r.info.client.HSet(ctx, key, "startedAt", entity.TimeNow())
	} else if value.ReplicationSwitchDowntimeOpts != emptyDowntimeOpts {
		if value.LastStatus != entity.StatusNotStarted {
			return NewRedisFailedOperationStatus(fmt.Errorf("downtime switch must start in NotStarted status"))
		}
		optsRes = r.info.client.HSet(ctx, key, value.ReplicationSwitchDowntimeOpts)
	}
	return NewRedisOperationStatus(func() error {
		if err := infoRes.Get(); err != nil {
			return fmt.Errorf("unable to set switch info: %w", err)
		}
		if indexRes != nil {
			if _, err := indexRes.Get(); err != nil {
				return fmt.Errorf("unable to update active zero-downtime index: %w", err)
			}
		}
		if optsRes != nil {
			if err := optsRes.Err(); err != nil {
				return fmt.Errorf("unable to set switch options: %w", err)
			}
		}
		return nil
	})
}

func (r *BucketReplicationSwitchStore) UpdateStatusOp(ctx context.Context, replicationID entity.BucketReplicationPolicy, prevStatus, newStatus entity.ReplicationSwitchStatus, message string) OperationStatus {
	res := map[string]OperationStatus{}
	res["update last status"] = r.info.SetFieldOp(ctx, replicationID.LookupID(), "lastStatus", newStatus).Status()
	if newStatus == entity.StatusInProgress {
		res["set startedAt"] = r.info.SetFieldOp(ctx, replicationID.LookupID(), "startedAt", entity.TimeNow()).Status()
	}
	if newStatus == entity.StatusDone {
		res["set doneAt"] = r.info.SetFieldOp(ctx, replicationID.LookupID(), "doneAt", entity.TimeNow()).Status()
	}
	if newStatus != entity.StatusInProgress {
		res["remove index"] = r.activeZeroDTIndex.RemoveOp(ctx, replicationID.User, replicationID.FromBucket).Status()
	}
	history := fmt.Sprintf("%s | %s -> %s: %s", entity.TimeNow().Format(time.RFC3339), prevStatus, newStatus, message)
	res["add history"] = r.history.AddRightOp(ctx, replicationID.LookupID(), history)

	return JoinRedisOperationStatusWithMessages("update switch status", res)
}

func (r *BucketReplicationSwitchStore) UpdateDowntimeOpts(ctx context.Context, replication entity.BucketReplicationPolicy, value *entity.ReplicationSwitchDowntimeOpts) error {
	tx := r.info.TxExecutor()
	result := r.WithExecutor(tx).UpdateDowntimeOptsOp(ctx, replication, value)
	_ = tx.Exec(ctx)
	return result.Get()
}

func (r *BucketReplicationSwitchStore) UpdateDowntimeOptsOp(ctx context.Context, replication entity.BucketReplicationPolicy, value *entity.ReplicationSwitchDowntimeOpts) OperationStatus {
	id := entity.BucketReplicationPolicyID{
		User:       replication.User,
		FromBucket: replication.FromBucket,
	}
	var fieldMap map[string]any
	if value != nil {
		fieldMap = makeRedisFieldMap(*value)
	} else {
		fieldMap = makeRedisFieldMap(entity.ReplicationSwitchDowntimeOpts{})
	}
	res := make(map[string]OperationResult[uint64], len(fieldMap))
	for k, v := range fieldMap {
		if value == nil || v == nil {
			res[k] = r.info.DelFieldOp(ctx, id, k)
		} else {
			res[k] = r.info.SetFieldOp(ctx, id, k, v)
		}
	}

	collectFunc := func() error {
		for k, r := range res {
			if _, err := r.Get(); err != nil {
				return fmt.Errorf("unable to update downtime opts field %s: %w", k, err)
			}
		}
		return nil
	}

	return NewRedisOperationStatus(collectFunc)
}
