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
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
)

var (
	luaHIncrByEx = redis.NewScript(`if redis.call('exists',KEYS[1]) == 1 then return redis.call("hincrby", KEYS[1], ARGV[1], ARGV[2]) else return 0 end`)
	luaHSetEx    = redis.NewScript(`if redis.call('exists',KEYS[1]) == 1 then redis.call("hset", KEYS[1], ARGV[1], ARGV[2]); return 1 else return 0 end`)

	luaUpdateTsIfGreater = redis.NewScript(`local function YearInSec(y)
  if ((y % 400) == 0) or (((y % 4) == 0) and ((y % 100) ~= 0)) then
    return 31622400 -- 366 * 24 * 60 * 60
  else
    return 31536000 -- 365 * 24 * 60 * 60
  end
end
local function IsLeapYear(y)
  return ((y % 400) == 0) or (((y % 4) == 0) and ((y % 100) ~= 0))
end

local function unixtime(date)
  local days = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
  local daysec = 86400
  local y,m,d,h,mi,s = date:match("(%d+)-(%d+)-(%d+)T(%d+):(%d+):(%d+)%..+") 
  local time = 0
  for n = 1970,(y - 1) do 
    time = time + YearInSec(n)
  end
  for n = 1,(m - 1) do
    time = time + (days[n] * daysec)
  end
  time = time + (d - 1) * daysec
  if IsLeapYear(y) and (m + 0 > 2) then
    time = time + daysec
  end
  return time + (h * 3600) + (mi * 60) + s
end

local prev = redis.call("hget", KEYS[1], ARGV[1])
if not prev or unixtime(prev) < unixtime(ARGV[2]) then return redis.call("hset", KEYS[1], ARGV[1],ARGV[2]) else return 0 end`)
)

type ReplicationPolicyStatus struct {
	CreatedAt       time.Time `redis:"created_at"`
	IsPaused        bool      `redis:"paused"`
	IsArchived      bool      `redis:"archived"`
	InitObjListed   int64     `redis:"obj_listed"`
	InitObjDone     int64     `redis:"obj_done"`
	InitBytesListed int64     `redis:"bytes_listed"`
	InitBytesDone   int64     `redis:"bytes_done"`
	Events          int64     `redis:"events"`
	EventsDone      int64     `redis:"events_done"`
	AgentURL        string    `redis:"agent_url,omitempty"`

	InitDoneAt      *time.Time `redis:"init_done_at,omitempty"`
	LastEmittedAt   *time.Time `redis:"last_emitted_at,omitempty"`
	LastProcessedAt *time.Time `redis:"last_processed_at,omitempty"`
	ArchivedAt      *time.Time `redis:"archived_at,omitempty"`

	ListingStarted bool `redis:"listing_started"`

	HasSwitch bool `redis:"-"`
}

func (r *ReplicationPolicyStatus) InitDone() bool {
	return r.ListingStarted && r.InitDoneAt != nil && r.InitObjDone >= r.InitObjListed
}

type ReplicationPolicyStatusExtended struct {
	ReplicationPolicyStatus
	User     string
	Bucket   string
	From     string
	To       string
	ToBucket string
}

type ReplicationPolicies struct {
	From string
	To   map[ReplicationPolicyDest]tasks.Priority
}

type ReplicationPolicyDest string

func (d ReplicationPolicyDest) Parse() (storage string, bucket string) {
	if arr := strings.Split(string(d), ":"); len(arr) == 2 {
		return arr[0], arr[1]
	}
	return string(d), ""
}

func ReplicationIDFromStr(s string) (ReplicationID, error) {
	if s == "" {
		return ReplicationID{}, fmt.Errorf("%w: replication id is empty", dom.ErrInvalidArg)
	}
	s = strings.TrimPrefix(s, "p:repl_st:")
	arr := strings.Split(s, ":")
	if len(arr) < 4 {
		return ReplicationID{}, fmt.Errorf("%w: invalid replication id %q", dom.ErrInvalidArg, s)
	}
	res := ReplicationID{
		User:        arr[0],
		FromBucket:  arr[1],
		FromStorage: arr[2],
		ToStorage:   arr[3],
		ToBucket:    arr[4],
	}
	return res, nil
}

// TODO: refactor Service interface to use ReplicationID instead of separate user, bucket, from, to, toBucket arguments
type ReplicationID struct {
	User        string
	FromStorage string
	FromBucket  string
	ToStorage   string
	ToBucket    string
}

func (r ReplicationID) Validate() error {
	if r.User == "" {
		return fmt.Errorf("%w: user is required", dom.ErrInvalidArg)
	}
	if r.FromStorage == "" {
		return fmt.Errorf("%w: from is required", dom.ErrInvalidArg)
	}
	if r.FromBucket == "" {
		return fmt.Errorf("%w: bucket is required", dom.ErrInvalidArg)
	}
	if r.ToStorage == "" {
		return fmt.Errorf("%w: to is required", dom.ErrInvalidArg)
	}
	if r.ToBucket == "" {
		return fmt.Errorf("%w: toBucket cannot be empty", dom.ErrInvalidArg)
	}
	//check that fields do not contain ":"
	if strings.ContainsAny(r.User, ":") {
		return fmt.Errorf("%w: user cannot contain ':'", dom.ErrInvalidArg)
	}
	if strings.ContainsAny(r.FromBucket, ":") {
		return fmt.Errorf("%w: bucket cannot contain ':'", dom.ErrInvalidArg)
	}
	if strings.ContainsAny(r.FromStorage, ":") {
		return fmt.Errorf("%w: from cannot contain ':'", dom.ErrInvalidArg)
	}
	if strings.ContainsAny(r.ToStorage, ":") {
		return fmt.Errorf("%w: to cannot contain ':'", dom.ErrInvalidArg)
	}
	if strings.ContainsAny(r.ToBucket, ":") {
		return fmt.Errorf("%w: toBucket cannot contain ':'", dom.ErrInvalidArg)
	}
	return nil
}

func (r ReplicationID) String() string {
	return fmt.Sprintf("%s:%s:%s:%s:%s", r.User, r.FromBucket, r.FromStorage, r.ToStorage, r.ToBucket)
}

func (r ReplicationID) StatusKey() string {
	return "p:repl_st:" + r.String()
}

func (r ReplicationID) SwitchKey() string {
	return fmt.Sprintf("p:switch:%s:%s", r.User, r.FromBucket)
}

func (r ReplicationID) SwitchHistoryKey() string {
	return fmt.Sprintf("p:switch-hist:%s:%s", r.User, r.FromBucket)
}

func (r ReplicationID) RoutingKey() string {
	return fmt.Sprintf("p:route:%s:%s", r.User, r.FromBucket)
}

// // go:generate go tool mockery --name=Service --filename=service_mock.go --inpackage --structname=MockService
type Service interface {
	// -------------- Routing policy related methods: --------------

	// GetRoutingPolicy returns destination storage name.
	// Errors:
	//   dom.ErrRoutingBlock - if access to bucket should be blocked because bucket is used as replication destination.
	//   dom.ErrNotFound - if replication is not configured.
	GetRoutingPolicy(ctx context.Context, user, bucket string) (string, error)
	isRoutingBlocked(ctx context.Context, storage, bucket string) (bool, error)
	AddRoutingBlock(ctx context.Context, storage, bucket string) error
	DeleteRoutingBlock(ctx context.Context, storage, bucket string) error
	getBucketRoutingPolicy(ctx context.Context, user, bucket string) (string, error)
	addBucketRoutingPolicy(ctx context.Context, user, bucket, toStorage string, replace bool) error
	GetUserRoutingPolicy(ctx context.Context, user string) (string, error)
	AddUserRoutingPolicy(ctx context.Context, user, toStorage string) error

	// -------------- Replication switch related methods: --------------

	// Upsert downtime replication switch. If switch already exists and not in progress, it will be updated.
	SetDowntimeReplicationSwitch(ctx context.Context, replID entity.ReplicationStatusID, opts *SwitchDowntimeOpts) error
	// Change downtime replication switch status. Makes required adjustments to routing and replication policies.
	// According to switch status and configured options.
	UpdateDowntimeSwitchStatus(ctx context.Context, replID entity.ReplicationStatusID, newStatus SwitchStatus, description string, startedAt, doneAt *time.Time) error
	// Creates new zero downtime replication switch.
	AddZeroDowntimeReplicationSwitch(ctx context.Context, replID entity.ReplicationStatusID, opts *SwitchZeroDowntimeOpts) error
	// Completes zero downtime replication switch.
	CompleteZeroDowntimeReplicationSwitch(ctx context.Context, replID entity.ReplicationStatusID) error
	// Deletes any replication switch if exists and reverts routing policy if switch was not done.
	DeleteReplicationSwitch(ctx context.Context, replID entity.ReplicationStatusID) error
	// Returns replication switch config and status information.
	GetReplicationSwitchInfo(ctx context.Context, replID entity.ReplicationStatusID) (SwitchInfo, error)
	ListReplicationSwitchInfo(ctx context.Context) ([]SwitchInfo, error)
	// GetInProgressZeroDowntimeSwitchInfo shortcut method for chorus proxy to get required information
	// to adjust route only when zero downtime switch is in progress.
	GetInProgressZeroDowntimeSwitchInfo(ctx context.Context, user, bucket string) (ZeroDowntimeSwitchInProgressInfo, error)

	// -------------- Replication policy related methods: --------------

	GetBucketReplicationPolicies(ctx context.Context, user, bucket string) (ReplicationPolicies, error)
	GetUserReplicationPolicies(ctx context.Context, user string) (ReplicationPolicies, error)
	AddUserReplicationPolicy(ctx context.Context, user string, from string, to string, priority tasks.Priority) error
	DeleteUserReplication(ctx context.Context, user string, from string, to string) error

	AddBucketReplicationPolicy(ctx context.Context, id entity.ReplicationStatusID, priority tasks.Priority, agentURL *string) error
	GetReplicationPolicyInfo(ctx context.Context, id entity.ReplicationStatusID) (ReplicationPolicyStatus, error)
	ListReplicationPolicyInfo(ctx context.Context) ([]ReplicationPolicyStatusExtended, error)
	IsReplicationPolicyExists(ctx context.Context, id entity.ReplicationStatusID) (bool, error)
	IsReplicationPolicyPaused(ctx context.Context, id entity.ReplicationStatusID) (bool, error)
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
	ArchiveReplication(ctx context.Context, replID entity.ReplicationStatusID) error
	DeleteBucketReplicationsByUser(ctx context.Context, user, from string, to string) ([]string, error)
}

func NewService(client redis.UniversalClient) Service {
	return &policySvc{client: client}
}

type policySvc struct {
	bucketRoutingBlockStore       *store.RoutingBlockStore
	bucketRoutingPolicyStore      *store.BucketRoutingPolicyStore
	bucketReplicationPolicyStore  *store.BucketReplicationPolicyStore
	replicationStatusStore        *store.ReplicationStatusStore
	replicationSwitchStore        *store.ReplicationSwitchInfoStore
	replicationSwitchHistoryStore *store.ReplicationSwitchHistoryStore
	client                        redis.UniversalClient
}

func (r *policySvc) ObjListStarted(ctx context.Context, id entity.ReplicationStatusID) error {
	if err := r.replicationStatusStore.SetListingStarted(ctx, id); err != nil {
		return fmt.Errorf("unable to set listing started: %w", err)
	}
	return nil
}

func (r *policySvc) getRoutingPolicy(ctx context.Context, user, bucket string) (string, error) {
	storage, err := r.getBucketRoutingPolicy(ctx, user, bucket)
	if err == nil {
		return storage, nil
	}
	if !errors.Is(err, dom.ErrNotFound) {
		return "", err
	}
	// bucket policy not found, try user policy:
	return r.GetUserRoutingPolicy(ctx, user)
}

func (r *policySvc) GetRoutingPolicy(ctx context.Context, user, bucket string) (string, error) {
	storage, err := r.getRoutingPolicy(ctx, user, bucket)
	if err != nil {
		return "", err
	}
	blocked, err := r.isRoutingBlocked(ctx, storage, bucket)
	if err != nil {
		return "", err
	}
	if blocked {
		return "", dom.ErrRoutingBlock
	}
	return storage, nil
}

func routingBlockSetKey(storage string) string {
	return fmt.Sprintf("p:rout_block:%s", storage)
}

func (r *policySvc) AddRoutingBlock(ctx context.Context, storage, bucket string) error {
	return addRoutingBlockWithClient(ctx, r.client, storage, bucket)
}

func addRoutingBlockWithClient(ctx context.Context, client redis.Cmdable, storage, bucket string) error {
	return client.SAdd(ctx, routingBlockSetKey(storage), bucket).Err()
}

func (r *policySvc) DeleteRoutingBlock(ctx context.Context, storage, bucket string) error {
	return deleteRoutingBlockWithClient(ctx, r.client, storage, bucket)
}

func deleteRoutingBlockWithClient(ctx context.Context, client redis.Cmdable, storage, bucket string) error {
	err := client.SRem(ctx, routingBlockSetKey(storage), bucket).Err()
	if errors.Is(err, redis.Nil) {
		return nil
	}
	return err
}

func (r *policySvc) isRoutingBlocked(ctx context.Context, storage, bucket string) (bool, error) {
	return r.client.SIsMember(ctx, routingBlockSetKey(storage), bucket).Result()
}

func (r *policySvc) GetUserRoutingPolicy(ctx context.Context, user string) (string, error) {
	if user == "" {
		return "", fmt.Errorf("%w: user is required to get routing policy", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:route:%s", user)
	toStor, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", fmt.Errorf("%w: no routing policy for user %q", dom.ErrNotFound, user)
		}
		return "", err
	}
	return toStor, nil
}

func (r *policySvc) getBucketRoutingPolicy(ctx context.Context, user, bucket string) (string, error) {
	if user == "" {
		return "", fmt.Errorf("%w: user is required to get routing policy", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return "", fmt.Errorf("%w: bucket is required to get routing policy", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:route:%s:%s", user, bucket)
	toStor, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", fmt.Errorf("%w: no routing policy for user %q, bucket %q", dom.ErrNotFound, user, bucket)
		}
		return "", err
	}
	return toStor, nil
}

func (r *policySvc) AddUserRoutingPolicy(ctx context.Context, user, toStorage string) error {
	if user == "" {
		return fmt.Errorf("%w: user is required to add user routing policy", dom.ErrInvalidArg)
	}
	if toStorage == "" {
		return fmt.Errorf("%w: toStorage is required to add user routing policy", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:route:%s", user)
	set, err := r.client.SetNX(ctx, key, toStorage, 0).Result()
	if err != nil {
		return err
	}
	if !set {
		return fmt.Errorf("%w: user %q routing policy already exists", dom.ErrAlreadyExists, user)
	}
	return nil
}

func (r *policySvc) addBucketRoutingPolicy(ctx context.Context, user, bucket, toStorage string, replace bool) error {
	return addBucketRoutingPolicyWithClient(ctx, r.client, user, bucket, toStorage, replace)
}

func addBucketRoutingPolicyWithClient(ctx context.Context, client redis.Cmdable, user, bucket, toStorage string, replace bool) error {
	if user == "" {
		return fmt.Errorf("%w: user is required to add bucket routing policy", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return fmt.Errorf("%w: bucket is required to add bucket routing policy", dom.ErrInvalidArg)
	}
	if toStorage == "" {
		return fmt.Errorf("%w: toStorage is required to add bucket routing policy", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:route:%s:%s", user, bucket)
	if replace {
		return client.Set(ctx, key, toStorage, 0).Err()
	}
	// set only if not exists
	set, err := client.SetNX(ctx, key, toStorage, 0).Result()
	if err != nil {
		return err
	}
	if !set {
		return fmt.Errorf("%w: bucket routing policy %s:%s already exists", dom.ErrAlreadyExists, user, bucket)
	}
	return nil
}

func (r *policySvc) GetBucketReplicationPolicies(ctx context.Context, user, bucket string) (ReplicationPolicies, error) {
	if user == "" {
		return ReplicationPolicies{}, fmt.Errorf("%w: user is required to get replication policy", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return ReplicationPolicies{}, fmt.Errorf("%w: bucket is required to get replication policy", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:repl:%s:%s", user, bucket)
	return r.getReplicationPolicies(ctx, key)
}

func (r *policySvc) getReplicationPolicies(ctx context.Context, key string) (ReplicationPolicies, error) {
	res, err := r.client.ZRangeWithScores(ctx, key, 0, -1).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return ReplicationPolicies{}, fmt.Errorf("%w: no replication from policy for user %q", dom.ErrNotFound, key)
		}
		return ReplicationPolicies{}, err
	}
	if len(res) == 0 {
		return ReplicationPolicies{}, fmt.Errorf("%w: no replication from policy for user %q", dom.ErrNotFound, key)
	}
	var from string
	toMap := map[ReplicationPolicyDest]tasks.Priority{}
	for _, pol := range res {
		member, ok := pol.Member.(string)
		if !ok {
			return ReplicationPolicies{}, fmt.Errorf("%w: invalid replication policy key: cannot cast to string %+v", dom.ErrInternal, pol)
		}
		memberArr := strings.SplitN(member, ":", 2)
		if len(memberArr) != 2 {
			return ReplicationPolicies{}, fmt.Errorf("%w: invalid replication policy key: should contain from:to, got: %s", dom.ErrInternal, member)
		}
		f, to := memberArr[0], memberArr[1]
		if from == "" {
			from = f
		}
		if from != f {
			return ReplicationPolicies{}, fmt.Errorf("%w: invalid replication policy key: all keys should have same from: %+v", dom.ErrInternal, res)
		}
		if from == to {
			return ReplicationPolicies{}, fmt.Errorf("%w: invalid replication policy key: from and to should be different: %+v", dom.ErrInternal, res)
		}
		priority := uint8(pol.Score)
		if priority > uint8(tasks.PriorityHighest5) {
			return ReplicationPolicies{}, fmt.Errorf("%w: invalid replication policy key %q score: %d", dom.ErrInternal, member, priority)
		}
		toMap[ReplicationPolicyDest(to)] = tasks.Priority(priority)
	}
	return ReplicationPolicies{
		From: from,
		To:   toMap,
	}, nil
}

func (r *policySvc) GetReplicationPolicyInfo(ctx context.Context, id entity.ReplicationStatusID) (ReplicationPolicyStatus, error) {
	if err := id.Validate(); err != nil {
		return ReplicationPolicyStatus{}, err
	}

	fKey := id.StatusKey()
	switchKey := id.SwitchKey()

	res := ReplicationPolicyStatus{}
	var getRes *redis.MapStringStringCmd
	var switchReplID *redis.StringCmd
	_, err := r.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		getRes = pipe.HGetAll(ctx, fKey)
		switchReplID = r.client.HGet(ctx, switchKey, "entity.ReplicationStatusID")
		return nil
	})
	if err != nil {
		return ReplicationPolicyStatus{}, err
	}
	err = getRes.Scan(&res)
	if err != nil {
		return ReplicationPolicyStatus{}, err
	}
	if res.CreatedAt.IsZero() {
		return ReplicationPolicyStatus{}, fmt.Errorf("%w: no replication policy status for user %q, from %q/%q, to %q/%q", dom.ErrNotFound, id.User, id.FromStorage, id.FromBucket, id.ToStorage, id.ToBucket)
	}
	if switchReplID.Err() != nil {
		if !errors.Is(switchReplID.Err(), redis.Nil) {
			return ReplicationPolicyStatus{}, err
		}
	} else {
		// switch exists
		res.HasSwitch = id.String() == switchReplID.Val()
	}

	return res, nil
}

func (r *policySvc) ListReplicationPolicyInfo(ctx context.Context) ([]ReplicationPolicyStatusExtended, error) {
	iter := r.client.Scan(ctx, 0, "p:repl_st:*", 0).Iterator()

	resCh := make(chan ReplicationPolicyStatusExtended)
	defer close(resCh)
	g, gCtx := errgroup.WithContext(ctx)
	for iter.Next(ctx) {
		key := iter.Val()
		key = strings.TrimPrefix(key, "p:repl_st:")
		vals := strings.SplitN(key, ":", 4)
		if len(vals) != 4 {
			zerolog.Ctx(ctx).Error().Msgf("invalid replication policy status key %s", key)
			continue
		}
		user, bucket, from, to := vals[0], vals[1], vals[2], vals[3]
		var toBucket string
		if toArr := strings.Split(to, ":"); len(toArr) == 2 {
			to = toArr[0]
			toBucket = toArr[1]
		}
		g.Go(func() error {
			id := entity.ReplicationStatusID{
				User:        user,
				FromStorage: from,
				ToStorage:   to,
				FromBucket:  bucket,
				ToBucket:    toBucket,
			}
			policy, err := r.GetReplicationPolicyInfo(gCtx, id)
			if err != nil {
				zerolog.Ctx(gCtx).Err(err).Msg("error during list replication policies")
				return err
			}
			select {
			case <-gCtx.Done():
				return gCtx.Err()
			default:
			}
			resCh <- ReplicationPolicyStatusExtended{
				ReplicationPolicyStatus: policy,
				User:                    user,
				Bucket:                  bucket,
				From:                    from,
				To:                      to,
				ToBucket:                toBucket,
			}
			return nil
		})
	}
	if err := iter.Err(); err != nil && !errors.Is(err, context.Canceled) {
		zerolog.Ctx(ctx).Err(err).Msg("iterate over replications error")
	}
	go func() { _ = g.Wait() }()
	var res []ReplicationPolicyStatusExtended
	for {
		select {
		case <-gCtx.Done():
			return res, nil
		case rep := <-resCh:
			res = append(res, rep)
		}
	}
}

func (r *policySvc) IsReplicationPolicyExists(ctx context.Context, id entity.ReplicationStatusID) (bool, error) {
	ruleKey := fmt.Sprintf("p:repl:%s:%s:%s", id.User, id.FromBucket, id.ToBucket)
	ruleVal := fmt.Sprintf("%s:%s", id.FromStorage, id.ToStorage)
	err := r.client.ZRank(ctx, ruleKey, ruleVal).Err()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (r *policySvc) IsReplicationPolicyPaused(ctx context.Context, id entity.ReplicationStatusID) (bool, error) {
	fKey := fmt.Sprintf("p:repl_st:%s", id.String())
	paused, err := r.client.HGet(ctx, fKey, "paused").Bool()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, fmt.Errorf("%w: no replication policy status for user %q, from %q/%q, to %q/%q", dom.ErrNotFound, id.User, id.FromStorage, id.FromBucket, id.ToStorage, id.ToBucket)
		}
		return false, err
	}
	return paused, nil
}

func (r *policySvc) IncReplInitObjListed(ctx context.Context, id entity.ReplicationStatusID, bytes uint64, eventTime time.Time) error {
	key := fmt.Sprintf("p:repl_st:%s", id.String())
	err := r.incIfKeyExists(ctx, key, "obj_listed", 1)
	if err != nil {
		return err
	}
	err = r.client.HSet(ctx, key, "last_emitted_at", eventTime.UTC()).Err()
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("unable to update last_emitted_at for init replication")
	}
	if bytes != 0 {
		return r.incIfKeyExists(ctx, key, "bytes_listed", int64(bytes))
	}
	return nil
}

func (r *policySvc) IncReplInitObjDone(ctx context.Context, id entity.ReplicationStatusID, bytes uint64, eventTime time.Time) error {
	key := fmt.Sprintf("p:repl_st:%s", id.String())
	err := r.incIfKeyExists(ctx, key, "obj_done", 1)
	if err != nil {
		return err
	}
	if bytes != 0 {
		err = r.incIfKeyExists(ctx, key, "bytes_done", int64(bytes))
		if err != nil {
			return err
		}
	}
	r.updateProcessedAt(ctx, key, eventTime)

	updated, err := r.client.HMGet(ctx, key, "obj_listed", "obj_done").Result()
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("unable to get updated replication policy status")
		return nil
	}
	if len(updated) == 2 {
		listedStr, ok := updated[0].(string)
		if !ok || listedStr == "" {
			return nil
		}
		listed, err := strconv.Atoi(listedStr)
		if err != nil {
			return nil
		}

		doneStr, ok := updated[1].(string)
		if !ok || doneStr == "" {
			return nil
		}
		done, err := strconv.Atoi(doneStr)
		if err != nil {
			return nil
		}
		if listed > done {
			return nil
		}
		err = r.client.HSetNX(ctx, key, "init_done_at", time.Now().UTC()).Err()
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Msg("unable to set init_done_at for replication policy status")
			return nil
		}
	}
	return nil
}

func (r *policySvc) IncReplEvents(ctx context.Context, id entity.ReplicationStatusID, eventTime time.Time) error {
	key := fmt.Sprintf("p:repl_st:%s", id.String())
	err := r.incIfKeyExists(ctx, key, "events", 1)
	if err != nil {
		return err
	}

	err = r.client.HSet(ctx, key, "last_emitted_at", eventTime.UTC()).Err()
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("unable to update last_emitted_at for event replication")
	}
	return nil
}

func (r *policySvc) IncReplEventsDone(ctx context.Context, id entity.ReplicationStatusID, eventTime time.Time) error {
	key := fmt.Sprintf("p:repl_st:%s", id.String())
	err := r.incIfKeyExists(ctx, key, "events_done", 1)
	if err != nil {
		return err
	}
	r.updateProcessedAt(ctx, key, eventTime)
	return nil
}

func (r *policySvc) updateProcessedAt(ctx context.Context, key string, eventTime time.Time) {
	result, err := luaUpdateTsIfGreater.Run(ctx, r.client, []string{key}, "last_processed_at", eventTime.UTC().Format(time.RFC3339Nano)).Result()
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("unable to update policy last_processed_at")
		return
	}
	inc, ok := result.(int64)
	if !ok {
		zerolog.Ctx(ctx).Error().Msgf("unable to cast luaUpdateTsIfGreater result %T to int64", result)
		return
	}
	if inc == 0 {
		zerolog.Ctx(ctx).Info().Msg("policy last_processed_at is not updated")
	}
}

func (r *policySvc) incIfKeyExists(ctx context.Context, key, field string, val int64) error {
	result, err := luaHIncrByEx.Run(ctx, r.client, []string{key}, field, val).Result()
	if err != nil {
		return err
	}
	inc, ok := result.(int64)
	if !ok {
		return fmt.Errorf("%w: unable to cast luaHIncrByEx result %T to int64", dom.ErrInternal, result)
	}
	if inc == 0 {
		return dom.ErrNotFound
	}
	return nil
}

func (r *policySvc) hSetKeyExists(ctx context.Context, key, field string, val interface{}) error {
	result, err := luaHSetEx.Run(ctx, r.client, []string{key}, field, val).Result()
	if err != nil {
		return err
	}
	inc, ok := result.(int64)
	if !ok {
		return fmt.Errorf("%w: unable to cast luaHSetEx result %T to int64", dom.ErrInternal, result)
	}
	if inc == 0 {
		return dom.ErrNotFound
	}
	return nil
}

func (r *policySvc) GetUserReplicationPolicies(ctx context.Context, user string) (ReplicationPolicies, error) {
	if user == "" {
		return ReplicationPolicies{}, fmt.Errorf("%w: user is required to get replication policy", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:repl:%s", user)
	return r.getReplicationPolicies(ctx, key)
}

func (r *policySvc) AddUserReplicationPolicy(ctx context.Context, user string, from string, to string, priority tasks.Priority) error {
	if user == "" {
		return fmt.Errorf("%w: user is required to add replication policy", dom.ErrInvalidArg)
	}
	if from == "" {
		return fmt.Errorf("%w: from is required to add replication policy", dom.ErrInvalidArg)
	}
	if to == "" {
		return fmt.Errorf("%w: to is required to add replication policy", dom.ErrInvalidArg)
	}
	if from == to {
		return fmt.Errorf("%w: invalid replication policy: from and to should be different", dom.ErrInvalidArg)
	}
	if priority > tasks.PriorityHighest5 {
		return fmt.Errorf("%w: invalid priority value", dom.ErrInvalidArg)
	}

	route, err := r.GetUserRoutingPolicy(ctx, user)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("%w: get routing error", err)
	}
	if err == nil && route != from {
		return fmt.Errorf("%w: unable to create user %s replciation from %s because it is different from routing %s", dom.ErrInternal, user, from, route)
	}

	prev, err := r.GetUserReplicationPolicies(ctx, user)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return err
	}
	if err == nil && from != prev.From {
		return fmt.Errorf("%w: all replication policies should have the same from value: got %s, current %s", dom.ErrInvalidArg, from, prev.From)
	}
	if _, ok := prev.To[ReplicationPolicyDest(to)]; ok {
		return dom.ErrAlreadyExists
	}
	key := fmt.Sprintf("p:repl:%s", user)
	val := fmt.Sprintf("%s:%s", from, to)
	added, err := r.client.ZAddNX(ctx, key, redis.Z{Member: val, Score: float64(priority)}).Result()
	if err != nil {
		return err
	}
	if added == 0 {
		return dom.ErrAlreadyExists
	}
	return nil
}

func (r *policySvc) DeleteUserReplication(ctx context.Context, user string, from string, to string) error {
	key := fmt.Sprintf("p:repl:%s", user)
	val := fmt.Sprintf("%s:%s", from, to)
	removed, err := r.client.ZRem(ctx, key, val).Result()
	if err != nil {
		return err
	}
	if removed != 1 {
		return dom.ErrNotFound
	}
	size, err := r.client.ZCard(ctx, key).Result()
	if err != nil {
		return err
	}
	if size == 0 {
		return r.client.Del(ctx, key).Err()
	}
	return nil
}

func (r *policySvc) DeleteBucketReplicationsByUser(ctx context.Context, user, from string, to string) ([]string, error) {
	var deleted []string
	iter := r.client.Scan(ctx, 0, fmt.Sprintf("p:repl_st:%s:*", user), 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		key = strings.TrimPrefix(key, "p:repl_st:")
		vals := strings.SplitN(key, ":", 4)
		if len(vals) != 4 {
			return nil, fmt.Errorf("%w: invalid replication policy status key %s", dom.ErrInternal, key)
		}
		bucket, gotFrom, gotTo := vals[1], vals[2], vals[3]
		var toBucket *string = nil
		if toArr := strings.Split(gotTo, ":"); len(toArr) == 2 {
			gotTo = toArr[0]
			toBucket = &toArr[1]
		}
		if gotFrom != from || gotTo != to {
			continue
		}
		id := entity.ReplicationStatusID{
			User:        user,
			FromStorage: from,
			ToStorage:   to,
			FromBucket:  bucket,
			ToBucket:    *toBucket,
		}
		err := r.DeleteReplication(ctx, id)
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Msg("error during list replication policies")
			continue
		}
		deleted = append(deleted, bucket)
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate over replications error", err)
	}
	return deleted, nil
}

func (r *policySvc) AddBucketReplicationPolicy(ctx context.Context, id entity.ReplicationStatusID, priority tasks.Priority, agentURL *string) error {
	// create composite destination key if custom destination bucket is set
	dest := fmt.Sprintf("%s:%s", id.ToStorage, id.ToBucket)
	if id.FromStorage == dest {
		return fmt.Errorf("%w: invalid replication policy: from and to should be different", dom.ErrInvalidArg)
	}
	if priority > tasks.PriorityHighest5 {
		return fmt.Errorf("%w: invalid priority value", dom.ErrInvalidArg)
	}

	route, err := r.GetRoutingPolicy(ctx, id.User, id.FromBucket)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("%w: get routing error", err)
	}
	if err == nil && route != id.FromStorage {
		return fmt.Errorf("%w: unable to create bucket %s replication from %s because it is different from routing %s", dom.ErrInternal, id.FromBucket, id.FromStorage, route)
	}

	prev, err := r.GetBucketReplicationPolicies(ctx, id.User, id.FromBucket)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return err
	}
	if err == nil && id.FromStorage != prev.From {
		return fmt.Errorf("%w: all replication policies should have the same from value (u: %s b: %s): got %s, current %s", dom.ErrInvalidArg, id.User, id.FromBucket, id.FromStorage, prev.From)
	}
	if _, ok := prev.To[ReplicationPolicyDest(dest)]; ok {
		return dom.ErrAlreadyExists
	}
	key := fmt.Sprintf("p:repl:%s:%s", id.User, id.FromBucket)
	val := fmt.Sprintf("%s:%s:%s", id.FromStorage, id.ToStorage, id.ToBucket)
	added, err := r.client.ZAddNX(ctx, key, redis.Z{Member: val, Score: float64(priority)}).Result()
	if err != nil {
		return err
	}
	if added == 0 {
		return dom.ErrAlreadyExists
	}
	defer func() {
		if err != nil {
			r.client.ZRem(context.Background(), key, val)
		}
	}()

	statusKey := fmt.Sprintf("p:repl_st:%s:%s:%s:%s:%s", id.User, id.FromBucket, id.FromStorage, id.ToStorage, id.ToBucket)
	res := ReplicationPolicyStatus{
		CreatedAt: time.Now().UTC(),
		AgentURL:  fromStrPtr(agentURL),
	}

	if err = r.client.HSet(ctx, statusKey, res).Err(); err != nil {
		return fmt.Errorf("unable to set value in hash: %w", err)
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
	_, err := r.GetReplicationPolicyInfo(ctx, id)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("p:repl_st:%s", id.String())
	return r.client.HSet(ctx, key, "paused", true).Err()
}

func (r *policySvc) ResumeReplication(ctx context.Context, id entity.ReplicationStatusID) error {
	_, err := r.GetReplicationPolicyInfo(ctx, id)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("p:repl_st:%s", id.String())
	return r.client.HSet(ctx, key, "paused", false).Err()
}

func (r *policySvc) DeleteReplication(ctx context.Context, id entity.ReplicationStatusID) error {
	key := fmt.Sprintf("p:repl:%s:%s", id.User, id.FromBucket)
	val := fmt.Sprintf("%s:%s:%s", id.FromStorage, id.ToStorage, id.ToBucket)
	statusKey := fmt.Sprintf("p:repl_st:%s", id.String())

	_, err := r.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.ZRem(ctx, key, val)
		pipe.Del(ctx, statusKey)
		pipe.SRem(ctx, routingBlockSetKey(id.ToStorage), id.ToBucket)
		return nil
	})
	if err != nil {
		return err
	}
	return err
}

func (r *policySvc) ArchiveReplication(ctx context.Context, replID entity.ReplicationStatusID) error {
	pipe := r.client.Pipeline()
	if err := archiveReplicationWithClient(ctx, pipe, replID); err != nil {
		return err
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	return nil
}

func archiveReplicationWithClient(ctx context.Context, client redis.Cmdable, replID entity.ReplicationStatusID) error {
	key := fmt.Sprintf("p:repl:%s:%s", replID.User, replID.FromBucket)
	// build composite destination if custom destination bucket set
	val := fmt.Sprintf("%s:%s:%s", replID.FromStorage, replID.ToStorage, replID.ToBucket)
	statusKey := replID.StatusKey()

	client.ZRem(ctx, key, val)
	client.HSet(ctx, statusKey, "archived", true)
	client.HSet(ctx, statusKey, "archived_at", time.Now())
	return nil
}
