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

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
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

type ReplicationSwitch struct {
	IsDone       bool          `redis:"IsDone"`
	OldMain      string        `redis:"OldMain"`
	OldFollowers string        `redis:"OldFollowers"`
	MultipartTTL time.Duration `redis:"MultipartTTL"`
	StartedAt    time.Time     `redis:"StartedAt"`
	DoneAt       *time.Time    `redis:"DoneAt,omitempty"`
}

func (r *ReplicationSwitch) GetOldFollowers() map[string]tasks.Priority {
	res := map[string]tasks.Priority{}
	arr := strings.Split(r.OldFollowers, ",")
	for _, s := range arr {
		val := strings.Split(s, ":")
		if len(val) != 2 {
			continue
		}
		toStor := val[0]
		toPriority, _ := strconv.Atoi(val[1])
		res[toStor] = tasks.Priority(toPriority)
	}
	return res
}

func (r *ReplicationSwitch) SetOldFollowers(f map[string]tasks.Priority) {
	followers := make([]string, 0, len(f))
	for k, v := range f {
		followers = append(followers, k+":"+strconv.Itoa(int(v)))
	}
	r.OldFollowers = strings.Join(followers, ",")
}

type ReplicationPolicyStatus struct {
	CreatedAt       time.Time `redis:"created_at"`
	IsPaused        bool      `redis:"paused"`
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

	ListingStarted bool `redis:"listing_started"`

	SwitchStatus SwitchStatus `redis:"-"`
}

type SwitchStatus int

const (
	NotStarted SwitchStatus = iota
	InProgress
	Done
)

type ReplicationPolicyStatusExtended struct {
	ReplicationPolicyStatus
	User   string
	Bucket string
	From   string
	To     string
}

type ReplicationPolicies struct {
	From string
	To   map[string]tasks.Priority
}

type Service interface {
	GetRoutingPolicy(ctx context.Context, user, bucket string) (string, error)
	getBucketRoutingPolicy(ctx context.Context, user, bucket string) (string, error)
	addBucketRoutingPolicy(ctx context.Context, user, bucket, toStorage string) error
	GetUserRoutingPolicy(ctx context.Context, user string) (string, error)
	AddUserRoutingPolicy(ctx context.Context, user, toStorage string) error

	IsReplicationSwitchInProgress(ctx context.Context, user, bucket string) (bool, error)
	GetReplicationSwitch(ctx context.Context, user, bucket string) (ReplicationSwitch, error)
	DoReplicationSwitch(ctx context.Context, user, bucket, newMain string) error
	ReplicationSwitchDone(ctx context.Context, user, bucket string) error

	GetBucketReplicationPolicies(ctx context.Context, user, bucket string) (ReplicationPolicies, error)
	GetUserReplicationPolicies(ctx context.Context, user string) (ReplicationPolicies, error)
	AddUserReplicationPolicy(ctx context.Context, user string, from string, to string, priority tasks.Priority) error
	DeleteUserReplication(ctx context.Context, user string, from string, to string) error

	AddBucketReplicationPolicy(ctx context.Context, user, bucket, from string, to string, priority tasks.Priority, agentURL *string) error
	GetReplicationPolicyInfo(ctx context.Context, user, bucket, from, to string) (ReplicationPolicyStatus, error)
	ListReplicationPolicyInfo(ctx context.Context) ([]ReplicationPolicyStatusExtended, error)
	IsReplicationPolicyExists(ctx context.Context, user, bucket, from, to string) (bool, error)
	IsReplicationPolicyPaused(ctx context.Context, user, bucket, from, to string) (bool, error)
	IncReplInitObjListed(ctx context.Context, user, bucket, from, to string, bytes int64, eventTime time.Time) error
	IncReplInitObjDone(ctx context.Context, user, bucket, from, to string, bytes int64, eventTime time.Time) error
	ObjListStarted(ctx context.Context, user, bucket, from, to string) error
	IncReplEvents(ctx context.Context, user, bucket, from, to string, eventTime time.Time) error
	IncReplEventsDone(ctx context.Context, user, bucket, from, to string, eventTime time.Time) error

	PauseReplication(ctx context.Context, user, bucket, from string, to string) error
	ResumeReplication(ctx context.Context, user, bucket, from string, to string) error
	DeleteReplication(ctx context.Context, user, bucket, from string, to string) error
	DeleteBucketReplicationsByUser(ctx context.Context, user, from string, to string) ([]string, error)
}

func NewService(client *redis.Client) Service {
	return &policySvc{client: client}
}

type policySvc struct {
	client *redis.Client
}

func (s *policySvc) ObjListStarted(ctx context.Context, user, bucket, from, to string) error {
	if user == "" {
		return fmt.Errorf("%w: user is required to get replication policy status", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return fmt.Errorf("%w: bucket is required to get replication policy status", dom.ErrInvalidArg)
	}
	if from == "" {
		return fmt.Errorf("%w: from is required to get replication policy status", dom.ErrInvalidArg)
	}
	if to == "" {
		return fmt.Errorf("%w: to is required to get replication policy status", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:repl_st:%s:%s:%s:%s", user, bucket, from, to)
	return s.hSetKeyExists(ctx, key, "listing_started", true)
}

func (s *policySvc) GetRoutingPolicy(ctx context.Context, user, bucket string) (string, error) {
	storage, err := s.getBucketRoutingPolicy(ctx, user, bucket)
	if err == nil {
		return storage, nil
	}
	if !errors.Is(err, dom.ErrNotFound) {
		return "", err
	}
	// bucket policy not found, try user policy:
	return s.GetUserRoutingPolicy(ctx, user)
}

func (s *policySvc) GetUserRoutingPolicy(ctx context.Context, user string) (string, error) {
	if user == "" {
		return "", fmt.Errorf("%w: user is required to get routing policy", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:route:%s", user)
	toStor, err := s.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("%w: no routing policy for user %q", dom.ErrNotFound, user)
		}
		return "", err
	}
	return toStor, nil
}

func (s *policySvc) getBucketRoutingPolicy(ctx context.Context, user, bucket string) (string, error) {
	if user == "" {
		return "", fmt.Errorf("%w: user is required to get routing policy", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return "", fmt.Errorf("%w: bucket is required to get routing policy", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:route:%s:%s", user, bucket)
	toStor, err := s.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("%w: no routing policy for user %q, bucket %q", dom.ErrNotFound, user, bucket)
		}
		return "", err
	}
	return toStor, nil
}

func (s *policySvc) AddUserRoutingPolicy(ctx context.Context, user, toStorage string) error {
	if user == "" {
		return fmt.Errorf("%w: user is required to add user routing policy", dom.ErrInvalidArg)
	}
	if toStorage == "" {
		return fmt.Errorf("%w: toStorage is required to add user routing policy", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:route:%s", user)
	set, err := s.client.SetNX(ctx, key, toStorage, 0).Result()
	if err != nil {
		return err
	}
	if !set {
		return fmt.Errorf("%w: user %q routing policy already exists", dom.ErrAlreadyExists, user)
	}
	return nil
}

func (s *policySvc) addBucketRoutingPolicy(ctx context.Context, user, bucket, toStorage string) error {
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
	set, err := s.client.SetNX(ctx, key, toStorage, 0).Result()
	if err != nil {
		return err
	}
	if !set {
		return fmt.Errorf("%w: bucket routing policy %s:%s already exists", dom.ErrAlreadyExists, user, bucket)
	}
	return nil
}

func (s *policySvc) GetReplicationSwitch(ctx context.Context, user, bucket string) (ReplicationSwitch, error) {
	if user == "" {
		return ReplicationSwitch{}, fmt.Errorf("%w: user is required to get replication Switch", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return ReplicationSwitch{}, fmt.Errorf("%w: bucket is required to get replication Switch", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:switch:%s:%s", user, bucket)
	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return ReplicationSwitch{}, err
	}
	if exists != 1 {
		return ReplicationSwitch{}, fmt.Errorf("%w: no replication switch for user %q, bucket %q", dom.ErrNotFound, user, bucket)
	}
	var res ReplicationSwitch
	err = s.client.HGetAll(ctx, key).Scan(&res)
	return res, err
}

func (s *policySvc) IsReplicationSwitchInProgress(ctx context.Context, user, bucket string) (bool, error) {
	if user == "" {
		return false, fmt.Errorf("%w: user is required to get replication policy", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return false, fmt.Errorf("%w: bucket is required to get replication policy", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:switch:%s:%s", user, bucket)
	isDoneStr, err := s.client.HGet(ctx, key, "IsDone").Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}
	isDone, _ := strconv.ParseBool(isDoneStr)
	return isDone, nil
}

func (s *policySvc) GetBucketReplicationPolicies(ctx context.Context, user, bucket string) (ReplicationPolicies, error) {
	if user == "" {
		return ReplicationPolicies{}, fmt.Errorf("%w: user is required to get replication policy", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return ReplicationPolicies{}, fmt.Errorf("%w: bucket is required to get replication policy", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:repl:%s:%s", user, bucket)
	return s.getReplicationPolicies(ctx, key)
}

func (s *policySvc) getReplicationPolicies(ctx context.Context, key string) (ReplicationPolicies, error) {
	res, err := s.client.ZRangeWithScores(ctx, key, 0, -1).Result()
	if err != nil {
		if err == redis.Nil {
			return ReplicationPolicies{}, fmt.Errorf("%w: no replication from policy for user %q", dom.ErrNotFound, key)
		}
		return ReplicationPolicies{}, err
	}
	if len(res) == 0 {
		return ReplicationPolicies{}, fmt.Errorf("%w: no replication from policy for user %q", dom.ErrNotFound, key)
	}
	var from string
	toMap := map[string]tasks.Priority{}
	for _, pol := range res {
		member, ok := pol.Member.(string)
		if !ok {
			return ReplicationPolicies{}, fmt.Errorf("%w: invalid replication policy key: cannot cast to string %+v", dom.ErrInternal, pol)
		}
		memberArr := strings.Split(member, ":")
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
		toMap[to] = tasks.Priority(priority)
	}
	return ReplicationPolicies{
		From: from,
		To:   toMap,
	}, nil
}

func (s *policySvc) GetReplicationPolicyInfo(ctx context.Context, user, bucket, from, to string) (ReplicationPolicyStatus, error) {
	if user == "" {
		return ReplicationPolicyStatus{}, fmt.Errorf("%w: user is required to get replication policy status", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return ReplicationPolicyStatus{}, fmt.Errorf("%w: bucket is required to get replication policy status", dom.ErrInvalidArg)
	}
	if from == "" {
		return ReplicationPolicyStatus{}, fmt.Errorf("%w: from is required to get replication policy status", dom.ErrInvalidArg)
	}
	if to == "" {
		return ReplicationPolicyStatus{}, fmt.Errorf("%w: to is required to get replication policy status", dom.ErrInvalidArg)
	}

	fKey := fmt.Sprintf("p:repl_st:%s:%s:%s:%s", user, bucket, from, to)
	switchKey := fmt.Sprintf("p:switch:%s:%s", user, bucket)

	res := ReplicationPolicyStatus{}
	var getRes *redis.MapStringStringCmd
	var switchDone *redis.StringCmd
	_, err := s.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		getRes = pipe.HGetAll(ctx, fKey)
		switchDone = s.client.HGet(ctx, switchKey, "IsDone")
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
		return ReplicationPolicyStatus{}, fmt.Errorf("%w: no replication policy status for user %q, bucket %q, from %q, to %q", dom.ErrNotFound, user, bucket, from, to)
	}
	if switchDone.Err() != nil {
		if switchDone.Err() != redis.Nil {
			return ReplicationPolicyStatus{}, err
		}
		res.SwitchStatus = NotStarted
	} else {
		// switch exists
		isDone, err := switchDone.Bool()
		if err != nil {
			return ReplicationPolicyStatus{}, fmt.Errorf("%w: unable to parse replication switch isDone", err)
		}
		if isDone {
			res.SwitchStatus = Done
		} else {
			res.SwitchStatus = InProgress
		}
	}

	return res, nil
}

func (s *policySvc) ListReplicationPolicyInfo(ctx context.Context) ([]ReplicationPolicyStatusExtended, error) {
	iter := s.client.Scan(ctx, 0, "p:repl_st:*", 0).Iterator()

	resCh := make(chan ReplicationPolicyStatusExtended)
	defer close(resCh)
	g, gCtx := errgroup.WithContext(ctx)
	for iter.Next(ctx) {
		key := iter.Val()
		key = strings.TrimPrefix(key, "p:repl_st:")
		vals := strings.Split(key, ":")
		if len(vals) != 4 {
			zerolog.Ctx(ctx).Error().Msgf("invalid replication policy status key %s", key)
			continue
		}
		user, bucket, from, to := vals[0], vals[1], vals[2], vals[3]
		g.Go(func() error {
			policy, err := s.GetReplicationPolicyInfo(gCtx, user, bucket, from, to)
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

func (s *policySvc) IsReplicationPolicyExists(ctx context.Context, user, bucket, from, to string) (bool, error) {
	ruleKey := fmt.Sprintf("p:repl:%s:%s", user, bucket)
	ruleVal := fmt.Sprintf("%s:%s", from, to)
	err := s.client.ZRank(ctx, ruleKey, ruleVal).Err()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *policySvc) IsReplicationPolicyPaused(ctx context.Context, user, bucket, from, to string) (bool, error) {
	if user == "" {
		return false, fmt.Errorf("%w: user is required to get replication policy status", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return false, fmt.Errorf("%w: bucket is required to get replication policy status", dom.ErrInvalidArg)
	}
	if from == "" {
		return false, fmt.Errorf("%w: from is required to get replication policy status", dom.ErrInvalidArg)
	}
	if to == "" {
		return false, fmt.Errorf("%w: to is required to get replication policy status", dom.ErrInvalidArg)
	}

	fKey := fmt.Sprintf("p:repl_st:%s:%s:%s:%s", user, bucket, from, to)
	paused, err := s.client.HGet(ctx, fKey, "paused").Bool()
	if err != nil {
		if err == redis.Nil {
			return false, fmt.Errorf("%w: no replication policy status for user %q, bucket %q, from %q, to %q", dom.ErrNotFound, user, bucket, from, to)
		}
		return false, err
	}
	return paused, nil
}

func (s *policySvc) IncReplInitObjListed(ctx context.Context, user, bucket, from, to string, bytes int64, eventTime time.Time) error {
	if user == "" {
		return fmt.Errorf("%w: user is required to get replication policy status", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return fmt.Errorf("%w: bucket is required to get replication policy status", dom.ErrInvalidArg)
	}
	if from == "" {
		return fmt.Errorf("%w: from is required to get replication policy status", dom.ErrInvalidArg)
	}
	if to == "" {
		return fmt.Errorf("%w: to is required to get replication policy status", dom.ErrInvalidArg)
	}
	if bytes < 0 {
		return fmt.Errorf("%w: bytes must be positive", dom.ErrInvalidArg)
	}

	key := fmt.Sprintf("p:repl_st:%s:%s:%s:%s", user, bucket, from, to)
	err := s.incIfKeyExists(ctx, key, "obj_listed", 1)
	if err != nil {
		return err
	}
	err = s.client.HSet(ctx, key, "last_emitted_at", eventTime.UTC()).Err()
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("unable to update last_emitted_at for init replication")
	}
	if bytes != 0 {
		return s.incIfKeyExists(ctx, key, "bytes_listed", bytes)
	}
	return nil
}

func (s *policySvc) IncReplInitObjDone(ctx context.Context, user, bucket, from, to string, bytes int64, eventTime time.Time) error {
	if user == "" {
		return fmt.Errorf("%w: user is required to get replication policy status", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return fmt.Errorf("%w: bucket is required to get replication policy status", dom.ErrInvalidArg)
	}
	if from == "" {
		return fmt.Errorf("%w: from is required to get replication policy status", dom.ErrInvalidArg)
	}
	if to == "" {
		return fmt.Errorf("%w: to is required to get replication policy status", dom.ErrInvalidArg)
	}
	if bytes < 0 {
		return fmt.Errorf("%w: bytes must be positive", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:repl_st:%s:%s:%s:%s", user, bucket, from, to)
	err := s.incIfKeyExists(ctx, key, "obj_done", 1)
	if err != nil {
		return err
	}
	if bytes != 0 {
		err = s.incIfKeyExists(ctx, key, "bytes_done", bytes)
		if err != nil {
			return err
		}
	}
	s.updateProcessedAt(ctx, key, eventTime)

	updated, err := s.client.HMGet(ctx, key, "obj_listed", "obj_done").Result()
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
		err = s.client.HSetNX(ctx, key, "init_done_at", time.Now().UTC()).Err()
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Msg("unable to set init_done_at for replication policy status")
			return nil
		}
	}
	return nil
}

func (s *policySvc) IncReplEvents(ctx context.Context, user, bucket, from, to string, eventTime time.Time) error {
	if user == "" {
		return fmt.Errorf("%w: user is required to inc replication policy status", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return fmt.Errorf("%w: bucket is required to inc replication policy status", dom.ErrInvalidArg)
	}
	if from == "" {
		return fmt.Errorf("%w: from is required to inc replication policy status", dom.ErrInvalidArg)
	}
	if to == "" {
		return fmt.Errorf("%w: to is required to inc replication policy status", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:repl_st:%s:%s:%s:%s", user, bucket, from, to)
	err := s.incIfKeyExists(ctx, key, "events", 1)
	if err != nil {
		return err
	}

	err = s.client.HSet(ctx, key, "last_emitted_at", eventTime.UTC()).Err()
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("unable to update last_emitted_at for event replication")
	}
	return nil
}

func (s *policySvc) IncReplEventsDone(ctx context.Context, user, bucket, from, to string, eventTime time.Time) error {
	if user == "" {
		return fmt.Errorf("%w: user is required to inc replication policy status", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return fmt.Errorf("%w: bucket is required to inc replication policy status", dom.ErrInvalidArg)
	}
	if from == "" {
		return fmt.Errorf("%w: from is required to inc replication policy status", dom.ErrInvalidArg)
	}
	if to == "" {
		return fmt.Errorf("%w: to is required to inc replication policy status", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:repl_st:%s:%s:%s:%s", user, bucket, from, to)
	err := s.incIfKeyExists(ctx, key, "events_done", 1)
	if err != nil {
		return err
	}
	s.updateProcessedAt(ctx, key, eventTime)
	return nil
}

func (s *policySvc) updateProcessedAt(ctx context.Context, key string, eventTime time.Time) {
	result, err := luaUpdateTsIfGreater.Run(ctx, s.client, []string{key}, "last_processed_at", eventTime.UTC().Format(time.RFC3339Nano)).Result()
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

func (s *policySvc) incIfKeyExists(ctx context.Context, key, field string, val int64) (err error) {
	result, err := luaHIncrByEx.Run(ctx, s.client, []string{key}, field, val).Result()
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

func (s *policySvc) hSetKeyExists(ctx context.Context, key, field string, val interface{}) (err error) {
	result, err := luaHSetEx.Run(ctx, s.client, []string{key}, field, val).Result()
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

func (s *policySvc) GetUserReplicationPolicies(ctx context.Context, user string) (ReplicationPolicies, error) {
	if user == "" {
		return ReplicationPolicies{}, fmt.Errorf("%w: user is required to get replication policy", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:repl:%s", user)
	return s.getReplicationPolicies(ctx, key)
}

func (s *policySvc) AddUserReplicationPolicy(ctx context.Context, user string, from string, to string, priority tasks.Priority) error {
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

	route, err := s.GetUserRoutingPolicy(ctx, user)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("%w: get routing error", err)
	}
	if err == nil && route != from {
		return fmt.Errorf("%w: unable to create user %s replciation from %s because it is different from routing %s", dom.ErrInternal, user, from, route)
	}

	prev, err := s.GetUserReplicationPolicies(ctx, user)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return err
	}
	if err == nil && from != prev.From {
		return fmt.Errorf("%w: all replication policies should have the same from value: got %s, current %s", dom.ErrInvalidArg, from, prev.From)
	}
	if _, ok := prev.To[to]; ok {
		return dom.ErrAlreadyExists
	}
	key := fmt.Sprintf("p:repl:%s", user)
	val := fmt.Sprintf("%s:%s", from, to)
	added, err := s.client.ZAddNX(ctx, key, redis.Z{Member: val, Score: float64(priority)}).Result()
	if err != nil {
		return err
	}
	if added == 0 {
		return dom.ErrAlreadyExists
	}
	return nil
}

func (s *policySvc) DeleteUserReplication(ctx context.Context, user string, from string, to string) error {
	key := fmt.Sprintf("p:repl:%s", user)
	val := fmt.Sprintf("%s:%s", from, to)
	removed, err := s.client.ZRem(ctx, key, val).Result()
	if err != nil {
		return err
	}
	if removed != 1 {
		return dom.ErrNotFound
	}
	size, err := s.client.ZCard(ctx, key).Result()
	if err != nil {
		return err
	}
	if size == 0 {
		return s.client.Del(ctx, key).Err()
	}
	return nil
}

func (s *policySvc) DeleteBucketReplicationsByUser(ctx context.Context, user, from string, to string) ([]string, error) {
	var deleted []string
	iter := s.client.Scan(ctx, 0, fmt.Sprintf("p:repl_st:%s:*", user), 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		key = strings.TrimPrefix(key, "p:repl_st:")
		vals := strings.Split(key, ":")
		if len(vals) != 4 {
			return nil, fmt.Errorf("%w: invalid replication policy status key %s", dom.ErrInternal, key)
		}
		bucket, gotFrom, gotTo := vals[1], vals[2], vals[3]
		if gotFrom != from || gotTo != to {
			continue
		}
		err := s.DeleteReplication(ctx, user, bucket, from, to)
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

func (s *policySvc) AddBucketReplicationPolicy(ctx context.Context, user, bucket, from string, to string, priority tasks.Priority, agentURL *string) (err error) {
	if user == "" {
		return fmt.Errorf("%w: user is required to add replication policy", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return fmt.Errorf("%w: bucket is required to add replication policy", dom.ErrInvalidArg)
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

	route, err := s.GetRoutingPolicy(ctx, user, bucket)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("%w: get routing error", err)
	}
	if err == nil && route != from {
		return fmt.Errorf("%w: unable to create bucket %s replciation from %s because it is different from routing %s", dom.ErrInternal, bucket, from, route)
	}

	prev, err := s.GetBucketReplicationPolicies(ctx, user, bucket)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return err
	}
	if err == nil && from != prev.From {
		return fmt.Errorf("%w: all replication policies should have the same from value (u: %s b: %s): got %s, current %s", dom.ErrInvalidArg, user, bucket, from, prev.From)
	}
	if _, ok := prev.To[to]; ok {
		return dom.ErrAlreadyExists
	}
	key := fmt.Sprintf("p:repl:%s:%s", user, bucket)
	val := fmt.Sprintf("%s:%s", from, to)
	added, err := s.client.ZAddNX(ctx, key, redis.Z{Member: val, Score: float64(priority)}).Result()
	if err != nil {
		return err
	}
	if added == 0 {
		return dom.ErrAlreadyExists
	}
	defer func() {
		if err != nil {
			s.client.ZRem(context.Background(), key, val)
		}
	}()

	statusKey := fmt.Sprintf("p:repl_st:%s:%s:%s:%s", user, bucket, from, to)
	res := ReplicationPolicyStatus{
		CreatedAt: time.Now().UTC(),
		AgentURL:  fromStrPtr(agentURL),
	}

	err = s.client.HSet(ctx, statusKey, res).Err()
	return
}

func fromStrPtr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func (s *policySvc) PauseReplication(ctx context.Context, user, bucket, from string, to string) error {
	_, err := s.GetReplicationPolicyInfo(ctx, user, bucket, from, to)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("p:repl_st:%s:%s:%s:%s", user, bucket, from, to)
	return s.client.HSet(ctx, key, "paused", true).Err()
}

func (s *policySvc) ResumeReplication(ctx context.Context, user, bucket, from string, to string) error {
	_, err := s.GetReplicationPolicyInfo(ctx, user, bucket, from, to)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("p:repl_st:%s:%s:%s:%s", user, bucket, from, to)
	return s.client.HSet(ctx, key, "paused", false).Err()
}

func (s *policySvc) DeleteReplication(ctx context.Context, user, bucket, from string, to string) error {
	key := fmt.Sprintf("p:repl:%s:%s", user, bucket)
	val := fmt.Sprintf("%s:%s", from, to)
	statusKey := fmt.Sprintf("p:repl_st:%s:%s:%s:%s", user, bucket, from, to)
	switchKey := fmt.Sprintf("p:switch:%s:%s", user, bucket)

	_, err := s.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.ZRem(ctx, key, val)
		pipe.Del(ctx, statusKey)
		pipe.Del(ctx, switchKey)
		return nil
	})
	if err != nil {
		return err
	}
	return err
}

func (s *policySvc) DoReplicationSwitch(ctx context.Context, user, bucket, newMain string) error {
	_, err := s.GetReplicationSwitch(ctx, user, bucket)
	if !errors.Is(err, dom.ErrNotFound) {
		return fmt.Errorf("%w: switch aleady exists", dom.ErrAlreadyExists)
	}

	prevMain, err := s.GetRoutingPolicy(ctx, user, bucket)
	if err != nil {
		return fmt.Errorf("%w: unable to get routing policy", err)
	}
	if prevMain == newMain {
		return fmt.Errorf("%w: storage %s is laready main for bucket %s", dom.ErrAlreadyExists, newMain, bucket)
	}
	replPolicies, err := s.GetBucketReplicationPolicies(ctx, user, bucket)
	if err != nil {
		return fmt.Errorf("%w: unable to get replication policy", err)
	}
	if _, ok := replPolicies.To[newMain]; !ok {
		return fmt.Errorf("%w: no previous replication policy to switch", dom.ErrInvalidArg)
	}
	for prevFollower := range replPolicies.To {
		prevReplication, err := s.GetReplicationPolicyInfo(ctx, user, bucket, prevMain, prevFollower)
		if err != nil {
			return err
		}
		if prevReplication.IsPaused {
			return fmt.Errorf("%w: previous replication to %s is paused", dom.ErrInvalidArg, prevFollower)
		}
		if !prevReplication.ListingStarted {
			return fmt.Errorf("%w: previous replication to %s is not started", dom.ErrInvalidArg, prevFollower)
		}
		if prevReplication.InitObjListed > prevReplication.InitObjDone {
			return fmt.Errorf("%w: previous replication to %s init phase is not done. %d objects remaining", dom.ErrInvalidArg, prevFollower, prevReplication.InitObjListed-prevReplication.InitObjDone)
		}
		if prevReplication.AgentURL != "" {
			return fmt.Errorf("%w: switch is not supported for Chorus-agent setup. Use setup with Chorus-proxy", dom.ErrInvalidArg)
		}
	}
	oldFollowers := replPolicies.To
	const multipartTTL = time.Hour // todo: move to config or api param
	_, err = s.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		// adjust route policy
		routeKey := fmt.Sprintf("p:route:%s:%s", user, bucket)
		_ = pipe.Set(ctx, routeKey, newMain, 0)
		// create replication switch
		switchKey := fmt.Sprintf("p:switch:%s:%s", user, bucket)
		switchVal := ReplicationSwitch{
			IsDone:       false,
			OldMain:      prevMain,
			OldFollowers: "",
			MultipartTTL: multipartTTL,
			StartedAt:    time.Now().UTC(),
			DoneAt:       nil,
		}
		switchVal.SetOldFollowers(oldFollowers)
		_ = pipe.HSet(ctx, switchKey, switchVal)

		// adjust replication policies
		replKey := fmt.Sprintf("p:repl:%s:%s", user, bucket)
		pipe.Del(ctx, replKey)
		delete(oldFollowers, newMain)
		for follower, priority := range oldFollowers {
			pipe.ZAdd(ctx, replKey, redis.Z{
				Score:  float64(priority),
				Member: fmt.Sprintf("%s:%s", newMain, follower),
			})

			statusKey := fmt.Sprintf("p:repl_st:%s:%s:%s:%s", user, bucket, newMain, follower)
			now := time.Now().UTC()
			res := ReplicationPolicyStatus{
				CreatedAt:      now,
				IsPaused:       false,
				ListingStarted: true,
			}
			_ = pipe.HSet(ctx, statusKey, res)
			_ = pipe.HSet(ctx, statusKey, "init_done_at", now)
		}
		return nil
	})
	return err
}

func (s *policySvc) ReplicationSwitchDone(ctx context.Context, user, bucket string) error {
	if user == "" {
		return fmt.Errorf("%w: user is required to get replication Switch", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return fmt.Errorf("%w: bucket is required to get replication Switch", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:switch:%s:%s", user, bucket)
	err := s.hSetKeyExists(ctx, key, "IsDone", true)
	if err != nil {
		return err
	}
	return s.hSetKeyExists(ctx, key, "DoneAt", time.Now().UTC())
}
