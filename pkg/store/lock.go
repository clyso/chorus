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

	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/util"
)

const (
	defaultLockDuration = time.Second
)

type Lock struct {
	logger  *zerolog.Logger
	lock    *redislock.Lock
	overlap time.Duration
}

func NewLock(logger *zerolog.Logger, lock *redislock.Lock, overlap time.Duration) *Lock {
	return &Lock{
		logger:  logger,
		lock:    lock,
		overlap: overlap,
	}
}

func (r *Lock) Refresh(ctx context.Context, duration time.Duration) error {
	r.logger.Debug().Str("refresh", duration.String()).Msg("lock-service: refreshing the lock")
	err := r.lock.Refresh(ctx, duration, nil)
	r.logger.Debug().Err(err).Str("refresh", duration.String()).Msg("lock-service: lock refreshed")
	return err
}

func (r *Lock) Release(ctx context.Context) {
	r.logger.Debug().Msg("lock-service: releasing the lock")
	releaseErr := r.lock.Release(context.Background())
	if releaseErr != nil && !errors.Is(releaseErr, redislock.ErrLockNotHeld) {
		zerolog.Ctx(ctx).Warn().Err(releaseErr).Msg("unable to release lock")
	}
	r.logger.Debug().Err(releaseErr).Msg("lock-service: lock released")
}

func (r *Lock) Do(ctx context.Context, refresh time.Duration, work func() error) error {
	refreshInterval := refresh + r.overlap
	err := r.Refresh(ctx, refreshInterval)
	if errors.Is(err, redislock.ErrNotObtained) {
		return fmt.Errorf("%w: lock not obtained during refresh", &dom.ErrRateLimitExceeded{RetryIn: util.DurationJitter(time.Second*5, time.Second*30)})
	}
	if err != nil {
		return err
	}
	errCh := make(chan error)
	defer close(errCh)
	go func() {
		errCh <- work()
	}()
	timer := time.NewTimer(refresh)
	defer timer.Stop()
	i := 0
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: refresh context canceled: %w", context.Canceled, ctx.Err())
		case <-timer.C:
			i++
			err = r.Refresh(ctx, refreshInterval)
			if err != nil {
				zerolog.Ctx(ctx).Err(err).
					Str("refresh_period", refresh.String()).
					Str("refresh_overlap", r.overlap.String()).
					Int("refresh_iteration", i).
					Msg("unable to refresh lock")
				continue
			}
			timer.Reset(refresh)
		case err = <-errCh:
			return err
		}
	}
}

type lockOptions struct {
	duration time.Duration
	retry    bool
}

type LockOpt interface {
	apply(*lockOptions)
}

type durationOption time.Duration

func (d durationOption) apply(opts *lockOptions) {
	opts.duration = time.Duration(d)
}

func WithDuration(d time.Duration) LockOpt {
	return durationOption(d)
}

type retryOption bool

func (d retryOption) apply(opts *lockOptions) {
	opts.retry = bool(d)
}

func WithRetry(retry bool) LockOpt {
	return retryOption(retry)
}

type RedisIDKeyLocker[ID any] struct {
	locker *redislock.Client
	RedisIDCommonStore[ID]
	overlap time.Duration
}

func NewRedisIDKeyLocker[ID any](client redis.Cmdable, keyPrefix string,
	tokenizeID SingleToMultiValueConverter[ID, string], restoreID MultiToSingleValueConverter[string, ID],
	overlap time.Duration) *RedisIDKeyLocker[ID] {
	return &RedisIDKeyLocker[ID]{
		overlap:            overlap,
		locker:             redislock.New(client),
		RedisIDCommonStore: *NewRedisIDCommonStore[ID](client, keyPrefix, tokenizeID, restoreID),
	}
}

func (r *RedisIDKeyLocker[ID]) Lock(ctx context.Context, id ID, opts ...LockOpt) (*Lock, error) {
	lockKey, err := r.MakeKey(id)
	if err != nil {
		return nil, err
	}
	lockOpts := lockOptions{
		duration: defaultLockDuration,
	}
	for _, o := range opts {
		o.apply(&lockOpts)
	}

	logger := zerolog.Ctx(ctx).With().Str("lock_key", lockKey).Str("duration", lockOpts.duration.String()).Logger()
	var opt *redislock.Options
	if lockOpts.retry {
		opt = &redislock.Options{RetryStrategy: redislock.LimitRetry(redislock.ExponentialBackoff(time.Millisecond*50, time.Second*5), 100)}
	}
	logger.Debug().Msg("lock-service: obtaining lock")
	lock, err := r.locker.Obtain(ctx, lockKey, lockOpts.duration, opt)
	if errors.Is(err, redislock.ErrNotObtained) {
		return nil, fmt.Errorf("%w: lock not obtained %q", &dom.ErrRateLimitExceeded{RetryIn: util.DurationJitter(time.Second, time.Second*5)}, lockKey)
	}
	if err != nil {
		return nil, err
	}
	logger.Debug().Msg("lock-service: lock obtained")
	return NewLock(&logger, lock, r.overlap), nil
}
