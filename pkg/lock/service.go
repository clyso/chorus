/*
 * Copyright © 2023 Clyso GmbH
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

package lock

import (
	"context"
	"errors"
	"fmt"
	"github.com/bsm/redislock"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/util"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"time"
)

const defaultLockDuration = time.Second

type Service interface {
	Lock(ctx context.Context, key key, opts ...Opt) (release func(), refresh func(time.Duration) error, err error)
}

type key interface {
	key() (string, error)
}

func ObjKey(storage string, o dom.Object) key {
	if storage == "" {
		return &keyImpl{
			err: fmt.Errorf("%w: invalid obj lock key: storage is empty", dom.ErrInvalidArg),
		}
	}
	if o.Bucket == "" {
		return &keyImpl{
			err: fmt.Errorf("%w: invalid obj lock key: bucket is empty", dom.ErrInvalidArg),
		}
	}
	if o.Name == "" {
		return &keyImpl{
			err: fmt.Errorf("%w: invalid obj lock key: name is empty", dom.ErrInvalidArg),
		}
	}
	k := "lk:"
	if storage != "" {
		k += storage + ":"
	}
	k += o.Key()
	return &keyImpl{
		k:   k,
		err: nil,
	}
}

func BucketKey(storage, bucket string) key {
	if storage == "" {
		return &keyImpl{
			err: fmt.Errorf("%w: invalid bucket lock key: storage is empty", dom.ErrInvalidArg),
		}
	}
	if bucket == "" {
		return &keyImpl{
			err: fmt.Errorf("%w: invalid bucket lock key: bucket is empty", dom.ErrInvalidArg),
		}
	}

	k := "lkb:"
	if storage != "" {
		k += storage + ":"
	}
	k += bucket

	return &keyImpl{
		k:   k,
		err: nil,
	}
}

func UserKey(user string) key {
	if user == "" {
		return &keyImpl{
			err: fmt.Errorf("%w: invalid user lock key: user is empty", dom.ErrInvalidArg),
		}
	}
	k := "lku:" + user
	return &keyImpl{
		k:   k,
		err: nil,
	}
}

func MigrationKey(fromStorage, toStorage, user string) key {
	if fromStorage == "" {
		return &keyImpl{
			err: fmt.Errorf("%w: invalid migration lock key: fromStorage is empty", dom.ErrInvalidArg),
		}
	}
	if toStorage == "" {
		return &keyImpl{
			err: fmt.Errorf("%w: invalid migration lock key: toStorage is empty", dom.ErrInvalidArg),
		}
	}
	if user == "" {
		return &keyImpl{
			err: fmt.Errorf("%w: invalid migration lock key: user is empty", dom.ErrInvalidArg),
		}
	}

	return &keyImpl{
		k:   fmt.Sprintf("lkm:%s:%s:%s", fromStorage, toStorage, user),
		err: nil,
	}
}
func MigrationCostsKey(fromStorage, toStorage string) key {
	if fromStorage == "" {
		return &keyImpl{
			err: fmt.Errorf("%w: invalid migration costs lock key: fromStorage is empty", dom.ErrInvalidArg),
		}
	}
	if toStorage == "" {
		return &keyImpl{
			err: fmt.Errorf("%w: invalid migration costs lock key: toStorage is empty", dom.ErrInvalidArg),
		}
	}

	return &keyImpl{
		k:   fmt.Sprintf("lkmc:%s:%s", fromStorage, toStorage),
		err: nil,
	}
}

func New(redisClient redis.UniversalClient) Service {
	return &svc{locker: redislock.New(redisClient)}
}

type svc struct {
	locker *redislock.Client
}

func (s *svc) Lock(ctx context.Context, key key, opts ...Opt) (release func(), refresh func(time.Duration) error, err error) {
	lockKey, err := key.key()
	if err != nil {
		return nil, nil, err
	}
	lockOpts := options{
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
	lock, err := s.locker.Obtain(ctx, lockKey, lockOpts.duration, opt)
	if err != nil {
		if errors.Is(err, redislock.ErrNotObtained) {
			return nil, nil, fmt.Errorf("%w: lock not obtained %q", &dom.ErrRateLimitExceeded{RetryIn: util.DurationJitter(time.Second, time.Second*5)}, lockKey)
		}
		return nil, nil, err
	}
	logger.Debug().Msg("lock-service: lock obtained")
	lockCtx, cancelFn := context.WithCancel(ctx)
	go func() {
		<-lockCtx.Done()
		logger.Debug().Msg("lock-service: releasing the lock")
		releaseErr := lock.Release(context.Background())
		if releaseErr != nil && !errors.Is(releaseErr, redislock.ErrLockNotHeld) {
			zerolog.Ctx(ctx).Warn().Err(releaseErr).Str("lock_key", lockKey).Msg("unable to release lock")
		}
		logger.Debug().Err(releaseErr).Msg("lock-service: lock released")
	}()

	return cancelFn, func(d time.Duration) error {
		logger.Debug().Str("refresh", d.String()).Msg("lock-service: refreshing the lock")
		err := lock.Refresh(lockCtx, d, nil)
		logger.Debug().Err(err).Str("refresh", d.String()).Msg("lock-service: lock refreshed")
		return err
	}, nil
}

type keyImpl struct {
	k   string
	err error
}

func (k keyImpl) key() (string, error) {
	return k.k, k.err
}

type options struct {
	duration time.Duration
	retry    bool
}

type Opt interface {
	apply(*options)
}

type durationOption time.Duration

func (d durationOption) apply(opts *options) {
	opts.duration = time.Duration(d)
}

func WithDuration(d time.Duration) Opt {
	return durationOption(d)
}

type retryOption bool

func (d retryOption) apply(opts *options) {
	opts.retry = bool(d)
}

func WithRetry(retry bool) Opt {
	return retryOption(retry)
}
