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

package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/xid"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/util"
)

const leaseInterval = time.Second * 2
const leaseOverlap = time.Second
const maxRetries = 1000

// Semaphore storage rate limit based on N concurrent resource clients.
type Semaphore interface {
	// TryAcquire tries to acquire storage resource until work function is done.
	// Returns release function. Also, will be released when ctx cancelled.
	// Returns dom.ErrRateLimitExceeded if resource is not acquired.
	TryAcquire(ctx context.Context) (release func(), err error)
	// TryAcquireN - see TryAcquire
	TryAcquireN(ctx context.Context, n int64) (release func(), err error)
}

type SemaphoreConfig struct {
	Enabled  bool          `yaml:"enabled"`
	Limit    int64         `yaml:"limit"`
	RetryMin time.Duration `yaml:"retryMin"`
	RetryMax time.Duration `yaml:"retryMax"`
}

func LocalSemaphore(c SemaphoreConfig, name string) *Local {
	return &Local{c: c, name: name}
}

var _ Semaphore = &Local{}

type Local struct {
	name  string
	c     SemaphoreConfig
	usage int64
}

func (l *Local) TryAcquire(ctx context.Context) (func(), error) {
	return l.TryAcquireN(ctx, 1)
}

func (l *Local) TryAcquireN(ctx context.Context, n int64) (func(), error) {
	logger := zerolog.Ctx(ctx).With().Str("limit_name", l.name).Int64("acquire", n).Logger()
	if !l.c.Enabled {
		logger.Debug().Msg("local rate limit is disabled")
		return func() {}, nil
	}
	if n > l.c.Limit {
		return nil, fmt.Errorf("%w: invalid ratelimit config: N (%d) is more than total limit (%d)", dom.ErrInvalidArg, n, l.c.Limit)
	}

	newUsage := atomic.AddInt64(&l.usage, n)
	if newUsage > l.c.Limit {
		_ = atomic.AddInt64(&l.usage, -n)
		retryIn := util.DurationJitter(l.c.RetryMin, l.c.RetryMax)
		logger.Info().
			Int64("new_usage", newUsage).
			Int64("limit", l.c.Limit).
			Str("retry_in", retryIn.String()).
			Msg("local rate limit exceeded")
		return nil, &dom.ErrRateLimitExceeded{RetryIn: retryIn}
	}

	logger.Debug().Msg("local rate limit acquired")

	ctxAcq, release := context.WithCancel(ctx)
	go func() {
		<-ctxAcq.Done()
		atomic.AddInt64(&l.usage, -n)
		logger.Debug().Msg("local rate limit released")
	}()
	return release, nil
}

func GlobalSemaphore(rc redis.UniversalClient, c SemaphoreConfig, name string) *Global {
	return &Global{
		c:    c,
		rc:   rc,
		name: name,
	}
}

var _ Semaphore = &Global{}

type Global struct {
	c    SemaphoreConfig
	rc   redis.UniversalClient
	name string
}

func (g *Global) TryAcquire(ctx context.Context) (func(), error) {
	return g.TryAcquireN(ctx, 1)
}

func (g *Global) TryAcquireN(ctx context.Context, n int64) (func(), error) {
	acquireID := xid.New().String()
	logger := zerolog.Ctx(ctx).With().Str("limit_name", g.name).Str("acquire_id", acquireID).Int64("acquire", n).Logger()
	if !g.c.Enabled {
		logger.Debug().Msg("global rate limit is disabled")
		return func() {}, nil
	}
	if n > g.c.Limit {
		return nil, fmt.Errorf("%w: invalid ratelimit config: N (%d) is more than total limit (%d)", dom.ErrInvalidArg, n, g.c.Limit)
	}

	err := g.acquire(ctx, logger, n, acquireID)
	if err != nil {
		return nil, err
	}
	acqCtx, release := context.WithCancel(ctx)
	go func() {
		timer := time.NewTimer(leaseInterval)
		defer timer.Stop()
		for {
			select {
			case <-acqCtx.Done():
				err = g.rc.ZRem(context.TODO(), g.key(), acquireID).Err()
				if err != nil {
					logger.Info().Err(err).Msg("unable to release global rate limit")
				}
				logger.Debug().Msg("global rate limit released")
				return
			case <-timer.C:
				err = g.rc.ZAdd(ctx, g.key(), redis.Z{
					Score:  float64(time.Now().Add(leaseOverlap).UnixMilli()),
					Member: acquireID,
				}).Err()
				if err != nil {
					zerolog.Ctx(ctx).Err(err).Msg("unable to refresh global rate limit")
				}
				timer.Reset(leaseInterval)
			}
		}
	}()

	return release, nil

}

func (g *Global) key() string {
	return fmt.Sprintf("chorus:sem:%s", g.name)
}

func (g *Global) acquire(ctx context.Context, logger zerolog.Logger, n int64, acquireID string) error {
	deleteBefore := time.Now().Add(-(leaseInterval + leaseOverlap)).UnixMilli()
	err := g.rc.ZRemRangeByScore(ctx, g.key(), "0", strconv.Itoa(int(deleteBefore))).Err()
	if err != nil {
		return fmt.Errorf("%w: unable to clean up semaphore", err)
	}

	// Transactional function.
	txf := func(tx *redis.Tx) error {
		usage, err := g.rc.ZCard(ctx, g.key()).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		}
		if usage+n > g.c.Limit {
			retryIn := util.DurationJitter(g.c.RetryMin, g.c.RetryMax)
			logger.Debug().
				Int64("current_usage", usage).
				Int64("limit", g.c.Limit).
				Str("retry_in", retryIn.String()).
				Msg("global rate limit exceeded")
			return &dom.ErrRateLimitExceeded{RetryIn: retryIn}
		}

		// Operation is committed only if the watched keys remain unchanged.
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			return pipe.ZAdd(ctx, g.key(), redis.Z{
				Score:  float64(time.Now().Add(leaseOverlap).UnixMilli()),
				Member: acquireID,
			}).Err()
		})
		return err
	}

	// Retry if the key has been changed.
	for i := 0; i < maxRetries; i++ {
		err = g.rc.Watch(ctx, txf, g.key())
		if errors.Is(err, redis.TxFailedErr) {
			// Optimistic lock lost. Retry.
			continue
		}
		return err
	}

	return fmt.Errorf("%w: unable to do redis tx", &dom.ErrRateLimitExceeded{RetryIn: util.DurationJitter(g.c.RetryMin, g.c.RetryMax)})
}
