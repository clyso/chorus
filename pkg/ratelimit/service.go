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
	"fmt"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/log"
)

type RateLimit struct {
	Enabled bool `yaml:"enabled"`
	RPM     int  `yaml:"rpm"`
}

// RPM storage rate limit based on requests per minute
type RPM interface {
	// StorReq acquires storage api request rate limit based on RPM config.
	StorReq(ctx context.Context, storage string) error
	// StorReqN acquires n storage api requests rate limit based on RPM config.
	StorReqN(ctx context.Context, storage string, n int) error
}

func New(rc redis.UniversalClient, conf map[string]RateLimit) *Svc {
	limiter := redis_rate.NewLimiter(rc)

	return &Svc{
		limiter: limiter,
		conf:    conf,
	}
}

var _ RPM = &Svc{}

type Svc struct {
	limiter *redis_rate.Limiter
	conf    map[string]RateLimit
}

func (s *Svc) StorReqN(ctx context.Context, storage string, n int) error {
	conf, ok := s.conf[storage]
	if !ok || !conf.Enabled {
		zerolog.Ctx(ctx).Debug().Str(log.Storage, storage).Msg("rate limit disabled")
		return nil
	}

	res, err := s.limiter.AllowN(ctx, fmt.Sprintf("lim:%s", storage), redis_rate.PerMinute(conf.RPM), n)
	if err != nil {
		// rate limiter failure should not affect business logic.
		// log and return error here:
		zerolog.Ctx(ctx).Err(err).Str(log.Storage, storage).Msg("rate limit error")
		return nil
	}
	if res.Allowed == 0 {
		return &dom.ErrRateLimitExceeded{RetryIn: res.RetryAfter}
	}
	return nil
}

func (s *Svc) StorReq(ctx context.Context, storage string) error {
	conf, ok := s.conf[storage]
	if !ok || !conf.Enabled {
		zerolog.Ctx(ctx).Debug().Str(log.Storage, storage).Msg("rate limit disabled")
		return nil
	}
	res, err := s.limiter.Allow(ctx, fmt.Sprintf("lim:%s", storage), redis_rate.PerMinute(conf.RPM))
	if err != nil {
		// rate limiter failure should not affect business logic.
		// log and return error here:
		zerolog.Ctx(ctx).Err(err).Str(log.Storage, storage).Msg("rate limit error")
		return nil
	}
	if res.Allowed == 0 {
		return &dom.ErrRateLimitExceeded{RetryIn: res.RetryAfter}
	}
	return nil
}
