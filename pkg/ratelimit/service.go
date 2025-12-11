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
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/swift"
)

type RateLimit struct {
	Enabled            bool `yaml:"enabled"`
	IncludeMetadataAPI bool `yaml:"includeMetadataAPI"`
	RPM                int  `yaml:"rpm"`
}

// RPM storage rate limit based on requests per minute
type RPM interface {
	// StorReq acquires storage api request rate limit based on RPM config.
	StorReq(ctx context.Context, storage string, opts ...Opt) error
	// StorReqN acquires n storage api requests rate limit based on RPM config.
	StorReqN(ctx context.Context, storage string, n int, opts ...Opt) error
}

func New(rc redis.UniversalClient, conf map[string]RateLimit) *Svc {
	limiter := redis_rate.NewLimiter(rc)

	return &Svc{
		limiter: limiter,
		conf:    conf,
	}
}

type Opt interface {
	apply(*opts)
}

type opts struct {
	s3Methods    []s3.Method
	swiftMethods []swift.Method
}

type s3MethodOpt s3.Method

func (o s3MethodOpt) apply(opts *opts) {
	opts.s3Methods = append(opts.s3Methods, s3.Method(o))
}

func S3Method(m s3.Method) Opt {
	return s3MethodOpt(m)
}

type swiftMethodOpt swift.Method

func (o swiftMethodOpt) apply(opts *opts) {
	opts.swiftMethods = append(opts.swiftMethods, swift.Method(o))
}

func SwiftMethod(m swift.Method) Opt {
	return swiftMethodOpt(m)
}

var _ RPM = &Svc{}

type Svc struct {
	limiter *redis_rate.Limiter
	conf    map[string]RateLimit
}

func (s *Svc) StorReqN(ctx context.Context, storage string, n int, options ...Opt) error {
	conf, ok := s.conf[storage]
	if !ok || !conf.Enabled {
		zerolog.Ctx(ctx).Debug().Str(log.Storage, storage).Msg("rate limit disabled")
		return nil
	}
	n = filterRateLimitMethods(conf, n, options...)
	if n <= 0 {
		zerolog.Ctx(ctx).Debug().Str(log.Storage, storage).Msg("rate limit disabled for non upload/download methods")
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

func (s *Svc) StorReq(ctx context.Context, storage string, options ...Opt) error {
	conf, ok := s.conf[storage]
	if !ok || !conf.Enabled {
		zerolog.Ctx(ctx).Debug().Str(log.Storage, storage).Msg("rate limit disabled")
		return nil
	}
	n := filterRateLimitMethods(conf, 1, options...)
	if n <= 0 {
		zerolog.Ctx(ctx).Debug().Str(log.Storage, storage).Msg("rate limit disabled for non upload/download methods")
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

var s3UploadDownloadMethods = map[s3.Method]struct{}{
	s3.GetObject:               {},
	s3.PutObject:               {},
	s3.CompleteMultipartUpload: {},
}

var swiftUploadDownloadMethods = map[swift.Method]struct{}{
	swift.GetObject: {},
	swift.PutObject: {},
}

func filterRateLimitMethods(conf RateLimit, n int, options ...Opt) int {
	if conf.IncludeMetadataAPI {
		// all methods included to rate limit
		return n
	}
	mehtods := &opts{}
	for _, o := range options {
		o.apply(mehtods)
	}
	for _, m := range mehtods.s3Methods {
		if _, ok := s3UploadDownloadMethods[m]; !ok {
			// exclude metadata methods
			n--
		}
	}
	for _, m := range mehtods.swiftMethods {
		if _, ok := swiftUploadDownloadMethods[m]; !ok {
			// exclude metadata methods
			n--
		}
	}
	return n
}
