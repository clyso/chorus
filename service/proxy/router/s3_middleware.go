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

package router

import (
	"context"
	"fmt"
	"net/http"

	"github.com/rs/zerolog"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/util"
)

func S3Middleware(policySvc policy.Service) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := log.WithStorType(r.Context(), "S3")
			bucket, object, method := s3.ParseReq(r)

			ctx = log.WithBucket(ctx, bucket)
			ctx = log.WithObjName(ctx, object)
			ctx = log.WithMethod(ctx, method)
			ctx = log.WithFlow(ctx, xctx.Event)
			if method == s3.UndefinedMethod {
				zerolog.Ctx(ctx).Warn().Str("request_url", r.Method+": "+r.URL.Path+"?"+r.URL.RawQuery).Msg("unable to define s3 method")
			}

			policyCtx, err := initPolicyContext(ctx, policySvc)
			if err != nil {
				util.WriteError(ctx, w, err)
				return
			}
			ctx = policyCtx

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func initPolicyContext(ctx context.Context, policySvc policy.Service) (context.Context, error) {
	user := xctx.GetUser(ctx)
	bucket := xctx.GetBucket(ctx)
	if bucket == "" {
		// handle ListBuckets request here. It is the only request without a bucket.
		if xctx.GetMethod(ctx) != s3.ListBuckets {
			// should never happen
			return nil, fmt.Errorf("%w: bucket is not defined in context for s3 method %s", dom.ErrInternal, xctx.GetMethod(ctx).String())
		}
		return policySvc.BuildProxyNoBucketContext(ctx, user)
	}
	return policySvc.BuildProxyContext(ctx, user, bucket)
}
