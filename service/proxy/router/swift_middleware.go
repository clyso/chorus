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
	"net/http"

	"github.com/rs/zerolog"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/swift"
	"github.com/clyso/chorus/pkg/util"
)

func SwiftMiddleware(policySvc policy.Service) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := log.WithStorType(r.Context(), "swift")
			ctx = log.WithFlow(ctx, xctx.Event)

			account, bucket, object, method := swift.ParseReq(r)
			ctx = log.WithUser(ctx, account)
			ctx = log.WithBucket(ctx, bucket)
			ctx = log.WithObjName(ctx, object)
			ctx = log.WithSwiftMethod(ctx, method)
			if method == swift.UndefinedMethod {
				zerolog.Ctx(ctx).Warn().Str("request_url", r.Method+": "+r.URL.Path+"?"+r.URL.RawQuery).Msg("unable to define swift method")
			}
			policyCtx, err := initSwiftPolicyContext(ctx, policySvc, account, bucket)
			if err != nil {
				util.WriteError(ctx, w, err)
				return
			}
			ctx = policyCtx

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func initSwiftPolicyContext(ctx context.Context, policySvc policy.Service, account, bucket string) (context.Context, error) {
	if bucket == "" {
		return policySvc.BuildProxyNoBucketContext(ctx, account)
	}
	return policySvc.BuildProxyContext(ctx, account, bucket)
}
