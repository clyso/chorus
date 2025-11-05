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

package router

import (
	"context"
	"fmt"
	"net/http"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/util"
)

func PolicyMiddleware(policySvc policy.Service) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			user := xctx.GetUser(ctx)
			bucket := xctx.GetBucket(ctx)

			policyCtx, err := initPolicyContext(ctx, policySvc, user, bucket)
			if err != nil {
				util.WriteError(ctx, w, err)
				return
			}
			ctx = policyCtx
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func initPolicyContext(ctx context.Context, policySvc policy.Service, user, bucket string) (context.Context, error) {
	if user == "" {
		return nil, fmt.Errorf("%w: user is not defined in proxy context", dom.ErrInternal)
	}
	if bucket == "" {
		return policySvc.BuildProxyNoBucketContext(ctx, user)
	}
	return policySvc.BuildProxyContext(ctx, user, bucket)
}
