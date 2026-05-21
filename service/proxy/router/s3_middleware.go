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
	"net/http"
	"net/url"
	"strings"

	"github.com/rs/zerolog"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/s3"
)

func S3Middleware(endpointAddress string) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			res := s3.ParseReqForHost(r, endpointAddress)

			ctx := r.Context()
			ctx = log.WithBucket(ctx, res.Bucket)
			ctx = log.WithObjName(ctx, res.Object)
			ctx = log.WithMethod(ctx, res.Method)
			ctx = log.WithObjVer(ctx, res.ObjVersionID)
			ctx = log.WithFlow(ctx, xctx.Event)
			if res.VirtualHost {
				if err := rewriteVirtualHostPath(r.URL, res.Bucket); err != nil {
					zerolog.Ctx(ctx).Err(err).Str("request_url", r.URL.String()).Msg("unable to rewrite virtual-host-style s3 request")
					http.Error(w, "invalid virtual-host-style request", http.StatusBadRequest)
					return
				}
			}
			if res.Method == s3.UndefinedMethod {
				zerolog.Ctx(ctx).Warn().Str("request_url", r.Method+": "+r.URL.Path+"?"+r.URL.RawQuery).Msg("unable to define s3 method")
			}
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func rewriteVirtualHostPath(u *url.URL, bucket string) error {
	escapedPath := strings.TrimPrefix(u.EscapedPath(), "/")
	rewrittenRawPath := "/" + url.PathEscape(bucket)
	if escapedPath != "" {
		rewrittenRawPath += "/" + escapedPath
	}

	rewrittenPath, err := url.PathUnescape(rewrittenRawPath)
	if err != nil {
		return err
	}

	u.Path = rewrittenPath
	u.RawPath = rewrittenRawPath
	return nil
}
