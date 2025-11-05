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

package log

import (
	"net/http"

	"github.com/rs/zerolog"

	xctx "github.com/clyso/chorus/pkg/ctx"
)

func HttpMiddleware(cfg *Config, app, appID string, flow xctx.Flow) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			l := CreateLogger(cfg, app, appID)
			builder := l.With()
			if zerolog.GlobalLevel() < zerolog.InfoLevel {
				builder = builder.Str(httpMethod, r.Method).
					Str(httpPath, r.URL.Path).
					Str(httpQuery, r.URL.RawQuery)
			}
			newLogger := builder.Logger()

			ctx := newLogger.WithContext(r.Context())
			ctx = WithFlow(ctx, flow)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
