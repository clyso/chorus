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
	"io"
	"net/http"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"

	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/replication"
	"github.com/clyso/chorus/pkg/util"
)

func Serve(router Router, replSvc replication.Service) http.Handler {
	// Use a custom handler function instead of ServeMux to avoid automatic redirects
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := otel.Tracer("").Start(r.Context(), "Route")
		defer span.End()
		r = r.WithContext(ctx)
		logger := zerolog.Ctx(r.Context())
		logger.Debug().Msg("proxy: new request received")

		resp, taskList, storage, isApiErr, err := router.Route(r)
		if err != nil {
			util.WriteError(r.Context(), w, err)
			return
		}
		defer func() {
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
		}()
		ctx = log.WithStorage(ctx, storage)
		// TODO: is it reachable? This branch is active only if err == nil
		if isApiErr {
			zerolog.Ctx(ctx).Info().Err(err).Msg("s3 api error returned")
			// create replication tasks according to replication rules
		} else {
			replCtx, cancel := log.StartNew(ctx)
			defer cancel()
			for _, task := range taskList {
				replErr := replSvc.Replicate(replCtx, storage, task)
				if replErr != nil {
					logger.Err(replErr).Msg("unable to handle replication")
				}
			}
		}
		// Forward response to original client
		for k, v := range resp.Header {
			w.Header().Set(k, v[0])
		}
		w.WriteHeader(resp.StatusCode)
		_, err = io.Copy(w, resp.Body)
		if err != nil {
			logger.Err(err).Msg("unable to copy response body")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	})
}
