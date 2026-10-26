/*
 * Copyright © 2026 Clyso GmbH
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
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/replication"
	"github.com/clyso/chorus/pkg/tasks"
)

// backendStub returns a response with backend CORS headers and no replication tasks.
type backendStub struct{}

func (backendStub) Route(*http.Request) (*http.Response, []tasks.ReplicationTask, string, bool, error) {
	h := http.Header{}
	h.Set("Access-Control-Allow-Origin", "http://backend.com")
	h.Set("Access-Control-Expose-Headers", "ETag")
	h.Set("Vary", "Accept-Encoding")
	return &http.Response{StatusCode: http.StatusOK, Header: h, Body: io.NopCloser(strings.NewReader(""))}, nil, "main", false, nil
}

func TestNew_BackendCorsHeaders(t *testing.T) {
	passThrough := func(next http.Handler) http.Handler { return next }
	setPolicy := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(xctx.SetRoutingPolicy(r.Context(), "main")))
		})
	}
	proxyCors := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Add("Vary", "Origin")
			next.ServeHTTP(w, r)
		})
	}
	tests := []struct {
		corsMiddleware func(next http.Handler) http.Handler
		name           string
		wantACAO       string
		wantACEH       string
		wantVary       []string
	}{
		{name: "proxy cors enabled: backend headers dropped", corsMiddleware: proxyCors, wantACAO: "*", wantACEH: "", wantVary: []string{"Origin", "Accept-Encoding"}},
		{name: "proxy cors disabled: backend headers pass through", corsMiddleware: nil, wantACAO: "http://backend.com", wantACEH: "ETag", wantVary: []string{"Accept-Encoding"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			handler, err := New(Config{
				Storages: map[dom.StorageType]StorageProxy{dom.S3: {
					Router:             backendStub{},
					Replicator:         struct{ replication.Service }{},
					ReqParseMiddleware: passThrough,
				}},
				LogMiddleware:    passThrough,
				PolicyMiddleware: setPolicy,
				CorsMiddleware:   tt.corsMiddleware,
			})
			r.NoError(err)

			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/bucket/object", nil))

			r.Equal(http.StatusOK, w.Code)
			r.Equal(tt.wantACAO, w.Header().Get("Access-Control-Allow-Origin"))
			r.Equal(tt.wantACEH, w.Header().Get("Access-Control-Expose-Headers"))
			r.Equal(tt.wantVary, w.Header().Values("Vary"))
		})
	}
}
