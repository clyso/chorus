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

package metrics

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	xctx "github.com/clyso/chorus/pkg/ctx"
)

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func NewResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{w, http.StatusOK}
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// Proxy HTTP metrics are registered at package init so ProxyMiddleware
// can be applied more than once — once per configured storage type
// (S3, Swift, …). Registering inside the closure panicked with
// "duplicate metrics collector registration attempted" the second time
// the middleware was wired up.
var (
	proxyTotalRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "proxy_requests_total",
			Help: "Number of requests to chorus s3 proxy.",
		},
		[]string{"method"},
	)

	proxyResponseStatus = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "proxy_response_status",
			Help: "Status of chorus s3 proxy response.",
		},
		[]string{"status"},
	)

	proxyHTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "proxy_response_time_seconds",
		Help:    "Duration of chorus s3 proxy requests.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method"})
)

func ProxyMiddleware() func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			method := xctx.GetMethod(r.Context())

			timer := prometheus.NewTimer(proxyHTTPDuration.WithLabelValues(method.String()))
			rw := NewResponseWriter(w)
			next.ServeHTTP(rw, r)
			statusCode := rw.statusCode

			proxyResponseStatus.WithLabelValues(strconv.Itoa(statusCode)).Inc()
			proxyTotalRequests.WithLabelValues(method.String()).Inc()
			timer.ObserveDuration()
		})
	}
}

func AgentMiddleware(next http.Handler) http.Handler {
	var totalRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agent_requests_total",
			Help: "Number of requests to chorus agent.",
		},
		[]string{"status"},
	)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		rw := NewResponseWriter(w)
		next.ServeHTTP(rw, r)
		statusCode := rw.statusCode

		totalRequests.WithLabelValues(strconv.Itoa(statusCode)).Inc()
	})
}
