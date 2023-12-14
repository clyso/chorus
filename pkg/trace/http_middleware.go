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

package trace

import (
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/log"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	"net/http"
)

func HttpMiddleware(tp trace.TracerProvider, next http.Handler) http.Handler {
	return otelhttp.NewHandler(addTraceID(next), "", otelhttp.WithSpanNameFormatter(func(operation string, r *http.Request) string {
		return xctx.GetMethod(r.Context()).String()
	}), otelhttp.WithTracerProvider(tp))
}

func addTraceID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := oteltrace.SpanFromContext(r.Context()).
			SpanContext().
			TraceID()
		ctx := log.WithTraceID(r.Context(), traceID.String())
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
