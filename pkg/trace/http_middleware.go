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
