package router

import (
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/rs/zerolog"
	"net/http"
)

func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket, object, method := s3.ParseReq(r)
		ctx := log.WithBucket(r.Context(), bucket)
		ctx = log.WithObjName(ctx, object)
		ctx = log.WithMethod(ctx, method)
		ctx = log.WithFlow(ctx, xctx.Event)
		if method == s3.UndefinedMethod {
			zerolog.Ctx(ctx).Warn().Str("request_url", r.Method+": "+r.URL.Path+"?"+r.URL.RawQuery).Msg("unable to define s3 method")
		}

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
