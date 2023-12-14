package log

import (
	"github.com/rs/zerolog"
	"net/http"
)

func HttpMiddleware(cfg *Config, app, appID string, next http.Handler) http.Handler {
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
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
