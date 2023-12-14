package router

import (
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/replication"
	"github.com/clyso/chorus/pkg/util"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"io"
	"net/http"
)

func Serve(router Router, replSvc replication.Service) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
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
		if isApiErr {
			zerolog.Ctx(ctx).Info().Err(err).Msg("s3 api error returned")
		}
		// create replication tasks according to replication rules
		if err == nil && !isApiErr {
			replCtx, cancel := log.StartNew(ctx)
			defer cancel()
			for _, task := range taskList {
				replErr := replSvc.Replicate(replCtx, task)
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
	return mux
}
