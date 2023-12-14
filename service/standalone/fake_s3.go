package standalone

import (
	"context"
	"fmt"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"net/http"
)

func serveFakeS3(ctx context.Context, port int) error {
	mux := http.NewServeMux()
	mux.Handle("/", gofakes3.New(s3mem.New()).Server())
	server := http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux}
	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()
	return server.ListenAndServe()
}
