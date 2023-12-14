package standalone

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
)

//go:embed all:static
var staticAssets embed.FS

func serveUI(ctx context.Context, port int) error {
	mux := http.NewServeMux()
	//static, err := fs.Sub(staticAssets, "static")
	//if err != nil {
	//	return err
	//}
	//h := http.FileServer(http.FS(static))
	mux.HandleFunc("/", handleSPA)
	server := http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux}
	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()
	return server.ListenAndServe()
}

func handleSPA(w http.ResponseWriter, r *http.Request) {
	buildPath := "static"
	f, err := staticAssets.Open(filepath.Join(buildPath, r.URL.Path))
	if os.IsNotExist(err) {
		index, err := staticAssets.ReadFile(filepath.Join(buildPath, "index.html"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		w.Write(index)
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()
	http.FileServer(BuildHTTPFS()).ServeHTTP(w, r)
}

func BuildHTTPFS() http.FileSystem {
	build, err := fs.Sub(staticAssets, "static")
	if err != nil {
		panic(err)
	}
	return http.FS(build)
}
