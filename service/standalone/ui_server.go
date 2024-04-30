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

package standalone

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"

	"github.com/clyso/chorus/pkg/dom"
)

//go:embed all:static
var staticAssets embed.FS

func serveUI(ctx context.Context, port int) (func() error, error) {
	f, err := staticAssets.Open("static/index.html")
	if err != nil {
		return nil, dom.ErrNotFound
	}
	_ = f.Close()
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleSPA)
	server := http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux}
	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()
	return server.ListenAndServe, nil
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
