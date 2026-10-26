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

package test

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/service/proxy/cors"
	"github.com/clyso/chorus/test/app"
)

func TestProxy_Cors(t *testing.T) {
	const (
		bucket = "cors-bucket"
		object = "cors-object"
		origin = "http://example.com"
	)
	setup := func(t *testing.T, corsConf *cors.Config) (app.EmbeddedEnv, string) {
		t.Helper()
		r := require.New(t)
		pc, err := app.DeepCopyStruct(proxyConf)
		r.NoError(err)
		pc.Cors = corsConf
		e := app.SetupEmbedded(t, workerConf, pc)
		r.NoError(e.ProxyClient.MakeBucket(t.Context(), bucket, mclient.MakeBucketOptions{}))
		return e, e.ProxyClient.EndpointURL().String()
	}
	do := func(t *testing.T, req *http.Request) *http.Response {
		t.Helper()
		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()
		return res
	}
	preflight := func(t *testing.T, url string) *http.Request {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodOptions, url, nil)
		require.NoError(t, err)
		req.Header.Set("Origin", origin)
		req.Header.Set("Access-Control-Request-Method", http.MethodPut)
		req.Header.Set("Access-Control-Request-Headers", "content-type")
		return req
	}

	t.Run("enabled", func(t *testing.T) {
		e, proxyURL := setup(t, &cors.Config{Enabled: true, Whitelist: []string{origin}})
		ctx := t.Context()

		t.Run("preflight", func(t *testing.T) {
			r := require.New(t)
			res := do(t, preflight(t, proxyURL+"/"+bucket+"/"+object))
			r.Equal(http.StatusNoContent, res.StatusCode)
			r.Equal(origin, res.Header.Get("Access-Control-Allow-Origin"))
			r.Equal(http.MethodPut, res.Header.Get("Access-Control-Allow-Methods"))
			r.Equal("content-type", res.Header.Get("Access-Control-Allow-Headers"))
		})

		t.Run("presigned put", func(t *testing.T) {
			r := require.New(t)
			u, err := e.ProxyClient.PresignedPutObject(ctx, bucket, object, time.Minute)
			r.NoError(err)
			req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), strings.NewReader("data"))
			r.NoError(err)
			req.Header.Set("Origin", origin)
			res := do(t, req)
			r.Equal(http.StatusOK, res.StatusCode)
			r.Equal(origin, res.Header.Get("Access-Control-Allow-Origin"))
			r.Equal("*", res.Header.Get("Access-Control-Expose-Headers"))
			r.NotEmpty(res.Header.Get("ETag"))

			_, err = e.ProxyClient.StatObject(ctx, bucket, object, mclient.StatObjectOptions{})
			r.NoError(err)
		})

		t.Run("unsigned request error keeps cors headers", func(t *testing.T) {
			r := require.New(t)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, proxyURL+"/"+bucket+"/"+object, nil)
			r.NoError(err)
			req.Header.Set("Origin", origin)
			res := do(t, req)
			r.GreaterOrEqual(res.StatusCode, http.StatusBadRequest)
			r.Equal(origin, res.Header.Get("Access-Control-Allow-Origin"))
			r.Equal("*", res.Header.Get("Access-Control-Expose-Headers"))
		})
	})

	t.Run("allowAll", func(t *testing.T) {
		r := require.New(t)
		_, proxyURL := setup(t, &cors.Config{Enabled: true, AllowAll: true})

		res := do(t, preflight(t, proxyURL+"/"+bucket+"/"+object))
		r.Equal(http.StatusNoContent, res.StatusCode)
		r.Equal("*", res.Header.Get("Access-Control-Allow-Origin"))
	})

	t.Run("disabled", func(t *testing.T) {
		r := require.New(t)
		_, proxyURL := setup(t, &cors.Config{Enabled: false, AllowAll: true})

		res := do(t, preflight(t, proxyURL+"/"+bucket+"/"+object))
		for k := range res.Header {
			r.NotContains(k, "Access-Control-")
		}
	})
}
