package router

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/s3"
)

func TestS3MiddlewareRewritesVirtualHostRequestToPathStyle(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "http://bucket.s3.example.com/folder/file%20name.txt?versionId=1", nil)
	req.Host = "bucket.s3.example.com"

	var called bool
	handler := S3Middleware("http://s3.example.com")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		require.Equal(t, "bucket", xctx.GetBucket(r.Context()))
		require.Equal(t, "folder/file name.txt", xctx.GetObject(r.Context()))
		require.Equal(t, s3.GetObject, xctx.GetMethod(r.Context()))
		require.Equal(t, "/bucket/folder/file name.txt", r.URL.Path)
		require.Equal(t, "/bucket/folder/file%20name.txt", r.URL.RawPath)
	}))

	handler.ServeHTTP(httptest.NewRecorder(), req)

	require.True(t, called)
}

func TestS3MiddlewareKeepsPathStyleRequestPath(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "http://s3.example.com/bucket/object", nil)
	req.Host = "s3.example.com"

	var called bool
	handler := S3Middleware("http://s3.example.com")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		require.Equal(t, "bucket", xctx.GetBucket(r.Context()))
		require.Equal(t, "object", xctx.GetObject(r.Context()))
		require.Equal(t, "/bucket/object", r.URL.Path)
		require.Empty(t, r.URL.RawPath)
	}))

	handler.ServeHTTP(httptest.NewRecorder(), req)

	require.True(t, called)
}
