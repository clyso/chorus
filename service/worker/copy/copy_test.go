package copy

import (
	"net/http"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
)

func TestApplyStandardHeaders(t *testing.T) {
	expires := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	stat := minio.ObjectInfo{
		Metadata: http.Header{
			"Cache-Control":       {"max-age=3600"},
			"Content-Encoding":    {"gzip"},
			"Content-Disposition": {`attachment; filename="h.bin"`},
			"Content-Language":    {"en-US"},
		},
		Expires: expires,
	}

	var opts minio.PutObjectOptions
	applyStandardHeaders(&opts, stat)
	require.Equal(t, "max-age=3600", opts.CacheControl)
	require.Equal(t, "gzip", opts.ContentEncoding)
	require.Equal(t, `attachment; filename="h.bin"`, opts.ContentDisposition)
	require.Equal(t, "en-US", opts.ContentLanguage)
	require.True(t, opts.Expires.Equal(expires))

	var none minio.PutObjectOptions
	applyStandardHeaders(&none, minio.ObjectInfo{})
	require.Equal(t, minio.PutObjectOptions{}, none)
}
