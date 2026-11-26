package copy

import (
	"net/http"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
)

func TestObjectUpToDate(t *testing.T) {
	withSourceETag := func(etag string) http.Header {
		h := http.Header{}
		h.Set(CChorusSourceETagMetaHeader, etag)
		return h
	}
	tests := []struct {
		name         string
		from         minio.ObjectInfo
		to           minio.ObjectInfo
		bySourceETag bool
		want         bool
	}{
		{
			name: "same etag and size",
			from: minio.ObjectInfo{ETag: "md5", Size: 10},
			to:   minio.ObjectInfo{ETag: "md5", Size: 10},
			want: true,
		},
		{
			name: "same etag, different size",
			from: minio.ObjectInfo{ETag: "md5", Size: 10},
			to:   minio.ObjectInfo{ETag: "md5", Size: 11},
			want: false,
		},
		{
			name: "recorded source etag is ignored when the feature is off",
			from: minio.ObjectInfo{ETag: "md5", Size: 10},
			to:   minio.ObjectInfo{ETag: "sha256", Size: 10, Metadata: withSourceETag("md5")},
			want: false,
		},
		{
			name:         "recorded source etag matches",
			from:         minio.ObjectInfo{ETag: "md5", Size: 10},
			to:           minio.ObjectInfo{ETag: "sha256", Size: 10, Metadata: withSourceETag("md5")},
			bySourceETag: true,
			want:         true,
		},
		{
			name:         "recorded source etag is stale",
			from:         minio.ObjectInfo{ETag: "md5-new", Size: 10},
			to:           minio.ObjectInfo{ETag: "sha256", Size: 10, Metadata: withSourceETag("md5-old")},
			bySourceETag: true,
			want:         false,
		},
		{
			name:         "recorded source etag matches but size differs",
			from:         minio.ObjectInfo{ETag: "md5", Size: 12},
			to:           minio.ObjectInfo{ETag: "sha256", Size: 10, Metadata: withSourceETag("md5")},
			bySourceETag: true,
			want:         false,
		},
		{
			name:         "different etag, nothing recorded",
			from:         minio.ObjectInfo{ETag: "md5", Size: 10},
			to:           minio.ObjectInfo{ETag: "sha256", Size: 10},
			bySourceETag: true,
			want:         false,
		},
		{
			name:         "empty source etag never matches a recorded value",
			from:         minio.ObjectInfo{ETag: "", Size: 0},
			to:           minio.ObjectInfo{ETag: "sha256", Size: 0, Metadata: withSourceETag("")},
			bySourceETag: true,
			want:         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, objectUpToDate(tt.from, tt.to, tt.bySourceETag))
		})
	}
}
