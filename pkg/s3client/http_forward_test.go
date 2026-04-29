package s3client

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRewriteVirtualHostPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		rawURL      string
		bucket      string
		wantPath    string
		wantRawPath string
	}{
		{
			name:        "bucket root",
			rawURL:      "http://bucket.s3.example.com/",
			bucket:      "bucket",
			wantPath:    "/bucket",
			wantRawPath: "/bucket",
		},
		{
			name:        "object path keeps escaping",
			rawURL:      "http://bucket.s3.example.com/folder/file%20name.txt",
			bucket:      "bucket",
			wantPath:    "/bucket/folder/file name.txt",
			wantRawPath: "/bucket/folder/file%20name.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			u, err := url.Parse(tt.rawURL)
			require.NoError(t, err)

			err = rewriteVirtualHostPath(u, tt.bucket)
			require.NoError(t, err)
			require.Equal(t, tt.wantPath, u.Path)
			require.Equal(t, tt.wantRawPath, u.RawPath)
		})
	}
}
