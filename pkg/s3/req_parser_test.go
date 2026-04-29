package s3

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseBucketAndObjectForHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		target          string
		host            string
		endpointAddress string
		wantBucket      string
		wantObject      string
		wantVirtualHost bool
	}{
		{
			name:            "path style",
			target:          "http://s3.example.com/bucket/object/nested",
			host:            "s3.example.com",
			endpointAddress: "http://s3.example.com",
			wantBucket:      "bucket",
			wantObject:      "object/nested",
		},
		{
			name:            "virtual host object",
			target:          "http://bucket.s3.example.com/object/nested",
			host:            "bucket.s3.example.com",
			endpointAddress: "http://s3.example.com",
			wantBucket:      "bucket",
			wantObject:      "object/nested",
			wantVirtualHost: true,
		},
		{
			name:            "virtual host bucket root",
			target:          "http://bucket.s3.example.com/",
			host:            "bucket.s3.example.com:9669",
			endpointAddress: "http://s3.example.com:9669",
			wantBucket:      "bucket",
			wantVirtualHost: true,
		},
		{
			name:            "dotted bucket virtual host",
			target:          "http://my.bucket.s3.example.com/photos/2026/04/image.jpg",
			host:            "my.bucket.s3.example.com",
			endpointAddress: "http://s3.example.com",
			wantBucket:      "my.bucket",
			wantObject:      "photos/2026/04/image.jpg",
			wantVirtualHost: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest("GET", tt.target, nil)
			req.Host = tt.host

			bucket, object, virtualHost := ParseBucketAndObjectForHost(req, tt.endpointAddress)
			require.Equal(t, tt.wantBucket, bucket)
			require.Equal(t, tt.wantObject, object)
			require.Equal(t, tt.wantVirtualHost, virtualHost)
		})
	}
}

func TestParseReqForHostRoutesVirtualHostBucketRequests(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest("GET", "http://bucket.s3.example.com/?acl", nil)
	req.Host = "bucket.s3.example.com"

	res := ParseReqForHost(req, "http://s3.example.com")

	require.Equal(t, "bucket", res.Bucket)
	require.Empty(t, res.Object)
	require.True(t, res.VirtualHost)
	require.Equal(t, GetBucketAcl, res.Method)
}
