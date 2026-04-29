package auth

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		path            string
		host            string
		endpointAddress string
		want            string
	}{
		{
			name:            "path style",
			path:            "/bucket/object",
			host:            "s3.example.com",
			endpointAddress: "http://s3.example.com",
			want:            "/bucket/object",
		},
		{
			name:            "virtual host object",
			path:            "/object/nested",
			host:            "bucket.s3.example.com",
			endpointAddress: "http://s3.example.com",
			want:            "/bucket/object/nested",
		},
		{
			name:            "virtual host bucket root",
			path:            "/",
			host:            "bucket.s3.example.com:9669",
			endpointAddress: "http://s3.example.com:9669",
			want:            "/bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := getResource(tt.path, tt.host, tt.endpointAddress)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
