package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/minio/minio-go/v7/pkg/signer"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/s3"
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

func TestDoesSignatureV2MatchAcceptsSignedVirtualHostRequest(t *testing.T) {
	t.Parallel()

	const (
		accessKey = "test-access-key"
		secretKey = "test-secret-key"
		user      = "test-user"
	)

	req := httptest.NewRequest(http.MethodGet, "/object/nested?acl", nil)
	req.Host = "bucket.s3.example.com"
	req = signer.SignV2(*req, accessKey, secretKey, true)

	m := &middleware{
		allowV2: true,
		custom: map[string]credMeta{
			accessKey: {
				cred: s3.CredentialsV4{
					AccessKeyID:     accessKey,
					SecretAccessKey: secretKey,
				},
				user: user,
			},
		},
		endpoint: "http://s3.example.com",
	}

	got, err := m.doesSignatureV2Match(req)
	require.NoError(t, err)
	require.Equal(t, user, got)
}
