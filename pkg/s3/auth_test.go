package s3

import (
	"maps"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
)

func TestExtractSignedHeadersRejectsUnsignedHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		signedHeaders []string
		headers       http.Header
		wantErr       bool
	}{
		{
			name:          "host is not signed",
			signedHeaders: []string{"x-amz-date"},
			headers:       http.Header{"X-Amz-Date": {"20260910T120000Z"}},
			wantErr:       true,
		},
		{
			name:          "content type is present but not signed",
			signedHeaders: []string{"host"},
			headers:       http.Header{"Content-Type": {"application/octet-stream"}},
			wantErr:       true,
		},
		{
			name:          "amz header is present but not signed",
			signedHeaders: []string{"host"},
			headers:       http.Header{"X-Amz-Meta-Foo": {"bar"}},
			wantErr:       true,
		},
		{
			name:          "one of several amz headers is not signed",
			signedHeaders: []string{"host", "x-amz-content-sha256"},
			headers: http.Header{
				"X-Amz-Content-Sha256": {"UNSIGNED-PAYLOAD"},
				"X-Amz-Acl":            {"public-read"},
			},
			wantErr: true,
		},
		{
			name:          "content type and amz headers are signed",
			signedHeaders: []string{"content-type", "host", "x-amz-content-sha256", "x-amz-meta-foo"},
			headers: http.Header{
				"Content-Type":         {"application/octet-stream"},
				"X-Amz-Content-Sha256": {"UNSIGNED-PAYLOAD"},
				"X-Amz-Meta-Foo":       {"bar"},
			},
		},
		{
			name:          "headers that need no signature may stay unsigned",
			signedHeaders: []string{"host"},
			headers: http.Header{
				"Accept-Encoding": {"gzip"},
				"If-Match":        {"etag"},
				"User-Agent":      {"curl/8.7.1"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := require.New(t)

			req := httptest.NewRequest(http.MethodPut, "http://s3.example.com/bucket/object", nil)
			maps.Copy(req.Header, tc.headers)

			extracted, err := ExtractSignedHeaders(tc.signedHeaders, req)
			if tc.wantErr {
				r.ErrorIs(err, dom.ErrAuth)
				r.Nil(extracted)
				return
			}
			r.NoError(err)
			r.Len(extracted, len(tc.signedHeaders))
		})
	}
}
