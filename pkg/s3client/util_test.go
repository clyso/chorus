package s3client

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessHeaders(t *testing.T) {
	t.Parallel()

	// signature over host only: everything else is unsigned by the client
	const authHostOnly = "AWS4-HMAC-SHA256 Credential=access/20260910/us-east-1/s3/aws4_request," +
		"SignedHeaders=host,Signature=0000000000000000000000000000000000000000000000000000000000000000"
	const authHostAndType = "AWS4-HMAC-SHA256 Credential=access/20260910/us-east-1/s3/aws4_request," +
		"SignedHeaders=content-type;host,Signature=0000000000000000000000000000000000000000000000000000000000000000"

	tests := []struct {
		name          string
		origin        http.Header
		wantToSign    []string
		wantNotToSign []string
		wantDropped   []string
	}{
		{
			name: "unsigned content type and amz headers are signed anyway",
			origin: http.Header{
				"Authorization":        {authHostOnly},
				"Content-Type":         {"application/octet-stream"},
				"X-Amz-Content-Sha256": {"UNSIGNED-PAYLOAD"},
				"X-Amz-Meta-Foo":       {"bar"},
				"X-Amz-Acl":            {"public-read"},
			},
			wantToSign: []string{
				"Content-Type",
				"X-Amz-Content-Sha256",
				"X-Amz-Meta-Foo",
				"X-Amz-Acl",
			},
			wantDropped: []string{"Authorization"},
		},
		{
			name: "headers signed by the client stay signed",
			origin: http.Header{
				"Authorization": {authHostAndType},
				"Content-Type":  {"application/octet-stream"},
			},
			wantToSign:  []string{"Content-Type"},
			wantDropped: []string{"Authorization"},
		},
		{
			name: "unsigned non amz headers are forwarded unsigned",
			origin: http.Header{
				"Authorization":   {authHostOnly},
				"Content-Type":    {"application/octet-stream"},
				"Accept-Encoding": {"gzip"},
				"If-Match":        {"etag"},
			},
			wantToSign:    []string{"Content-Type"},
			wantNotToSign: []string{"Accept-Encoding", "If-Match"},
			wantDropped:   []string{"Authorization"},
		},
		{
			name: "proxy hop headers are never signed",
			origin: http.Header{
				"Authorization":   {authHostOnly},
				"X-Forwarded-For": {"10.0.0.1"},
				"X-Real-Ip":       {"10.0.0.1"},
				"Connection":      {"keep-alive"},
			},
			wantNotToSign: []string{"X-Forwarded-For", "X-Real-Ip", "Connection"},
			wantDropped:   []string{"Authorization"},
		},
		{
			name: "date is dropped so the signer sets a fresh one",
			origin: http.Header{
				"Authorization": {authHostOnly},
				"X-Amz-Date":    {"20260910T120000Z"},
			},
			wantDropped: []string{"Authorization", "X-Amz-Date"},
		},
		{
			name: "everything is signed when the client signature is missing",
			origin: http.Header{
				"Content-Type": {"application/octet-stream"},
				"If-Match":     {"etag"},
			},
			wantToSign: []string{"Content-Type", "If-Match"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := require.New(t)

			toSign, notToSign := processHeaders(tc.origin)

			for _, name := range tc.wantToSign {
				r.Equal(tc.origin[name], toSign[name], "%s must be signed", name)
				r.NotContains(notToSign, name)
			}
			for _, name := range tc.wantNotToSign {
				r.Equal(tc.origin[name], notToSign[name], "%s must be forwarded unsigned", name)
				r.NotContains(toSign, name)
			}
			for _, name := range tc.wantDropped {
				r.NotContains(toSign, name, "%s must not be forwarded", name)
				r.NotContains(notToSign, name, "%s must not be forwarded", name)
			}
			r.Len(toSign, len(tc.wantToSign))
			r.Len(notToSign, len(tc.wantNotToSign))
		})
	}
}
