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

package auth

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	mclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/signer"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/s3"
)

const (
	presignTestAccessKey = "test-access-key"
	presignTestSecretKey = "test-secret-key"
	presignTestUser      = "test-user"
)

func presignTestMiddleware() *middleware {
	return &middleware{
		custom: map[string]credMeta{
			presignTestAccessKey: {
				cred: s3.CredentialsV4{
					AccessKeyID:     presignTestAccessKey,
					SecretAccessKey: presignTestSecretKey,
				},
				user: presignTestUser,
			},
		},
		endpoint: "http://s3.example.com:9669",
	}
}

func presignTestRequest(t *testing.T, location string, expires int64) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/bucket-test/object.png", nil)
	req.Host = "s3.example.com:9669"
	return signer.PreSignV4(*req, presignTestAccessKey, presignTestSecretKey, "", location, expires)
}

// presignWithQueryPayloadHash signs req as a presigned request whose payload
// hash is carried by the X-Amz-Content-Sha256 query parameter. minio-go's
// signer only ever takes the hash from the header, so the fixture is built
// with the helpers the verification itself uses.
func presignWithQueryPayloadHash(t *testing.T, req *http.Request, hashedPayload string) *http.Request {
	t.Helper()

	now := time.Now().UTC()
	scope := strings.Join([]string{now.Format("20060102"), "us-east-1", "s3", "aws4_request"}, "/")

	query := req.URL.Query()
	query.Set(s3.AmzAlgorithm, "AWS4-HMAC-SHA256")
	query.Set(s3.AmzCredential, presignTestAccessKey+"/"+scope)
	query.Set(s3.AmzDate, now.Format("20060102T150405Z"))
	query.Set(s3.AmzExpires, "3600")
	query.Set(s3.AmzSignedHeaders, "host")
	req.URL.RawQuery = query.Encode()

	signedHeaders := http.Header{}
	signedHeaders.Set("Host", req.Host)
	canonicalRequest := getCanonicalV4Request(signedHeaders, hashedPayload, req.URL.RawQuery, req.URL.Path, req.Method)
	signingKey := getV4SigningKey(presignTestSecretKey, now, "us-east-1")
	signature := getV4Signature(signingKey, getV4StringToSign(canonicalRequest, now, scope))

	query.Set(s3.AmzSignature, signature)
	req.URL.RawQuery = query.Encode()
	return req
}

// signWithoutPayloadHash signs req with the Authorization header while
// omitting X-Amz-Content-Sha256, hashing the payload as sha256("") - the value
// the verification assumes for such a request. minio-go's signer instead signs
// UNSIGNED-PAYLOAD in that case, so the fixture is hand-built.
func signWithoutPayloadHash(t *testing.T, req *http.Request) *http.Request {
	t.Helper()

	now := time.Now().UTC()
	scope := strings.Join([]string{now.Format("20060102"), "us-east-1", "s3", "aws4_request"}, "/")
	req.Header.Set(s3.AmzDate, now.Format("20060102T150405Z"))

	signedHeaders := http.Header{}
	signedHeaders.Set("Host", req.Host)
	signedHeaders.Set(s3.AmzDate, req.Header.Get(s3.AmzDate))
	canonicalRequest := getCanonicalV4Request(signedHeaders, emptySHA256, req.URL.Query().Encode(), req.URL.Path, req.Method)
	signingKey := getV4SigningKey(presignTestSecretKey, now, "us-east-1")
	signature := getV4Signature(signingKey, getV4StringToSign(canonicalRequest, now, scope))

	req.Header.Set(s3.Authorization, "AWS4-HMAC-SHA256 Credential="+presignTestAccessKey+"/"+scope+
		",SignedHeaders=host;x-amz-date,Signature="+signature)
	return req
}

func TestDoesPresignedSignatureV4Match(t *testing.T) {
	t.Parallel()

	t.Run("valid presigned request", func(t *testing.T) {
		t.Parallel()
		req := presignTestRequest(t, "us-east-1", 3600)
		m := presignTestMiddleware()

		got, _, err := m.doesPresignedSignatureV4Match(req)
		require.NoError(t, err)
		require.Equal(t, presignTestUser, got)
	})

	t.Run("non-default region in credential scope", func(t *testing.T) {
		t.Parallel()
		// Ceph RGW clients commonly presign with the zonegroup name
		// (e.g. "default") instead of an AWS region.
		req := presignTestRequest(t, "default", 3600)
		m := presignTestMiddleware()

		got, _, err := m.doesPresignedSignatureV4Match(req)
		require.NoError(t, err)
		require.Equal(t, presignTestUser, got)
	})

	t.Run("preserves non-auth query params", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodGet, "/bucket-test/object.png?response-content-disposition=attachment", nil)
		req.Host = "s3.example.com:9669"
		req = signer.PreSignV4(*req, presignTestAccessKey, presignTestSecretKey, "", "us-east-1", 3600)
		m := presignTestMiddleware()

		got, _, err := m.doesPresignedSignatureV4Match(req)
		require.NoError(t, err)
		require.Equal(t, presignTestUser, got)
	})

	t.Run("wrong secret key", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodGet, "/bucket-test/object.png", nil)
		req.Host = "s3.example.com:9669"
		req = signer.PreSignV4(*req, presignTestAccessKey, "wrong-secret-key", "", "us-east-1", 3600)
		m := presignTestMiddleware()

		_, _, err := m.doesPresignedSignatureV4Match(req)
		var s3Err mclient.ErrorResponse
		require.ErrorAs(t, err, &s3Err)
		require.Equal(t, "SignatureDoesNotMatch", s3Err.Code)
	})

	t.Run("unknown access key", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodGet, "/bucket-test/object.png", nil)
		req.Host = "s3.example.com:9669"
		req = signer.PreSignV4(*req, "unknown-access-key", presignTestSecretKey, "", "us-east-1", 3600)
		m := presignTestMiddleware()

		_, _, err := m.doesPresignedSignatureV4Match(req)
		var s3Err mclient.ErrorResponse
		require.ErrorAs(t, err, &s3Err)
		require.Equal(t, "InvalidAccessKeyId", s3Err.Code)
	})

	t.Run("expired request", func(t *testing.T) {
		t.Parallel()
		req := presignTestRequest(t, "us-east-1", 3600)
		// rewrite the signing date to the past: the expiry check fires
		// before the signature check.
		query := req.URL.Query()
		query.Set(s3.AmzDate, time.Now().UTC().Add(-2*time.Hour).Format("20060102T150405Z"))
		req.URL.RawQuery = query.Encode()
		m := presignTestMiddleware()

		_, _, err := m.doesPresignedSignatureV4Match(req)
		var s3Err mclient.ErrorResponse
		require.ErrorAs(t, err, &s3Err)
		require.Equal(t, "AccessDenied", s3Err.Code)
		require.Equal(t, "Request has expired", s3Err.Message)
	})

	t.Run("request not valid yet", func(t *testing.T) {
		t.Parallel()
		req := presignTestRequest(t, "us-east-1", 3600)
		query := req.URL.Query()
		query.Set(s3.AmzDate, time.Now().UTC().Add(2*time.Hour).Format("20060102T150405Z"))
		req.URL.RawQuery = query.Encode()
		m := presignTestMiddleware()

		_, _, err := m.doesPresignedSignatureV4Match(req)
		var s3Err mclient.ErrorResponse
		require.ErrorAs(t, err, &s3Err)
		require.Equal(t, "AccessDenied", s3Err.Code)
		require.Equal(t, "Request is not valid yet", s3Err.Message)
	})

	t.Run("expires out of bounds", func(t *testing.T) {
		t.Parallel()
		req := presignTestRequest(t, "us-east-1", 700000) // > 7 days
		m := presignTestMiddleware()

		_, _, err := m.doesPresignedSignatureV4Match(req)
		var s3Err mclient.ErrorResponse
		require.ErrorAs(t, err, &s3Err)
		require.Equal(t, "AuthorizationQueryParametersError", s3Err.Code)
	})
}

func TestIsReqAuthenticatedPresigned(t *testing.T) {
	t.Parallel()

	t.Run("dispatches presigned request", func(t *testing.T) {
		t.Parallel()
		req := presignTestRequest(t, "us-east-1", 3600)
		m := presignTestMiddleware()

		got, hashedPayload, err := m.isReqAuthenticated(req)
		require.NoError(t, err)
		require.Equal(t, presignTestUser, got)
		require.Equal(t, unsignedPayload, hashedPayload)
	})

	t.Run("request without any credentials is still rejected", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodGet, "/bucket-test/object.png", nil)
		req.Host = "s3.example.com:9669"
		m := presignTestMiddleware()

		_, _, err := m.isReqAuthenticated(req)
		var s3Err mclient.ErrorResponse
		require.ErrorAs(t, err, &s3Err)
		require.Equal(t, "CredentialsNotSupported", s3Err.Code)
	})
}

func TestWrapStripsPresignParams(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "/bucket-test/object.png?response-content-disposition=attachment", nil)
	req.Host = "s3.example.com:9669"
	req = signer.PreSignV4(*req, presignTestAccessKey, presignTestSecretKey, "", "us-east-1", 3600)
	m := presignTestMiddleware()

	var forwarded *http.Request
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		forwarded = r
	})
	rec := httptest.NewRecorder()
	m.Wrap(next).ServeHTTP(rec, req)

	require.NotNil(t, forwarded, "request should have been forwarded to the next handler")
	query := forwarded.URL.Query()
	for _, param := range []string{
		s3.AmzAlgorithm, s3.AmzCredential, s3.AmzDate, s3.AmzExpires,
		s3.AmzSignedHeaders, s3.AmzSignature,
	} {
		require.NotContains(t, query, param)
	}
	// non-auth query params must be preserved for the backend.
	require.Equal(t, "attachment", query.Get("response-content-disposition"))
}

func TestWrapSetsVerifiedPayloadHash(t *testing.T) {
	t.Parallel()

	const payloadHash = "b1a4cf30d3f2b0b1b0dbbfd8bdd44b0d9dd8a6dbb45d69f7ec9ee3d5f6ea1b16"

	tests := []struct {
		name    string
		request func(*testing.T) *http.Request
		want    string
	}{
		{
			name: "presigned without payload checksum",
			request: func(t *testing.T) *http.Request {
				return presignTestRequest(t, "us-east-1", 3600)
			},
			want: unsignedPayload,
		},
		{
			name: "presigned with payload checksum in query",
			request: func(t *testing.T) *http.Request {
				req := httptest.NewRequest(http.MethodPut, "/bucket-test/object.png?"+s3.AmzContentSha256+"="+payloadHash, nil)
				req.Host = "s3.example.com:9669"
				return presignWithQueryPayloadHash(t, req, payloadHash)
			},
			want: payloadHash,
		},
		{
			name: "presigned with payload checksum in header",
			request: func(t *testing.T) *http.Request {
				req := httptest.NewRequest(http.MethodPut, "/bucket-test/object.png", nil)
				req.Host = "s3.example.com:9669"
				req.Header.Set(s3.AmzContentSha256, payloadHash)
				return signer.PreSignV4(*req, presignTestAccessKey, presignTestSecretKey, "", "us-east-1", 3600)
			},
			want: payloadHash,
		},
		{
			// the storage must verify the payload against the hash the proxy
			// verified, not against UNSIGNED-PAYLOAD.
			name: "header signature without payload hash",
			request: func(t *testing.T) *http.Request {
				req := httptest.NewRequest(http.MethodGet, "/bucket-test/object.png", nil)
				req.Host = "s3.example.com:9669"
				return signWithoutPayloadHash(t, req)
			},
			want: emptySHA256,
		},
		{
			name: "header signature with payload hash",
			request: func(t *testing.T) *http.Request {
				req := httptest.NewRequest(http.MethodPut, "/bucket-test/object.png", nil)
				req.Host = "s3.example.com:9669"
				req.Header.Set(s3.AmzContentSha256, payloadHash)
				return signer.SignV4(*req, presignTestAccessKey, presignTestSecretKey, "", "us-east-1")
			},
			want: payloadHash,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := require.New(t)

			var forwarded *http.Request
			next := http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
				forwarded = req
			})
			rec := httptest.NewRecorder()
			presignTestMiddleware().Wrap(next).ServeHTTP(rec, tc.request(t))

			r.NotNil(forwarded, "request should have been forwarded to the next handler")
			r.Equal(tc.want, forwarded.Header.Get(s3.AmzContentSha256))
			r.NotContains(forwarded.URL.Query(), s3.AmzContentSha256)
		})
	}
}
