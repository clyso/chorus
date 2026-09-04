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

func TestDoesPresignedSignatureV4Match(t *testing.T) {
	t.Parallel()

	t.Run("valid presigned request", func(t *testing.T) {
		t.Parallel()
		req := presignTestRequest(t, "us-east-1", 3600)
		m := presignTestMiddleware()

		got, err := m.doesPresignedSignatureV4Match(req)
		require.NoError(t, err)
		require.Equal(t, presignTestUser, got)
	})

	t.Run("non-default region in credential scope", func(t *testing.T) {
		t.Parallel()
		// Ceph RGW clients commonly presign with the zonegroup name
		// (e.g. "default") instead of an AWS region.
		req := presignTestRequest(t, "default", 3600)
		m := presignTestMiddleware()

		got, err := m.doesPresignedSignatureV4Match(req)
		require.NoError(t, err)
		require.Equal(t, presignTestUser, got)
	})

	t.Run("preserves non-auth query params", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodGet, "/bucket-test/object.png?response-content-disposition=attachment", nil)
		req.Host = "s3.example.com:9669"
		req = signer.PreSignV4(*req, presignTestAccessKey, presignTestSecretKey, "", "us-east-1", 3600)
		m := presignTestMiddleware()

		got, err := m.doesPresignedSignatureV4Match(req)
		require.NoError(t, err)
		require.Equal(t, presignTestUser, got)
	})

	t.Run("wrong secret key", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodGet, "/bucket-test/object.png", nil)
		req.Host = "s3.example.com:9669"
		req = signer.PreSignV4(*req, presignTestAccessKey, "wrong-secret-key", "", "us-east-1", 3600)
		m := presignTestMiddleware()

		_, err := m.doesPresignedSignatureV4Match(req)
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

		_, err := m.doesPresignedSignatureV4Match(req)
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

		_, err := m.doesPresignedSignatureV4Match(req)
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

		_, err := m.doesPresignedSignatureV4Match(req)
		var s3Err mclient.ErrorResponse
		require.ErrorAs(t, err, &s3Err)
		require.Equal(t, "AccessDenied", s3Err.Code)
		require.Equal(t, "Request is not valid yet", s3Err.Message)
	})

	t.Run("expires out of bounds", func(t *testing.T) {
		t.Parallel()
		req := presignTestRequest(t, "us-east-1", 700000) // > 7 days
		m := presignTestMiddleware()

		_, err := m.doesPresignedSignatureV4Match(req)
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

		got, err := m.isReqAuthenticated(req)
		require.NoError(t, err)
		require.Equal(t, presignTestUser, got)
	})

	t.Run("request without any credentials is still rejected", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodGet, "/bucket-test/object.png", nil)
		req.Host = "s3.example.com:9669"
		m := presignTestMiddleware()

		_, err := m.isReqAuthenticated(req)
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
