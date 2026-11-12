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

package s3

import (
	"bytes"
	"crypto/sha256"
	"hash"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/signer"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
)

type sha256Hasher struct{ hash.Hash }

func (sha256Hasher) Close() {}

// signedStream returns an aws-chunked body signed by minio-go and the signer
// that verifies it.
func signedStream(t *testing.T, now time.Time, data []byte, secret string, trailer http.Header) ([]byte, *ChunkSigner) {
	req := httptest.NewRequest(http.MethodPut, "http://localhost/bucket/obj", bytes.NewReader(data))
	req.Trailer = trailer
	req = signer.StreamingSignV4(req, "access", secret, "", "us-east-1", int64(len(data)), now, sha256Hasher{sha256.New()})
	auth, err := ParseSignV4(req.Header.Get(Authorization))
	require.NoError(t, err)
	body, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	return body, &ChunkSigner{
		Key:   SigningKeyV4(secret, now, "us-east-1"),
		Date:  req.Header.Get(AmzDate),
		Scope: auth.Credential.GetScope(),
		Prev:  auth.Signature,
	}
}

func TestResignChunks(t *testing.T) {
	for name, tc := range map[string]struct {
		size    int
		trailer http.Header
	}{
		"empty":           {size: 0},
		"one_chunk":       {size: 1000},
		"many_chunks":     {size: 200 << 10},
		"trailer":         {size: 200 << 10, trailer: http.Header{"X-Amz-Checksum-Crc32": {"AAAAAA=="}}},
		"trailer_no_data": {size: 0, trailer: http.Header{"X-Amz-Checksum-Crc32": {"AAAAAA=="}}},
	} {
		t.Run(name, func(t *testing.T) {
			r := require.New(t)
			data := bytes.Repeat([]byte("0123456789"), tc.size/10+1)[:tc.size]
			withTrailer := tc.trailer != nil
			now := time.Now().UTC()
			clientBody, clientSigner := signedStream(t, now, data, "client-secret", tc.trailer)
			// what minio-go sends with the target credentials and the same seed
			targetBody, targetSigner := signedStream(t, now, data, "target-secret", tc.trailer)

			resigned, err := io.ReadAll(ResignChunks(io.NopCloser(bytes.NewReader(clientBody)), withTrailer, clientSigner, targetSigner))
			r.NoError(err)
			r.Equal(string(targetBody), string(resigned))
		})
	}
}

func TestResignChunks_rejectsTampering(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 100<<10)
	for name, tamper := range map[string]func(body []byte) []byte{
		"data": func(body []byte) []byte {
			i := bytes.Index(body, []byte("xxx"))
			body[i] = 'y'
			return body
		},
		"truncated": func(body []byte) []byte { return body[:len(body)/2] },
	} {
		t.Run(name, func(t *testing.T) {
			body, verify := signedStream(t, time.Now().UTC(), data, "secret", nil)
			out, err := io.ReadAll(ResignChunks(io.NopCloser(bytes.NewReader(tamper(body))), false, verify, nil))
			require.Error(t, err)
			require.Less(t, len(out), len(body))
		})
	}
	t.Run("wrong_key", func(t *testing.T) {
		body, verify := signedStream(t, time.Now().UTC(), data, "secret", nil)
		verify.Key = SigningKeyV4("other", time.Now().UTC(), "us-east-1")
		_, err := io.ReadAll(ResignChunks(io.NopCloser(bytes.NewReader(body)), false, verify, nil))
		require.ErrorIs(t, err, dom.ErrAuth)
	})
	t.Run("trailer", func(t *testing.T) {
		body, verify := signedStream(t, time.Now().UTC(), data, "secret", http.Header{"X-Amz-Checksum-Crc32": {"AAAAAA=="}})
		body = bytes.Replace(body, []byte("AAAAAA=="), []byte("BBBBBB=="), 1)
		_, err := io.ReadAll(ResignChunks(io.NopCloser(bytes.NewReader(body)), true, verify, nil))
		require.ErrorIs(t, err, dom.ErrAuth)
	})
}
