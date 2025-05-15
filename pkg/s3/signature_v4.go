/*
 * Copyright © 2023 Clyso GmbH
 * Copyright © 2025 STRATO GmbH
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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7/pkg/s3utils"
)

func IsRequestSignatureV4(r *http.Request) bool {
	return strings.HasPrefix(r.Header.Get(Authorization), SignV4Algorithm)
}

func ComputeSignatureV4(req *http.Request, accessKeyID, secretAccessKey, region string, date time.Time, extractedSignedHeaders http.Header) string {
	hashedPayload := getContentSha256Cksum(req)
	queryStr := req.URL.Query().Encode()
	canonicalRequest := getCanonicalV4Request(extractedSignedHeaders, hashedPayload, queryStr, req.URL.Path, req.Method)
	scope := strings.Join([]string{
		date.Format(TimeYyyymmdd),
		region,
		"s3",
		"aws4_request",
	}, "/")
	stringToSign := getV4StringToSign(canonicalRequest, date, scope)
	signingKey := getV4SigningKey(secretAccessKey, date, region)
	return getV4Signature(signingKey, stringToSign)
}

func getCanonicalV4Request(extractedSignedHeaders http.Header, payload, queryStr, urlPath, method string) string {
	rawQuery := strings.ReplaceAll(queryStr, "+", "%20")
	encodedPath := s3utils.EncodePath(urlPath)
	canonicalRequest := strings.Join([]string{
		method,
		encodedPath,
		rawQuery,
		getCanonicalV4Headers(extractedSignedHeaders),
		getSignedV4Headers(extractedSignedHeaders),
		payload,
	}, "\n")
	return canonicalRequest
}

func getCanonicalV4Headers(signedHeaders http.Header) string {
	headers := make([]string, 0, len(signedHeaders))
	vals := make(http.Header)
	for k, vv := range signedHeaders {
		headers = append(headers, strings.ToLower(k))
		vals[strings.ToLower(k)] = vv
	}
	sort.Strings(headers)

	var buf bytes.Buffer
	for _, k := range headers {
		buf.WriteString(k)
		buf.WriteByte(':')
		for idx, v := range vals[k] {
			if idx > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(signV4TrimAll(v))
		}
		buf.WriteByte('\n')
	}
	return buf.String()
}
func signV4TrimAll(input string) string {
	return strings.Join(strings.Fields(input), " ")
}

func getSignedV4Headers(signedHeaders http.Header) string {
	headers := make([]string, 0, len(signedHeaders))
	for k := range signedHeaders {
		headers = append(headers, strings.ToLower(k))
	}
	sort.Strings(headers)
	return strings.Join(headers, ";")
}

func getV4StringToSign(canonicalRequest string, t time.Time, scope string) string {
	stringToSign := SignV4Algorithm + "\n" + t.Format(TimeIso8601Format) + "\n"
	stringToSign += scope + "\n"
	canonicalRequestBytes := sha256.Sum256([]byte(canonicalRequest))
	stringToSign += hex.EncodeToString(canonicalRequestBytes[:])
	return stringToSign
}

func getV4SigningKey(secretKey string, t time.Time, region string) []byte {
	date := sumHMAC([]byte("AWS4"+secretKey), []byte(t.Format(TimeYyyymmdd)))
	regionBytes := sumHMAC(date, []byte(region))
	service := sumHMAC(regionBytes, []byte("s3"))
	signingKey := sumHMAC(service, []byte("aws4_request"))
	return signingKey
}

func getV4Signature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))
}

func sumHMAC(key []byte, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(data)
	return hash.Sum(nil)
}

func getContentSha256Cksum(r *http.Request) string {
	var (
		defaultSha256Cksum string
		v                  []string
		ok                 bool
	)
	defaultSha256Cksum = emptySHA256
	v, ok = r.Header[AmzContentSha256]
	if ok {
		return v[0]
	}
	return defaultSha256Cksum
}

// Streaming AWS Signature Version '4' constants.
const (
	emptySHA256            = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	unsignedPayload        = "UNSIGNED-PAYLOAD"
	unsignedPayloadTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
)
