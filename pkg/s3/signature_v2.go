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
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/minio/minio-go/v7/pkg/s3utils"

	"github.com/clyso/chorus/pkg/dom"
)

// Whitelist resource list that will be used in query string for signature-V2 calculation.
//
// This list should be kept alphabetically sorted, do not hastily edit.
var resourceList = []string{
	"acl",
	"cors",
	"delete",
	"encryption",
	"legal-hold",
	"lifecycle",
	"location",
	"logging",
	"notification",
	"partNumber",
	"policy",
	"requestPayment",
	"response-cache-control",
	"response-content-disposition",
	"response-content-encoding",
	"response-content-language",
	"response-content-type",
	"response-expires",
	"retention",
	"select",
	"select-type",
	"tagging",
	"torrent",
	"uploadId",
	"uploads",
	"versionId",
	"versioning",
	"versions",
	"website",
}

func GetReqAccessKeyV2(r *http.Request) (string, error) {
	v2Auth := r.Header.Get(Authorization)
	if v2Auth == "" {
		return "", fmt.Errorf("%w: auth header is empty", dom.ErrAuth)
	}

	// Verify if the header algorithm is supported or not.
	if !strings.HasPrefix(v2Auth, SignV2Algorithm) {
		return "", fmt.Errorf("%w: unsupported sign alhorithm", dom.ErrAuth)
	}

	if accessKey := r.Form.Get(AmzAccessKeyID); accessKey != "" {
		return accessKey, nil
	}

	// below is V2 Signed Auth header format, splitting on `space` (after the `AWS` string).
	// Authorization = "AWS" + " " + AWSAccessKeyId + ":" + Signature
	authFields := strings.Split(r.Header.Get(Authorization), " ")
	if len(authFields) != 2 {
		return "", fmt.Errorf("%w: missing auth access key", dom.ErrAuth)
	}

	// Then will be splitting on ":", this will seprate `AWSAccessKeyId` and `Signature` string.
	keySignFields := strings.Split(strings.TrimSpace(authFields[1]), ":")
	if len(keySignFields) != 2 {
		return "", fmt.Errorf("%w: missing auth access key", dom.ErrAuth)
	}

	return keySignFields[0], nil
}

func calculateSignatureV2(stringToSign string, secret string) string {
	hm := hmac.New(sha1.New, []byte(secret))
	hm.Write([]byte(stringToSign))
	return base64.StdEncoding.EncodeToString(hm.Sum(nil))
}

// Return the signature v2 of a given request.
func signatureV2(secretAccessKey, method, encodedResource, encodedQuery string, headers http.Header) string {
	stringToSign := getStringToSignV2(method, encodedResource, encodedQuery, headers, "")
	signature := calculateSignatureV2(stringToSign, secretAccessKey)
	return signature
}

// Return canonical headers.
func canonicalizedAmzHeadersV2(headers http.Header) string {
	keys := make([]string, 0)
	keyval := make(map[string]string, len(headers))
	for key := range headers {
		lkey := strings.ToLower(key)
		if !strings.HasPrefix(lkey, "x-amz-") {
			continue
		}
		keys = append(keys, lkey)
		keyval[lkey] = strings.Join(headers[key], ",")
	}
	sort.Strings(keys)
	canonicalHeaders := make([]string, 0, len(keys))
	for _, key := range keys {
		canonicalHeaders = append(canonicalHeaders, key+":"+keyval[key])
	}
	return strings.Join(canonicalHeaders, "\n")
}

// Return canonical resource string.
func canonicalizedResourceV2(encodedResource, encodedQuery string) string {
	queries := strings.Split(encodedQuery, "&")
	keyval := make(map[string]string)
	for _, query := range queries {
		key := query
		val := ""
		index := strings.Index(query, "=")
		if index != -1 {
			key = query[:index]
			val = query[index+1:]
		}
		keyval[key] = val
	}

	canonicalQueries := make([]string, 0)
	for _, key := range resourceList {
		val, ok := keyval[key]
		if !ok {
			continue
		}
		if val == "" {
			canonicalQueries = append(canonicalQueries, key)
			continue
		}
		canonicalQueries = append(canonicalQueries, key+"="+val)
	}

	canonicalQuery := strings.Join(canonicalQueries, "&")
	if canonicalQuery != "" {
		return encodedResource + "?" + canonicalQuery
	}
	return encodedResource
}

func getStringToSignV2(method string, encodedResource, encodedQuery string, headers http.Header, expires string) string {
	canonicalHeaders := canonicalizedAmzHeadersV2(headers)
	if len(canonicalHeaders) > 0 {
		canonicalHeaders += "\n"
	}

	date := expires // Date is set to expires date for presign operations.
	if date == "" {
		date = headers.Get(Date)
	}

	stringToSign := strings.Join([]string{
		method,
		headers.Get(ContentMD5),
		headers.Get(ContentType),
		date,
		canonicalHeaders,
	}, "\n")

	return stringToSign + canonicalizedResourceV2(encodedResource, encodedQuery)
}

func unescapeQueries(encodedQuery string) (unescapedQueries []string, err error) {
	for _, query := range strings.Split(encodedQuery, "&") {
		var unescapedQuery string
		unescapedQuery, err = url.QueryUnescape(query)
		if err != nil {
			return nil, fmt.Errorf("%w: unable to unescape query %w", dom.ErrAuth, err)
		}
		unescapedQueries = append(unescapedQueries, unescapedQuery)
	}
	return unescapedQueries, nil
}

// Returns "/bucketName/objectName" for path-style or virtual-host-style requests.
func getResource(path string, host string, domains []string) (string, error) {
	hostParts := strings.SplitN(host, ".", 2)
	for _, domain := range domains {
		if domain == host {
			return path, nil
		}
		if hostParts[1] == domain {
			bucket := hostParts[0]
			return "/" + bucket + path, nil
		}
	}

	return path, nil
}

// Verify if request has AWS Signature Version '2'.
func IsRequestSignatureV2(r *http.Request) bool {
	return !strings.HasPrefix(r.Header.Get(Authorization), SignV4Algorithm) &&
		strings.HasPrefix(r.Header.Get(Authorization), SignV2Algorithm)
}

func ComputeSignatureV2(r *http.Request, secretAccessKey string, domains []string) (string, error) {
	var encodedResource, encodedQuery string
	if r.RequestURI != "" {
		// We are the server, r.RequestURI will have raw encoded URI as sent by the client.
		tokens := strings.SplitN(r.RequestURI, "?", 2)
		encodedResource = tokens[0]
		if len(tokens) == 2 {
			encodedQuery = tokens[1]
		}
	} else {
		// We are the client and are about to send a new request.
		encodedResource = s3utils.EncodePath(r.URL.Path)
		encodedQuery = r.URL.RawQuery
	}

	unescapedQueries, err := unescapeQueries(encodedQuery)
	if err != nil {
		return "", err
	}
	encodedResource, err = getResource(encodedResource, r.Host, domains)
	if err != nil {
		return "", err
	}

	return signatureV2(secretAccessKey, r.Method, encodedResource, strings.Join(unescapedQueries, "&"), r.Header), nil
}
