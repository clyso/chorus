package auth

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/s3"
	mclient "github.com/minio/minio-go/v7"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// AWS Signature Version '4' constants.
const (
	signV2Algorithm = "AWS"
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

// Verify if request has AWS Signature Version '2'.
func isRequestSignatureV2(r *http.Request) bool {
	return !strings.HasPrefix(r.Header.Get(s3.Authorization), signV4Algorithm) &&
		strings.HasPrefix(r.Header.Get(s3.Authorization), signV2Algorithm)
}

func (m *middleware) doesSignatureV2Match(r *http.Request) (string, error) {
	accessKey, err := getReqAccessKeyV2(r)
	if err != nil {
		return "", err
	}
	credInfo, err := m.getCred(accessKey)
	if err != nil {
		return "", err
	}
	cred := credInfo.cred

	// r.RequestURI will have raw encoded URI as sent by the client.
	tokens := strings.SplitN(r.RequestURI, "?", 2)
	encodedResource := tokens[0]
	encodedQuery := ""
	if len(tokens) == 2 {
		encodedQuery = tokens[1]
	}

	unescapedQueries, err := unescapeQueries(encodedQuery)
	if err != nil {
		return "", err
	}
	encodedResource, err = getResource(encodedResource, r.Host, nil)
	if err != nil {
		return "", err
	}
	expectedAuth := signatureV2(cred, r.Method, encodedResource, strings.Join(unescapedQueries, "&"), r.Header)

	v2Auth := r.Header.Get(s3.Authorization)
	prefix := fmt.Sprintf("%s %s:", signV2Algorithm, cred.AccessKeyID)
	if !strings.HasPrefix(v2Auth, prefix) {
		return "", fmt.Errorf("%w: unsupported sign alhorithm", dom.ErrAuth)
	}
	v2Auth = v2Auth[len(prefix):]
	if !compareSignatureV2(v2Auth, expectedAuth) {
		return "", mclient.ErrorResponse{
			XMLName:    xml.Name{},
			Code:       "SignatureDoesNotMatch",
			Message:    "The request signature that the server calculated does not match the signature that you provided. Check your AWS secret access key and signing method. For more information, see REST Authentication and SOAP Authentication.",
			BucketName: xctx.GetBucket(r.Context()),
			Key:        xctx.GetObject(r.Context()),
			StatusCode: http.StatusForbidden,
		}
	}
	return credInfo.user, nil
}

func getReqAccessKeyV2(r *http.Request) (string, error) {
	v2Auth := r.Header.Get(s3.Authorization)
	if v2Auth == "" {
		return "", fmt.Errorf("%w: auth header is empty", dom.ErrAuth)
	}

	// Verify if the header algorithm is supported or not.
	if !strings.HasPrefix(v2Auth, signV2Algorithm) {
		return "", fmt.Errorf("%w: unsupported sign alhorithm", dom.ErrAuth)
	}

	if accessKey := r.Form.Get(s3.AmzAccessKeyID); accessKey != "" {
		return accessKey, nil
	}

	// below is V2 Signed Auth header format, splitting on `space` (after the `AWS` string).
	// Authorization = "AWS" + " " + AWSAccessKeyId + ":" + Signature
	authFields := strings.Split(r.Header.Get(s3.Authorization), " ")
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
func signatureV2(cred s3.CredentialsV4, method string, encodedResource string, encodedQuery string, headers http.Header) string {
	stringToSign := getStringToSignV2(method, encodedResource, encodedQuery, headers, "")
	signature := calculateSignatureV2(stringToSign, cred.SecretAccessKey)
	return signature
}

func compareSignatureV2(sig1, sig2 string) bool {
	signature1, err := base64.StdEncoding.DecodeString(sig1)
	if err != nil {
		return false
	}
	signature2, err := base64.StdEncoding.DecodeString(sig2)
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare(signature1, signature2) == 1
}

// Return canonical headers.
func canonicalizedAmzHeadersV2(headers http.Header) string {
	var keys []string
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
	var canonicalHeaders []string
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

	var canonicalQueries []string
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
		date = headers.Get(s3.Date)
	}

	stringToSign := strings.Join([]string{
		method,
		headers.Get(s3.ContentMD5),
		headers.Get(s3.ContentType),
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
			return nil, fmt.Errorf("%w: unable to unescape query %v", dom.ErrAuth, err)
		}
		unescapedQueries = append(unescapedQueries, unescapedQuery)
	}
	return unescapedQueries, nil
}

// Returns "/bucketName/objectName" for path-style or virtual-host-style requests.
func getResource(path string, host string, domains []string) (string, error) {
	if len(domains) == 0 {
		return path, nil
	}

	// If virtual-host-style is enabled construct the "resource" properly.
	// todo: support virtual host
	return path, nil
}
