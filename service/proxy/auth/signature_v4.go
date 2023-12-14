package auth

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/s3"
	mclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	"net/http"
	"sort"
	"strings"
	"time"
)

const (
	signV4Algorithm = "AWS4-HMAC-SHA256"
	iso8601Format   = "20060102T150405Z"
	yyyymmdd        = "20060102"
)

func isRequestSignatureV4(r *http.Request) bool {
	return strings.HasPrefix(r.Header.Get(s3.Authorization), signV4Algorithm)
}

func compareSignatureV4(sig1, sig2 string) bool {
	return subtle.ConstantTimeCompare([]byte(sig1), []byte(sig2)) == 1
}

func (m *middleware) doesSignatureV4Match(hashedPayload string, r *http.Request) (string, error) {
	req := *r

	v4Auth := req.Header.Get(s3.Authorization)

	signV4Values, err := s3.ParseSignV4(v4Auth)
	if err != nil {
		return "", err
	}

	extractedSignedHeaders, err := s3.ExtractSignedHeaders(signV4Values.SignedHeaders, r)
	if err != nil {
		return "", err
	}

	credInfo, err := m.getCred(signV4Values.Credential.AccessKey)
	if err != nil {
		return "", err
	}
	cred := credInfo.cred

	var date string
	if date = req.Header.Get(s3.AmzDate); date == "" {
		if date = r.Header.Get(s3.Date); date == "" {
			return "", fmt.Errorf("%w: invalid signature: %q header is missing", dom.ErrAuth, s3.AmzDate)
		}
	}

	t, e := time.Parse(iso8601Format, date)
	if e != nil {
		return "", fmt.Errorf("%w: invalid signature: %q - %q invalid date format", dom.ErrAuth, s3.AmzDate, date)
	}
	queryStr := req.URL.Query().Encode()
	canonicalRequest := getCanonicalV4Request(extractedSignedHeaders, hashedPayload, queryStr, req.URL.Path, req.Method)
	stringToSign := getV4StringToSign(canonicalRequest, t, signV4Values.Credential.GetScope())
	signingKey := getV4SigningKey(cred.SecretAccessKey, signV4Values.Credential.Scope.Date,
		signV4Values.Credential.Scope.Region)
	newSignature := getV4Signature(signingKey, stringToSign)

	if !compareSignatureV4(newSignature, signV4Values.Signature) {
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
	var headers []string
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
	var headers []string
	for k := range signedHeaders {
		headers = append(headers, strings.ToLower(k))
	}
	sort.Strings(headers)
	return strings.Join(headers, ";")
}

func getV4StringToSign(canonicalRequest string, t time.Time, scope string) string {
	stringToSign := signV4Algorithm + "\n" + t.Format(iso8601Format) + "\n"
	stringToSign += scope + "\n"
	canonicalRequestBytes := sha256.Sum256([]byte(canonicalRequest))
	stringToSign += hex.EncodeToString(canonicalRequestBytes[:])
	return stringToSign
}

func getV4SigningKey(secretKey string, t time.Time, region string) []byte {
	date := sumHMAC([]byte("AWS4"+secretKey), []byte(t.Format(yyyymmdd)))
	regionBytes := sumHMAC(date, []byte(region))
	service := sumHMAC(regionBytes, []byte("s3"))
	signingKey := sumHMAC(service, []byte("aws4_request"))
	return signingKey
}

func getV4Signature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))
}
