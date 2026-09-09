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
	"encoding/xml"
	"net/http"
	"time"

	mclient "github.com/minio/minio-go/v7"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/s3"
)

const (
	// presignedMinExpires and presignedMaxExpires bound the X-Amz-Expires
	// parameter of a presigned URL, as in AWS S3 (1 second to 7 days).
	presignedMinExpires = time.Second
	presignedMaxExpires = 7 * 24 * time.Hour
	// presignedMaxClockSkew is the clock skew allowed between the client
	// and the proxy when checking that a presigned URL is not used before
	// its signing date.
	presignedMaxClockSkew = 15 * time.Minute
)

// isRequestPresignedSignatureV4 verifies if the request has AWS Signature
// Version '4' authentication parameters in the query string (presigned URL).
// See https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html.
func isRequestPresignedSignatureV4(r *http.Request) bool {
	_, ok := r.URL.Query()[s3.AmzCredential]
	return ok
}

// doesPresignedSignatureV4Match verifies the signature of a presigned
// (query-string authenticated) AWS Signature Version '4' request and returns
// the authenticated user together with the payload hash it verified against.
func (m *middleware) doesPresignedSignatureV4Match(r *http.Request) (string, string, error) {
	query := r.URL.Query()

	psv, err := s3.ParsePreSignV4(query)
	if err != nil {
		return "", unsignedPayload, err
	}

	credInfo, err := m.getCred(psv.Credential.AccessKey)
	if err != nil {
		return "", unsignedPayload, err
	}
	cred := credInfo.cred

	if psv.Expires < presignedMinExpires || psv.Expires > presignedMaxExpires {
		return "", unsignedPayload, mclient.ErrorResponse{
			XMLName:    xml.Name{},
			Code:       "AuthorizationQueryParametersError",
			Message:    "X-Amz-Expires must be from 1 second to 604800 seconds (7 days).",
			BucketName: xctx.GetBucket(r.Context()),
			Key:        xctx.GetObject(r.Context()),
			StatusCode: http.StatusBadRequest,
		}
	}
	now := time.Now().UTC()
	if psv.Date.After(now.Add(presignedMaxClockSkew)) {
		return "", unsignedPayload, mclient.ErrorResponse{
			XMLName:    xml.Name{},
			Code:       "AccessDenied",
			Message:    "Request is not valid yet",
			BucketName: xctx.GetBucket(r.Context()),
			Key:        xctx.GetObject(r.Context()),
			StatusCode: http.StatusForbidden,
		}
	}
	if now.Sub(psv.Date) > psv.Expires {
		return "", unsignedPayload, mclient.ErrorResponse{
			XMLName:    xml.Name{},
			Code:       "AccessDenied",
			Message:    "Request has expired",
			BucketName: xctx.GetBucket(r.Context()),
			Key:        xctx.GetObject(r.Context()),
			StatusCode: http.StatusForbidden,
		}
	}

	extractedSignedHeaders, err := s3.ExtractSignedHeaders(psv.SignedHeaders, r)
	if err != nil {
		return "", unsignedPayload, err
	}

	// The payload of a presigned request is not signed: the recommended value
	// UNSIGNED-PAYLOAD is used unless the client signed a payload checksum,
	// through the X-Amz-Content-Sha256 query parameter or, as MinIO also
	// accepts, through the header of the same name.
	hashedPayload := query.Get(s3.AmzContentSha256)
	if hashedPayload == "" {
		hashedPayload = r.Header.Get(s3.AmzContentSha256)
	}
	if hashedPayload == "" {
		hashedPayload = unsignedPayload
	}

	// The signature covers every query parameter except X-Amz-Signature.
	query.Del(s3.AmzSignature)
	queryStr := query.Encode()

	canonicalRequest := getCanonicalV4Request(extractedSignedHeaders, hashedPayload, queryStr, r.URL.Path, r.Method)
	stringToSign := getV4StringToSign(canonicalRequest, psv.Date, psv.Credential.GetScope())
	signingKey := getV4SigningKey(cred.SecretAccessKey, psv.Credential.Scope.Date, psv.Credential.Scope.Region)
	newSignature := getV4Signature(signingKey, stringToSign)

	if !compareSignatureV4(newSignature, psv.Signature) {
		return "", unsignedPayload, mclient.ErrorResponse{
			XMLName:    xml.Name{},
			Code:       "SignatureDoesNotMatch",
			Message:    "The request signature that the server calculated does not match the signature that you provided. Check your AWS secret access key and signing method. For more information, see REST Authentication and SOAP Authentication.",
			BucketName: xctx.GetBucket(r.Context()),
			Key:        xctx.GetObject(r.Context()),
			StatusCode: http.StatusForbidden,
		}
	}

	return credInfo.user, hashedPayload, nil
}

// removePresignParams strips the AWS Signature Version '4' query-string
// authentication parameters from the request URL. It is called after a
// presigned request has been authenticated, so that the request forwarded
// to the storage backend carries a single authentication mechanism: the
// Authorization header computed by the proxy.
func removePresignParams(r *http.Request) {
	query := r.URL.Query()
	for _, param := range []string{
		s3.AmzAlgorithm,
		s3.AmzCredential,
		s3.AmzDate,
		s3.AmzExpires,
		s3.AmzSignedHeaders,
		s3.AmzSignature,
		s3.AmzSecurityToken,
		s3.AmzContentSha256,
	} {
		query.Del(param)
	}
	r.URL.RawQuery = query.Encode()
}
