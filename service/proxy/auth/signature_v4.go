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

package auth

import (
	"crypto/subtle"
	"encoding/xml"
	"fmt"
	"net/http"
	"time"

	mclient "github.com/minio/minio-go/v7"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/s3"
)

func compareSignatureV4(sig1, sig2 string) bool {
	return subtle.ConstantTimeCompare([]byte(sig1), []byte(sig2)) == 1
}

func (m *middleware) doesSignatureV4Match(r *http.Request) (string, error) {
	v4Auth := r.Header.Get(s3.Authorization)

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

	var date string
	if date = r.Header.Get(s3.AmzDate); date == "" {
		if date = r.Header.Get(s3.Date); date == "" {
			return "", fmt.Errorf("%w: invalid signature: %q header is missing", dom.ErrAuth, s3.AmzDate)
		}
	}

	t, e := time.Parse(s3.TimeIso8601Format, date)
	if e != nil {
		return "", fmt.Errorf("%w: invalid signature: %q - %q invalid date format", dom.ErrAuth, s3.AmzDate, date)
	}

	newSignature, _ := s3.ComputeSignatureV4(r, credInfo.cred.AccessKeyID, credInfo.cred.SecretAccessKey,
		signV4Values.Credential.Scope.Region, t, extractedSignedHeaders)
	if err != nil {
		return "", err
	}

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
