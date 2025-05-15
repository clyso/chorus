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
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	mclient "github.com/minio/minio-go/v7"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/s3"
)

func (m *middleware) doesSignatureV2Match(r *http.Request, domains []string) (string, error) {
	accessKey, err := s3.GetReqAccessKeyV2(r)
	if err != nil {
		return "", err
	}
	credInfo, err := m.getCred(accessKey)
	if err != nil {
		return "", err
	}

	expectedAuth, err := s3.ComputeSignatureV2(r, credInfo.cred.SecretAccessKey, domains)
	if err != nil {
		return "", err
	}

	v2Auth := r.Header.Get(s3.Authorization)
	prefix := fmt.Sprintf("%s %s:", s3.SignV2Algorithm, credInfo.cred.AccessKeyID)
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
