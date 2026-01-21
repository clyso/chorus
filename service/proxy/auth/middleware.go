/*
 * Copyright © 2023 Clyso GmbH
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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/xml"
	"net/http"

	mclient "github.com/minio/minio-go/v7"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/util"
)

func Middleware(conf *Config, credsSvc objstore.CredsService) *middleware {
	custom := map[string]credMeta{}
	for user, cred := range conf.Custom {
		custom[cred.AccessKeyID] = credMeta{
			cred: cred,
			user: user,
		}
	}
	return &middleware{
		allowV2:     conf.AllowV2Signature,
		custom:      custom,
		storageName: conf.UseStorage,
		credsSvc:    credsSvc,
	}
}

type credMeta struct {
	cred s3.CredentialsV4
	user string
}

type middleware struct {
	allowV2     bool
	custom      map[string]credMeta
	storageName string
	credsSvc    objstore.CredsService
}

func (m *middleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, err := m.isReqAuthenticated(r)
		if err != nil {
			util.WriteError(r.Context(), w, err)
			return
		}
		ctx := log.WithUser(r.Context(), user)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

var authDeniedErr = mclient.ErrorResponse{
	XMLName:    xml.Name{},
	Code:       "InvalidAccessKeyId",
	Message:    "The AWS access key ID that you provided does not exist in our records.",
	StatusCode: http.StatusForbidden,
}

func (m *middleware) getCred(accessKey string) (credMeta, error) {
	if m.storageName != "" {
		// check storage creds
		user, cred, err := m.credsSvc.FindS3Credentials(m.storageName, accessKey)
		if err == nil {
			// found:
			return credMeta{
				cred: cred,
				user: user,
			}, nil
		}
		// fallback to custom creds
	}
	// check custom creds
	res, ok := m.custom[accessKey]
	if !ok {
		return res, authDeniedErr
	}
	return res, nil
}

func (m *middleware) isReqAuthenticated(r *http.Request) (string, error) {
	if isRequestSignatureV4(r) {
		sha256sum := getContentSha256Cksum(r)
		return m.doesSignatureV4Match(sha256sum, r)
	} else if m.allowV2 && isRequestSignatureV2(r) {
		return m.doesSignatureV2Match(r)
	}
	return "", mclient.ErrorResponse{
		XMLName:    xml.Name{},
		Code:       "CredentialsNotSupported",
		Message:    "This request does not support given credentials type.",
		BucketName: xctx.GetBucket(r.Context()),
		Key:        xctx.GetObject(r.Context()),
		StatusCode: http.StatusBadRequest,
	}
}

func getContentSha256Cksum(r *http.Request) string {
	var (
		defaultSha256Cksum string
		v                  []string
		ok                 bool
	)
	defaultSha256Cksum = emptySHA256
	v, ok = r.Header[s3.AmzContentSha256]
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

func sumHMAC(key []byte, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(data)
	return hash.Sum(nil)
}
