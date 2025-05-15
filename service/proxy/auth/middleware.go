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
	"encoding/xml"
	"net/http"

	mclient "github.com/minio/minio-go/v7"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/util"
)

func Middleware(conf *Config, storages map[string]s3.Storage) *middleware {
	credentials := map[string]credMeta{}
	for user, cred := range storages[conf.UseStorage].Credentials {
		credentials[cred.AccessKeyID] = credMeta{
			cred: cred,
			user: user,
		}
	}
	for user, cred := range conf.Custom {
		credentials[cred.AccessKeyID] = credMeta{
			cred: cred,
			user: user,
		}
	}
	return &middleware{
		allowV2:     conf.AllowV2Signature,
		credentials: credentials,
		storage:     storages[conf.UseStorage],
	}
}

func Credentials(conf *Config, storages map[string]s3.Storage) []s3.CredentialsV4 {
	credentialsCount := len(conf.Custom) + len(storages)
	credentials := make([]s3.CredentialsV4, 0, credentialsCount)
	for _, cred := range storages[conf.UseStorage].Credentials {
		credentials = append(credentials, cred)
	}
	for _, cred := range conf.Custom {
		credentials = append(credentials, cred)
	}
	return credentials
}

type credMeta struct {
	cred s3.CredentialsV4
	user string
}

type middleware struct {
	allowV2     bool
	credentials map[string]credMeta
	storage     s3.Storage
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

func (m *middleware) getCred(accessKey string) (credMeta, error) {
	res, ok := m.credentials[accessKey]
	if !ok {
		return res, mclient.ErrorResponse{
			XMLName:    xml.Name{},
			Code:       "InvalidAccessKeyId",
			Message:    "The AWS access key ID that you provided does not exist in our records.",
			StatusCode: http.StatusForbidden,
		}
	}
	return res, nil
}

func (m *middleware) isReqAuthenticated(r *http.Request) (string, error) {
	if s3.IsRequestSignatureV4(r) {
		return m.doesSignatureV4Match(r)
	} else if m.allowV2 && s3.IsRequestSignatureV2(r) {
		domains := make([]string, len(m.storage.Domains))
		for i, dom := range m.storage.Domains {
			domains[i] = dom.Value()
		}
		return m.doesSignatureV2Match(r, domains)
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
