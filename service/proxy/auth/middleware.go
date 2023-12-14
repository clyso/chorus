package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/xml"
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/util"
	mclient "github.com/minio/minio-go/v7"
	"net/http"
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
	}
}

func Credentials(conf *Config, storages map[string]s3.Storage) []s3.CredentialsV4 {
	var credentials []s3.CredentialsV4
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
