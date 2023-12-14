package auth

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
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

	//clientETag, err := fromContentMD5(r.Header)
	//if err != nil {
	//	return err
	//}
	//
	//// Extract either 'X-Amz-Content-Sha256' header or 'X-Amz-Content-Sha256' query parameter (if V4 presigned)
	//// Do not verify 'X-Amz-Content-Sha256' if skipSHA256.
	//var contentSHA256 []byte
	//skipSHA256 := skipContentSha256Cksum(r)
	//if _, ok := r.Header[s3.AmzContentSha256]; !skipSHA256 && ok {
	//	contentSHA256, err = hex.DecodeString(r.Header.Get(s3.AmzContentSha256))
	//	if err != nil || len(contentSHA256) == 0 {
	//		return fmt.Errorf("%w: content sha256 missmatch, %v, %s", dom.ErrAuth, err, r.Header.Get(s3.AmzContentSha256))
	//	}
	//}
	//
	//r.Body, err = newHashCheckReader(r.Body, clientETag, hex.EncodeToString(contentSHA256))
	//return  err
}

func fromContentMD5(h http.Header) (string, error) {
	v, ok := h["Content-Md5"]
	if !ok {
		return "", nil
	}
	if v[0] == "" {
		return "", nil
	}
	b, err := base64.StdEncoding.Strict().DecodeString(v[0])
	if err != nil {
		return "", err
	}
	if len(b) > 16 && !(len(b) >= 32) && bytes.ContainsRune(b, '-') {
		return hex.EncodeToString(b[:16]) + string(b[16:]), nil
	}
	return hex.EncodeToString(b), nil
}

func skipContentSha256Cksum(r *http.Request) bool {
	var (
		v  []string
		ok bool
	)

	v, ok = r.Header[s3.AmzContentSha256]

	// Skip if no header was set.
	if !ok {
		return true
	}

	// If x-amz-content-sha256 is set and the value is not
	// 'UNSIGNED-PAYLOAD' we should validate the content sha256.
	switch v[0] {
	case unsignedPayload, unsignedPayloadTrailer:
		return true
	case emptySHA256:
		return r.ContentLength > 0
	}
	return false
}

// Returns SHA256 for calculating canonical-request.
func getContentSha256Cksum(r *http.Request) string {
	var (
		defaultSha256Cksum string
		v                  []string
		ok                 bool
	)

	// X-Amz-Content-Sha256, if not set in signed requests, checksum
	// will default to sha256([]byte("")).
	defaultSha256Cksum = emptySHA256
	v, ok = r.Header[s3.AmzContentSha256]

	// We found 'X-Amz-Content-Sha256' return the captured value.
	if ok {
		return v[0]
	}

	// We couldn't find 'X-Amz-Content-Sha256'.
	return defaultSha256Cksum
}

// Streaming AWS Signature Version '4' constants.
const (
	emptySHA256     = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	unsignedPayload = "UNSIGNED-PAYLOAD"
	// http Header "x-amz-content-sha256" == "STREAMING-UNSIGNED-PAYLOAD-TRAILER" indicates that the
	// client did not calculate sha256 of the payload and there is a trailer.
	unsignedPayloadTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
)

func sumHMAC(key []byte, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(data)
	return hash.Sum(nil)
}
