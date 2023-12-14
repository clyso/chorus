package s3

import (
	"fmt"
	"github.com/clyso/chorus/pkg/dom"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

// AWS Signature Version '4' constants.
const (
	signV4Algorithm = "AWS4-HMAC-SHA256"
	yyyymmdd        = "20060102"
)

type SignValues struct {
	Credential    CredentialHeader
	SignedHeaders []string
	Signature     string
}

func ParseSignV4(v4Auth string) (sv SignValues, err error) {
	// credElement is fetched first to skip replacing the space in access key.
	credElement := strings.TrimPrefix(strings.Split(strings.TrimSpace(v4Auth), ",")[0], signV4Algorithm)
	// Replace all spaced strings, some clients can send spaced
	// parameters and some won't. So we pro-actively remove any spaces
	// to make parsing easier.
	v4Auth = strings.ReplaceAll(v4Auth, " ", "")
	if v4Auth == "" {
		return sv, fmt.Errorf("%w: parse signature v4 is empty", dom.ErrAuth)
	}

	// Verify if the header algorithm is supported or not.
	if !strings.HasPrefix(v4Auth, signV4Algorithm) {
		return sv, fmt.Errorf("%w: parse signature: signature type is not supported", dom.ErrAuth)
	}

	// Strip off the Algorithm prefix.
	v4Auth = strings.TrimPrefix(v4Auth, signV4Algorithm)
	authFields := strings.Split(strings.TrimSpace(v4Auth), ",")
	if len(authFields) != 3 {
		return sv, fmt.Errorf("%w: parse signature: fields missing %+v", dom.ErrAuth, authFields)
	}

	// Initialize signature version '4' structured header.
	signV4Values := SignValues{}

	// Save credential values.
	signV4Values.Credential, err = parseCredentialHeader(strings.TrimSpace(credElement))
	if err != nil {
		return sv, err
	}

	// Save signed headers.
	signV4Values.SignedHeaders, err = parseSignedHeader(authFields[1])
	if err != nil {
		return sv, err
	}

	// Save signature.
	signV4Values.Signature, err = parseSignature(authFields[2])
	if err != nil {
		return sv, err
	}

	// Return the structure here.
	return signV4Values, nil
}

type CredentialHeader struct {
	AccessKey string
	Scope     struct {
		Date    time.Time
		Region  string
		Service string
		Request string
	}
}

func (c CredentialHeader) GetScope() string {
	return strings.Join([]string{
		c.Scope.Date.Format(yyyymmdd),
		c.Scope.Region,
		c.Scope.Service,
		c.Scope.Request,
	}, "/")
}

// parse credentialHeader string into its structured form.
func parseCredentialHeader(credElement string) (ch CredentialHeader, err error) {
	creds := strings.SplitN(strings.TrimSpace(credElement), "=", 2)
	if len(creds) != 2 {
		return ch, fmt.Errorf("%w: invalid credential header %+v", dom.ErrAuth, creds)
	}
	if creds[0] != "Credential" {
		return ch, fmt.Errorf("%w: invalid credential header Credential %+v", dom.ErrAuth, creds[0])
	}
	credElements := strings.Split(strings.TrimSpace(creds[1]), "/")
	if len(credElements) < 5 {
		return ch, fmt.Errorf("%w: invalid credential header credElements %+v", dom.ErrAuth, credElements)
	}
	accessKey := strings.Join(credElements[:len(credElements)-4], "/") // The access key may contain one or more `/`

	// Save access key id.
	cred := CredentialHeader{
		AccessKey: accessKey,
	}
	credElements = credElements[len(credElements)-4:]
	var e error
	cred.Scope.Date, e = time.Parse(yyyymmdd, credElements[0])
	if e != nil {
		return ch, fmt.Errorf("%w: invalid credential header date format %+v", dom.ErrAuth, credElements[0])
	}

	cred.Scope.Region = credElements[1]
	// Verify if region is valid.
	if credElements[2] != "s3" {
		return ch, fmt.Errorf("%w: invalid credential header service type %+v", dom.ErrAuth, credElements[2])
	}
	cred.Scope.Service = credElements[2]
	if credElements[3] != "aws4_request" {
		return ch, fmt.Errorf("%w: invalid credential header request %+v", dom.ErrAuth, credElements[3])
	}
	cred.Scope.Request = credElements[3]
	return cred, nil
}

// Parse signature from signature tag.
func parseSignature(signElement string) (string, error) {
	signFields := strings.Split(strings.TrimSpace(signElement), "=")
	if len(signFields) != 2 {
		return "", fmt.Errorf("%w: invalid signature fileds %+v", dom.ErrAuth, signFields)
	}
	if signFields[0] != "Signature" {
		return "", fmt.Errorf("%w: invalid signature Signature %+v", dom.ErrAuth, signFields[0])
	}
	if signFields[1] == "" {
		return "", fmt.Errorf("%w: invalid empty signature filed 1", dom.ErrAuth)
	}
	signature := signFields[1]
	return signature, nil
}

// Parse slice of signed headers from signed headers tag.
func parseSignedHeader(signedHdrElement string) ([]string, error) {
	signedHdrFields := strings.Split(strings.TrimSpace(signedHdrElement), "=")
	if len(signedHdrFields) != 2 {
		return nil, fmt.Errorf("%w: invalid signed header fileds %+v", dom.ErrAuth, signedHdrFields)
	}
	if signedHdrFields[0] != "SignedHeaders" {
		return nil, fmt.Errorf("%w: invalid signed header SignedHeaders %+v", dom.ErrAuth, signedHdrFields[0])
	}
	if signedHdrFields[1] == "" {
		return nil, fmt.Errorf("%w: invalid empty signed header filed 1", dom.ErrAuth)
	}
	signedHeaders := strings.Split(signedHdrFields[1], ";")
	return signedHeaders, nil
}

// ExtractSignedHeaders extract signed headers from Authorization header
func ExtractSignedHeaders(signedHeaders []string, r *http.Request) (http.Header, error) {
	reqHeaders := r.Header
	reqQueries := r.Form

	if !slices.Contains(signedHeaders, "host") {
		return nil, fmt.Errorf("%w: signed headers does not contain host", dom.ErrAuth)
	}
	extractedSignedHeaders := make(http.Header)
	for _, header := range signedHeaders {
		val, ok := reqHeaders[http.CanonicalHeaderKey(header)]
		if !ok {
			val, ok = reqQueries[header]
		}
		if ok {
			extractedSignedHeaders[http.CanonicalHeaderKey(header)] = val
			continue
		}
		switch header {
		case "expect":
			extractedSignedHeaders.Set(header, "100-continue")
		case "host":
			extractedSignedHeaders.Set(header, r.Host)
		case "transfer-encoding":
			extractedSignedHeaders[http.CanonicalHeaderKey(header)] = r.TransferEncoding
		case "content-length":
			extractedSignedHeaders.Set(header, strconv.FormatInt(r.ContentLength, 10))
		default:
			return nil, fmt.Errorf("%w: unexpected signed header %q", dom.ErrAuth, header)
		}
	}
	return extractedSignedHeaders, nil
}
