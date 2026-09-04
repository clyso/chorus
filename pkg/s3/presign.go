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

package s3

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/clyso/chorus/pkg/dom"
)

const iso8601Format = "20060102T150405Z"

// PreSignValues holds the parsed authentication values of a presigned
// (query-string authenticated) AWS Signature Version '4' request.
type PreSignValues struct {
	Date time.Time
	SignValues
	Expires time.Duration
}

// ParsePreSignV4 parses the query-string authentication parameters of a
// presigned AWS Signature Version '4' request into PreSignValues.
// See https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html.
func ParsePreSignV4(query url.Values) (psv PreSignValues, err error) {
	if query.Get(AmzAlgorithm) != signV4Algorithm {
		return psv, fmt.Errorf("%w: parse presign v4: algorithm %q is not supported", dom.ErrAuth, query.Get(AmzAlgorithm))
	}

	// Save credential.
	psv.Credential, err = parseCredentialHeader("Credential=" + query.Get(AmzCredential))
	if err != nil {
		return psv, err
	}

	// Save date.
	psv.Date, err = time.Parse(iso8601Format, query.Get(AmzDate))
	if err != nil {
		return psv, fmt.Errorf("%w: parse presign v4: %q - %q invalid date format", dom.ErrAuth, AmzDate, query.Get(AmzDate))
	}

	// Save expires.
	expiresStr := query.Get(AmzExpires)
	if expiresStr == "" {
		return psv, fmt.Errorf("%w: parse presign v4: %q param is missing", dom.ErrAuth, AmzExpires)
	}
	expires, e := strconv.ParseInt(expiresStr, 10, 64)
	if e != nil {
		return psv, fmt.Errorf("%w: parse presign v4: %q - %q is invalid", dom.ErrAuth, AmzExpires, expiresStr)
	}
	psv.Expires = time.Duration(expires) * time.Second

	// Save signed headers.
	signedHeaders := query.Get(AmzSignedHeaders)
	if signedHeaders == "" {
		return psv, fmt.Errorf("%w: parse presign v4: %q param is missing", dom.ErrAuth, AmzSignedHeaders)
	}
	psv.SignedHeaders = strings.Split(signedHeaders, ";")

	// Save signature.
	psv.Signature = query.Get(AmzSignature)
	if psv.Signature == "" {
		return psv, fmt.Errorf("%w: parse presign v4: %q param is missing", dom.ErrAuth, AmzSignature)
	}

	return psv, nil
}
