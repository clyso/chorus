/*
 * Copyright © 2024 Clyso GmbH
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

package handler

import (
	"errors"
	"net/http"

	mclient "github.com/minio/minio-go/v7"
)

// retryable4xx lists the 4xx codes that are about load or timing, not about the request.
var retryable4xx = map[string]struct{}{
	"SlowDown":             {},
	"RequestTimeout":       {},
	"RequestTimeTooSkewed": {},
	"TooManyRequests":      {},
	"Throttling":           {},
	"ThrottlingException":  {},
	"RequestLimitExceeded": {},
	"OperationAborted":     {},
	"ExpiredToken":         {},
}

// isPermanentS3Error tells whether the storage rejected the request itself, so that
// a retry gets the same answer. Not permanent: 5xx, throttling and timing, and what
// an operator can fix while the task waits (401, 403: credentials and permissions;
// 404: a missing bucket).
func isPermanentS3Error(err error) bool {
	var s3Err mclient.ErrorResponse
	if !errors.As(err, &s3Err) {
		return false
	}
	if s3Err.StatusCode < 400 || s3Err.StatusCode >= 500 {
		return false
	}
	switch s3Err.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound,
		http.StatusRequestTimeout, http.StatusConflict, http.StatusLocked, http.StatusTooManyRequests:
		return false
	}
	_, retryable := retryable4xx[s3Err.Code]
	return !retryable
}
