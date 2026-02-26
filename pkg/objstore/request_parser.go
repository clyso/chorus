// Copyright 2025 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objstore

import (
	"net/http"
	"strings"

	"github.com/clyso/chorus/pkg/dom"
)

// StorageTypeFromRequest detects storage type from http request
func StorageTypeFromRequest(r *http.Request) dom.StorageType {
	for header := range r.Header {
		if strings.HasPrefix(strings.ToLower(header), "x-amz-") {
			return dom.S3
		}
		if strings.EqualFold(header, "Authorization") {
			return dom.S3
		}
		if strings.EqualFold(header, "X-Auth-Token") {
			return dom.Swift
		}
		if strings.EqualFold(header, "X-Service-Token") {
			return dom.Swift
		}
		if strings.HasPrefix(strings.ToLower(header), "x-container-") {
			return dom.Swift
		}
		if strings.HasPrefix(strings.ToLower(header), "x-object-") {
			return dom.Swift
		}
		if strings.HasPrefix(strings.ToLower(header), "x-account-") {
			return dom.Swift
		}
	}
	var (
		path  = strings.Trim(r.URL.Path, "/")
		query = r.URL.Query()
	)

	if query.Has("temp_url_sig") {
		return dom.Swift
	}
	if r.Method == "GET" && query.Has("format") {
		return dom.Swift
	}

	if strings.HasPrefix(path, "v1/") {
		return dom.Swift
	}
	if strings.Contains(path, "/v1/") {
		return dom.Swift
	}
	if r.Method == "GET" && path == "info" {
		return dom.Swift
	}
	return dom.S3
}
