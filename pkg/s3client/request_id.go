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

package s3client

import (
	"fmt"
	"net/http"
)

type RequestID struct {
	ID       string
	Extended string
}

func (r RequestID) String() string {
	return r.ID + "_" + r.Extended
}

func GetRequestID(resp *http.Response) (RequestID, error) {
	res := RequestID{}
	res.ID = resp.Header.Get("x-amz-request-id")
	res.Extended = resp.Header.Get("x-amz-id-2")
	if res.ID == "" && res.Extended == "" {
		return res, fmt.Errorf("request id is not provided in s3 response")
	}
	return res, nil
}

func MustRequestID(resp *http.Response) RequestID {
	res := RequestID{}
	res.ID = resp.Header.Get("x-amz-request-id")
	res.Extended = resp.Header.Get("x-amz-id-2")
	return res
}
