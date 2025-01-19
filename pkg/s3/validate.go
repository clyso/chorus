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

package s3

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/clyso/chorus/pkg/dom"
)

var (
	validBucketName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9\.\-\_\:]{1,61}[A-Za-z0-9]$`)
	ipAddress       = regexp.MustCompile(`^(\d+\.){3}\d+$`)
)

func ValidateBucketName(bucketName string) error {
	if strings.TrimSpace(bucketName) == "" {
		return fmt.Errorf("%w: bucket name cannot be empty", dom.ErrInvalidArg)
	}
	if len(bucketName) < 3 {
		return fmt.Errorf("%w: bucket name cannot be shorter than 3 characters", dom.ErrInvalidArg)
	}
	if len(bucketName) > 63 {
		return fmt.Errorf("%w: bucket name cannot be longer than 63 characters", dom.ErrInvalidArg)
	}
	if ipAddress.MatchString(bucketName) {
		return fmt.Errorf("%w: bucket name cannot be an ip address", dom.ErrInvalidArg)
	}
	if strings.Contains(bucketName, "..") || strings.Contains(bucketName, ".-") || strings.Contains(bucketName, "-.") {
		return fmt.Errorf("%w: bucket name contains invalid characters", dom.ErrInvalidArg)
	}

	if !validBucketName.MatchString(bucketName) {
		return fmt.Errorf("%w: bucket name contains invalid characters", dom.ErrInvalidArg)
	}

	return nil
}
