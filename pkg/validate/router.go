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

package validate

import (
	"errors"
	"fmt"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
)

func UserUploadObjectID(id entity.UserUploadObjectID) error {
	errs := make([]error, 0)
	if id.User == "" {
		err := fmt.Errorf("%w: user is required to set uploadID", dom.ErrInvalidArg)
		errs = append(errs, err)
	}
	if id.Bucket == "" {
		err := fmt.Errorf("%w: bucket is required to set uploadID", dom.ErrInvalidArg)
		errs = append(errs, err)
	}

	if len(errs) != 0 {
		return errors.Join(errs...)
	}
	return nil
}

func UserUploadObject(val entity.UserUploadObject) error {
	if val.UploadID == "" {
		return fmt.Errorf("%w: uploadID is required", dom.ErrInvalidArg)
	}
	return nil
}
