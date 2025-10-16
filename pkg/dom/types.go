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

package dom

import "fmt"

type StorageType string

const (
	S3    StorageType = "S3"
	Swift StorageType = "SWIFT"
)

type Storage struct {
	Type StorageType

	Address     string
	Credentials map[string]Credentials
	Provider    string
	IsMain      bool
}

type Credentials struct {
	AccessKey string
}

type Storages map[string]Storage

func (s Storages) CheckSwift(in string) error {
	if got, ok := s[in]; !ok || got.Type != Swift {
		return fmt.Errorf("%w: unknown storage %s", ErrInvalidArg, in)
	}
	return nil
}

func (s Storages) Check(stor, user string) error {
	got, ok := s[stor]
	if !ok {
		return fmt.Errorf("%w: unknown storage %s", ErrInvalidArg, stor)
	}
	if got.Type == S3 {
		if _, ok = got.Credentials[user]; !ok {
			return fmt.Errorf("%w: unknown user %s for storage %s", ErrInvalidArg, user, stor)
		}
	}
	return nil
}

func (s Storages) Main() string {
	for k, v := range s {
		if v.IsMain {
			return k
		}
	}
	return ""
}
