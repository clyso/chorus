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
	"net/url"
	"strings"
	"time"

	"github.com/clyso/chorus/pkg/dom"
)

const (
	defaultHealthCheckInterval = time.Second * 5
	defaultHttpTimeout         = time.Minute * 10
)

type Storage struct {
	Credentials   map[string]CredentialsV4 `yaml:"credentials"`
	Address       string                   `yaml:"address"`
	Provider      string                   `yaml:"provider"`
	DefaultRegion string                   `yaml:"defaultRegion"`

	HealthCheckInterval time.Duration `yaml:"healthCheckInterval"`
	HttpTimeout         time.Duration `yaml:"httpTimeout"`
	IsSecure            bool          `yaml:"isSecure"`
}

func (s *Storage) HasUser(user string) bool {
	_, ok := s.Credentials[user]
	return ok
}

func (s *Storage) UserList() []string {
	users := make([]string, 0, len(s.Credentials))
	for user := range s.Credentials {
		users = append(users, user)
	}
	return users
}

type CredentialsV4 struct {
	AccessKeyID     string `yaml:"accessKeyID"`
	SecretAccessKey string `yaml:"secretAccessKey"`
}

func (s *Storage) Validate() error {
	if len(s.Credentials) == 0 {
		return fmt.Errorf("%w: no credentials in S3 storage config", dom.ErrInvalidStorageConfig)
	}
	for user, cred := range s.Credentials {
		if cred.SecretAccessKey == "" {
			return fmt.Errorf("%w: secretAccessKey for S3 user %q is not set", dom.ErrInvalidStorageConfig, user)
		}
		if cred.AccessKeyID == "" {
			return fmt.Errorf("%w: accessKeyID for S3 user %q is not set", dom.ErrInvalidStorageConfig, user)
		}
	}

	if s.HealthCheckInterval == 0 {
		s.HealthCheckInterval = defaultHealthCheckInterval
	}
	if s.HttpTimeout == 0 {
		s.HttpTimeout = defaultHttpTimeout
	}
	if s.Provider == "" {
		return fmt.Errorf("app config: storage provider required")
	}
	if s.Address == "" {
		return fmt.Errorf("app config: storage address required")
	}
	if !strings.HasPrefix(s.Address, "http") {
		if s.IsSecure {
			s.Address = "https://" + s.Address
		} else {
			s.Address = "http://" + s.Address
		}
	}
	if s.IsSecure && !strings.HasPrefix(s.Address, "https://") {
		return fmt.Errorf("%w: invalid storage address schema for secure connection", dom.ErrInvalidStorageConfig)
	}
	if !s.IsSecure && !strings.HasPrefix(s.Address, "http://") {
		return fmt.Errorf("%w: invalid storage address schema for insecure connection", dom.ErrInvalidStorageConfig)
	}
	if _, err := url.ParseRequestURI(s.Address); err != nil {
		return fmt.Errorf("%w: invalid storage address", err)
	}

	return nil
}
