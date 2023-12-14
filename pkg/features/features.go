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

package features

import "context"

type Config struct {
	Versioning bool `yaml:"versioning"`
	Tagging    bool `yaml:"tagging"`
	ACL        bool `yaml:"acl"`
	Lifecycle  bool `yaml:"lifecycle"`
	Policy     bool `yaml:"policy"`
}

var val *Config

func Set(c *Config) {
	val = c
}

func Versioning(_ context.Context) bool {
	return val.Versioning
}

func Tagging(_ context.Context) bool {
	return val.Tagging
}

func ACL(_ context.Context) bool {
	return val.ACL
}

func Lifecycle(_ context.Context) bool {
	return val.Lifecycle
}

func Policy(_ context.Context) bool {
	return val.Policy
}
