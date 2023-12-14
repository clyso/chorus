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

package rclone

import (
	"github.com/clyso/chorus/pkg/ratelimit"
	"time"
)

type Config struct {
	MemoryLimit     MemoryLimit               `yaml:"memoryLimit"`
	MemoryCalc      MemoryCalc                `yaml:"memoryCalc"`
	LocalFileLimit  ratelimit.SemaphoreConfig `yaml:"localFileLimit"`
	GlobalFileLimit ratelimit.SemaphoreConfig `yaml:"globalFileLimit"`
}

type MemoryLimit struct {
	Enabled  bool          `yaml:"enabled"`
	Limit    string        `yaml:"limit"`
	RetryMin time.Duration `yaml:"retryMin"`
	RetryMax time.Duration `yaml:"retryMax"`
}

type MemoryCalc struct {
	Const string  `yaml:"const"`
	Mul   float64 `yaml:"mul"`
}
