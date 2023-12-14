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

package agent

import (
	"embed"
	"fmt"
	"github.com/clyso/chorus/pkg/config"
	"io/fs"
)

//go:embed config.yaml
var configFile embed.FS

func defaultConfig() fs.File {
	defaultFile, err := configFile.Open("config.yaml")
	if err != nil {
		panic(err)
	}
	return defaultFile
}

type Config struct {
	config.Common `yaml:",inline,omitempty" mapstructure:",squash"`

	Port        int    `yaml:"port"`
	FromStorage string `yaml:"fromStorage"`
	URL         string `yaml:"url"`
}

func (c *Config) Validate() error {
	if err := c.Common.Validate(); err != nil {
		return err
	}
	if c.Port <= 0 {
		return fmt.Errorf("port is not set")
	}
	if c.FromStorage == "" {
		return fmt.Errorf("FromStorage is not set")
	}

	return nil
}

func GetConfig(src ...config.Src) (*Config, error) {
	dc := defaultConfig()
	var conf Config
	cfgSource := []config.Src{config.Reader(dc, "agent_default_cfg")}
	cfgSource = append(cfgSource, src...)
	err := config.Get(&conf, cfgSource...)
	_ = dc.Close()
	if err != nil {
		return nil, err
	}
	return &conf, err
}
