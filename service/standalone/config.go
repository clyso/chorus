/*
 * Copyright © 2024 Clyso GmbH
 * Copyright © 2025 STRATO GmbH
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

package standalone

import (
	"embed"
	"fmt"
	"io/fs"

	"github.com/clyso/chorus/pkg/config"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/service/proxy/auth"
	"github.com/clyso/chorus/service/proxy/cors"
	"github.com/clyso/chorus/service/worker"
)

//go:embed config.yaml
var configFile embed.FS

//go:embed test-conf.yaml
var testConfigFile embed.FS

func defaultConfig() fs.File {
	defaultFile, err := configFile.Open("config.yaml")
	if err != nil {
		panic(err)
	}
	return defaultFile
}

func testConfig() fs.File {
	testFile, err := testConfigFile.Open("test-conf.yaml")
	if err != nil {
		panic(err)
	}
	return testFile
}

type Config struct {
	worker.Config `yaml:",inline,omitempty" mapstructure:",squash"`

	UIPort int `yaml:"uiPort"`

	Proxy struct {
		Enabled bool         `yaml:"enabled"`
		Auth    *auth.Config `yaml:"auth,omitempty"`
		Port    int          `yaml:"port"`
		Address s3.ConfAddr  `yaml:"address"`
		Cors    *cors.Config `yaml:"cors"`
	} `yaml:"proxy"`
}

func (c *Config) Validate() error {
	if err := c.Config.Validate(); err != nil {
		return err
	}
	if c.Storage == nil {
		return fmt.Errorf("chorus config: empty storages config")
	}
	if err := c.Storage.Init(); err != nil {
		return err
	}
	if c.UIPort <= 0 {
		return fmt.Errorf("chorus config: uiPort must be positive: %d", c.Concurrency)
	}

	if c.Proxy.Enabled {
		if c.Proxy.Auth == nil {
			return fmt.Errorf("chorus config: empty Auth config")
		}
		if c.Proxy.Auth.UseStorage != "" {
			if _, ok := c.Storage.Storages[c.Proxy.Auth.UseStorage]; !ok {
				return fmt.Errorf("chorus config: auth UseStorage points to unknown storage")
			}
		}
		if len(c.Proxy.Auth.Custom) != 0 {
			var storCreds map[string]s3.CredentialsV4
			for _, storage := range c.Storage.Storages {
				storCreds = storage.Credentials
				break
			}
			for user := range c.Proxy.Auth.Custom {
				if _, ok := storCreds[user]; !ok {
					return fmt.Errorf("proxy config: auth custom credentials unknown user %q", user)
				}
			}
		}
		if c.Proxy.Auth.UseStorage == "" && len(c.Proxy.Auth.Custom) == 0 {
			return fmt.Errorf("chorus config: auth credentials enabled but not set")
		}

		if c.Proxy.Port <= 0 {
			return fmt.Errorf("chorus config: Port must be positive: %d", c.Proxy.Port)
		}
	}
	return nil
}

func GetConfig(src ...config.Src) (*Config, error) {
	dc := defaultConfig()
	var conf Config
	cfgSource := []config.Src{config.Reader(dc, "chorus_default_cfg")}
	if len(src) == 0 {
		src = []config.Src{config.Reader(testConfig(), "chorus_test_cfg")}
	}

	cfgSource = append(cfgSource, src...)
	err := config.Get(&conf, cfgSource...)
	_ = dc.Close()
	if err != nil {
		return nil, err
	}
	return &conf, err
}
