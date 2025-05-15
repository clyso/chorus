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

package proxy

import (
	"embed"
	"fmt"
	"io/fs"

	"github.com/clyso/chorus/pkg/config"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/service/proxy/auth"
	"github.com/clyso/chorus/service/proxy/cors"
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

	Storage *s3.StorageConfig `yaml:"storage,omitempty"`
	Auth    *auth.Config      `yaml:"auth,omitempty"`
	Port    int               `yaml:"port"`
	Address s3.ConfAddr       `yaml:"address"`
	Cors    *cors.Config      `yaml:"cors"`
}

func (c *Config) Validate() error {
	if err := c.Common.Validate(); err != nil {
		return err
	}
	if c.Storage == nil {
		return fmt.Errorf("app config: empty storages config")
	}
	if err := c.Storage.Init(); err != nil {
		return err
	}
	if c.Auth == nil {
		return fmt.Errorf("proxy config: empty Auth config")
	}
	if c.Auth.UseStorage != "" {
		if _, ok := c.Storage.Storages[c.Auth.UseStorage]; !ok {
			return fmt.Errorf("proxy config: auth UseStorage points to unknown storage")
		}
	}
	if len(c.Auth.Custom) != 0 {
		var storCreds map[string]s3.CredentialsV4
		for _, storage := range c.Storage.Storages {
			storCreds = storage.Credentials
			break
		}
		for user := range c.Auth.Custom {
			if _, ok := storCreds[user]; !ok {
				return fmt.Errorf("proxy config: auth custom credentials unknown user %q", user)
			}
		}
	}
	if c.Auth.UseStorage == "" && len(c.Auth.Custom) == 0 {
		return fmt.Errorf("proxy config: auth credentials enabled but not set")
	}

	if c.Port <= 0 {
		return fmt.Errorf("proxy config: Port must be positive: %d", c.Port)
	}

	return nil
}

func GetConfig(src ...config.Src) (*Config, error) {
	dc := defaultConfig()
	var conf Config
	cfgSource := []config.Src{config.Reader(dc, "proxy_default_cfg")}
	cfgSource = append(cfgSource, src...)
	err := config.Get(&conf, cfgSource...)
	_ = dc.Close()
	if err != nil {
		return nil, err
	}
	return &conf, err
}
