/*
 * Copyright © 2024 Clyso GmbH
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
	"slices"

	"github.com/clyso/chorus/pkg/config"
	"github.com/clyso/chorus/service/proxy"
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
		Storage proxy.Storages `yaml:"storage,omitempty"`
		Enabled bool           `yaml:"enabled"`
		Auth    *auth.Config   `yaml:"auth,omitempty"`
		Port    int            `yaml:"port"`
		Address string         `yaml:"address"`
		Cors    *cors.Config   `yaml:"cors"`
	} `yaml:"proxy"`
}

func (c *Config) Validate() error {
	if err := c.Config.Validate(); err != nil {
		return err
	}
	if c.UIPort <= 0 {
		return fmt.Errorf("chorus config: uiPort must be positive: %d", c.Concurrency)
	}

	if c.Proxy.Enabled {
		if err := proxy.ValidateAuth(c.Proxy.Storage, c.Proxy.Auth); err != nil {
			return err
		}
		if c.Proxy.Port <= 0 {
			return fmt.Errorf("chorus config: Port must be positive: %d", c.Proxy.Port)
		}
		if err := c.Proxy.Storage.Validate(); err != nil {
			return fmt.Errorf("chorus config: invalid proxy storage config: %w", err)
		}
		// check that proxy and worker storage configs match
		if c.Storage.Main != c.Proxy.Storage.Main {
			return fmt.Errorf("chorus config: proxy main storage %q does not match worker main storage %q", c.Proxy.Storage.Main, c.Storage.Main)
		}
		if len(c.Storage.Storages) != len(c.Proxy.Storage.Storages) {
			return fmt.Errorf("chorus config: number of proxy storages %d does not match number of worker storages %d", len(c.Proxy.Storage.Storages), len(c.Storage.Storages))
		}
		for name, workerStorage := range c.Storage.Storages {
			proxyStorage, ok := c.Proxy.Storage.Storages[name]
			if !ok {
				return fmt.Errorf("chorus config: proxy storage %q not found in worker storages", name)
			}
			if workerStorage.Type != proxyStorage.Type {
				return fmt.Errorf("chorus config: proxy storage %q type %q does not match worker storage type %q", name, proxyStorage.Type, workerStorage.Type)
			}
		}
		// check that proxy and worker storage users match
		workerUserList := c.Storage.GetMain().UserList()
		proxUserList := c.Proxy.Storage.GetMain().UserList()
		if !slices.Equal(workerUserList, proxUserList) {
			return fmt.Errorf("chorus config: proxy main storage users %v do not match worker main storage users %v", proxUserList, workerUserList)
		}
	}
	return nil
}

func GetConfig(src ...config.Opt) (*Config, error) {
	dc := defaultConfig()
	var conf Config
	cfgSource := []config.Opt{config.Reader(dc, "chorus_default_cfg")}
	if len(src) == 0 {
		src = []config.Opt{config.Reader(testConfig(), "chorus_test_cfg")}
	}

	cfgSource = append(cfgSource, src...)
	err := config.Get(&conf, cfgSource...)
	_ = dc.Close()
	if err != nil {
		return nil, err
	}
	return &conf, err
}
