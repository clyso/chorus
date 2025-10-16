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

package worker

import (
	"embed"
	"fmt"
	"io/fs"
	"time"

	"github.com/clyso/chorus/pkg/api"
	"github.com/clyso/chorus/pkg/config"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/rclone"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/swift"
	"github.com/clyso/chorus/service/worker/handler"
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
	Storage       *s3.StorageConfig   `yaml:"storage,omitempty"`
	Swift         *swift.ClientConfig `yaml:"swift,omitempty"`

	Concurrency     int           `yaml:"concurrency"`
	ShutdownTimeout time.Duration `yaml:"shutdownTimeout"`

	Api    *api.Config     `yaml:"api,omitempty"`
	RClone *rclone.Config  `yaml:"rclone,omitempty"`
	Lock   *Lock           `yaml:"lock,omitempty"`
	Worker *handler.Config `yaml:"worker,omitempty"`
}

type Lock struct {
	Overlap time.Duration `yaml:"overlap"`
}

func (c *Config) MainStorage() string {
	if c.Storage != nil {
		res := c.Storage.Main()
		if res != "" {
			return res
		}
	}
	if c.Swift != nil {
		return c.Swift.MainStorage
	}
	return ""
}

func (c *Config) GetStorages() dom.Storages {
	res := dom.Storages{}
	if c.Storage != nil {
		for k, v := range c.Storage.Storages {
			res[k] = dom.Storage{
				Type:        dom.S3,
				Address:     v.Address,
				Credentials: map[string]dom.Credentials{},
				Provider:    v.Provider,
				IsMain:      v.IsMain,
			}
			for ck, cv := range v.Credentials {
				res[k].Credentials[ck] = dom.Credentials{
					AccessKey: cv.AccessKeyID,
				}
			}
		}
	}
	if c.Swift != nil {
		for k, v := range c.Swift.Storages {
			res[k] = dom.Storage{
				Type:     dom.Swift,
				Address:  v.AuthURL,
				Provider: "",
				IsMain:   c.Swift.MainStorage == k,
			}
		}
	}
	return res
}

func (c *Config) Validate() error {
	if err := c.Common.Validate(); err != nil {
		return err
	}
	if c.Concurrency <= 0 {
		return fmt.Errorf("worker config: concurency config must be positive: %d", c.Concurrency)
	}
	if c.Api == nil {
		return fmt.Errorf("worker config: empty Api config")
	}
	if c.RClone == nil {
		return fmt.Errorf("app config: empty RClone config")
	}
	if c.Lock == nil {
		return fmt.Errorf("app config: empty Lock config")
	}
	s3Enabled := c.Storage != nil && len(c.Storage.Storages) != 0
	swiftEnabled := c.Swift != nil && len(c.Swift.Storages) != 0
	if !s3Enabled && !swiftEnabled {
		return fmt.Errorf("app config: empty storages config")
	}
	if s3Enabled {
		if err := c.Storage.Init(); err != nil {
			return err
		}
	}

	return nil
}

func GetConfig(src ...config.Src) (*Config, error) {
	dc := defaultConfig()
	var conf Config
	cfgSource := []config.Src{config.Reader(dc, "worker_default_cfg")}
	cfgSource = append(cfgSource, src...)
	err := config.Get(&conf, cfgSource...)
	_ = dc.Close()
	if err != nil {
		return nil, err
	}
	return &conf, err
}
