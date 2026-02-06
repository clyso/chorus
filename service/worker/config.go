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
	"github.com/clyso/chorus/pkg/objstore"
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
	Storage       objstore.Config `yaml:"storage,omitempty"`

	Concurrency     int           `yaml:"concurrency"`
	ShutdownTimeout time.Duration `yaml:"shutdownTimeout"`

	Api    *api.Config     `yaml:"api,omitempty"`
	Lock   *Lock           `yaml:"lock,omitempty"`
	Worker *handler.Config `yaml:"worker,omitempty"`
}

type Lock struct {
	Overlap time.Duration `yaml:"overlap"`
}

func (c *Config) Validate() error {
	if err := c.Common.Validate(); err != nil {
		return err
	}
	if err := c.Storage.Validate(); err != nil {
		return err
	}
	if c.Concurrency <= 0 {
		return fmt.Errorf("worker config: concurency config must be positive: %d", c.Concurrency)
	}
	if c.Api == nil {
		return fmt.Errorf("worker config: empty Api config")
	}
	if c.Lock == nil {
		return fmt.Errorf("app config: empty Lock config")
	}
	return nil
}

func GetConfig(src ...config.Opt) (*Config, error) {
	dc := defaultConfig()
	var conf Config
	cfgSource := []config.Opt{config.Reader(dc, "worker_default_cfg")}
	cfgSource = append(cfgSource, src...)
	err := config.Get(&conf, cfgSource...)
	_ = dc.Close()
	if err != nil {
		return nil, err
	}
	return &conf, err
}
