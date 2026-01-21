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

package proxy

import (
	"embed"
	"fmt"
	"io/fs"

	"github.com/clyso/chorus/pkg/config"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/swift"
	"github.com/clyso/chorus/service/proxy/auth"
	"github.com/clyso/chorus/service/proxy/cors"
	"github.com/clyso/chorus/service/proxy/router"
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

type Storages = objstore.StoragesConfig[*s3.Storage, *router.SwiftStorage]
type Storage = objstore.GenericStorage[*s3.Storage, *router.SwiftStorage]

type Config struct {
	config.Common `yaml:",inline,omitempty" mapstructure:",squash"`

	Storage Storages     `yaml:"storage,omitempty"`
	Auth    *auth.Config `yaml:"auth,omitempty"`
	Port    int          `yaml:"port"`
	Address string       `yaml:"address"`
	Cors    *cors.Config `yaml:"cors"`
}

func (c *Config) Validate() error {
	if err := c.Common.Validate(); err != nil {
		return err
	}
	if err := c.Storage.Validate(); err != nil {
		return err
	}
	if err := ValidateAuth(c.Storage, c.Auth); err != nil {
		return err
	}
	return nil
}

func ValidateAuth(storage Storages, auth *auth.Config) error {
	if len(storage.S3Storages()) == 0 {
		return nil
	}
	// S3 storages set: validate auth config
	if auth == nil {
		return fmt.Errorf("proxy config: empty Auth config")
	}
	if auth.UseStorage != "" {
		if _, ok := storage.Storages[auth.UseStorage]; !ok {
			return fmt.Errorf("proxy config: auth UseStorage points to unknown storage")
		}
	}
	if len(auth.Custom) != 0 {
		for user := range auth.Custom {
			if err := storage.Exists(storage.Main, user); err != nil {
				return fmt.Errorf("proxy config: auth custom credentials unknown user %q", user)
			}
		}
	}
	if auth.UseStorage == "" && len(auth.Custom) == 0 {
		return fmt.Errorf("proxy config: auth credentials enabled but not set")
	}
	return nil
}

func GetConfig(src ...config.Opt) (*Config, error) {
	dc := defaultConfig()
	var conf Config
	cfgSource := []config.Opt{config.Reader(dc, "proxy_default_cfg"), config.Decoder(Storages{}.ViperUnmarshallerHookFunc())}
	cfgSource = append(cfgSource, src...)
	err := config.Get(&conf, cfgSource...)
	_ = dc.Close()
	if err != nil {
		return nil, err
	}
	return &conf, err
}

func ProxyToCredsConf(in Storages) (objstore.Config, error) {
	res := objstore.Config{
		Storages:           map[string]objstore.GenericStorage[*s3.Storage, *swift.Storage]{},
		Main:               in.Main,
		DynamicCredentials: in.DynamicCredentials,
	}

	for name, val := range in.Storages {
		switch val.Type {
		case dom.S3:
			c := *val.S3
			res.Storages[name] = objstore.Storage{
				S3:           &c,
				CommonConfig: val.CommonConfig,
			}
		case dom.Swift:
		// ignore - swift proxy conf does not contains credentials
		default:
			return objstore.Config{}, fmt.Errorf("unsupported storage type %q for storage %q", val.Type, name)
		}
	}
	return res, nil
}
