/*
 * Copyright © 2023 Clyso GmbH
 * Copyright © 2025 Strato GmbH
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

package config

import (
	"embed"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	stdlog "github.com/rs/zerolog/log"
	"github.com/spf13/viper"

	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/trace"

	"github.com/mitchellh/mapstructure"
)

//go:embed config.yaml
var configFile embed.FS

type Common struct {
	Log          *log.Config      `yaml:"log,omitempty"`
	Trace        *trace.Config    `yaml:"trace,omitempty"`
	Metrics      *metrics.Config  `yaml:"metrics,omitempty"`
	Redis        *Redis           `yaml:"redis,omitempty"`
	Features     *features.Config `yaml:"features,omitempty"`
	PrintSecrets bool             `yaml:"printSecret,omitempty"`
}

type Redis struct {
	// Deprecated: Address is deprecated: use Addresses
	// If Addresses set, Address will be ignored
	Address   s3.ConfAddr   `yaml:"address"`
	Addresses []s3.ConfAddr `yaml:"addresses"`
	Sentinel  RedisSentinel `yaml:"sentinel"`
	User      string        `yaml:"user"`
	Password  string        `yaml:"password"`
	TLS       TLS           `yaml:"tls"`
	MetaDB    int           `yaml:"metaDB"`
	QueueDB   int           `yaml:"queueDB"`
	LockDB    int           `yaml:"lockDB"`
	ConfigDB  int           `yaml:"configDB"`
}

type RedisSentinel struct {
	MasterName string `yaml:"masterName"`
	Password   string `yaml:"password"`
	User       string `yaml:"user"`
}

type TLS struct {
	Enabled  bool `yaml:"enabled"`
	Insecure bool `yaml:"insecure"`
}

func (r *Redis) validate() error {
	if len(r.GetAddresses()) == 0 {
		return fmt.Errorf("invalid redis config: address is not set")
	}
	return nil
}

func (r *Redis) GetAddresses() []string {
	if len(r.Addresses) != 0 {
		addrs := make([]string, len(r.Addresses))
		for i, addr := range r.Addresses {
			addrs[i] = addr.ValueWithProtocol()
		}
		return addrs
	}
	if r.Address.IsSet() {
		return []string{r.Address.ValueWithProtocol()}
	}
	return nil
}

func Get(conf any, sources ...Src) error {
	data, err := configFile.Open("config.yaml")
	if err != nil {
		return fmt.Errorf("%w: unable to read config.yaml", err)
	}
	defer data.Close()

	v := viper.NewWithOptions(viper.EnvKeyReplacer(strings.NewReplacer(".", "_")))
	v.SetConfigType("yaml")
	err = v.ReadConfig(data)
	if err != nil {
		return err
	}

	stdlog.Info().Msg("app config: reading default common config")

	for _, source := range sources {
		switch src := source.(type) {
		case pathOpt:
			_, err = os.Stat(string(src))
			if err != nil {
				stdlog.Warn().Msgf("app config: no config file %q", string(src))
				continue
			}
			v.SetConfigFile(string(src))
			err = v.MergeInConfig()
			if err != nil {
				return fmt.Errorf("%w: unable merge config file %q", err, string(src))
			}
			stdlog.Info().Msgf("app config: override with: %s", string(src))
		case readerOpt:
			err = v.MergeConfig(src.Reader)
			if err != nil {
				return fmt.Errorf("%w: unable merge config reader", err)
			}
			stdlog.Info().Msgf("app config: override with: %s", src.Name)
		}
	}

	// Override config values if there are envs
	v.AutomaticEnv()
	v.SetEnvPrefix("CFG")

	/*
	 * Unfortunately, viper does not support custom UnmarshalYAML methods
	 * (https://github.com/spf13/viper/issues/338, still open), nor does it support `yaml:`
	 * struct annotations (https://github.com/spf13/viper/issues/385, closed, by design).
	 * Instead, a custom decode hook is required to teach mapstructure (which is used by viper
	 * internally) how to decode the s3.ConfAddr type. As an unlucky design choice the
	 * mapstructure API only allows to configure all the hooks at once, not adding an additional
	 * one. Thus we prepend the default hooks mentioned in the viper documentation
	 * (https://pkg.go.dev/github.com/spf13/viper#DecodeHook).
	 */
	decodeHooks := mapstructure.ComposeDecodeHookFunc(
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
		func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
			if f.Kind() == reflect.String && t == reflect.TypeOf(s3.ConfAddr{}) {
				return s3.NewConfAddr(data.(string)), nil
			}
			return data, nil
		},
	)
	err = v.Unmarshal(&conf, viper.DecodeHook(decodeHooks))
	if err != nil {
		return fmt.Errorf("%w: unable to unmarshal config", err)
	}

	return nil
}

func (c *Common) Validate() error {
	if c.Features == nil {
		return fmt.Errorf("app config: empty Features config")
	}
	if c.Log == nil {
		return fmt.Errorf("app config: empty Log config")
	}
	if c.Redis == nil {
		return fmt.Errorf("app config: empty Redis config")
	}
	if err := c.Redis.validate(); err != nil {
		return err
	}
	if c.Metrics == nil {
		return fmt.Errorf("app config: empty Metrics config")
	}
	if c.Trace == nil {
		return fmt.Errorf("app config: empty Trace config")
	}

	return nil
}

type options struct {
	sources []any
}

type Src interface {
	apply(*options)
}

type pathOpt string

func (p pathOpt) apply(opts *options) {
	opts.sources = append(opts.sources, p)
}

func Path(path string) Src {
	return pathOpt(path)
}

type readerOpt struct {
	io.Reader
	Name string
}

func (r readerOpt) apply(opts *options) {
	opts.sources = append(opts.sources, r)
}

func Reader(reader io.Reader, name string) Src {
	return readerOpt{reader, name}
}
