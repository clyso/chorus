package config

import (
	"embed"
	"fmt"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/trace"
	stdlog "github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"io"
	"os"
	"strings"
)

//go:embed config.yaml
var configFile embed.FS

type Common struct {
	Log      *log.Config      `yaml:"log,omitempty"`
	Trace    *trace.Config    `yaml:"trace,omitempty"`
	Metrics  *metrics.Config  `yaml:"metrics,omitempty"`
	Redis    *Redis           `yaml:"redis,omitempty"`
	Features *features.Config `yaml:"features,omitempty"`
}

type Redis struct {
	Address  string `yaml:"address"`
	Password string `yaml:"password"`
	MetaDB   int    `yaml:"metaDB"`
	QueueDB  int    `yaml:"queueDB"`
	LockDB   int    `yaml:"lockDB"`
	ConfigDB int    `yaml:"configDB"`
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

	err = v.Unmarshal(&conf)
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
