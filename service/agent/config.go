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
