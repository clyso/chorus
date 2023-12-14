package rclone

import (
	"github.com/clyso/chorus/pkg/ratelimit"
	"time"
)

type Config struct {
	MemoryLimit     MemoryLimit               `yaml:"memoryLimit"`
	MemoryCalc      MemoryCalc                `yaml:"memoryCalc"`
	LocalFileLimit  ratelimit.SemaphoreConfig `yaml:"localFileLimit"`
	GlobalFileLimit ratelimit.SemaphoreConfig `yaml:"globalFileLimit"`
}

type MemoryLimit struct {
	Enabled  bool          `yaml:"enabled"`
	Limit    string        `yaml:"limit"`
	RetryMin time.Duration `yaml:"retryMin"`
	RetryMax time.Duration `yaml:"retryMax"`
}

type MemoryCalc struct {
	Const string  `yaml:"const"`
	Mul   float64 `yaml:"mul"`
}
