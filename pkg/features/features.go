package features

import "context"

type Config struct {
	Versioning bool `yaml:"versioning"`
	Tagging    bool `yaml:"tagging"`
	ACL        bool `yaml:"acl"`
	Lifecycle  bool `yaml:"lifecycle"`
	Policy     bool `yaml:"policy"`
}

var val *Config

func Set(c *Config) {
	val = c
}

func Versioning(_ context.Context) bool {
	return val.Versioning
}

func Tagging(_ context.Context) bool {
	return val.Tagging
}

func ACL(_ context.Context) bool {
	return val.ACL
}

func Lifecycle(_ context.Context) bool {
	return val.Lifecycle
}

func Policy(_ context.Context) bool {
	return val.Policy
}
