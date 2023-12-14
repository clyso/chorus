package auth

import (
	"github.com/clyso/chorus/pkg/s3"
)

type Config struct {
	AllowV2Signature bool                        `yaml:"allowV2Signature"`
	UseStorage       string                      `yaml:"useStorage"`
	Custom           map[string]s3.CredentialsV4 `yaml:"custom"`
}
