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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/clyso/chorus/pkg/config"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/service/proxy/auth"
)

func TestGetConfigDefaults(t *testing.T) {
	r := require.New(t)
	_, err := GetConfig()
	r.NoError(err)
}

// TestHelmEnvOverrides tests env vars from deploy/chorus/templates/proxy/deployment-proxy.yaml
func TestHelmEnvOverrides(t *testing.T) {
	const storagesOverride = `storage:
  main: s3
  storages:
    s3:
      type: S3
      provider: Ceph
      credentials:
        user1:
          alias1:
            accessKeyID: key
            secretAccessKey: secret
      address: s3.example.com
auth:
  useStorage: s3`

	t.Run("CFG_REDIS_PASSWORD", func(t *testing.T) {
		r := require.New(t)
		t.Setenv("CFG_REDIS_PASSWORD", "secret-from-helm")
		conf, err := GetConfig(config.Reader(strings.NewReader(storagesOverride), "test"))
		r.NoError(err)
		r.Equal("secret-from-helm", conf.Redis.Password)
	})

	t.Run("CFG_REDIS_SENTINEL_MASTERNAME", func(t *testing.T) {
		r := require.New(t)
		t.Setenv("CFG_REDIS_SENTINEL_MASTERNAME", "mymaster")
		conf, err := GetConfig(config.Reader(strings.NewReader(storagesOverride), "test"))
		r.NoError(err)
		r.Equal("mymaster", conf.Redis.Sentinel.MasterName)
	})

	t.Run("CFG_STORAGE_DYNAMICCREDENTIALS_MASTERPASSWORD", func(t *testing.T) {
		r := require.New(t)
		t.Setenv("CFG_STORAGE_DYNAMICCREDENTIALS_MASTERPASSWORD", "encryption-key!!")
		conf, err := GetConfig(config.Reader(strings.NewReader(storagesOverride), "test"))
		r.NoError(err)
		r.Equal("encryption-key!!", conf.Storage.DynamicCredentials.MasterPassword)
	})
}

func validProxyStorages() Storages {
	return Storages{
		Main: "main",
		Storages: map[string]Storage{
			"main": {
				CommonConfig: objstore.CommonConfig{Type: dom.S3},
				S3: &s3.ProxyStorage{
					Credentials: map[string]map[string]s3.CredentialsV4{
						"user1": {
							"alias1": {AccessKeyID: "key1", SecretAccessKey: "secret1"},
							"alias2": {AccessKeyID: "key2", SecretAccessKey: "secret2"},
						},
					},
					StorageAddress: s3.StorageAddress{
						Address:  "s3.example.com",
						Provider: s3.ProviderCeph,
					},
				},
			},
		},
	}
}

func TestStorages_NestedYAMLUnmarshal(t *testing.T) {
	r := require.New(t)
	const storagesYAML = `main: s3
storages:
  s3:
    type: S3
    provider: Ceph
    address: s3.example.com
    credentials:
      user1:
        laptop:
          accessKeyID: key1
          secretAccessKey: secret1
        ci:
          accessKeyID: key2
          secretAccessKey: secret2
      user2:
        laptop:
          accessKeyID: key3
          secretAccessKey: secret3`

	var conf Storages
	r.NoError(yaml.Unmarshal([]byte(storagesYAML), &conf))
	r.NoError(conf.Validate())
	stor := conf.Storages["s3"].S3
	r.NotNil(stor)
	r.Len(stor.Credentials, 2)
	r.EqualValues("key1", stor.Credentials["user1"]["laptop"].AccessKeyID)
	r.EqualValues("secret1", stor.Credentials["user1"]["laptop"].SecretAccessKey)
	r.EqualValues("key2", stor.Credentials["user1"]["ci"].AccessKeyID)
	r.EqualValues("key3", stor.Credentials["user2"]["laptop"].AccessKeyID)
	r.True(stor.HasUser("user1"))
	r.True(stor.HasUserAlias("user1", "ci"))
	r.False(stor.HasUserAlias("user1", "unknown"))
	r.ElementsMatch([]string{"user1", "user2"}, stor.UserList())
}

func TestProxyStorage_Validate(t *testing.T) {
	validAddr := s3.StorageAddress{
		Address:  "s3.example.com",
		Provider: s3.ProviderCeph,
	}
	validCred := s3.CredentialsV4{AccessKeyID: "key", SecretAccessKey: "secret"}
	tests := []struct {
		name    string
		creds   map[string]map[string]s3.CredentialsV4
		wantErr bool
	}{
		{
			name:  "valid",
			creds: map[string]map[string]s3.CredentialsV4{"user1": {"alias1": validCred}},
		},
		{
			name:    "empty alias name",
			creds:   map[string]map[string]s3.CredentialsV4{"user1": {"": validCred}},
			wantErr: true,
		},
		{
			name:    "colon in alias name",
			creds:   map[string]map[string]s3.CredentialsV4{"user1": {"ali:as": validCred}},
			wantErr: true,
		},
		{
			name:    "colon in user name",
			creds:   map[string]map[string]s3.CredentialsV4{"us:er": {"alias1": validCred}},
			wantErr: true,
		},
		{
			name:    "user without aliases",
			creds:   map[string]map[string]s3.CredentialsV4{"user1": {}},
			wantErr: true,
		},
		{
			name:    "invalid credential",
			creds:   map[string]map[string]s3.CredentialsV4{"user1": {"alias1": {AccessKeyID: "key"}}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			stor := &s3.ProxyStorage{
				Credentials:    tt.creds,
				StorageAddress: validAddr,
			}
			err := stor.Validate()
			if tt.wantErr {
				r.Error(err)
			} else {
				r.NoError(err)
			}
		})
	}
}

func TestValidateAuth_CustomAlias(t *testing.T) {
	r := require.New(t)
	storage := validProxyStorages()

	// custom credential matching (user, alias) from main storage
	r.NoError(ValidateAuth(storage, &auth.Config{
		Custom: map[string]map[string]s3.CredentialsV4{
			"user1": {"alias1": {AccessKeyID: "custom-key", SecretAccessKey: "custom-secret"}},
		},
	}))

	// unknown user
	r.Error(ValidateAuth(storage, &auth.Config{
		Custom: map[string]map[string]s3.CredentialsV4{
			"unknown": {"alias1": {AccessKeyID: "custom-key", SecretAccessKey: "custom-secret"}},
		},
	}))

	// known user, unknown alias
	r.Error(ValidateAuth(storage, &auth.Config{
		Custom: map[string]map[string]s3.CredentialsV4{
			"user1": {"unknown": {AccessKeyID: "custom-key", SecretAccessKey: "custom-secret"}},
		},
	}))

	// useStorage must exist
	r.Error(ValidateAuth(storage, &auth.Config{UseStorage: "unknown"}))
	r.NoError(ValidateAuth(storage, &auth.Config{UseStorage: "main"}))

	// auth credentials must be set
	r.Error(ValidateAuth(storage, &auth.Config{}))
}
