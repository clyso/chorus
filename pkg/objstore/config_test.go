package objstore

import (
	"strings"
	"testing"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/swift"
)

const testStoragesYAML = `main: s3storage
storages:
  s3storage:
    type: S3
    address: clyso.com
    provider: Ceph
    isSecure: false
    credentials:
      user1:
        accessKeyID: id1
        secretAccessKey: key1
      user2:
        accessKeyID: id2
        secretAccessKey: key2
  swiftstorage:
    type: SWIFT
    authURL: http://auth.url
    storageEndpointName: endpoint
    credentials:
      user1:
        username: username1
        password: password1
        domainName: domain1
        tenantName: tenant1
      user2:
        username: username2
        password: password2
        domainName: domain2
        tenantName: tenant2`

func TestStoragesConfig_Validate(t *testing.T) {
	var (
		validS3 = s3.Storage{
			Credentials: map[string]s3.CredentialsV4{
				"user": {
					AccessKeyID:     "id",
					SecretAccessKey: "key",
				},
			},
			Address:  "clyso.com",
			Provider: "Ceph",
			IsSecure: false,
		}
		invalidS3 = s3.Storage{
			Credentials: nil, // missing credentials
			Address:     "clyso.com",
			Provider:    "Ceph",
			IsSecure:    false,
		}
		validSwift = swift.Storage{
			Credentials: map[string]swift.Credentials{
				"user": {
					Username:   "username",
					Password:   "password",
					DomainName: "domain",
					TenantName: "tenant",
				},
			},
			StorageEndpointName: "endpoint",
			AuthURL:             "http://auth.url",
		}
		invalidSwift = swift.Storage{
			Credentials:         nil, // missing credentials
			StorageEndpointName: "endpoint",
			AuthURL:             "http://auth.url",
		}
	)

	tests := []struct {
		name    string
		in      Config
		wantErr bool
	}{
		{
			name: "valid",
			in: Config{
				Main: "s3storage",
				Storages: map[string]Storage{
					"s3storage": {
						CommonConfig: CommonConfig{Type: dom.S3},
						S3:           &validS3,
					},
					"swiftstorage": {
						CommonConfig: CommonConfig{Type: dom.Swift},
						Swift:        &validSwift,
					},
				},
			},
		},
		{
			name: "missing main storage",
			in: Config{
				Main: "missingstorage",
				Storages: map[string]Storage{
					"s3storage": {
						CommonConfig: CommonConfig{Type: dom.S3},
						S3:           &validS3,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "no storages",
			in: Config{
				Main:     "",
				Storages: map[string]Storage{},
			},
			wantErr: true,
		},
		{
			name: "invalid s3 storage config",
			in: Config{
				Main: "s3storage",
				Storages: map[string]Storage{
					"s3storage": {
						CommonConfig: CommonConfig{Type: dom.S3},
						S3:           &invalidS3,
					},
					"swiftstorage": {
						CommonConfig: CommonConfig{Type: dom.Swift},
						Swift:        &validSwift,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid swift storage config",
			in: Config{
				Main: "swiftstorage",
				Storages: map[string]Storage{
					"s3storage": {
						CommonConfig: CommonConfig{Type: dom.S3},
						S3:           &validS3,
					},
					"swiftstorage": {
						CommonConfig: CommonConfig{Type: dom.Swift},
						Swift:        &invalidSwift,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "unknown storage type",
			in: Config{
				Main: "unknownstorage",
				Storages: map[string]Storage{
					"unknownstorage": {
						CommonConfig: CommonConfig{Type: "UnknownType"},
						S3:           &validS3,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "missing storage type",
			in: Config{
				Main: "missingtypestorage",
				Storages: map[string]Storage{
					"missingtypestorage": {
						CommonConfig: CommonConfig{},
						S3:           &validS3,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "wrong config for storage type",
			in: Config{
				Main: "storageswiftwiths3config",
				Storages: map[string]Storage{
					"storageswiftwiths3config": {
						CommonConfig: CommonConfig{Type: dom.Swift},
						S3:           &validS3, // S3 config set for Swift type
					},
				},
			},
			wantErr: true,
		},
		{
			name: "wrong config for storage type 2",
			in: Config{
				Main: "storages3withswiftconfig",
				Storages: map[string]Storage{
					"storages3withswiftconfig": {
						CommonConfig: CommonConfig{Type: dom.S3},
						Swift:        &validSwift, // Swift config set for S3 type
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.in.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("StoragesConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStoragesConfig_Exists(t *testing.T) {
	r := require.New(t)
	conf := Config{
		Storages: map[string]Storage{
			"s3": {
				CommonConfig: CommonConfig{Type: dom.S3},
				S3: &s3.Storage{
					Credentials: map[string]s3.CredentialsV4{
						"user1": {},
						"user2": {},
					},
				},
			},
			"swift": {
				CommonConfig: CommonConfig{Type: dom.Swift},
				Swift: &swift.Storage{
					Credentials: map[string]swift.Credentials{
						"user1": {},
						"user3": {},
					},
				},
			},
		},
	}
	r.NoError(conf.Exists("s3", "user1"))
	r.NoError(conf.Exists("s3", "user2"))
	r.Error(conf.Exists("s3", "user3"))
	r.Error(conf.Exists("s3", ""))
	r.Error(conf.Exists("s3", "asdf"))

	r.NoError(conf.Exists("swift", "user1"))
	r.NoError(conf.Exists("swift", "user3"))
	r.Error(conf.Exists("swift", "user2"))
	r.Error(conf.Exists("swift", ""))
	r.Error(conf.Exists("swift", "asdf"))

	r.Error(conf.Exists("nonexistent", "user1"))
	r.Error(conf.Exists("nonexistent", "user2"))
	r.Error(conf.Exists("nonexistent", "user3"))

	r.Error(conf.Exists("", "user1"))
	r.Error(conf.Exists("", "user2"))
	r.Error(conf.Exists("", "user3"))
}

func TestStoragesConfig_RateLimitConf(t *testing.T) {
	r := require.New(t)
	conf := Config{
		Storages: map[string]Storage{
			"s3": {
				CommonConfig: CommonConfig{Type: dom.S3,
					RateLimit: ratelimit.RateLimit{
						Enabled: false,
						RPM:     1,
					},
				},
				S3: &s3.Storage{},
			},
			"swift": {
				CommonConfig: CommonConfig{Type: dom.Swift,
					RateLimit: ratelimit.RateLimit{
						Enabled: false,
						RPM:     2,
					},
				},
				Swift: &swift.Storage{},
			},
		},
	}
	rlConf := conf.RateLimitConf()
	r.Len(rlConf, 2)
	r.EqualValues(1, rlConf["s3"].RPM)
	r.EqualValues(false, rlConf["s3"].Enabled)
	r.EqualValues(2, rlConf["swift"].RPM)
	r.EqualValues(false, rlConf["swift"].Enabled)
}

func TestStoragesConfig_Map(t *testing.T) {
	r := require.New(t)
	conf := Config{
		Storages: map[string]Storage{
			"s3": {
				CommonConfig: CommonConfig{Type: dom.S3},
				S3: &s3.Storage{
					Address: "clyso.com",
				},
			},
			"swift": {
				CommonConfig: CommonConfig{Type: dom.Swift},
				Swift: &swift.Storage{
					AuthURL: "http://auth.url",
				},
			},
		},
	}
	s3Storages := conf.S3Storages()
	r.Len(s3Storages, 1)
	s3Storage, ok := s3Storages["s3"]
	r.True(ok)
	r.EqualValues("clyso.com", s3Storage.Address)

	swiftStorages := conf.SwiftStorages()
	r.Len(swiftStorages, 1)
	swiftStorage, ok := swiftStorages["swift"]
	r.True(ok)
	r.EqualValues("http://auth.url", swiftStorage.AuthURL)
}

func Test_YAML_Unmarshal(t *testing.T) {
	r := require.New(t)
	var conf Config
	err := yaml.Unmarshal([]byte(testStoragesYAML), &conf)
	r.NoError(err)
	r.EqualValues("s3storage", conf.Main)
	r.Len(conf.Storages, 2)
	r.NoError(conf.Validate())

	s3Storage, ok := conf.Storages["s3storage"]
	r.True(ok)
	r.EqualValues(dom.S3, s3Storage.Type)
	r.NotNil(s3Storage.S3)
	r.EqualValues("http://clyso.com", s3Storage.S3.Address)
	r.EqualValues("Ceph", s3Storage.S3.Provider)
	r.EqualValues(false, s3Storage.S3.IsSecure)
	r.Len(s3Storage.S3.Credentials, 2)
	cred1, ok := s3Storage.S3.Credentials["user1"]
	r.True(ok)
	r.EqualValues("id1", cred1.AccessKeyID)
	r.EqualValues("key1", cred1.SecretAccessKey)
	cred2, ok := s3Storage.S3.Credentials["user2"]
	r.True(ok)
	r.EqualValues("id2", cred2.AccessKeyID)
	r.EqualValues("key2", cred2.SecretAccessKey)

	swiftStorage, ok := conf.Storages["swiftstorage"]
	r.True(ok)
	r.EqualValues(dom.Swift, swiftStorage.Type)
	r.NotNil(swiftStorage.Swift)
	r.EqualValues("http://auth.url", swiftStorage.Swift.AuthURL)
	r.EqualValues("endpoint", swiftStorage.Swift.StorageEndpointName)
	r.Len(swiftStorage.Swift.Credentials, 2)
	sCred1, ok := swiftStorage.Swift.Credentials["user1"]
	r.True(ok)
	r.EqualValues("username1", sCred1.Username)
	r.EqualValues("password1", sCred1.Password)
	r.EqualValues("domain1", sCred1.DomainName)
	r.EqualValues("tenant1", sCred1.TenantName)
	sCred2, ok := swiftStorage.Swift.Credentials["user2"]
	r.True(ok)
	r.EqualValues("username2", sCred2.Username)
	r.EqualValues("password2", sCred2.Password)
	r.EqualValues("domain2", sCred2.DomainName)
	r.EqualValues("tenant2", sCred2.TenantName)
}

func Test_YAML_Marshal(t *testing.T) {
	r := require.New(t)
	conf := Config{
		Main: "s3storage",
		Storages: map[string]Storage{
			"s3storage": {
				CommonConfig: CommonConfig{
					Type: dom.S3,
					RateLimit: ratelimit.RateLimit{
						Enabled: false,
						RPM:     1,
					},
				},
				S3: &s3.Storage{
					Address:  "clyso.com",
					Provider: "Ceph",
					IsSecure: false,
					Credentials: map[string]s3.CredentialsV4{
						"user1": {
							AccessKeyID:     "id1",
							SecretAccessKey: "key1",
						},
						"user2": {
							AccessKeyID:     "id2",
							SecretAccessKey: "key2",
						},
					},
				},
			},
			"swiftstorage": {
				CommonConfig: CommonConfig{
					Type: dom.Swift,
					RateLimit: ratelimit.RateLimit{
						Enabled: true,
						RPM:     2,
					},
				},
				Swift: &swift.Storage{
					AuthURL:             "http://auth.url",
					StorageEndpointName: "endpoint",
					Credentials: map[string]swift.Credentials{
						"user1": {
							Username:   "username1",
							Password:   "password1",
							DomainName: "domain1",
							TenantName: "tenant1",
						},
						"user2": {
							Username:   "username2",
							Password:   "password2",
							DomainName: "domain2",
							TenantName: "tenant2",
						},
					},
				},
			},
		},
	}
	r.NoError(conf.Validate())
	data, err := yaml.Marshal(&conf)
	r.NoError(err)

	fromYaml := Config{}
	r.NoError(yaml.Unmarshal(data, &fromYaml))
	r.EqualValues(conf, fromYaml)
	r.NoError(fromYaml.Validate())
}

func Test_Viper(t *testing.T) {

	r := require.New(t)
	v := viper.NewWithOptions(viper.WithDecodeHook(mapstructure.ComposeDecodeHookFunc(mapstructure.StringToTimeDurationHookFunc(), Config{}.ViperUnmarshallerHookFunc())))
	v.SetConfigType("yaml")
	r.NoError(v.ReadConfig(strings.NewReader(testStoragesYAML)))

	// read via viper
	var confFromViper Config
	r.NoError(v.Unmarshal(&confFromViper))
	r.EqualValues("s3storage", confFromViper.Main)
	r.Len(confFromViper.Storages, 2)
	r.NoError(confFromViper.Validate())

	// read via yaml
	var confFromYAML Config
	r.NoError(yaml.Unmarshal([]byte(testStoragesYAML), &confFromYAML))
	r.NoError(confFromYAML.Validate())
	r.EqualValues(confFromYAML, confFromViper)
}

func TestStorage_UserList(t *testing.T) {
	r := require.New(t)
	var confFromYAML Config
	r.NoError(yaml.Unmarshal([]byte(testStoragesYAML), &confFromYAML))
	r.NoError(confFromYAML.Validate())

	s3Storage, ok := confFromYAML.Storages["s3storage"]
	r.True(ok)
	s3Users := s3Storage.UserList()
	r.Len(s3Users, 2)
	r.Contains(s3Users, "user1")
	r.Contains(s3Users, "user2")
}
