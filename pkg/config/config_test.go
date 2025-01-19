package config

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/s3"
)

func TestGet(t *testing.T) {
	r := require.New(t)
	var conf Common
	err := Get(&conf)
	r.NoError(err)

	r.EqualValues(9090, conf.Metrics.Port)
	r.EqualValues("info", conf.Log.Level)
	r.EqualValues(false, conf.Log.Json)
}

func TestOverride(t *testing.T) {
	r := require.New(t)
	var conf Common
	err := Get(&conf, Path("override_test.yaml"))
	r.NoError(err)

	r.EqualValues(69, conf.Metrics.Port)
	r.EqualValues("info", conf.Log.Level)
	r.EqualValues(true, conf.Log.Json)
	r.NotEmpty(conf.Redis.Address)

	r.EqualValues("user", conf.Redis.User)
	r.EqualValues("pass", conf.Redis.Password)
	r.EqualValues("sentinel", conf.Redis.Sentinel.User)
	r.EqualValues("sentinel-pass", conf.Redis.Sentinel.Password)
	r.EqualValues("master", conf.Redis.Sentinel.MasterName)
	r.True(conf.Redis.TLS.Enabled)
	r.True(conf.Redis.TLS.Insecure)

}

func TestOverride2(t *testing.T) {
	r := require.New(t)
	var conf Common
	err := Get(&conf, Path("override_test.yaml"), Path("override_test2.yaml"))
	r.NoError(err)
	r.NoError(conf.Validate())

	r.EqualValues(420, conf.Metrics.Port)
	r.EqualValues("info", conf.Log.Level)
	r.EqualValues(true, conf.Log.Json)
	r.NotEmpty(conf.Redis.Address)
	r.Empty(conf.Redis.Addresses)
	r.EqualValues([]string{conf.Redis.Address}, conf.Redis.GetAddresses())
}

func TestOverrideEnv(t *testing.T) {
	t.Setenv("CFG_METRICS_PORT", "55")
	t.Setenv("CFG_REDIS_PASSWORD", "secret")
	t.Setenv("CFG_REDIS_ADDRESSES", "a,b,c")

	r := require.New(t)
	var conf Common
	err := Get(&conf, Path("override_test.yaml"), Path("override_test2.yaml"))
	r.NoError(err)
	r.NoError(conf.Validate())
	r.EqualValues(55, conf.Metrics.Port)
	r.EqualValues("info", conf.Log.Level)
	r.EqualValues(true, conf.Log.Json)
	r.EqualValues("secret", conf.Redis.Password)
	r.NotEmpty(conf.Redis.Address)
	r.EqualValues([]string{"a", "b", "c"}, conf.Redis.Addresses, "redis addresses set from env")
	r.EqualValues([]string{"a", "b", "c"}, conf.Redis.GetAddresses(), "address is ignored")
}

func TestStorageConfig_RateLimitConf(t *testing.T) {
	r := require.New(t)
	conf := s3.StorageConfig{Storages: map[string]s3.Storage{
		"main": {RateLimit: s3.RateLimit{
			Enabled: false,
			RPM:     1,
		}},
		"f1": {RateLimit: s3.RateLimit{
			Enabled: true,
			RPM:     2,
		}},
		"f2": {RateLimit: s3.RateLimit{
			Enabled: false,
			RPM:     3,
		}},
	}}
	res := conf.RateLimitConf()
	r.EqualValues(s3.RateLimit{
		Enabled: false,
		RPM:     1,
	}, res["main"])
	r.EqualValues(s3.RateLimit{
		Enabled: true,
		RPM:     2,
	}, res["f1"])
	r.EqualValues(s3.RateLimit{
		Enabled: false,
		RPM:     3,
	}, res["f2"])
	r.EqualValues(s3.RateLimit{
		Enabled: false,
		RPM:     1,
	}, res["main"])
	r.EqualValues(conf.Storages["main"].RateLimit, res["main"])
	r.EqualValues(conf.Storages["f1"].RateLimit, res["f1"])
	r.EqualValues(conf.Storages["f2"].RateLimit, res["f2"])
}

func TestRedis_validate(t *testing.T) {
	type fields struct {
		Address   string
		Addresses []string
		Sentinel  RedisSentinel
		User      string
		Password  string
		UseTLS    bool
		MetaDB    int
		QueueDB   int
		LockDB    int
		ConfigDB  int
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "invalid: no address set",
			fields: fields{
				Address:   "",
				Addresses: []string{},
			},
			wantErr: true,
		},
		{
			name: "valid: both addresses set",
			fields: fields{
				Address:   "addr",
				Addresses: []string{"addr"},
			},
			wantErr: false,
		},
		{
			name: "valid: only address set",
			fields: fields{
				Address:   "addr",
				Addresses: []string{},
			},
			wantErr: false,
		},
		{
			name: "valid: only addresses set",
			fields: fields{
				Address:   "",
				Addresses: []string{"addr"},
			},
			wantErr: false,
		},
		{
			name: "valid: addresses is nil",
			fields: fields{
				Address: "addr",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Redis{
				Address:   tt.fields.Address,
				Addresses: tt.fields.Addresses,
				Sentinel:  tt.fields.Sentinel,
				User:      tt.fields.User,
				Password:  tt.fields.Password,
				MetaDB:    tt.fields.MetaDB,
				QueueDB:   tt.fields.QueueDB,
				LockDB:    tt.fields.LockDB,
				ConfigDB:  tt.fields.ConfigDB,
			}
			if err := r.validate(); (err != nil) != tt.wantErr {
				t.Errorf("Redis.validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRedis_GetAddresses(t *testing.T) {
	type fields struct {
		Address   string
		Addresses []string
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{
			name: "only address is set",
			fields: fields{
				Address:   "a",
				Addresses: []string{},
			},
			want: []string{"a"},
		},
		{
			name: "only addresses are set",
			fields: fields{
				Address:   "",
				Addresses: []string{"a", "b"},
			},
			want: []string{"a", "b"},
		},
		{
			name: "address is ignored when addresses set",
			fields: fields{
				Address:   "a",
				Addresses: []string{"b", "c"},
			},
			want: []string{"b", "c"},
		},
		{
			name:   "nothing is set",
			fields: fields{},
			want:   nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Redis{
				Address:   tt.fields.Address,
				Addresses: tt.fields.Addresses,
			}
			if got := r.GetAddresses(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Redis.GetAddresses() = %v, want %v", got, tt.want)
			}
		})
	}
}
