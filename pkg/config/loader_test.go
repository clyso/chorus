package config

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestEnvOverride(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		env      map[string]string
		validate func(r *require.Assertions, yaml string, err error)
	}{
		{
			name: "string override",
			yaml: `password: original`,
			env:  map[string]string{"CFG_PASSWORD": "secret"},
			validate: func(r *require.Assertions, yaml string, err error) {
				r.NoError(err)
				r.Contains(yaml, "password: secret")
			},
		},
		{
			name: "int override",
			yaml: `port: 3000`,
			env:  map[string]string{"CFG_PORT": "8080"},
			validate: func(r *require.Assertions, yaml string, err error) {
				r.NoError(err)
				r.Contains(yaml, "port: 8080")
			},
		},
		{
			name: "bool override",
			yaml: `enabled: false`,
			env:  map[string]string{"CFG_ENABLED": "true"},
			validate: func(r *require.Assertions, yaml string, err error) {
				r.NoError(err)
				r.Contains(yaml, "enabled: true")
			},
		},
		{
			name: "nested field override",
			yaml: "redis:\n  sentinel:\n    masterName: original",
			env:  map[string]string{"CFG_REDIS_SENTINEL_MASTERNAME": "newmaster"},
			validate: func(r *require.Assertions, yaml string, err error) {
				r.NoError(err)
				r.Contains(yaml, "masterName: newmaster")
			},
		},
		{
			name: "case insensitive matching",
			yaml: `camelCaseField: original`,
			env:  map[string]string{"CFG_CAMELCASEFIELD": "updated"},
			validate: func(r *require.Assertions, yaml string, err error) {
				r.NoError(err)
				r.Contains(yaml, "camelCaseField: updated")
			},
		},
		{
			name: "field not in yaml returns error",
			yaml: `existingField: value`,
			env:  map[string]string{"CFG_NONEXISTENT": "value"},
			validate: func(r *require.Assertions, yaml string, err error) {
				r.Error(err)
				r.Contains(err.Error(), "CFG_NONEXISTENT")
				r.Contains(err.Error(), "no matching config field")
			},
		},
		{
			name: "typo in env var name returns error",
			yaml: `password: value`,
			env:  map[string]string{"CFG_PASWORD": "secret"}, // typo
			validate: func(r *require.Assertions, yaml string, err error) {
				r.Error(err)
				r.Contains(err.Error(), "CFG_PASWORD")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			m, err := readYAMLMap(strings.NewReader(tt.yaml))
			r.NoError(err)

			err = applyEnvOverrides(m, "CFG")

			var yamlOut string
			if err == nil {
				yamlOut = mapToString(m)
			}
			tt.validate(r, yamlOut, err)
		})
	}
}

func mapToString(m map[string]any) string {
	b, err := yaml.Marshal(m)
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return string(b)
}

func TestEnvOverrideIntegration(t *testing.T) {
	t.Run("duration from yaml", func(t *testing.T) {
		r := require.New(t)
		type conf struct {
			Timeout time.Duration `yaml:"timeout"`
		}
		var c conf
		err := Get(&c, Reader(strings.NewReader(`timeout: 5m`), "test"))
		r.NoError(err)
		r.Equal(5*time.Minute, c.Timeout)
	})

	t.Run("duration from env", func(t *testing.T) {
		r := require.New(t)
		t.Setenv("CFG_TIMEOUT", "30s")
		type conf struct {
			Timeout time.Duration `yaml:"timeout"`
		}
		var c conf
		err := Get(&c, Reader(strings.NewReader(`timeout: 5m`), "test"))
		r.NoError(err)
		r.Equal(30*time.Second, c.Timeout)
	})

	t.Run("int from env", func(t *testing.T) {
		r := require.New(t)
		t.Setenv("CFG_PORT", "8080")
		type conf struct {
			Port int `yaml:"port"`
		}
		var c conf
		err := Get(&c, Reader(strings.NewReader(`port: 3000`), "test"))
		r.NoError(err)
		r.Equal(8080, c.Port)
	})

	t.Run("bool from env", func(t *testing.T) {
		r := require.New(t)
		t.Setenv("CFG_ENABLED", "true")
		type conf struct {
			Enabled bool `yaml:"enabled"`
		}
		var c conf
		err := Get(&c, Reader(strings.NewReader(`enabled: false`), "test"))
		r.NoError(err)
		r.True(c.Enabled)
	})
}

func TestDeepMerge(t *testing.T) {
	tests := []struct {
		name string
		dst  map[string]any
		src  map[string]any
		want map[string]any
	}{
		{
			name: "simple override",
			dst:  map[string]any{"a": 1},
			src:  map[string]any{"a": 2},
			want: map[string]any{"a": 2},
		},
		{
			name: "add new key",
			dst:  map[string]any{"a": 1},
			src:  map[string]any{"b": 2},
			want: map[string]any{"a": 1, "b": 2},
		},
		{
			name: "nested merge",
			dst:  map[string]any{"a": map[string]any{"x": 1}},
			src:  map[string]any{"a": map[string]any{"y": 2}},
			want: map[string]any{"a": map[string]any{"x": 1, "y": 2}},
		},
		{
			name: "nested override",
			dst:  map[string]any{"a": map[string]any{"x": 1}},
			src:  map[string]any{"a": map[string]any{"x": 2}},
			want: map[string]any{"a": map[string]any{"x": 2}},
		},
		{
			name: "non-map overwrites map",
			dst:  map[string]any{"a": map[string]any{"x": 1}},
			src:  map[string]any{"a": "string"},
			want: map[string]any{"a": "string"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			got := deepMerge(tt.dst, tt.src)
			r.Equal(tt.want, got)
		})
	}
}

// TestDeepMergeHelmPattern tests the real-world Helm pattern:
// ConfigMap (addresses) + Secret (credentials) merged into single config.
func TestDeepMergeHelmPattern(t *testing.T) {
	r := require.New(t)
	base := `storage:
  main: mys3
  storages:
    mys3:
      type: S3`

	configmap := `storage:
  storages:
    mys3:
      address: s3.example.com
      provider: Ceph`

	secret := `storage:
  storages:
    mys3:
      credentials:
        user1:
          accessKeyID: mykey
          secretAccessKey: mysecret`

	type conf struct {
		Storage struct {
			Main     string                            `yaml:"main"`
			Storages map[string]map[string]interface{} `yaml:"storages"`
		} `yaml:"storage"`
	}

	var c conf
	err := Get(&c,
		Reader(strings.NewReader(base), "base"),
		Reader(strings.NewReader(configmap), "configmap"),
		Reader(strings.NewReader(secret), "secret"),
	)
	r.NoError(err)

	r.Equal("mys3", c.Storage.Main)

	mys3 := c.Storage.Storages["mys3"]
	r.NotNil(mys3)
	r.Equal("S3", mys3["type"])
	r.Equal("s3.example.com", mys3["address"])
	r.Equal("Ceph", mys3["provider"])

	creds := mys3["credentials"].(map[string]any)
	user1 := creds["user1"].(map[string]any)
	r.Equal("mykey", user1["accessKeyID"])
	r.Equal("mysecret", user1["secretAccessKey"])
}

// TestEnvOverrideFieldNotInYAML documents the fundamental limitation:
// env vars can only override keys that exist in YAML.
func TestEnvOverrideFieldNotInYAML(t *testing.T) {
	r := require.New(t)
	t.Setenv("CFG_MISSING", "value")

	type conf struct {
		Existing string `yaml:"existing"`
		Missing  string `yaml:"missing"` // in struct, not in YAML
	}

	var c conf
	err := Get(&c, Reader(strings.NewReader(`existing: value`), "test"))

	r.Error(err)
	r.Contains(err.Error(), "CFG_MISSING")
	r.Contains(err.Error(), "no matching config field")
}

// TestHelmChartRedisEnvOverrides tests Redis env var patterns used by the Chorus Helm chart.
// These are from deploy/chorus/templates/*.yaml and apply to pkg/config.Common.
func TestHelmChartRedisEnvOverrides(t *testing.T) {
	tests := []struct {
		name     string
		env      map[string]string
		validate func(r *require.Assertions, conf *Common)
	}{
		{
			name: "CFG_REDIS_PASSWORD",
			env:  map[string]string{"CFG_REDIS_PASSWORD": "secret-from-k8s"},
			validate: func(r *require.Assertions, conf *Common) {
				r.Equal("secret-from-k8s", conf.Redis.Password)
			},
		},
		{
			name: "CFG_REDIS_SENTINEL_MASTERNAME",
			env:  map[string]string{"CFG_REDIS_SENTINEL_MASTERNAME": "mymaster"},
			validate: func(r *require.Assertions, conf *Common) {
				r.Equal("mymaster", conf.Redis.Sentinel.MasterName)
			},
		},
		{
			name: "multiple redis env vars",
			env: map[string]string{
				"CFG_REDIS_PASSWORD":            "redis-secret",
				"CFG_REDIS_SENTINEL_MASTERNAME": "mymaster",
			},
			validate: func(r *require.Assertions, conf *Common) {
				r.Equal("redis-secret", conf.Redis.Password)
				r.Equal("mymaster", conf.Redis.Sentinel.MasterName)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			var conf Common
			err := Get(&conf)
			r.NoError(err)
			tt.validate(r, &conf)
		})
	}
}
