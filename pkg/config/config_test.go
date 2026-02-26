package config

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGet(t *testing.T) {
	r := require.New(t)
	var conf Common
	r.NoError(Get(&conf))

	r.Equal(9090, conf.Metrics.Port)
	r.Equal("info", conf.Log.Level)
	r.False(conf.Log.Json)
}

func TestOverride(t *testing.T) {
	r := require.New(t)
	var conf Common
	r.NoError(Get(&conf, Path("override_test.yaml")))

	r.Equal(69, conf.Metrics.Port)
	r.Equal("info", conf.Log.Level)
	r.True(conf.Log.Json)
	r.NotEmpty(conf.Redis.Address)
	r.Equal("user", conf.Redis.User)
	r.Equal("pass", conf.Redis.Password)
	r.Equal("sentinel", conf.Redis.Sentinel.User)
	r.Equal("sentinel-pass", conf.Redis.Sentinel.Password)
	r.Equal("master", conf.Redis.Sentinel.MasterName)
	r.True(conf.Redis.TLS.Enabled)
	r.True(conf.Redis.TLS.Insecure)
}

func TestOverrideMultiple(t *testing.T) {
	r := require.New(t)
	var conf Common
	r.NoError(Get(&conf, Path("override_test.yaml"), Path("override_test2.yaml")))
	r.NoError(conf.Validate())

	r.Equal(420, conf.Metrics.Port)
	r.Equal("info", conf.Log.Level)
	r.True(conf.Log.Json)
	r.NotEmpty(conf.Redis.Address)
	r.Empty(conf.Redis.Addresses)
	r.Equal([]string{conf.Redis.Address}, conf.Redis.GetAddresses())
}

func TestOverrideEnv(t *testing.T) {
	r := require.New(t)
	t.Setenv("CFG_METRICS_PORT", "55")
	t.Setenv("CFG_REDIS_PASSWORD", "secret")

	var conf Common
	r.NoError(Get(&conf, Path("override_test.yaml"), Path("override_test2.yaml")))
	r.NoError(conf.Validate())

	r.Equal(55, conf.Metrics.Port)
	r.Equal("secret", conf.Redis.Password)
}

func TestRedisValidate(t *testing.T) {
	tests := []struct {
		name      string
		address   string
		addresses []string
		wantErr   bool
	}{
		{"no address", "", nil, true},
		{"empty addresses", "", []string{}, true},
		{"only address", "addr", nil, false},
		{"only addresses", "", []string{"addr"}, false},
		{"both set", "addr", []string{"addr"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Redis{Address: tt.address, Addresses: tt.addresses}
			err := r.validate()
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRedisGetAddresses(t *testing.T) {
	tests := []struct {
		name      string
		address   string
		addresses []string
		want      []string
	}{
		{"only address", "a", nil, []string{"a"}},
		{"only addresses", "", []string{"a", "b"}, []string{"a", "b"}},
		{"both prefers addresses", "a", []string{"b", "c"}, []string{"b", "c"}},
		{"neither", "", nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Redis{Address: tt.address, Addresses: tt.addresses}
			require.Equal(t, tt.want, r.GetAddresses())
		})
	}
}

// TestCommonConfigYAMLCoverage verifies all struct fields have YAML defaults.
// This test fails if a developer adds a field without a config.yaml default.
// Env var overrides (CFG_*) only work for fields that exist in YAML.
func TestCommonConfigYAMLCoverage(t *testing.T) {
	r := require.New(t)
	data, err := configFile.Open("config.yaml")
	r.NoError(err)
	defer data.Close()

	yamlMap, err := readYAMLMap(data)
	r.NoError(err)

	missing := findMissingYAMLFields(reflect.TypeOf(Common{}), yamlMap, "")
	if len(missing) > 0 {
		t.Errorf("Config fields missing from config.yaml (env var overrides won't work):\n  %s\n\nAdd defaults to pkg/config/config.yaml",
			strings.Join(missing, "\n  "))
	}
}

func findMissingYAMLFields(t reflect.Type, m map[string]any, prefix string) []string {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}

	var missing []string
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		if field.Anonymous {
			missing = append(missing, findMissingYAMLFields(field.Type, m, prefix)...)
			continue
		}

		yamlTag := field.Tag.Get("yaml")
		if yamlTag == "-" {
			continue
		}

		yamlName := strings.Split(yamlTag, ",")[0]
		if yamlName == "" {
			yamlName = field.Name
		}

		fullPath := yamlName
		if prefix != "" {
			fullPath = prefix + "." + yamlName
		}

		val, exists := mapGetIgnoreCase(m, yamlName)
		if !exists {
			missing = append(missing, fullPath)
			continue
		}

		fieldType := field.Type
		if fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}
		if fieldType.Kind() == reflect.Struct {
			if nested, ok := val.(map[string]any); ok {
				missing = append(missing, findMissingYAMLFields(fieldType, nested, fullPath)...)
			}
		}
	}
	return missing
}

func mapGetIgnoreCase(m map[string]any, key string) (any, bool) {
	for k, v := range m {
		if strings.EqualFold(k, key) {
			return v, true
		}
	}
	return nil, false
}
