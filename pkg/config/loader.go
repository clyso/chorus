/*
 * Copyright © 2023 Clyso GmbH
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

package config

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

func readYAMLMap(r io.Reader) (map[string]any, error) {
	var m map[string]any
	if err := yaml.NewDecoder(r).Decode(&m); err != nil {
		return nil, err
	}
	if m == nil {
		m = make(map[string]any)
	}
	return m, nil
}

func deepMerge(dst, src map[string]any) map[string]any {
	out := make(map[string]any, len(dst))
	for k, v := range dst {
		out[k] = v
	}
	for k, srcVal := range src {
		dstVal, exists := out[k]
		if exists {
			srcMap, srcOK := srcVal.(map[string]any)
			dstMap, dstOK := dstVal.(map[string]any)
			if srcOK && dstOK {
				out[k] = deepMerge(dstMap, srcMap)
				continue
			}
		}
		out[k] = srcVal
	}
	return out
}

// applyEnvOverrides sets values in the map from environment variables.
//
// Env var format: {PREFIX}_{PATH} where PATH segments are separated by underscore.
// Example: CFG_REDIS_SENTINEL_MASTERNAME sets map["redis"]["sentinel"]["masterName"]
//
// Matching: path segments match map keys case-insensitively.
// Type conversion: values are stored as strings; yaml.Unmarshal handles conversion later.
// Limitation: returns error if env var path doesn't match any existing YAML key.
func applyEnvOverrides(m map[string]any, prefix string) error {
	pfx := strings.ToUpper(prefix) + "_"
	for _, env := range os.Environ() {
		eqIdx := strings.IndexByte(env, '=')
		if eqIdx < 0 {
			continue
		}
		key := env[:eqIdx]
		val := env[eqIdx+1:]
		if !strings.HasPrefix(key, pfx) {
			continue
		}
		rest := key[len(pfx):]
		parts := strings.Split(rest, "_")
		if !setMapValue(m, parts, val) {
			return fmt.Errorf("env var %s: no matching config field found; ensure the field exists in YAML config", key)
		}
	}
	return nil
}

func setMapValue(m map[string]any, path []string, value string) bool {
	if len(path) == 0 || m == nil {
		return false
	}

	var foundKey string
	for k := range m {
		if strings.EqualFold(k, path[0]) {
			foundKey = k
			break
		}
	}

	if foundKey == "" {
		return false
	}

	if len(path) == 1 {
		m[foundKey] = convertToOriginalType(m[foundKey], value)
		return true
	}

	nested, ok := m[foundKey].(map[string]any)
	if !ok {
		return false
	}
	return setMapValue(nested, path[1:], value)
}

// convertToOriginalType converts value string to match the original value's type.
// This ensures yaml.Marshal/Unmarshal round-trip works correctly.
func convertToOriginalType(original any, value string) any {
	switch original.(type) {
	case int:
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	case int64:
		if i, err := strconv.ParseInt(value, 10, 64); err == nil {
			return i
		}
	case float64:
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return f
		}
	case bool:
		if b, err := strconv.ParseBool(value); err == nil {
			return b
		}
	}
	return value
}
