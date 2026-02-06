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

package agent

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetConfigDefaults(t *testing.T) {
	r := require.New(t)
	_, err := GetConfig()
	r.NoError(err)
}

// TestHelmEnvOverrides tests env vars from deploy/chorus/templates/agent/deployment-agent.yaml
func TestHelmEnvOverrides(t *testing.T) {
	t.Run("CFG_REDIS_PASSWORD", func(t *testing.T) {
		r := require.New(t)
		t.Setenv("CFG_REDIS_PASSWORD", "secret-from-helm")
		conf, err := GetConfig()
		r.NoError(err)
		r.Equal("secret-from-helm", conf.Redis.Password)
	})

	t.Run("CFG_REDIS_SENTINEL_MASTERNAME", func(t *testing.T) {
		r := require.New(t)
		t.Setenv("CFG_REDIS_SENTINEL_MASTERNAME", "mymaster")
		conf, err := GetConfig()
		r.NoError(err)
		r.Equal("mymaster", conf.Redis.Sentinel.MasterName)
	})
}
