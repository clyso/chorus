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

package worker

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/config"
	"github.com/clyso/chorus/pkg/dom"
)

func TestGetConfig(t *testing.T) {
	r := require.New(t)
	const storagesOverride = `storage:
  main: my_s3_storage
  storages:
    my_s3_storage:
      type: S3
      provider: Ceph
      credentials:
        user1:
          accessKeyID: accesskeyuser1
          secretAccessKey: secretkeyuser1
        user2:
          accessKeyID: accesskeyuser2
          secretAccessKey: secretkeyuser2
      address: clyso.com
    my_swift_storage:
      type: SWIFT
      credentials:
        b6ebf758c9894224a105e5531eaa4ce9:
          username: username1
          password: password1
          domainName: domain1
          tenantName: tenant1
      storageEndpointName: swift
      authURL: http://auth.url/v3`

	res, err := GetConfig(config.Reader(strings.NewReader(storagesOverride), "test"))
	r.NoError(err)
	r.NoError(res.Validate())

	r.Equal("my_s3_storage", res.Storage.Main)
	r.Len(res.Storage.Storages, 2)

	s3Storage := res.Storage.Storages["my_s3_storage"]
	r.Equal(dom.S3, s3Storage.Type)
	r.Len(s3Storage.S3.Credentials, 2)

	swiftStorage := res.Storage.Storages["my_swift_storage"]
	r.Equal(dom.Swift, swiftStorage.Type)
	r.Len(swiftStorage.Swift.Credentials, 1)
}

func TestGetConfigDefaults(t *testing.T) {
	r := require.New(t)
	_, err := GetConfig()
	r.NoError(err)
}

// TestHelmEnvOverrides tests env vars from deploy/chorus/templates/worker/deployment-worker.yaml
func TestHelmEnvOverrides(t *testing.T) {
	const storagesOverride = `storage:
  main: s3
  storages:
    s3:
      type: S3
      provider: Ceph
      credentials:
        user1:
          accessKeyID: key
          secretAccessKey: secret
      address: s3.example.com`

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
