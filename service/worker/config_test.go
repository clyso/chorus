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
	const storagesOverride = `storage:
  main: my_s3_storage
  storages:
    my_s3_storage: 
      type: S3 
      rateLimit:
        enabled: false
        rpm: 60
      credentials:
        user1:
          accessKeyID: accesskeyuser1
          secretAccessKey: secretkeyuser1
        user2:
          accessKeyID: accesskeyuser2
          secretAccessKey: secretkeyuser2
      provider: Ceph
      isSecure: true #set false for http address
      address: clyso.com
      defaultRegion: ""
      healthCheckInterval: 5s
      httpTimeout: 10m0s
    my_swift_storage:
      type: SWIFT
      rateLimit:
        enabled: false
        rpm: 60
      credentials:
        b6ebf758c9894224a105e5531eaa4ce9:
          username: username1 # Equal to openstack OS_USERNAME
          password: password1 # Equal to openstack OS_PASSWORD
          domainName: domain1 # Equal to openstack OS_DOMAIN_NAME
          tenantName: tenant1 # Equal to openstack OS_TENANT_NAME
        38ylf758c9894224a105e5531edl834h:
          username: username2
          password: password2
          domainName: domain2
          tenantName: tenant2
      storageEndpointName: endpoint # endpoint name in keystone catalog
      storageEndpointType: object-store # endpoint type in keystone catalog
      region: "" # openstack region name
      authURL: http://auth.url/v3 # keystone auth url`
	confReader := strings.NewReader(storagesOverride)
	src := config.Reader(confReader, "test")

	r := require.New(t)
	res, err := GetConfig(src)
	r.NoError(err)
	t.Log(res.Storage.Storages)

	for k, v := range res.Storage.GetMain().S3.Credentials {
		t.Logf("S3 Credential Key: %s, Value: %+v", k, v)

	}
	r.NoError(res.Validate())
	storages := res.Storage
	r.Equal("my_s3_storage", storages.Main)
	r.Len(storages.Storages, 2)

	s3Storage, ok := storages.Storages["my_s3_storage"]
	r.True(ok)
	r.Equal(dom.S3, s3Storage.Type)
	r.NotNil(s3Storage.S3)
	r.Nil(s3Storage.Swift)
	r.Len(s3Storage.S3.Credentials, 2)
	_, ok = s3Storage.S3.Credentials["user1"]
	r.True(ok)
	_, ok = s3Storage.S3.Credentials["user2"]
	r.True(ok)

	swiftStorage, ok := storages.Storages["my_swift_storage"]
	r.True(ok)
	r.Equal(dom.Swift, swiftStorage.Type)
	r.NotNil(swiftStorage.Swift)
	r.Nil(swiftStorage.S3)
	r.Len(swiftStorage.Swift.Credentials, 2)
	_, ok = swiftStorage.Swift.Credentials["b6ebf758c9894224a105e5531eaa4ce9"]
	r.True(ok)
	_, ok = swiftStorage.Swift.Credentials["38ylf758c9894224a105e5531edl834h"]
	r.True(ok)
}
