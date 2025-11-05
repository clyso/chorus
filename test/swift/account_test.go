// Copyright 2025 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package swift

import (
	"maps"
	"net/http"
	"slices"
	"testing"

	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/tasks"
	swift_worker "github.com/clyso/chorus/service/worker/handler/swift"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/accounts"
	"github.com/stretchr/testify/require"
)

func Test_handleAccountUpdate(t *testing.T) {
	t.Skip()
	r := require.New(t)

	// setup clients
	svc := swift_worker.New(nil, clients, nil, nil, nil, nil, nil, nil)
	swiftClient, err := clients.AsSwift(tstCtx, swiftTestKey, testAcc)
	r.NoError(err, "failed to get swift client for test account")
	cephClient, err := clients.AsSwift(tstCtx, cephTestKey, testAcc)
	r.NoError(err, "failed to get ceph client for test account")

	// check swift account
	res := accounts.Get(tstCtx, swiftClient, accounts.GetOpts{
		Newest: false,
	})
	if !gophercloud.ResponseCodeIs(res.Err, http.StatusOK) {
		r.NoError(res.Err, "failed to get swift account")
		meta, err := res.ExtractMetadata()
		r.NoError(err, "failed to extract metadata from swift account")
		r.Empty(meta, "swift account metadata should be empty")
		headers, err := res.Extract()
		r.NoError(err, "failed to extract headers from swift account")
		r.Empty(headers.TempURLKey, "swift account TempURLKey should be empty")
		r.Empty(headers.TempURLKey2, "swift account TempURLKey should be empty")
	}
	// check ceph account
	res = accounts.Get(tstCtx, cephClient, accounts.GetOpts{})
	r.NoError(res.Err, "failed to get ceph account")
	meta, err := res.ExtractMetadata()
	r.NoError(err, "failed to extract metadata from ceph account")
	// r.Empty(meta, "ceph account metadata should be empty")
	headers, err := res.Extract()
	r.NoError(err, "failed to extract headers from ceph account")
	r.Empty(headers.TempURLKey, "ceph account TempURLKey should be empty")
	r.Empty(headers.TempURLKey2, "ceph account TempURLKey should be empty")

	// update swift account
	metaKey, metaValue := "Test-Key", "test-value"
	tempURLKey, tempURLKey2 := "my-temp-url-key", "my-temp-url-key-2"
	updRes := accounts.Update(tstCtx, swiftClient, accounts.UpdateOpts{
		Metadata: map[string]string{
			metaKey: metaValue,
		},
		TempURLKey:  tempURLKey,
		TempURLKey2: tempURLKey2,
	})
	r.NoError(updRes.Err, "failed to update swift account metadata")

	// sync to ceph
	task := tasks.SwiftAccountUpdatePayload{}
	task.SetReplicationID(entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
		User:        testAcc,
		FromStorage: swiftTestKey,
		ToStorage:   cephTestKey,
	}))
	err = svc.AccountUpdate(tstCtx, task)
	r.NoError(err, "handleAccountUpdate should not return an error")

	// check swift account
	res = accounts.Get(tstCtx, swiftClient, accounts.GetOpts{})
	r.NoError(res.Err, "failed to get swift account")
	meta, err = res.ExtractMetadata()
	r.NoError(err, "failed to extract metadata from swift account")
	t.Log(meta)
	r.Len(meta, 3, "swift account metadata should be empty")
	r.Equal(metaValue, meta[metaKey], "swift account metadata should contain updated value")
	headers, err = res.Extract()
	r.NoError(err, "failed to extract headers from swift account")
	r.Equal(tempURLKey, headers.TempURLKey, "swift account TempURLKey should match updated value")
	r.Equal(tempURLKey2, headers.TempURLKey2, "swift account TempURLKey2 should match updated value")

	// check ceph account
	res = accounts.Get(tstCtx, cephClient, accounts.GetOpts{})
	r.NoError(res.Err, "failed to get ceph account")
	meta, err = res.ExtractMetadata()
	r.NoError(err, "failed to extract metadata from ceph account")
	r.Len(meta, 4, "ceph account metadata should contain updated value")
	r.Equal(metaValue, meta[metaKey], "ceph account metadata should contain updated value")
	headers, err = res.Extract()
	r.NoError(err, "failed to extract headers from ceph account")
	r.Equal(tempURLKey, headers.TempURLKey, "ceph account TempURLKey should match updated value")
	r.Equal(tempURLKey2, headers.TempURLKey2, "ceph account TempURLKey2 should match updated value")

	keys := slices.Collect(maps.Keys(meta))

	// cleanup swift account
	updRes = accounts.Update(tstCtx, swiftClient, accounts.UpdateOpts{
		RemoveMetadata: keys,
	})
	r.NoError(updRes.Err, "failed to update swift account metadata")
	// sync to ceph
	err = svc.AccountUpdate(tstCtx, task)
	r.NoError(err, "handleAccountUpdate should not return an error")

	// check swift account
	res = accounts.Get(tstCtx, swiftClient, accounts.GetOpts{})
	if !gophercloud.ResponseCodeIs(res.Err, http.StatusOK) {
		r.NoError(res.Err, "failed to get swift account")
		meta, err = res.ExtractMetadata()
		r.NoError(err, "failed to extract metadata from swift account")
		r.Empty(meta, "swift account metadata should be empty")
		headers, err = res.Extract()
		r.NoError(err, "failed to extract headers from swift account")
		r.Empty(headers.TempURLKey, "swift account TempURLKey should be empty")
		r.Empty(headers.TempURLKey2, "swift account TempURLKey should be empty")
	}
	// check ceph account
	res = accounts.Get(tstCtx, cephClient, accounts.GetOpts{})
	r.NoError(res.Err, "failed to get ceph account")
	meta, err = res.ExtractMetadata()
	r.NoError(err, "failed to extract metadata from ceph account")
	r.Len(meta, 1, "ceph account metadata should be empty")
	headers, err = res.Extract()
	r.NoError(err, "failed to extract headers from ceph account")
	r.Empty(headers.TempURLKey, "ceph account TempURLKey should be empty")
	r.Empty(headers.TempURLKey2, "ceph account TempURLKey should be empty")
}
