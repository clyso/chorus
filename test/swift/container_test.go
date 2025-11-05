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
	"net/http"
	"strings"
	"testing"

	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/tasks"
	swift_worker "github.com/clyso/chorus/service/worker/handler/swift"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/accounts"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/containers"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
	"github.com/stretchr/testify/require"
)

func Test_handleContainerUpdate(t *testing.T) {
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
	}
	// check ceph account
	res = accounts.Get(tstCtx, cephClient, accounts.GetOpts{})
	r.NoError(res.Err, "failed to get ceph account")

	// create container with metadata in swift
	tstCont := "test-container"
	cRes := containers.Create(tstCtx, swiftClient, tstCont, containers.CreateOpts{
		Metadata: map[string]string{"test": "metadata"},
	})
	r.NoError(cRes.Err, "failed to create test container in swift")
	// defer cleanup src container in swift
	defer func() {
		_ = containers.Delete(tstCtx, swiftClient, tstCont)
	}()
	// sync container to ceph
	task := tasks.SwiftContainerUpdatePayload{
		Bucket: tstCont,
	}
	task.SetReplicationID(entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
		User:        testAcc,
		FromStorage: swiftTestKey,
		ToStorage:   cephTestKey,
	}))
	err = svc.ContainerUpdate(tstCtx, task)
	r.NoError(err, "handleContainerUpdate should not return an error")
	// check swift container
	cgRes := containers.Get(tstCtx, swiftClient, tstCont, containers.GetOpts{})
	r.NoError(cgRes.Err, "failed to get swift container")
	meta, err := cgRes.ExtractMetadata()
	r.NoError(err, "failed to extract metadata from swift container")
	r.Len(meta, 1, "swift container metadata should contain one item")
	r.Equal("metadata", meta["Test"], "swift container metadata should contain test value")
	// check ceph container
	cgRes = containers.Get(tstCtx, cephClient, tstCont, containers.GetOpts{})
	r.NoError(cgRes.Err, "failed to get ceph container")
	meta, err = cgRes.ExtractMetadata()
	r.NoError(err, "failed to extract metadata from ceph container")
	r.Len(meta, 1, "ceph container metadata should contain one item")
	r.Equal("metadata", meta["Test"], "ceph container metadata should contain test value")

	// update swift container metadata
	updRes := containers.Update(tstCtx, swiftClient, tstCont, containers.UpdateOpts{
		Metadata: map[string]string{
			"test2": "test-value",
		},
		RemoveMetadata: []string{"test"},
	})
	r.NoError(updRes.Err, "failed to update swift container metadata")
	// sync to ceph
	err = svc.ContainerUpdate(tstCtx, task)
	r.NoError(err, "handleContainerUpdate should not return an error")
	// check swift container
	cgRes = containers.Get(tstCtx, swiftClient, tstCont, containers.GetOpts{})
	r.NoError(cgRes.Err, "failed to get swift container")
	meta, err = cgRes.ExtractMetadata()
	r.NoError(err, "failed to extract metadata from swift container")
	r.Len(meta, 1, "swift container metadata should contain one item")
	r.Equal("test-value", meta["Test2"], "swift container metadata should contain updated value")
	// check ceph container
	cgRes = containers.Get(tstCtx, cephClient, tstCont, containers.GetOpts{})
	r.NoError(cgRes.Err, "failed to get ceph container")
	meta, err = cgRes.ExtractMetadata()
	r.NoError(err, "failed to extract metadata from ceph container")
	r.Len(meta, 1, "ceph container metadata should contain one item")
	r.Equal("test-value", meta["Test2"], "ceph container metadata should contain updated value")

	// delete swift container
	delRes := containers.Delete(tstCtx, swiftClient, tstCont)
	r.NoError(delRes.Err, "failed to delete swift container")
	// sync to ceph
	err = svc.ContainerUpdate(tstCtx, task)
	r.NoError(err, "handleContainerUpdate should not return an error")
	// check swift container
	cgRes = containers.Get(tstCtx, swiftClient, tstCont, containers.GetOpts{})
	if !gophercloud.ResponseCodeIs(cgRes.Err, http.StatusNotFound) {
		r.NoError(cgRes.Err, "failed to get swift container")
	}
	// check ceph container
	cgRes = containers.Get(tstCtx, cephClient, tstCont, containers.GetOpts{})
	if !gophercloud.ResponseCodeIs(cgRes.Err, http.StatusNotFound) {
		r.NoError(cgRes.Err, "failed to get ceph container")
	}

	t.Run("versions_enabled", func(t *testing.T) {
		t.Skip("")
		r := require.New(t)
		versionsCont := "test-versions-container"
		// create container with versions enabled in swift
		cRes := containers.Create(tstCtx, swiftClient, versionsCont, containers.CreateOpts{
			VersionsEnabled: true,
		})
		r.NoError(cRes.Err, "failed to create test versions container in swift")
		// defer cleanup src container in swift
		defer func() {
			_ = containers.Delete(tstCtx, swiftClient, versionsCont)
		}()
		// sync container to ceph
		taskVer := task
		taskVer.Bucket = versionsCont
		err = svc.ContainerUpdate(tstCtx, taskVer)
		r.NoError(err, "handleContainerUpdate should not return an error")
		// check swift container
		cgRes = containers.Get(tstCtx, swiftClient, versionsCont, containers.GetOpts{})
		r.NoError(cgRes.Err, "failed to get swift container")
		headers, err := cgRes.Extract()
		r.NoError(err, "failed to extract headers from swift container")
		r.True(headers.VersionsEnabled, "swift container should have versions enabled")
		// check ceph container
		cgRes = containers.Get(tstCtx, cephClient, versionsCont, containers.GetOpts{})
		r.NoError(cgRes.Err, "failed to get ceph container")
		headers, err = cgRes.Extract()
		r.NoError(err, "failed to extract headers from ceph container")
		// this one fails, because ceph does not support swift custom versioning middleware
		// ceph returns false
		// r.True(headers.VersionsEnabled, "ceph container should have versions enabled")

		obj := "test-versions-object"
		// create test object in swift
		oRes := objects.Create(tstCtx, swiftClient, versionsCont, obj, objects.CreateOpts{
			ContentType: "text/plain",
			Metadata:    map[string]string{"test": "object-metadata"},
			Content:     strings.NewReader("test-content"),
		})
		r.NoError(oRes.Err, "failed to create test object")

		oRes = objects.Create(tstCtx, swiftClient, versionsCont, obj, objects.CreateOpts{
			ContentType: "text/plain",
			Metadata:    map[string]string{"test": "object-metadata"},
			Content:     strings.NewReader("test-content-updated"),
		})
		r.NoError(oRes.Err, "failed to create test object")

		// list all objects with versions in swift container
		ol := objects.List(swiftClient, versionsCont, objects.ListOpts{
			Limit:    100,
			Versions: true,
		})
		r.NoError(ol.Err, "failed to list objects in swift container with versions enabled")
		lp, err := ol.AllPages(tstCtx)
		r.NoError(err, "failed to get all pages of objects in swift container with versions enabled")
		objectsList, err := objects.ExtractInfo(lp)
		r.NoError(err, "failed to extract objects from swift container with versions enabled")
		r.Len(objectsList, 2, "swift container should have 2 versions of the object")
		for _, o := range objectsList {
			t.Log("swift", o.Name, o.VersionID)
		}
		// create test object in ceph
		oRes = objects.Create(tstCtx, cephClient, versionsCont, obj, objects.CreateOpts{
			ContentType: "text/plain",
			Metadata:    map[string]string{"test": "object-metadata"},
			Content:     strings.NewReader("test-content"),
		})
		r.NoError(oRes.Err, "failed to create test object")

		oRes = objects.Create(tstCtx, cephClient, versionsCont, obj, objects.CreateOpts{
			ContentType: "text/plain",
			Metadata:    map[string]string{"test": "object-metadata"},
			Content:     strings.NewReader("test-content-updated"),
		})
		r.NoError(oRes.Err, "failed to create test object")
		// list all objects with versions in ceph container
		ol = objects.List(cephClient, versionsCont, objects.ListOpts{
			Limit:    100,
			Versions: true,
		})
		r.NoError(ol.Err, "failed to list objects in swift container with versions enabled")
		lp, err = ol.AllPages(tstCtx)
		r.NoError(err, "failed to get all pages of objects in swift container with versions enabled")
		objectsList, err = objects.ExtractInfo(lp)
		r.NoError(err, "failed to extract objects from swift container with versions enabled")
		// this one fails, because ceph does not support swift custom versioning middleware
		// ceph returns 1 object
		r.Len(objectsList, 2, "swift container should have 2 versions of the object")
		for _, o := range objectsList {
			t.Log("ceph", o.Name, o.VersionID)
		}

	})
}
