package handler

import (
	"strings"
	"testing"

	"github.com/clyso/chorus/pkg/swift"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/containers"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
	"github.com/stretchr/testify/require"
)

func Test_handleObjectDelete(t *testing.T) {
	r := require.New(t)
	bucket, obj := "test-obj-delete", "test-obj-del"

	// setup clients
	client, err := swift.New(swiftConf)
	r.NoError(err, "failed to create swift client")
	svc := &svc{swiftClients: client}
	swiftClient, err := client.For(tstCtx, swiftTestKey, testAcc)
	r.NoError(err, "failed to get swift client for test account")
	cephClient, err := client.For(tstCtx, cephTestKey, testAcc)
	r.NoError(err, "failed to get ceph client for test account")

	// create test container in swift
	cRes := containers.Create(tstCtx, swiftClient, bucket, containers.CreateOpts{})
	r.NoError(cRes.Err, "failed to create test bucket")
	// defer cleanup src container in swift
	defer func() {
		delRes := containers.Delete(tstCtx, swiftClient, bucket)
		r.NoError(delRes.Err, "failed to delete test bucket in swift")
	}()
	// create test container in ceph
	cRes = containers.Create(tstCtx, cephClient, bucket, containers.CreateOpts{})
	r.NoError(cRes.Err, "failed to create test bucket in ceph")
	// defer cleanup dest container in ceph
	defer func() {
		delRes := containers.Delete(tstCtx, cephClient, bucket)
		r.NoError(delRes.Err, "failed to delete test bucket in ceph")
	}()

	// create test object in swift
	oRes := objects.Create(tstCtx, swiftClient, bucket, obj, objects.CreateOpts{
		ContentType: "text/plain",
		Metadata:    map[string]string{"test": "object-metadata"},
		Content:     strings.NewReader("test-content"),
	})
	r.NoError(oRes.Err, "failed to create test object")
	defer func() {
		_ = objects.Delete(tstCtx, swiftClient, bucket, obj, objects.DeleteOpts{})
	}()
	// create test object in ceph
	oRes = objects.Create(tstCtx, cephClient, bucket, obj, objects.CreateOpts{
		ContentType: "text/plain",
		Metadata:    map[string]string{"test": "object-metadata"},
		Content:     strings.NewReader("test-content"),
	})
	r.NoError(oRes.Err, "failed to create test object")
	defer func() {
		_ = objects.Delete(tstCtx, cephClient, bucket, obj, objects.DeleteOpts{})
	}()

	// error when src object not deleted
	err = svc.handleObjectDelete(tstCtx, tasks.ObjectDeletePayload{
		Sync: tasks.Sync{
			FromStorage: swiftTestKey,
			FromAccount: testAcc,
			ToStorage:   cephTestKey,
			ToAccount:   testAcc,
		},
		Bucket:          bucket,
		Object:          obj,
		VersionID:       "",
		Date:            "",
		DeleteMultipart: false,
	})
	r.NoError(err, "task ignored when source object not deleted")
	// object still exists in swift
	err = objects.Get(tstCtx, swiftClient, bucket, obj, objects.GetOpts{Newest: true}).Err
	r.NoError(err, "expected object to still exist in swift after task ignored")

	// no error when dest already deleted
	err = svc.handleObjectDelete(tstCtx, tasks.ObjectDeletePayload{
		Sync: tasks.Sync{
			FromStorage: swiftTestKey,
			FromAccount: testAcc,
			ToStorage:   cephTestKey,
			ToAccount:   testAcc,
		},
		Bucket:          bucket,
		Object:          "non-existent-obj",
		VersionID:       "",
		Date:            "",
		DeleteMultipart: false,
	})
	r.NoError(err, "expected no error when destination object already deleted")
	// delete object in swift
	err = objects.Delete(tstCtx, swiftClient, bucket, obj, objects.DeleteOpts{}).Err
	r.NoError(err, "failed to delete test object in swift")
	// sync object delete
	err = svc.handleObjectDelete(tstCtx, tasks.ObjectDeletePayload{
		Sync: tasks.Sync{
			FromStorage: swiftTestKey,
			FromAccount: testAcc,
			ToStorage:   cephTestKey,
			ToAccount:   testAcc,
		},
		Bucket:          bucket,
		Object:          obj,
		VersionID:       "",
		Date:            "",
		DeleteMultipart: false,
	})
	r.NoError(err, "failed to sync object delete")
	// check that object was deleted in ceph
	err = objects.Get(tstCtx, cephClient, bucket, obj, objects.GetOpts{Newest: true}).Err
	r.True(gophercloud.ResponseCodeIs(err, 404), "expected object to be deleted in ceph, got: %v", err)
	// todo test SLO
	// todo test version delete
}
