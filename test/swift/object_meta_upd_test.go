package swift

import (
	"strings"
	"testing"

	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/tasks"
	swift_worker "github.com/clyso/chorus/service/worker/handler/swift"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/containers"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
	"github.com/stretchr/testify/require"
)

func Test_handleObjectMetaUpdate(t *testing.T) {
	t.Skip()
	r := require.New(t)
	bucket, obj := "test-obj-meta-bucket", "test-obj-meta-object"

	// setup clients
	svc := swift_worker.New(nil, clients, nil, nil, nil, nil, nil, nil)
	swiftClient, err := clients.AsSwift(tstCtx, swiftTestKey, testAcc)
	r.NoError(err, "failed to get swift client for test account")
	cephClient, err := clients.AsSwift(tstCtx, cephTestKey, testAcc)
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

	task := tasks.SwiftObjectMetaUpdatePayload{
		Bucket: bucket,
		Object: obj,
	}
	task.SetReplicationID(entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
		User:        testAcc,
		FromStorage: swiftTestKey,
		ToStorage:   cephTestKey,
	}))
	// handler retuns error if object does not exist
	err = svc.ObjectMetaUpdate(tstCtx, task)
	r.NoError(err, "handleObjectMetaUpdate should return an error if object does not exist in ceph")

	// crate a test object in ceph
	oRes = objects.Create(tstCtx, cephClient, bucket, obj, objects.CreateOpts{
		ContentType: "text/plain",
		Content:     strings.NewReader("test-content"),
	})
	r.NoError(oRes.Err, "failed to create test object in ceph")
	// defer cleanup dest object in ceph
	defer func() {
		delRes := objects.Delete(tstCtx, cephClient, bucket, obj, objects.DeleteOpts{})
		r.NoError(delRes.Err, "failed to delete test object in ceph")
	}()

	// get object from ceph and check no meta
	res := objects.Get(tstCtx, cephClient, bucket, obj, objects.GetOpts{})
	r.NoError(res.Err, "failed to get test object in ceph")
	meta, err := res.ExtractMetadata()
	r.NoError(err, "failed to extract object info from ceph")
	r.Empty(meta, "object metadata in ceph should be empty")
	// sync object metadata from swift to ceph
	err = svc.ObjectMetaUpdate(tstCtx, task)
	r.NoError(err, "handleObjectMetaUpdate should not return an error")
	// check object metadata in ceph
	res = objects.Get(tstCtx, cephClient, bucket, obj, objects.GetOpts{})
	r.NoError(res.Err, "failed to get test object in ceph after metadata update")
	meta, err = res.ExtractMetadata()
	r.NoError(err, "failed to extract object info from ceph after metadata update")
	r.NotEmpty(meta, "object metadata in ceph should not be empty after metadata update")
	r.Equal("object-metadata", meta["Test"], "object metadata in ceph should match swift metadata after update")

	// delete object from swift
	delRes := objects.Delete(tstCtx, swiftClient, bucket, obj, objects.DeleteOpts{})
	r.NoError(delRes.Err, "failed to delete test object in swift")
	// sync object metadata from swift to ceph
	err = svc.ObjectMetaUpdate(tstCtx, task)
	// no error. task should be skipped
	r.NoError(err, "handleObjectMetaUpdate should not return an error if object was deleted from swift")
	// object still in ceph
	res = objects.Get(tstCtx, cephClient, bucket, obj, objects.GetOpts{})
	r.NoError(res.Err, "failed to get test object in ceph after metadata update")
}
