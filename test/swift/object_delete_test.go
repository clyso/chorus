package swift

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/tasks"
	swift_worker "github.com/clyso/chorus/service/worker/handler/swift"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/containers"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
	"github.com/stretchr/testify/require"
)

func Test_handleObjectDelete(t *testing.T) {
	t.Skip()
	r := require.New(t)
	bucket, obj := "test-obj-delete", "test-obj-del"

	// setup clients
	svc := swift_worker.New(nil, clients, nil, nil, nil, nil, nil, nil, nil)
	swiftClient, err := clients.AsSwift(tstCtx, swiftTestKey, testAcc)
	r.NoError(err, "failed to get swift client for test account")
	cephClient, err := clients.AsSwift(tstCtx, cephTestKey, testAcc)
	r.NoError(err, "failed to get ceph client for test account")

	// create test container in swift
	cRes := containers.Create(tstCtx, swiftClient, bucket, containers.CreateOpts{})
	r.NoError(cRes.Err, "failed to create test bucket")
	defer func() {
		delRes := containers.Delete(tstCtx, swiftClient, bucket)
		r.NoError(delRes.Err, "failed to delete test bucket in swift")
	}()
	// create test container in ceph
	cRes = containers.Create(tstCtx, cephClient, bucket, containers.CreateOpts{})
	r.NoError(cRes.Err, "failed to create test bucket in ceph")
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

	// task ignored when src object not deleted
	task := tasks.SwiftObjectDeletePayload{
		Bucket:          bucket,
		Object:          obj,
		VersionID:       "",
		Date:            "",
		DeleteMultipart: false,
	}
	task.SetReplicationID(entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
		User:        testAcc,
		FromStorage: swiftTestKey,
		ToStorage:   cephTestKey,
	}))
	err = svc.ObjectDelete(tstCtx, task)
	r.NoError(err, "task ignored when source object not deleted")
	// object still exists in swift
	err = objects.Get(tstCtx, swiftClient, bucket, obj, objects.GetOpts{Newest: true}).Err
	r.NoError(err, "expected object to still exist in swift after task ignored")

	taskDeleted := task
	taskDeleted.Object = "non-existent-obj"
	// no error when dest already deleted
	err = svc.ObjectDelete(tstCtx, taskDeleted)
	r.NoError(err, "expected no error when destination object already deleted")
	// delete object in swift
	err = objects.Delete(tstCtx, swiftClient, bucket, obj, objects.DeleteOpts{}).Err
	r.NoError(err, "failed to delete test object in swift")
	// sync object delete
	err = svc.ObjectDelete(tstCtx, task)
	r.NoError(err, "failed to sync object delete")
	// check that object was deleted in ceph
	err = objects.Get(tstCtx, cephClient, bucket, obj, objects.GetOpts{Newest: true}).Err
	r.True(gophercloud.ResponseCodeIs(err, 404), "expected object to be deleted in ceph, got: %v", err)
	// todo test SLO
	// todo test version delete
}

type sloPart struct {
	Path      string `json:"path"`
	SizeBytes int    `json:"size_bytes"`
}

func Test_handleObjectDeleteMultipart(t *testing.T) {
	r := require.New(t)
	bucket, sloBucket, obj := "test-obj-delete-multipart", "tst-delete-slo", "test-obj-del-multipart"

	// setup clients
	svc := swift_worker.New(nil, clients, nil, nil, nil, nil, nil, nil, nil)
	cephClient, err := clients.AsSwift(tstCtx, cephTestKey, testAcc)
	r.NoError(err, "failed to get ceph client for test account")

	// create test container in ceph
	cRes := containers.Create(tstCtx, cephClient, bucket, containers.CreateOpts{})
	r.NoError(cRes.Err, "failed to create test bucket in ceph")
	defer func() {
		delRes := containers.Delete(tstCtx, cephClient, bucket)
		r.NoError(delRes.Err, "failed to delete test bucket in ceph")
	}()
	cRes = containers.Create(tstCtx, cephClient, sloBucket, containers.CreateOpts{})
	r.NoError(cRes.Err, "failed to create test bucket in ceph")
	defer func() {
		delRes := containers.Delete(tstCtx, cephClient, sloBucket)
		r.NoError(delRes.Err, "failed to delete test bucket in ceph")
	}()

	// upload SLO object parts to ceph
	parts := []string{"part1", "part2"}
	manifestParts := make([]sloPart, 0, len(parts))
	for _, part := range parts {
		partObj := obj + "-" + part
		content := "cont-" + part
		oRes := objects.Create(tstCtx, cephClient, sloBucket, partObj, objects.CreateOpts{
			ContentType: "text/plain",
			Content:     strings.NewReader(content),
		})
		r.NoError(oRes.Err, "failed to create test object part in ceph")
		err = objects.Get(tstCtx, cephClient, sloBucket, partObj, objects.GetOpts{}).Err
		r.NoError(err, "expected part object to exist in ceph: %s", partObj)
		manifestParts = append(manifestParts, sloPart{
			Path:      sloBucket + "/" + partObj,
			SizeBytes: len(content),
		})
	}
	jsonParts, err := json.Marshal(manifestParts)
	r.NoError(err, "failed to marshal SLO manifest parts to JSON")

	// upload SLO manifest json to ceph
	err = objects.Create(tstCtx, cephClient, bucket, obj, objects.CreateOpts{
		ContentType:       "application/json",
		Content:           bytes.NewReader(jsonParts),
		ContentLength:     int64(len(jsonParts)),
		NoETag:            true,
		MultipartManifest: "put",
	}).Err
	r.NoError(err, "failed to create SLO manifest object in ceph")
	// check that slo and parts exist
	for _, part := range parts {
		partObj := obj + "-" + part
		err = objects.Get(tstCtx, cephClient, sloBucket, partObj, objects.GetOpts{Newest: true}).Err
		r.NoError(err, "expected part object to exist in ceph: %s", partObj)
	}
	err = objects.Get(tstCtx, cephClient, bucket, obj, objects.GetOpts{Newest: true}).Err
	r.NoError(err, "expected SLO manifest object to exist in ceph: %s", obj)

	// sync object delete without multipart delete
	task := tasks.SwiftObjectDeletePayload{
		Bucket:          bucket,
		Object:          obj,
		DeleteMultipart: false,
	}
	task.SetReplicationID(entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
		User:        testAcc,
		FromStorage: swiftTestKey,
		ToStorage:   cephTestKey,
	}))
	err = svc.ObjectDelete(tstCtx, task)
	r.NoError(err, "failed to sync object delete with multipart delete")
	// check that SLO manifest was deleted, but parts still exist
	err = objects.Get(tstCtx, cephClient, bucket, obj, objects.GetOpts{Newest: true}).Err
	r.True(gophercloud.ResponseCodeIs(err, 404), "expected SLO manifest object to be deleted in ceph, got: %v", err)
	for _, part := range parts {
		partObj := obj + "-" + part
		err = objects.Get(tstCtx, cephClient, sloBucket, partObj, objects.GetOpts{Newest: true}).Err
		r.NoError(err, "expected part object to still exist in ceph: %s", partObj)
	}

	// revert slo manifest back
	err = objects.Create(tstCtx, cephClient, bucket, obj, objects.CreateOpts{
		ContentType:       "application/json",
		Content:           bytes.NewReader(jsonParts),
		ContentLength:     int64(len(jsonParts)),
		NoETag:            true,
		MultipartManifest: "put",
	}).Err
	r.NoError(err, "failed to recreate SLO manifest object in ceph")

	taskMultipart := task
	taskMultipart.DeleteMultipart = true
	// sync object delete with multipart delete
	err = svc.ObjectDelete(tstCtx, taskMultipart)
	r.NoError(err, "failed to sync object delete with multipart delete")
	// check that SLO manifest and parts were deleted
	err = objects.Get(tstCtx, cephClient, bucket, obj, objects.GetOpts{Newest: true}).Err
	r.True(gophercloud.ResponseCodeIs(err, 404), "expected SLO manifest object to be deleted in ceph, got: %v", err)
	for _, part := range parts {
		partObj := obj + "-" + part
		err = objects.Get(tstCtx, cephClient, sloBucket, partObj, objects.GetOpts{Newest: true}).Err
		r.True(gophercloud.ResponseCodeIs(err, 404), "expected part object to be deleted in ceph: %s, got: %v", partObj, err)
	}
}
