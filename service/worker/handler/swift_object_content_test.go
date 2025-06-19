package handler

import (
	"strings"
	"testing"
	"time"

	"github.com/clyso/chorus/pkg/swift"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/containers"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
	"github.com/stretchr/testify/require"
)

func Test_handleSwiftObjectContent(t *testing.T) {
	r := require.New(t)
	bucket, obj := "test-obj-cont-bucket", "test-obj-cont-object"

	// setup clients
	client, err := swift.New(swiftConf)
	r.NoError(err, "failed to create swift client")
	svc := &svc{swiftClients: client}
	swiftClient, err := client.For(tstCtx, swiftTestKey, testAcc)
	r.NoError(err, "failed to get swift client for test account")
	cephClient, err := client.For(tstCtx, cephTestKey, testAcc)
	r.NoError(err, "failed to get ceph client for test account")

	// create test container in swift
	cRes := containers.Create(tstCtx, swiftClient, bucket, containers.CreateOpts{
		Metadata: map[string]string{"test": "metadata"},
	})
	r.NoError(cRes.Err, "failed to create test bucket")
	// defer cleanup src container in swift
	defer func() {
		delRes := containers.Delete(tstCtx, swiftClient, bucket)
		r.NoError(delRes.Err, "failed to delete test bucket in swift")
	}()

	// create test object in swift
	oRes := objects.Create(tstCtx, swiftClient, bucket, obj, objects.CreateOpts{
		ContentType: "text/plain",
		Metadata:    map[string]string{"test": "object-metadata"},
		Content:     strings.NewReader("test-content"),
	})
	r.NoError(oRes.Err, "failed to create test object")
	// defer cleanup src object in swift
	defer func() {
		delRes := objects.Delete(tstCtx, swiftClient, bucket, obj, objects.DeleteOpts{})
		r.NoError(delRes.Err, "failed to delete test object in swift")
	}()

	objInfo, err := oRes.Extract()
	r.NoError(err, "failed to extract object info")

	// sync container to ceph
	err = svc.handleContainerUpdate(tstCtx, tasks.ContainerUpdatePayload{
		Sync: tasks.Sync{
			FromStorage: swiftTestKey,
			FromAccount: testAcc,
			ToStorage:   cephTestKey,
			ToAccount:   testAcc,
		},
		Bucket: bucket,
	})
	r.NoError(err, "handleContainerUpdate should not return an error")
	// defer cleanup dest container in ceph
	defer func() {
		delRes := containers.Delete(tstCtx, cephClient, bucket)
		r.NoError(delRes.Err, "failed to delete test bucket in ceph")
	}()

	// sync object to ceph
	err = svc.handleObjectUpdate(tstCtx, tasks.ObjectUpdatePayload{
		Sync: tasks.Sync{
			FromStorage: swiftTestKey,
			FromAccount: testAcc,
			ToStorage:   cephTestKey,
			ToAccount:   testAcc,
		},
		Bucket:       bucket,
		Object:       obj,
		VersionID:    "",
		Etag:         objInfo.ETag,
		LastModified: objInfo.LastModified.Format(time.RFC3339),
	})
	r.NoError(err, "handleObjectUpdate should not return an error")
	// defer cleanup dest object in ceph
	delRes := objects.Delete(tstCtx, cephClient, bucket, obj, objects.DeleteOpts{})
	r.NoError(delRes.Err, "failed to delete test object in ceph")
}
