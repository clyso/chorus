package swift

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/service/worker/handler"
	swift_worker "github.com/clyso/chorus/service/worker/handler/swift"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/containers"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
	"github.com/stretchr/testify/require"
)

func Test_handleSwiftObjectContent(t *testing.T) {
	t.Skip()
	r := require.New(t)
	bucket, obj := "test-obj-cont-bucket", "test-obj-cont-object"

	// setup clients
	svc := swift_worker.New(&handler.Config{}, clients, nil, nil, nil, nil, nil, nil)
	swiftClient, err := clients.AsSwift(tstCtx, swiftTestKey, testAcc)
	r.NoError(err, "failed to get swift client for test account")
	cephClient, err := clients.AsSwift(tstCtx, cephTestKey, testAcc)
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
	task := tasks.SwiftContainerUpdatePayload{
		Bucket: bucket,
	}
	task.SetReplicationID(entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
		User:        testAcc,
		FromStorage: swiftTestKey,
		ToStorage:   cephTestKey,
	}))
	err = svc.ContainerUpdate(tstCtx, task)
	r.NoError(err, "handleContainerUpdate should not return an error")
	// defer cleanup dest container in ceph
	defer func() {
		_ = containers.Delete(tstCtx, cephClient, bucket)
		_ = objects.Delete(tstCtx, cephClient, bucket, obj, objects.DeleteOpts{})
	}()

	// sync object to ceph
	taskObj := tasks.SwiftObjectUpdatePayload{
		Bucket:       bucket,
		Object:       obj,
		VersionID:    "",
		Etag:         objInfo.ETag,
		LastModified: objInfo.LastModified.Format(time.RFC3339),
	}
	taskObj.SetReplicationID(task.ID)
	err = svc.ObjectUpdate(tstCtx, taskObj)
	r.NoError(err, "handleObjectUpdate should not return an error for new object")
	// compare ceph obj content
	objRes := objects.Download(tstCtx, cephClient, bucket, obj, objects.DownloadOpts{})
	r.NoError(objRes.Err, "failed to get object from ceph")
	objBytes, err := io.ReadAll(objRes.Body)
	r.NoError(err, "failed to read object content from ceph")
	r.Equal("test-content", string(objBytes), "object content in ceph should match swift object content")

	// update object in swift
	oRes = objects.Create(tstCtx, swiftClient, bucket, obj, objects.CreateOpts{
		ContentType: "text/plain",
		Metadata:    map[string]string{"test": "updated-object-metadata"},
		Content:     strings.NewReader("updated-test-content"),
	})
	r.NoError(oRes.Err, "failed to update test object in swift")
	// sync object update to ceph
	taskObjUpdated := taskObj
	taskObjUpdated.Etag = oRes.Header.Get("Etag")
	taskObjUpdated.LastModified = oRes.Header.Get("Last-Modified")
	err = svc.ObjectUpdate(tstCtx, taskObjUpdated)
	r.NoError(err, "handleObjectUpdate should not return an error for updated object")
	// compare ceph obj content after update
	objRes = objects.Download(tstCtx, cephClient, bucket, obj, objects.DownloadOpts{})
	r.NoError(objRes.Err, "failed to get updated object from ceph")
	objBytes, err = io.ReadAll(objRes.Body)
	r.NoError(err, "failed to read updated object content from ceph")
	r.Equal("updated-test-content", string(objBytes), "object content in ceph should match updated swift object content")
	// check if object metadata is synced
	cephObjRes := objects.Get(tstCtx, cephClient, bucket, obj, objects.GetOpts{})
	r.NoError(cephObjRes.Err, "failed to get object from ceph")
	meta, err := cephObjRes.ExtractMetadata()
	r.NoError(err, "failed to extract object info from ceph")
	r.Equal("updated-object-metadata", meta["Test"], "object metadata in ceph should match updated swift object metadata")

	t.Run("task skipped if obj not exist in src container", func(t *testing.T) {
		r := require.New(t)
		taskNonExist := taskObj
		taskNonExist.Object = "non-existing-object"
		// should not return an error
		err = svc.ObjectUpdate(tstCtx, taskNonExist)
		r.NoError(err, "handleObjectUpdate should not return an error if object does not exist in source container")
	})

	t.Run("task retried if src swift is not consistent", func(t *testing.T) {
		r := require.New(t)
		futureDate := objInfo.LastModified.Add(time.Hour * 24).Format(time.RFC3339)
		taskInconsistent := taskObj
		taskInconsistent.LastModified = futureDate
		taskInconsistent.Etag = objInfo.ETag
		err = svc.ObjectUpdate(tstCtx, taskInconsistent)
		r.Error(err)
		var rateLimitErr *dom.ErrRateLimitExceeded
		r.True(errors.As(err, &rateLimitErr), "handleObjectUpdate should return rate limit exceeded error if swift is not consistent")
	})

	// sync object to ceph
	taskObj.Etag = objInfo.ETag
	taskObj.LastModified = objInfo.LastModified.Format(time.RFC3339)
	err = svc.ObjectUpdate(tstCtx, taskObj)
	r.NoError(err, "handleObjectUpdate should not return an error")
	// defer cleanup dest object in ceph
	delRes := objects.Delete(tstCtx, cephClient, bucket, obj, objects.DeleteOpts{})
	r.NoError(delRes.Err, "failed to delete test object in ceph")
	t.Run("deleteAt", func(t *testing.T) {
		r := require.New(t)
		objName := "tst-delete-at-object"
		// create test object in swift
		oRes = objects.Create(tstCtx, swiftClient, bucket, objName, objects.CreateOpts{
			ContentType: "text/plain",
			Metadata:    map[string]string{"test": "object-metadata"},
			Content:     strings.NewReader("test-content"),
			DeleteAfter: 5, // delete in 5 seconds
		})
		r.NoError(oRes.Err, "failed to create test object with deleteAt")
		// defer cleanup src object in swift
		t.Cleanup(func() {
			_ = objects.Delete(tstCtx, swiftClient, bucket, objName, objects.DeleteOpts{})
		})
		// sync obj to ceph
		taskDeleteAt := taskObj
		taskDeleteAt.Object = objName
		taskDeleteAt.Etag = oRes.Header.Get("Etag")
		taskDeleteAt.LastModified = oRes.Header.Get("Last-Modified")

		err = svc.ObjectUpdate(tstCtx, taskDeleteAt)
		r.NoError(err, "handleObjectUpdate should not return an error for object with deleteAt")
		// check if object was created in ceph
		getRes := objects.Get(tstCtx, cephClient, bucket, objName, objects.GetOpts{})
		r.NoError(getRes.Err, "failed to get object from ceph")
		// check if object has deleteAt headers
		headers, err := getRes.Extract()
		r.NoError(err, "failed to extract object headers from ceph")
		r.False(headers.DeleteAt.IsZero(), "object in ceph should have deleteAt header set")
		r.True(headers.DeleteAt.After(time.Now()), "object in ceph should have deleteAt header set to future time")
		// before in 5 secconds
		r.True(headers.DeleteAt.Before(time.Now().Add(5*time.Second)), "object in ceph should have deleteAt header set to future time")
		r.Eventually(func() bool {
			// check if object was deleted from ceph
			getRes = objects.Get(tstCtx, cephClient, bucket, objName, objects.GetOpts{})
			return gophercloud.ResponseCodeIs(getRes.Err, http.StatusNotFound)
		}, 6*time.Second, 1*time.Second, "object in ceph should be deleted after deleteAt time")
	})
	t.Run("DLO", func(t *testing.T) {
		r := require.New(t)
		dloBucket := "tst-dlo-bucket"
		dloPref := "dlo-pref"
		dloObj := "dlo-obj"
		r.NoError(containers.Create(tstCtx, swiftClient, dloBucket, &containers.CreateOpts{}).Err)
		t.Cleanup(func() {
			_ = containers.Delete(tstCtx, swiftClient, dloBucket)
		})
		r.NoError(objects.Create(tstCtx, swiftClient, bucket, dloObj, &objects.CreateOpts{
			NoETag:         true,
			ObjectManifest: dloBucket + "/" + dloPref,
		}).Err)
		t.Cleanup(func() {
			_ = objects.Delete(tstCtx, swiftClient, bucket, dloObj, &objects.DeleteOpts{})
		})

		// sync obj to ceph
		taskDLO := taskObj
		taskDLO.Object = dloObj
		taskDLO.Etag = ""
		taskDLO.LastModified = time.Now().Format(time.RFC3339)
		err = svc.ObjectUpdate(tstCtx, taskDLO)
		r.NoError(err, "handleObjectUpdate should not return an error for DLO object")

		// get object from ceph and check if it has X-Object-Manifest header
		getRes := objects.Get(tstCtx, cephClient, bucket, dloObj, objects.GetOpts{})
		r.Error(getRes.Err, "failed to get DLO object from ceph")

		// create dlo bucket in ceph
		err = containers.Create(tstCtx, cephClient, dloBucket, &containers.CreateOpts{}).Err
		r.NoError(err, "failed to create DLO bucket in ceph")
		t.Cleanup(func() {
			_ = objects.Delete(tstCtx, cephClient, bucket, dloObj, &objects.DeleteOpts{})
			_ = containers.Delete(tstCtx, cephClient, dloBucket)
		})
		getRes = objects.Get(tstCtx, cephClient, bucket, dloObj, objects.GetOpts{})
		r.NoError(getRes.Err, "failed to get DLO object from ceph")
		headers, err := getRes.Extract()
		r.NoError(err)
		r.Equal(dloBucket+"/"+dloPref, headers.ObjectManifest, "DLO object in ceph should have correct ObjectManifest header")
	})

	t.Run("SLO", func(t *testing.T) {
		r := require.New(t)
		sloBucket := "tst-slo-bucket"
		sloObj := "tst-slo-object"
		sloPart1 := "tst-slo-part1"
		sloPart2 := "tst-slo-part2"
		// create slo bucket in swift
		r.NoError(containers.Create(tstCtx, swiftClient, sloBucket, &containers.CreateOpts{}).Err)
		t.Cleanup(func() {
			_ = containers.Delete(tstCtx, swiftClient, sloBucket)
		})
		// create slo parts in swift
		pt1Res := objects.Create(tstCtx, swiftClient, sloBucket, sloPart1, &objects.CreateOpts{
			ContentType: "text/plain",
			Content:     strings.NewReader("111")})
		r.NoError(pt1Res.Err)

		t.Cleanup(func() {
			_ = objects.Delete(tstCtx, swiftClient, sloBucket, sloPart1, &objects.DeleteOpts{})
		})
		pt2Res := objects.Create(tstCtx, swiftClient, sloBucket, sloPart2, &objects.CreateOpts{
			ContentType: "text/plain",
			Content:     strings.NewReader("222")})
		r.NoError(pt2Res.Err)
		t.Cleanup(func() {
			_ = objects.Delete(tstCtx, swiftClient, sloBucket, sloPart2, &objects.DeleteOpts{})
		})
		// create json reader with swift slo multipart json body:
		sloManifest := `[
				{
					"size_bytes": 3,
					"path": "` + sloBucket + "/" + sloPart1 + `"
				},
				{
					"size_bytes": 3,
					"path": "` + sloBucket + "/" + sloPart2 + `"
				}]`
		sloObjReader := strings.NewReader(sloManifest)
		// create slo object in swift
		r.NoError(objects.Create(tstCtx, swiftClient, bucket, sloObj, &objects.CreateOpts{
			Content:           sloObjReader,
			ContentType:       "application/json",
			ContentLength:     int64(len(sloManifest)),
			NoETag:            true,
			MultipartManifest: "put",
		}).Err)

		t.Cleanup(func() {
			_ = objects.Delete(tstCtx, swiftClient, bucket, sloObj, &objects.DeleteOpts{
				MultipartManifest: "delete",
			})
		})
		// read obj from swift
		objRes := objects.Get(tstCtx, swiftClient, bucket, sloObj, objects.GetOpts{})
		r.NoError(objRes.Err, "failed to get SLO object from swift")
		// sync slo object to ceph
		taskSLO := taskObj
		taskSLO.Object = sloObj
		taskSLO.Etag = objRes.Header.Get("Etag")
		taskSLO.LastModified = objRes.Header.Get("Last-Modified")
		err = svc.ObjectUpdate(tstCtx, taskSLO)
		r.Error(err)
		var rateLimitErr *dom.ErrRateLimitExceeded
		r.True(errors.As(err, &rateLimitErr), "handleObjectUpdate should return rate limit exceeded error if parts are not copied yet")

		// create slo bucket in ceph
		err = containers.Create(tstCtx, cephClient, sloBucket, &containers.CreateOpts{}).Err
		r.NoError(err, "failed to create SLO bucket in ceph")
		t.Cleanup(func() {
			_ = containers.Delete(tstCtx, cephClient, sloBucket)
		})
		//upload parts to ceph
		pt1Res = objects.Create(tstCtx, cephClient, sloBucket, sloPart1, &objects.CreateOpts{
			ContentType: "text/plain",
			Content:     strings.NewReader("111")})
		r.NoError(pt1Res.Err)
		t.Cleanup(func() {
			_ = objects.Delete(tstCtx, cephClient, sloBucket, sloPart1, &objects.DeleteOpts{})
		})
		taskSLO.Etag = objRes.Header.Get("Etag")
		taskSLO.LastModified = objRes.Header.Get("Last-Modified")
		err = svc.ObjectUpdate(tstCtx, taskSLO)
		r.Error(err)
		r.True(errors.As(err, &rateLimitErr), "handleObjectUpdate should return rate limit exceeded error if parts are not copied yet")

		pt2Res = objects.Create(tstCtx, cephClient, sloBucket, sloPart2, &objects.CreateOpts{
			ContentType: "text/plain",
			Content:     strings.NewReader("222")})
		r.NoError(pt2Res.Err)
		t.Cleanup(func() {
			_ = objects.Delete(tstCtx, cephClient, sloBucket, sloPart2, &objects.DeleteOpts{})
		})

		taskSLO.Etag = objRes.Header.Get("Etag")
		taskSLO.LastModified = objRes.Header.Get("Last-Modified")
		// should work now
		err = svc.ObjectUpdate(tstCtx, taskSLO)
		r.NoError(err)
		t.Cleanup(func() {
			_ = objects.Delete(tstCtx, cephClient, bucket, sloObj, &objects.DeleteOpts{MultipartManifest: "delete"})
		})

	})
}
