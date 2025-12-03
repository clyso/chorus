package swift

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/clyso/chorus/pkg/swift"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/service/proxy"
	"github.com/clyso/chorus/service/worker"
	"github.com/clyso/chorus/test/app"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/accounts"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/containers"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

func Test_e2e(t *testing.T) {
	r := require.New(t)
	ctx := t.Context()

	// setup clients
	swiftClient, err := clients.AsSwift(ctx, swiftTestKey, testAcc)
	r.NoError(err, "failed to get swift client for test account")
	cephClient, err := clients.AsSwift(ctx, cephTestKey, testAcc)
	r.NoError(err, "failed to get ceph client for test account")

	// check swift account
	res := accounts.Get(ctx, swiftClient, accounts.GetOpts{
		Newest: false,
	})
	if !gophercloud.ResponseCodeIs(res.Err, http.StatusOK) {
		r.NoError(res.Err, "failed to get swift account")
	}
	// check ceph account
	res = accounts.Get(ctx, cephClient, accounts.GetOpts{})
	r.NoError(res.Err, "failed to get ceph account")

	workerConf, err := worker.GetConfig()
	r.NoError(err, "failed to get worker config")
	workerConf.RClone.MemoryLimit.Enabled = false
	workerConf.RClone.LocalFileLimit.Enabled = false
	workerConf.RClone.GlobalFileLimit.Enabled = false
	workerConf.Worker.QueueUpdateInterval = 500 * time.Millisecond
	workerConf.Worker.SwitchRetryInterval = time.Millisecond * 500
	workerConf.Worker.PauseRetryInterval = time.Millisecond * 500
	workerConf.Log.Level = "warn"
	workerConf.Storage = swiftConf

	proxyConf, err := proxy.GetConfig()
	r.NoError(err, "failed to get proxy config")
	proxyConf.Log.Level = "warn"
	proxyConf.Storage = proxyStorages

	// ----------------------------------------------- start chorus -----------------------------------------------
	e := app.SetupChorus(t, workerConf, proxyConf)
	chorusStorages, err := e.ChorusClient.GetStorages(ctx, &emptypb.Empty{})
	r.NoError(err, "failed to get storages from api")
	t.Log(chorusStorages.Storages)

	// register chorus proxy endpoint in keystone as swift endpoint
	registerSwiftEndpoitnt(t, "chorus-swift", "localhost", e.ProxyPort)

	// create proxy client
	c := *swiftConf.Storages[swiftTestKey].Swift
	c.StorageEndpointName = "chorus-swift"

	proxyClientSvc, err := swift.New(map[string]*swift.Storage{
		"chorus-swift": &c,
	})
	proxyClient, err := proxyClientSvc.For(ctx, "chorus-swift", testAcc)
	r.NoError(err, "failed to create swift client")

	// create container with metadata in proxy
	tstCont := "chorus-container"
	cRes := containers.Create(ctx, proxyClient, tstCont, containers.CreateOpts{
		Metadata: map[string]string{"test": "chorus-metadata"},
	})
	r.NoError(cRes.Err, "failed to create test container in swift")

	// get container from proxy and check metadata
	gRes := containers.Get(ctx, proxyClient, tstCont, containers.GetOpts{})
	r.NoError(gRes.Err, "failed to get container from proxy")
	r.Equal("chorus-metadata", gRes.Header.Get("X-Container-Meta-Test"), "container metadata should match")

	// get container from swift and check metadata
	gRes = containers.Get(ctx, swiftClient, tstCont, containers.GetOpts{})
	r.NoError(gRes.Err, "failed to get container from swift")
	r.Equal("chorus-metadata", gRes.Header.Get("X-Container-Meta-Test"), "container metadata should match")

	// container should not exists in ceph yet
	gRes = containers.Get(ctx, cephClient, tstCont, containers.GetOpts{})
	r.True(gophercloud.ResponseCodeIs(gRes.Err, http.StatusNotFound), "container should not exists in ceph yet")

	// upload 2 objects to proxy
	obj1Content := "this is object 1"
	obj1Name := "obj1.txt"
	obj1Res := objects.Create(ctx, proxyClient, tstCont, obj1Name, objects.CreateOpts{
		Content:     strings.NewReader(obj1Content),
		Metadata:    map[string]string{"test": "obj1-metadata"},
		ContentType: "text/plain",
	})
	r.NoError(obj1Res.Err, "failed to upload object 1 to proxy")

	obj2Content := "this is object 2"
	obj2Name := "dir/obj2.txt"
	obj2Res := objects.Create(ctx, proxyClient, tstCont, obj2Name, objects.CreateOpts{
		Content:     strings.NewReader(obj2Content),
		Metadata:    map[string]string{"test": "obj2-metadata"},
		ContentType: "text/plain",
	})
	r.NoError(obj2Res.Err, "failed to upload object 2 to proxy")

	// read obj1 from proxy and check content and metadata
	obj1GetRes := objects.Download(ctx, proxyClient, tstCont, obj1Name, objects.DownloadOpts{})
	r.NoError(obj1GetRes.Err, "failed to get object 1 from proxy")
	obj1Data, err := obj1GetRes.ExtractContent()
	r.NoError(err, "failed to read object 1 content from proxy")
	r.Equal(obj1Content, string(obj1Data), "object 1 content should match")
	r.Equal("obj1-metadata", obj1GetRes.Header.Get("X-Object-Meta-Test"), "object 1 metadata should match")

	// ----------------------------------------------- start migration -----------------------------------------------
	replID := &pb.ReplicationID{
		FromStorage: swiftTestKey,
		ToStorage:   cephTestKey,
		User:        testAcc,
	}
	_, err = e.PolicyClient.AddReplication(ctx, &pb.AddReplicationRequest{
		Id: replID,
	})
	r.NoError(err, "failed to start account migration")

	replList, err := e.PolicyClient.ListReplications(ctx, &pb.ListReplicationsRequest{})
	r.NoError(err, "failed to list replications")
	r.Len(replList.Replications, 1, "there should be one replication")
	r.Equal(testAcc, replList.Replications[0].Id.User)
	r.Equal(swiftTestKey, replList.Replications[0].Id.FromStorage)
	r.Equal(cephTestKey, replList.Replications[0].Id.ToStorage)

	repl, err := e.PolicyClient.GetReplication(ctx, replID)
	r.NoError(err, "failed to get replication")
	r.Equal(testAcc, repl.Id.User)
	r.Equal(swiftTestKey, repl.Id.FromStorage)
	r.Equal(cephTestKey, repl.Id.ToStorage)

	// create second container in proxy
	tstCont2 := "chorus-container-2"
	cRes = containers.Create(ctx, proxyClient, tstCont2, containers.CreateOpts{
		Metadata: map[string]string{"test": "chorus-metadata-2"},
	})
	r.NoError(cRes.Err, "failed to create test container 2 in proxy")

	// upload object to second container in proxy
	obj3Content := "this is object 3 in container 2"
	obj3Name := "obj3.txt"
	obj3Res := objects.Create(ctx, proxyClient, tstCont2, obj3Name, objects.CreateOpts{
		Content:     strings.NewReader(obj3Content),
		Metadata:    map[string]string{"test": "obj3-metadata"},
		ContentType: "text/plain",
	})
	r.NoError(obj3Res.Err, "failed to upload object 3 to proxy")

	// upload third objct to first container in proxy
	obj4Content := "this is object 4 in container 1"
	obj4Name := "obj4.txt"
	obj4Res := objects.Create(ctx, proxyClient, tstCont, obj4Name, objects.CreateOpts{
		Content:     strings.NewReader(obj4Content),
		Metadata:    map[string]string{"test": "obj4-metadata"},
		ContentType: "text/plain",
	})
	r.NoError(obj4Res.Err, "failed to upload object 4 to proxy")

	// delete first object in proxy
	delObj1Res := objects.Delete(ctx, proxyClient, tstCont, obj1Name, objects.DeleteOpts{})
	r.NoError(delObj1Res.Err, "failed to delete object 1 in proxy")

	// update second object payload and meta in proxy
	obj2UpdContent := "this is the updated object 2"
	obj2UpdRes := objects.Create(ctx, proxyClient, tstCont, obj2Name, objects.CreateOpts{
		Content:     strings.NewReader(obj2UpdContent),
		Metadata:    map[string]string{"test": "obj2-updated-metadata"},
		ContentType: "text/plain",
	})
	r.NoError(obj2UpdRes.Err, "failed to update object 2 in proxy")

	// wait for init replication to finish
	r.Eventually(func() bool {
		repl, err = e.PolicyClient.GetReplication(ctx, replID)
		if err != nil {
			return false
		}
		return repl.IsInitDone
	}, e.WaitLong, e.RetryLong)

	// check that containers exists in ceph
	gRes = containers.Get(ctx, cephClient, tstCont, containers.GetOpts{})
	r.NoError(gRes.Err, "failed to get container 1 from ceph after replication")
	gRes = containers.Get(ctx, cephClient, tstCont2, containers.GetOpts{})
	r.NoError(gRes.Err, "failed to get container 2 from ceph after replication")

	// ----------------------------------------------- finish migration -----------------------------------------------
	r.Eventually(func() bool {
		repl, err = e.PolicyClient.GetReplication(ctx, replID)
		if err != nil {
			return false
		}
		return repl.IsInitDone &&
			repl.Events != 0 &&
			repl.EventsDone == repl.Events
	}, e.WaitLong, e.RetryLong)

	// check container 1 in swift
	// should have second object updated and third object added and first object deleted
	// get fist object - should be deleted
	obj1GetRes = objects.Download(ctx, swiftClient, tstCont, obj1Name, objects.DownloadOpts{})
	r.True(gophercloud.ResponseCodeIs(obj1GetRes.Err, http.StatusNotFound))
	// get second object - should be updated
	obj2GetRes := objects.Download(ctx, swiftClient, tstCont, obj2Name, objects.DownloadOpts{})
	r.NoError(obj2GetRes.Err, "failed to get object 2 from swift")
	obj2Data, err := obj2GetRes.ExtractContent()
	r.NoError(err, "failed to read object 2 content from swift")
	r.Equal(obj2UpdContent, string(obj2Data), "object 2 content should match")
	r.Equal("obj2-updated-metadata", obj2GetRes.Header.Get("X-Object-Meta-Test"), "object 2 metadata should match")
	// get third object - should be present
	obj3GetRes := objects.Download(ctx, swiftClient, tstCont, obj4Name, objects.DownloadOpts{})
	r.NoError(obj3GetRes.Err, "failed to get object 4 from swift")
	obj3Data, err := obj3GetRes.ExtractContent()
	r.NoError(err, "failed to read object 4 content from swift")
	r.Equal(obj4Content, string(obj3Data), "object 4 content should match")
	r.Equal("obj4-metadata", obj3GetRes.Header.Get("X-Object-Meta-Test"), "object 4 metadata should match")

	// check container 1 in ceph
	// should have second object updated and third object added and first object deleted
	// get container metadata and check
	cgRes := containers.Get(ctx, cephClient, tstCont, containers.GetOpts{})
	r.NoError(cgRes.Err, "failed to get ceph container")
	r.Equal("chorus-metadata", cgRes.Header.Get("X-Container-Meta-Test"), "ceph container metadata should match")
	// get fist object - should be deleted
	obj1GetRes = objects.Download(ctx, cephClient, tstCont, obj1Name, objects.DownloadOpts{})
	r.True(gophercloud.ResponseCodeIs(obj1GetRes.Err, http.StatusNotFound), "object 1 should be deleted in ceph")
	// get second object - should be updated
	obj2GetRes = objects.Download(ctx, cephClient, tstCont, obj2Name, objects.DownloadOpts{})
	r.NoError(obj2GetRes.Err, "failed to get object 2 from ceph")
	obj2Data, err = obj2GetRes.ExtractContent()
	r.NoError(err, "failed to read object 2 content from ceph")
	r.Equal(obj2UpdContent, string(obj2Data), "object 2 content should match in ceph")
	r.Equal("obj2-updated-metadata", obj2GetRes.Header.Get("X-Object-Meta-Test"), "object 2 metadata should match in ceph")
	// get third object - should be present
	obj3GetRes = objects.Download(ctx, cephClient, tstCont, obj4Name, objects.DownloadOpts{})
	r.NoError(obj3GetRes.Err, "failed to get object 4 from ceph")
	obj3Data, err = obj3GetRes.ExtractContent()
	r.NoError(err, "failed to read object 4 content from ceph")
	r.Equal(obj4Content, string(obj3Data), "object 4 content should match in ceph")
	r.Equal("obj4-metadata", obj3GetRes.Header.Get("X-Object-Meta-Test"), "object 4 metadata should match in ceph")

	// check container 2 in ceph
	// should have object 3
	// get container metadata and check
	cgRes = containers.Get(ctx, cephClient, tstCont2, containers.GetOpts{})
	r.NoError(cgRes.Err, "failed to get ceph container 2")
	r.Equal("chorus-metadata-2", cgRes.Header.Get("X-Container-Meta-Test"), "ceph container 2 metadata should match")
	// get object - should be present
	obj3GetRes = objects.Download(ctx, cephClient, tstCont2, obj3Name, objects.DownloadOpts{})
	r.NoError(obj3GetRes.Err, "failed to get object 3 from ceph")
	obj3Data, err = obj3GetRes.ExtractContent()
	r.NoError(err, "failed to read object 3 content from ceph")
	r.Equal(obj3Content, string(obj3Data), "object 3 content should match in ceph")
	r.Equal("obj3-metadata", obj3GetRes.Header.Get("X-Object-Meta-Test"), "object 3 metadata should match in ceph")

	// list objects in container 1 from proxy: should be 2 (obj2 and obj4)
	objListRes := objects.List(proxyClient, tstCont, objects.ListOpts{
		Limit: 1000,
	})
	objList, err := objListRes.AllPages(ctx)
	r.NoError(err, "failed to list objects in container 1 from proxy")
	objInfos, err := objects.ExtractInfo(objList)
	r.NoError(err, "failed to extract object info from container 1 from proxy")
	r.Len(objInfos, 2, "there should be 2 objects in container 1 from proxy")

	// list objects in container 1 from ceph: should be 2 (obj2 and obj4)
	objListRes = objects.List(cephClient, tstCont, objects.ListOpts{
		Limit: 1000,
	})
	objList, err = objListRes.AllPages(ctx)
	r.NoError(err, "failed to list objects in container 1 from ceph")
	objInfos, err = objects.ExtractInfo(objList)
	r.NoError(err, "failed to extract object info from container 1 from ceph")
	r.Len(objInfos, 2, "there should be 2 objects in container 1 from ceph")

	// add 2 more objects to container 2 in proxy
	obj5Content := "this is object 5 in container 2"
	obj5Name := "obj5.txt"
	obj5Res := objects.Create(ctx, proxyClient, tstCont2, obj5Name, objects.CreateOpts{
		Content:     strings.NewReader(obj5Content),
		Metadata:    map[string]string{"test": "obj5-metadata"},
		ContentType: "text/plain",
	})
	r.NoError(obj5Res.Err, "failed to upload object 5 to proxy")

	obj6Content := "this is object 6 in container 2"
	obj6Name := "dir/obj6.txt"
	obj6Res := objects.Create(ctx, proxyClient, tstCont2, obj6Name, objects.CreateOpts{
		Content:     strings.NewReader(obj6Content),
		Metadata:    map[string]string{"test": "obj6-metadata"},
		ContentType: "text/plain",
	})
	r.NoError(obj6Res.Err, "failed to upload object 6 to proxy")
	// ------------------------------------------------ create switch -----------------------------------------------
	_, err = e.PolicyClient.SwitchWithDowntime(ctx, &pb.SwitchDowntimeRequest{
		ReplicationId: replID,
		DowntimeOpts: &pb.SwitchDowntimeOpts{
			StartOnInitDone: true,
		},
	})
	r.NoError(err, "failed to create switch")

	// wait for switch to be in progress
	r.Eventually(func() bool {
		switchStatus, err := e.PolicyClient.GetSwitchStatus(ctx, replID)
		if err != nil {
			return false
		}
		return switchStatus.LastStatus >= pb.ReplicationSwitch_IN_PROGRESS
	}, e.WaitShort, e.RetryShort)
	// create container in proxy should return error
	cont3 := "chorus-container-3"
	cRes = containers.Create(ctx, proxyClient, cont3, containers.CreateOpts{
		Metadata: map[string]string{"test": "chorus-metadata-3"},
	})
	r.Error(cRes.Err, "create container in proxy should return error after switch")

	//wait for switch to be done
	r.Eventually(func() bool {
		switchStatus, err := e.PolicyClient.GetSwitchStatus(ctx, replID)
		if err != nil {
			return false
		}
		return switchStatus.LastStatus == pb.ReplicationSwitch_DONE
	}, e.WaitLong*3, e.RetryLong)
	// ------------------------------------------------ switch done -----------------------------------------------

	// compare objects in container 2 in swift and ceph
	// should have obj3, obj5 and obj6
	objListRes = objects.List(swiftClient, tstCont2, objects.ListOpts{
		Limit: 1000,
	})
	objList, err = objListRes.AllPages(ctx)
	r.NoError(err, "failed to list objects in container 2 from swift")
	objInfosSwift, err := objects.ExtractInfo(objList)
	r.NoError(err, "failed to extract object info from container 2 from swift")

	objListRes = objects.List(cephClient, tstCont2, objects.ListOpts{
		Limit: 1000,
	})
	objList, err = objListRes.AllPages(ctx)
	r.NoError(err, "failed to list objects in container 2 from ceph")
	objInfosCeph, err := objects.ExtractInfo(objList)
	r.NoError(err, "failed to extract object info from container 2 from ceph")

	r.Equal(len(objInfosSwift), len(objInfosCeph), "number of objects in container 2 should be the same in swift and ceph")
	objNamesSwift := make(map[string]bool)
	for _, obj := range objInfosSwift {
		objNamesSwift[obj.Name] = true
	}
	for _, obj := range objInfosCeph {
		_, ok := objNamesSwift[obj.Name]
		r.True(ok, "object %s should be present in both swift and ceph", obj.Name)
	}

	// create object in proxy. now it should be only in ceph

	obj7Content := "this is object 7 in container 2"
	obj7Name := "obj7.txt"
	obj7Res := objects.Create(ctx, proxyClient, tstCont2, obj7Name, objects.CreateOpts{
		Content:     strings.NewReader(obj7Content),
		Metadata:    map[string]string{"test": "obj7-metadata"},
		ContentType: "text/plain",
	})
	r.NoError(obj7Res.Err, "failed to upload object 7 to proxy")

	// get from ceph
	obj7GetRes := objects.Download(ctx, cephClient, tstCont2, obj7Name, objects.DownloadOpts{})
	r.NoError(obj7GetRes.Err, "failed to get object 7 from ceph")
	obj7Data, err := obj7GetRes.ExtractContent()
	r.NoError(err, "failed to read object 7 content from ceph")
	r.Equal(obj7Content, string(obj7Data), "object 7 content should match in ceph")
	r.Equal("obj7-metadata", obj7GetRes.Header.Get("X-Object-Meta-Test"), "object 7 metadata should match in ceph")

	// get from swift should return not found
	obj7GetRes = objects.Download(ctx, swiftClient, tstCont2, obj7Name, objects.DownloadOpts{})
	r.True(gophercloud.ResponseCodeIs(obj7GetRes.Err, http.StatusNotFound), "object 7 should not be found in swift")

}
