package swift

import (
	"net/http"
	"strings"
	"testing"

	"github.com/clyso/chorus/pkg/swift"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/service/proxy"
	"github.com/clyso/chorus/service/worker"
	"github.com/clyso/chorus/test/app"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/containers"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

func Test_e2e_webhook(t *testing.T) {
	r := require.New(t)
	ctx := t.Context()

	swiftClient, err := clients.AsSwift(ctx, swiftTestKey, testAcc)
	r.NoError(err)
	cephClient, err := clients.AsSwift(ctx, cephTestKey, testAcc)
	r.NoError(err)

	workerConf, err := worker.GetConfig()
	r.NoError(err)
	workerConf.Log.Level = "warn"
	workerConf.Storage = swiftConf

	proxyConf, err := proxy.GetConfig()
	r.NoError(err)
	proxyConf.Log.Level = "warn"
	proxyConf.Storage = proxyStorages

	e := app.SetupChorus(t, workerConf, proxyConf)
	chorusStorages, err := e.ChorusClient.GetStorages(ctx, &emptypb.Empty{})
	r.NoError(err)
	t.Log(chorusStorages.Storages)

	registerSwiftEndpoitnt(t, "chorus-swift-agent", "localhost", e.ProxyPort)
	c := *swiftConf.Storages[swiftTestKey].Swift
	c.StorageEndpointName = "chorus-swift-agent"
	proxyClient, err := swift.NewClient(ctx, c.StorageAddress, c.Credentials[testAcc])
	r.NoError(err)

	sendWebhook := func(t *testing.T, events ...*pb.SwiftEvent) {
		t.Helper()
		_, err := e.WebhookClient.SwiftEvents(t.Context(), &pb.SwiftEventsRequest{
			Storage: swiftTestKey,
			Events:  events,
		})
		require.NoError(t, err)
	}

	t.Run("container_and_object_replication", func(t *testing.T) {
		r := require.New(t)
		ctx := t.Context()

		// pre-populate source with a container and object before replication
		cont := "wh-container"
		cRes := containers.Create(ctx, swiftClient, cont, containers.CreateOpts{
			Metadata: map[string]string{"env": "test"},
		})
		r.NoError(cRes.Err)

		objContent := "hello from webhook test"
		objName := "obj1.txt"
		oRes := objects.Create(ctx, swiftClient, cont, objName, objects.CreateOpts{
			Content:     strings.NewReader(objContent),
			Metadata:    map[string]string{"source": "webhook"},
			ContentType: "text/plain",
		})
		r.NoError(oRes.Err)

		// start replication
		replID := &pb.ReplicationID{
			FromStorage: swiftTestKey,
			ToStorage:   cephTestKey,
			User:        testAcc,
		}
		_, err := e.PolicyClient.AddReplication(ctx, &pb.AddReplicationRequest{Id: replID})
		r.NoError(err)

		// wait for initial migration to complete
		r.Eventually(func() bool {
			repl, err := e.PolicyClient.GetReplication(ctx, replID)
			if err != nil {
				return false
			}
			return repl.IsInitDone
		}, e.WaitLong, e.RetryLong)

		// verify initial data replicated to ceph
		gRes := containers.Get(ctx, cephClient, cont, containers.GetOpts{})
		r.NoError(gRes.Err)
		r.Equal("test", gRes.Header.Get("X-Container-Meta-Env"))

		dlRes := objects.Download(ctx, cephClient, cont, objName, objects.DownloadOpts{})
		r.NoError(dlRes.Err)
		data, err := dlRes.ExtractContent()
		r.NoError(err)
		r.Equal(objContent, string(data))

		// --- from here on, use webhook events instead of proxy ---

		// upload new object directly to source
		obj2Content := "object via webhook"
		obj2Name := "obj2.txt"
		oRes = objects.Create(ctx, swiftClient, cont, obj2Name, objects.CreateOpts{
			Content:     strings.NewReader(obj2Content),
			ContentType: "text/plain",
		})
		r.NoError(oRes.Err)

		// send OBJECT_CREATED webhook
		sendWebhook(t, &pb.SwiftEvent{
			Account:   testAcc,
			Container: cont,
			Object:    obj2Name,
			Operation: pb.SwiftOperation_OBJECT_CREATED,
		})

		// wait for replication
		r.Eventually(func() bool {
			dl := objects.Download(ctx, cephClient, cont, obj2Name, objects.DownloadOpts{})
			if dl.Err != nil {
				return false
			}
			d, err := dl.ExtractContent()
			return err == nil && string(d) == obj2Content
		}, e.WaitLong, e.RetryLong)

		// update object metadata directly on source
		oRes = objects.Create(ctx, swiftClient, cont, obj2Name, objects.CreateOpts{
			Content:     strings.NewReader(obj2Content),
			Metadata:    map[string]string{"updated": "true"},
			ContentType: "text/plain",
		})
		r.NoError(oRes.Err)

		// send OBJECT_METADATA_UPDATED webhook
		sendWebhook(t, &pb.SwiftEvent{
			Account:   testAcc,
			Container: cont,
			Object:    obj2Name,
			Operation: pb.SwiftOperation_OBJECT_METADATA_UPDATED,
		})

		r.Eventually(func() bool {
			dl := objects.Download(ctx, cephClient, cont, obj2Name, objects.DownloadOpts{})
			if dl.Err != nil {
				return false
			}
			return dl.Header.Get("X-Object-Meta-Updated") == "true"
		}, e.WaitLong, e.RetryLong)

		// delete object directly on source
		delRes := objects.Delete(ctx, swiftClient, cont, objName, objects.DeleteOpts{})
		r.NoError(delRes.Err)

		// send OBJECT_DELETED webhook
		sendWebhook(t, &pb.SwiftEvent{
			Account:   testAcc,
			Container: cont,
			Object:    objName,
			Operation: pb.SwiftOperation_OBJECT_DELETED,
		})

		r.Eventually(func() bool {
			dl := objects.Download(ctx, cephClient, cont, objName, objects.DownloadOpts{})
			return gophercloud.ResponseCodeIs(dl.Err, http.StatusNotFound)
		}, e.WaitLong, e.RetryLong)

		// create new container directly on source
		cont2 := "wh-container-2"
		cRes = containers.Create(ctx, swiftClient, cont2, containers.CreateOpts{
			Metadata: map[string]string{"from": "webhook"},
		})
		r.NoError(cRes.Err)

		// send CONTAINER_UPDATE webhook
		sendWebhook(t, &pb.SwiftEvent{
			Account:   testAcc,
			Container: cont2,
			Operation: pb.SwiftOperation_CONTAINER_UPDATE,
		})

		r.Eventually(func() bool {
			gRes := containers.Get(ctx, cephClient, cont2, containers.GetOpts{})
			return gRes.Err == nil && gRes.Header.Get("X-Container-Meta-From") == "webhook"
		}, e.WaitLong, e.RetryLong)

		// delete container
		dRes := containers.Delete(ctx, swiftClient, cont2)
		r.NoError(dRes.Err)

		// send CONTAINER_DELETE webhook
		sendWebhook(t, &pb.SwiftEvent{
			Account:   testAcc,
			Container: cont2,
			Operation: pb.SwiftOperation_CONTAINER_DELETE,
		})

		r.Eventually(func() bool {
			gRes := containers.Get(ctx, cephClient, cont2, containers.GetOpts{})
			return gophercloud.ResponseCodeIs(gRes.Err, http.StatusNotFound)
		}, e.WaitLong, e.RetryLong)

		// clean up replication
		_, err = e.PolicyClient.DeleteReplication(ctx, replID)
		r.NoError(err)
	})

	t.Run("batch_events", func(t *testing.T) {
		r := require.New(t)
		ctx := t.Context()

		cont := "wh-batch"
		cRes := containers.Create(ctx, swiftClient, cont, containers.CreateOpts{})
		r.NoError(cRes.Err)

		// create multiple objects on source
		for i, name := range []string{"a.txt", "b.txt", "c.txt"} {
			oRes := objects.Create(ctx, swiftClient, cont, name, objects.CreateOpts{
				Content:     strings.NewReader(strings.Repeat(name, i+1)),
				ContentType: "text/plain",
			})
			r.NoError(oRes.Err)
		}

		replID := &pb.ReplicationID{
			FromStorage: swiftTestKey,
			ToStorage:   cephTestKey,
			User:        testAcc,
		}
		_, err := e.PolicyClient.AddReplication(ctx, &pb.AddReplicationRequest{Id: replID})
		r.NoError(err)

		r.Eventually(func() bool {
			repl, err := e.PolicyClient.GetReplication(ctx, replID)
			if err != nil {
				return false
			}
			return repl.IsInitDone
		}, e.WaitLong, e.RetryLong)

		// upload 2 new objects and delete 1, all directly on source
		for _, name := range []string{"d.txt", "e.txt"} {
			oRes := objects.Create(ctx, swiftClient, cont, name, objects.CreateOpts{
				Content:     strings.NewReader("content-" + name),
				ContentType: "text/plain",
			})
			r.NoError(oRes.Err)
		}
		delRes := objects.Delete(ctx, swiftClient, cont, "a.txt", objects.DeleteOpts{})
		r.NoError(delRes.Err)

		// send all events in a single batch
		sendWebhook(t,
			&pb.SwiftEvent{
				Account:   testAcc,
				Container: cont,
				Object:    "d.txt",
				Operation: pb.SwiftOperation_OBJECT_CREATED,
			},
			&pb.SwiftEvent{
				Account:   testAcc,
				Container: cont,
				Object:    "e.txt",
				Operation: pb.SwiftOperation_OBJECT_CREATED,
			},
			&pb.SwiftEvent{
				Account:   testAcc,
				Container: cont,
				Object:    "a.txt",
				Operation: pb.SwiftOperation_OBJECT_DELETED,
			},
		)

		// verify all 3 events took effect
		r.Eventually(func() bool {
			// d.txt present
			dl := objects.Download(ctx, cephClient, cont, "d.txt", objects.DownloadOpts{})
			if dl.Err != nil {
				return false
			}
			// e.txt present
			dl = objects.Download(ctx, cephClient, cont, "e.txt", objects.DownloadOpts{})
			if dl.Err != nil {
				return false
			}
			// a.txt deleted
			dl = objects.Download(ctx, cephClient, cont, "a.txt", objects.DownloadOpts{})
			return gophercloud.ResponseCodeIs(dl.Err, http.StatusNotFound)
		}, e.WaitLong, e.RetryLong)

		_, err = e.PolicyClient.DeleteReplication(ctx, replID)
		r.NoError(err)
	})

	t.Run("no_replication_skips_events", func(t *testing.T) {
		// webhook should succeed even without active replication — events are just skipped
		sendWebhook(t, &pb.SwiftEvent{
			Account:   testAcc,
			Container: "nonexistent",
			Object:    "obj.txt",
			Operation: pb.SwiftOperation_OBJECT_CREATED,
		})
	})

	t.Run("unknown_storage_returns_ok", func(t *testing.T) {
		// unknown storage is a permanent error — webhook returns 200 to prevent
		// the caller (log shipper) from retrying pointlessly
		_, err := e.WebhookClient.SwiftEvents(ctx, &pb.SwiftEventsRequest{
			Storage: "no-such-storage",
			Events: []*pb.SwiftEvent{{
				Account:   testAcc,
				Container: "c",
				Object:    "o",
				Operation: pb.SwiftOperation_OBJECT_CREATED,
			}},
		})
		require.NoError(t, err)
	})

	// proxy should still work alongside webhook
	t.Run("proxy_coexists_with_webhook", func(t *testing.T) {
		r := require.New(t)
		ctx := t.Context()

		cont := "wh-proxy-coexist"
		cRes := containers.Create(ctx, proxyClient, cont, containers.CreateOpts{})
		r.NoError(cRes.Err)

		gRes := containers.Get(ctx, swiftClient, cont, containers.GetOpts{})
		r.NoError(gRes.Err)
	})
}

func Test_e2e_webhook_separate_ports(t *testing.T) {
	r := require.New(t)
	ctx := t.Context()

	swiftClient, err := clients.AsSwift(ctx, swiftTestKey, testAcc)
	r.NoError(err)
	cephClient, err := clients.AsSwift(ctx, cephTestKey, testAcc)
	r.NoError(err)

	workerConf, err := worker.GetConfig()
	r.NoError(err)
	workerConf.Log.Level = "warn"
	workerConf.Storage = swiftConf
	// set non-zero to trigger separate port allocation in SetupChorus
	workerConf.Api.Webhook.GrpcPort = 1
	workerConf.Api.Webhook.HttpPort = 1

	proxyConf, err := proxy.GetConfig()
	r.NoError(err)
	proxyConf.Log.Level = "warn"
	proxyConf.Storage = proxyStorages

	e := app.SetupChorus(t, workerConf, proxyConf)

	// create object on source, start replication, send webhook, verify on dest
	cont := "wh-separate-ports"
	cRes := containers.Create(ctx, swiftClient, cont, containers.CreateOpts{})
	r.NoError(cRes.Err)

	replID := &pb.ReplicationID{
		FromStorage: swiftTestKey,
		ToStorage:   cephTestKey,
		User:        testAcc,
	}
	_, err = e.PolicyClient.AddReplication(ctx, &pb.AddReplicationRequest{Id: replID})
	r.NoError(err)

	r.Eventually(func() bool {
		repl, err := e.PolicyClient.GetReplication(ctx, replID)
		if err != nil {
			return false
		}
		return repl.IsInitDone
	}, e.WaitLong, e.RetryLong)

	// upload object directly to source and notify via webhook on separate port
	objContent := "separate ports work"
	oRes := objects.Create(ctx, swiftClient, cont, "sep.txt", objects.CreateOpts{
		Content:     strings.NewReader(objContent),
		ContentType: "text/plain",
	})
	r.NoError(oRes.Err)

	_, err = e.WebhookClient.SwiftEvents(ctx, &pb.SwiftEventsRequest{
		Storage: swiftTestKey,
		Events: []*pb.SwiftEvent{{
			Account:   testAcc,
			Container: cont,
			Object:    "sep.txt",
			Operation: pb.SwiftOperation_OBJECT_CREATED,
		}},
	})
	r.NoError(err)

	r.Eventually(func() bool {
		dl := objects.Download(ctx, cephClient, cont, "sep.txt", objects.DownloadOpts{})
		if dl.Err != nil {
			return false
		}
		d, err := dl.ExtractContent()
		return err == nil && string(d) == objContent
	}, e.WaitLong, e.RetryLong)

	_, err = e.PolicyClient.DeleteReplication(ctx, replID)
	r.NoError(err)
}
