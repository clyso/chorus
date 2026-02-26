package agent

import (
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/service/worker"
	"github.com/clyso/chorus/test/app"
)

const (
	// Ceph batches notifications with ~20s delay, so we need a longer timeout
	// than the default WaitLong (20s) for webhook-triggered replication checks.
	webhookWait = 60 * time.Second
)

func notifID(user, bucket string) string {
	user = strings.ReplaceAll(user, "-", "--")
	bucket = strings.ReplaceAll(bucket, "-", "--")
	return fmt.Sprintf("chorus-%s-%s", user, bucket)
}

func objectCreatedRecord(key string, size int64) *pb.S3EventRecord {
	return &pb.S3EventRecord{
		EventName: "s3:ObjectCreated:Put",
		S3:        &pb.S3EventPayload{Object: &pb.S3ObjectInfo{Key: key, Size: size}},
	}
}

func objectRemovedRecord(key string) *pb.S3EventRecord {
	return &pb.S3EventRecord{
		EventName: "s3:ObjectRemoved:Delete",
		S3:        &pb.S3EventPayload{Object: &pb.S3ObjectInfo{Key: key}},
	}
}

func sendS3Notification(t *testing.T, client pb.WebhookClient, storage, user, bucket string, records ...*pb.S3EventRecord) {
	t.Helper()
	for _, r := range records {
		if r.S3 == nil {
			r.S3 = &pb.S3EventPayload{}
		}
		if r.S3.ConfigurationId == "" {
			r.S3.ConfigurationId = notifID(user, bucket)
		}
		if r.S3.Bucket == nil {
			r.S3.Bucket = &pb.S3BucketInfo{Name: bucket}
		}
	}
	_, err := client.S3Notifications(t.Context(), &pb.S3NotificationRequest{
		Storage: storage,
		Records: records,
	})
	require.NoError(t, err)
}

func Test_e2e_s3_notification_webhook(t *testing.T) {
	r := require.New(t)

	workerConf, err := worker.GetConfig()
	r.NoError(err)
	workerConf.Log.Level = "warn"
	workerConf.Storage = workerStorage
	workerConf.Api.HttpPort = workerHttpPort
	workerConf.Api.Webhook.BaseURL = fmt.Sprintf("http://host.testcontainers.internal:%d", workerHttpPort)

	e := app.SetupChorus(t, workerConf, nil)

	t.Run("bucket_replication", func(t *testing.T) {
		r := require.New(t)
		ctx := t.Context()

		bucket := "notif-bucket-1"
		err := cephClient.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		r.NoError(err)

		objContent := "hello from s3 notification test"
		_, err = cephClient.PutObject(ctx, bucket, "obj1.txt", strings.NewReader(objContent), int64(len(objContent)), minio.PutObjectOptions{ContentType: "text/plain"})
		r.NoError(err)

		fromBucket := bucket
		replID := &pb.ReplicationID{
			User:        user,
			FromStorage: cephKey,
			ToStorage:   minioKey,
			FromBucket:  &fromBucket,
			ToBucket:    &fromBucket,
		}
		_, err = e.PolicyClient.AddReplication(ctx, &pb.AddReplicationRequest{
			Id:   replID,
			Opts: &pb.ReplicationOpts{EventSource: pb.EventSource_EVENT_SOURCE_S3_NOTIFICATION},
		})
		r.NoError(err)

		// Wait for init replication to complete (lists+syncs existing objects).
		r.Eventually(func() bool {
			repl, err := e.PolicyClient.GetReplication(ctx, replID)
			if err != nil {
				return false
			}
			return repl.IsInitDone
		}, e.WaitLong, e.RetryLong)

		// Verify initial object replicated to MinIO.
		obj, err := minioClient.GetObject(ctx, bucket, "obj1.txt", minio.GetObjectOptions{})
		r.NoError(err)
		data, err := io.ReadAll(obj)
		r.NoError(err)
		r.Equal(objContent, string(data))

		// Verify API response fields.
		repl, err := e.PolicyClient.GetReplication(ctx, replID)
		r.NoError(err)
		r.Equal(pb.EventSource_EVENT_SOURCE_S3_NOTIFICATION, repl.GetEventSource())
		r.Contains(repl.GetWebhookUrl(), "/webhook/ceph/s3-notifications")

		// Put new object directly on Ceph — Ceph sends notification → worker replicates.
		obj2Content := "object via webhook"
		_, err = cephClient.PutObject(ctx, bucket, "obj2.txt", strings.NewReader(obj2Content), int64(len(obj2Content)), minio.PutObjectOptions{ContentType: "text/plain"})
		r.NoError(err)

		r.Eventually(func() bool {
			obj, err := minioClient.GetObject(ctx, bucket, "obj2.txt", minio.GetObjectOptions{})
			if err != nil {
				return false
			}
			d, err := io.ReadAll(obj)
			return err == nil && string(d) == obj2Content
		}, webhookWait, e.RetryLong)

		// Delete object on Ceph — Ceph sends notification → worker replicates delete.
		err = cephClient.RemoveObject(ctx, bucket, "obj1.txt", minio.RemoveObjectOptions{})
		r.NoError(err)

		r.Eventually(func() bool {
			_, err := minioClient.StatObject(ctx, bucket, "obj1.txt", minio.StatObjectOptions{})
			return err != nil
		}, webhookWait, e.RetryLong)

		_, err = e.PolicyClient.DeleteReplication(ctx, replID)
		r.NoError(err)
	})

	t.Run("no_replication_skips_events", func(t *testing.T) {
		sendS3Notification(t, e.WebhookClient, cephKey, user, "nonexistent",
			objectCreatedRecord("obj.txt", 10),
		)
	})

	t.Run("unknown_storage_returns_ok", func(t *testing.T) {
		_, err := e.WebhookClient.S3Notifications(t.Context(), &pb.S3NotificationRequest{
			Storage: "no-such-storage",
			Records: []*pb.S3EventRecord{
				objectCreatedRecord("obj.txt", 10),
			},
		})
		require.NoError(t, err)
	})

	t.Run("minio_webhook", func(t *testing.T) {
		t.Skip("MinIO doesn't support SNS CreateTopic; needs MINIO_NOTIFY_WEBHOOK_* pre-config")
	})
}
