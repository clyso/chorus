/*
 * Copyright © 2024 Clyso GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package migration

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"testing"
	"time"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

func TestApi_ZeroDowntimeSwitch(t *testing.T) {
	const (
		waitInterval  = 15 * time.Second
		retryInterval = 100 * time.Millisecond
		bucket        = "switch-bucket"
	)

	r := require.New(t)
	exists, err := mainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = f1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = f2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)

	// fill main bucket with init data
	err = mainClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{})
	r.NoError(err)
	defer cleanup(bucket)

	obj1 := getTestObj("obj1", bucket)
	_, err = mainClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj2 := getTestObj("photo/sept/obj2", bucket)
	_, err = mainClient.PutObject(tstCtx, obj2.bucket, obj2.name, bytes.NewReader(obj2.data), int64(len(obj2.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj3 := getTestObj("photo/obj3", bucket)
	_, err = mainClient.PutObject(tstCtx, obj3.bucket, obj3.name, bytes.NewReader(obj3.data), int64(len(obj3.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj4 := getTestObj("obj4", bucket)
	_, err = mainClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj5 := getTestObj("obj5", bucket)
	_, err = mainClient.PutObject(tstCtx, obj5.bucket, obj5.name, bytes.NewReader(obj5.data), int64(len(obj5.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj6 := getTestObj("obj6", bucket)
	_, err = mainClient.PutObject(tstCtx, obj6.bucket, obj6.name, bytes.NewReader(obj6.data), int64(len(obj6.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)

	exists, err = mainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(exists)
	exists, err = f1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = f2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)

	_, err = apiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		User:            user,
		From:            "main",
		To:              "f1",
		Buckets:         []string{bucket},
		IsForAllBuckets: false,
	})
	r.NoError(err)
	t.Cleanup(func() {
		apiClient.DeleteReplication(tstCtx, &pb.ReplicationRequest{
			User:   user,
			Bucket: bucket,
			From:   "main",
			To:     "f1",
		})
	})

	// wait until repl started
	r.Eventually(func() bool {
		repls, err := apiClient.ListReplications(tstCtx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		started := false
		for _, repl := range repls.Replications {
			if repl.To == "f1" {
				started = started || (repl.InitObjListed > 0)
			}
		}
		return started
	}, waitInterval, retryInterval)

	// perform updates after migration started
	obj7 := getTestObj("photo/sept/obj7", bucket)
	_, err = proxyClient.PutObject(tstCtx, obj7.bucket, obj7.name, bytes.NewReader(obj7.data), int64(len(obj7.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	obj8 := getTestObj("obj8", bucket)
	_, err = proxyClient.PutObject(tstCtx, obj8.bucket, obj8.name, bytes.NewReader(obj8.data), int64(len(obj8.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	obj1 = getTestObj(obj1.name, obj1.bucket)
	_, err = proxyClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	obj4 = getTestObj(obj4.name, obj4.bucket)
	_, err = proxyClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	var objects = []testObj{obj1, obj2, obj3, obj4, obj5, obj6, obj7, obj8}

	// check that storages are in sync
	r.Eventually(func() bool {
		f1e, _ := f1Client.BucketExists(tstCtx, bucket)
		return f1e
	}, waitInterval, retryInterval)

	r.Eventually(func() bool {
		obsf1, _ := listObjects(f1Client, bucket, "")
		return len(objects) == len(obsf1)
	}, waitInterval, retryInterval)

	r.Eventually(func() bool {
		diff, err := apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    bucket,
			From:      "main",
			To:        "f1",
			ShowMatch: false,
			User:      user,
		})
		if err != nil {
			return false
		}
		return diff.IsMatch
	}, waitInterval, retryInterval)

	var repl *pb.Replication
	repls, err := apiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	for i, rr := range repls.Replications {
		if rr.Bucket == bucket && rr.From == "main" && rr.To == "f1" {
			repl = repls.Replications[i]
			break
		}
	}
	t.Log("repl events", repl.Events, repl.EventsDone)
	r.NotNil(repl)
	r.False(repl.HasSwitch)
	r.True(repl.IsInitDone)

	writeCtx, cancel := context.WithCancel(tstCtx)
	defer cancel()
	go func() {
		defer cancel()
		t.Log("start write", time.Now())
		for n := 0; n < 500; n++ {
			select {
			case <-writeCtx.Done():
				return
			default:
			}
			i := rand.Intn(len(objects))
			action := rand.Intn(3)
			switch action {
			case 0:
				//delete
				_ = proxyClient.RemoveObject(writeCtx, bucket, objects[i].name, mclient.RemoveObjectOptions{})
				objects[i].data = nil
			case 1:
				// upload
				objects[i] = getTestObj(objects[i].name, objects[i].bucket)
				_, err := proxyClient.PutObject(writeCtx, objects[i].bucket, objects[i].name, bytes.NewReader(objects[i].data), int64(len(objects[i].data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
				r.NoError(err)
			case 2:
				// multipart upload
				objects[i] = getTestObj(objects[i].name, objects[i].bucket)

				uploadID, err := proxyAwsClient.CreateMultipartUpload(&aws_s3.CreateMultipartUploadInput{
					Bucket:      sPtr(bucket),
					ContentType: sPtr("binary/octet-stream"),
					Key:         sPtr(objects[i].name),
				})
				r.NoError(err)

				partBuf := make([]byte, 1024)
				objReader := bytes.NewReader(objects[i].data)
				partID := 1
				pn := len(objects[i].data) / 1024
				if len(objects[i].data)%1024 != 0 {
					pn++
				}
				parts := make([]*aws_s3.CompletedPart, pn)

				g, _ := errgroup.WithContext(tstCtx)
				for nn := len(partBuf); nn >= len(partBuf); {
					nn, _ = objReader.Read(partBuf)
					if nn == 0 {
						break
					}
					dd := make([]byte, nn)
					copy(dd, partBuf[:nn])
					partIDCopy := partID
					g.Go(func() error {
						partReader := bytes.NewReader(dd)
						part, err := proxyAwsClient.UploadPart(&aws_s3.UploadPartInput{
							Body:          partReader,
							Bucket:        sPtr(bucket),
							ContentLength: iPtr(int64(len(dd))),
							Key:           sPtr(objects[i].name),
							PartNumber:    iPtr(int64(partIDCopy)),
							UploadId:      uploadID.UploadId,
						})

						if err != nil {
							return err
						}
						parts[partIDCopy-1] = &aws_s3.CompletedPart{
							PartNumber: iPtr(int64(partIDCopy)),
							ETag:       part.ETag,
						}
						return nil
					})
					partID++
				}
				r.NoError(g.Wait())
				_, err = proxyAwsClient.CompleteMultipartUpload(&aws_s3.CompleteMultipartUploadInput{
					Bucket:          sPtr(bucket),
					Key:             sPtr(objects[i].name),
					MultipartUpload: &aws_s3.CompletedMultipartUpload{Parts: parts},
					UploadId:        uploadID.UploadId,
				})
				r.NoError(err)
			}
		}
		t.Log("end write", time.Now())
	}()
	time.Sleep(666 * time.Millisecond)
	replID := &pb.ReplicationRequest{
		User:   user,
		Bucket: bucket,
		From:   "main",
		To:     "f1",
	}
	_, err = apiClient.SwitchBucketZeroDowntime(tstCtx, &pb.SwitchBucketZeroDowntimeRequest{
		ReplicationId: replID,
		MultipartTtl:  durationpb.New(time.Minute),
	})
	r.NoError(err)
	t.Cleanup(func() {
		_, _ = apiClient.DeleteBucketSwitch(tstCtx, replID)
	})
	t.Log("switch created", time.Now())

	repl = nil
	repls, err = apiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	for i, rr := range repls.Replications {
		if rr.Bucket == bucket && rr.From == "main" && rr.To == "f1" {
			repl = repls.Replications[i]
			break
		}
	}
	r.NotNil(repl)
	r.True(repl.HasSwitch)

	switchInfo, err := apiClient.GetBucketSwitchStatus(tstCtx, replID)
	r.NoError(err)
	r.EqualValues(pb.GetBucketSwitchStatusResponse_InProgress, switchInfo.LastStatus)
	r.Nil(switchInfo.DowntimeOpts)
	r.NotNil(switchInfo.LastStartedAt)
	r.EqualValues(time.Minute, switchInfo.MultipartTtl.AsDuration())
	r.True(proto.Equal(replID, switchInfo.ReplicationId))
	r.True(switchInfo.ZeroDowntime)
	<-writeCtx.Done()

	r.Eventually(func() bool {
		switchInfo, err = apiClient.GetBucketSwitchStatus(tstCtx, replID)
		if err != nil {
			return false
		}
		return switchInfo.LastStatus == pb.GetBucketSwitchStatusResponse_Done
	}, waitInterval, retryInterval)

	repl = nil
	repls, err = apiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	for i, rr := range repls.Replications {
		if rr.Bucket == bucket && rr.From == "main" && rr.To == "f1" {
			repl = repls.Replications[i]
			break
		}
	}
	r.NotNil(repl)
	r.True(repl.HasSwitch)
	r.True(repl.IsInitDone)
	r.EqualValues(repl.Events, repl.EventsDone)
	t.Log("switch done", repl.Events, repl.EventsDone)

	//r.Eventually(func() bool {
	//	repls, err = apiClient.ListReplications(tstCtx, &emptypb.Empty{})
	//	if err != nil {
	//		return false
	//	}
	//	for _, rr := range repls.Replications {
	//		if rr.Bucket == bucket && rr.From == "main" && rr.To == "f1" {
	//			if rr.SwitchStatus != pb.Replication_Done {
	//				return false
	//			}
	//		}
	//		if rr.Bucket == bucket && rr.From == "f1" && rr.To == "f2" {
	//			if rr.SwitchStatus != pb.Replication_Done {
	//				return false
	//			}
	//		}
	//		if rr.Bucket == bucket && rr.From == "f1" && rr.To == "main" {
	//			if rr.SwitchStatus != pb.Replication_Done {
	//				return false
	//			}
	//		}
	//	}
	//	return true
	//}, waitInterval, retryInterval)

	for _, object := range objects {
		if object.data == nil {
			_, err = proxyClient.StatObject(tstCtx, bucket, object.name, mclient.StatObjectOptions{})
			r.Error(err, object.name)
			_, err = f1Client.StatObject(tstCtx, bucket, object.name, mclient.StatObjectOptions{})
			r.Error(err, object.name)
			continue
		}
		objData, err := proxyClient.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err := io.ReadAll(objData)
		r.NoError(err, object.name)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = f1Client.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err, object.name)
		r.True(bytes.Equal(object.data, objBytes), object.name)
	}

	switchInfo, err = apiClient.GetBucketSwitchStatus(tstCtx, replID)
	r.NoError(err)
	r.EqualValues(pb.GetBucketSwitchStatusResponse_Done, switchInfo.LastStatus)
	r.Nil(switchInfo.DowntimeOpts)
	r.NotNil(switchInfo.LastStartedAt)
	r.NotNil(switchInfo.DoneAt)
	r.EqualValues(time.Minute, switchInfo.MultipartTtl.AsDuration())
	r.True(proto.Equal(replID, switchInfo.ReplicationId))
	r.True(switchInfo.ZeroDowntime)
	r.NotEmpty(switchInfo.History)
}

func TestApi_switch_multipart(t *testing.T) {
	const (
		waitInterval  = 15 * time.Second
		retryInterval = 100 * time.Millisecond
		bucket        = "switch-bucket-multipart"
	)

	r := require.New(t)
	exists, err := mainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = f1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = f2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)

	// fill main bucket with init data
	err = mainClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{})
	r.NoError(err)
	defer cleanup(bucket)

	obj1 := getTestObj("obj1", bucket)
	_, err = mainClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj2 := getTestObj("photo/sept/obj2", bucket)
	_, err = mainClient.PutObject(tstCtx, obj2.bucket, obj2.name, bytes.NewReader(obj2.data), int64(len(obj2.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj3 := getTestObj("photo/obj3", bucket)
	_, err = mainClient.PutObject(tstCtx, obj3.bucket, obj3.name, bytes.NewReader(obj3.data), int64(len(obj3.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj4 := getTestObj("obj4", bucket)
	_, err = mainClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)

	exists, err = mainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(exists)
	exists, err = f1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = f2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)

	_, err = apiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		User:            user,
		From:            "main",
		To:              "f1",
		Buckets:         []string{bucket},
		IsForAllBuckets: false,
	})
	r.NoError(err)
	t.Cleanup(func() {
		apiClient.DeleteReplication(tstCtx, &pb.ReplicationRequest{
			User:   user,
			Bucket: bucket,
			From:   "main",
			To:     "f1",
		})
	})

	// wait until repl started
	r.Eventually(func() bool {
		repls, err := apiClient.ListReplications(tstCtx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		started := false
		for _, repl := range repls.Replications {
			if repl.To == "f1" {
				started = started || (repl.InitObjListed > 0)
			}
		}
		return started
	}, waitInterval, retryInterval)

	// perform updates after migration started
	obj7 := getTestObj("photo/sept/obj7", bucket)
	_, err = proxyClient.PutObject(tstCtx, obj7.bucket, obj7.name, bytes.NewReader(obj7.data), int64(len(obj7.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	obj4 = getTestObj(obj4.name, obj4.bucket)
	_, err = proxyClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	var objects = []testObj{obj1, obj2, obj3, obj4, obj7}

	// check that storages are in sync
	r.Eventually(func() bool {
		f1e, _ := f1Client.BucketExists(tstCtx, bucket)
		return f1e
	}, waitInterval, retryInterval)

	f1Stream, err := apiClient.StreamBucketReplication(tstCtx, &pb.ReplicationRequest{
		User:   user,
		Bucket: bucket,
		From:   "main",
		To:     "f1",
	})
	r.NoError(err)
	defer f1Stream.CloseSend()
	r.Eventually(func() bool {
		m, err := f1Stream.Recv()
		if err != nil {
			return false
		}
		return m.IsInitDone
	}, waitInterval, retryInterval)

	r.Eventually(func() bool {
		diff, err := apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    bucket,
			From:      "main",
			To:        "f1",
			ShowMatch: false,
			User:      user,
		})
		if err != nil {
			return false
		}
		return diff.IsMatch
	}, waitInterval, retryInterval)

	// start multipart upload
	// multipart upload
	obj4 = getTestObj(obj4.name, obj4.bucket)

	uploadID, err := proxyAwsClient.CreateMultipartUpload(&aws_s3.CreateMultipartUploadInput{
		Bucket:      sPtr(bucket),
		ContentType: sPtr("binary/octet-stream"),
		Key:         sPtr(obj4.name),
	})
	r.NoError(err)

	partBuf := make([]byte, 1024)
	objReader := bytes.NewReader(obj4.data)
	partID := 1
	pn := len(obj4.data) / 1024
	if len(obj4.data)%1024 != 0 {
		pn++
	}
	if pn < 3 {
		panic("increase multipart obj size or decrease part size")
	}
	replID := &pb.ReplicationRequest{
		User:   user,
		Bucket: bucket,
		From:   "main",
		To:     "f1",
	}

	parts := make([]*aws_s3.CompletedPart, pn)
	for n := len(partBuf); n >= len(partBuf); {
		if (partID - 1) == pn/2 {
			_, err = apiClient.SwitchBucketZeroDowntime(tstCtx, &pb.SwitchBucketZeroDowntimeRequest{
				ReplicationId: replID,
				MultipartTtl:  durationpb.New(time.Minute),
			})
			r.NoError(err)
			t.Log("switch started", partID)
			t.Cleanup(func() {
				_, _ = apiClient.DeleteBucketSwitch(tstCtx, replID)
			})
		}

		n, _ = objReader.Read(partBuf)
		if n == 0 {
			break
		}
		partReader := bytes.NewReader(partBuf[:n])
		part, err := proxyAwsClient.UploadPart(&aws_s3.UploadPartInput{
			Body:          partReader,
			Bucket:        sPtr(bucket),
			ContentLength: iPtr(int64(n)),
			Key:           sPtr(obj4.name),
			PartNumber:    iPtr(int64(partID)),
			UploadId:      uploadID.UploadId,
		})
		r.NoError(err)
		t.Log("part uploaded", partID)
		parts[partID-1] = &aws_s3.CompletedPart{
			PartNumber: iPtr(int64(partID)),
			ETag:       part.ETag,
		}
		partID++
	}

	_, err = proxyAwsClient.CompleteMultipartUpload(&aws_s3.CompleteMultipartUploadInput{
		Bucket:          sPtr(bucket),
		Key:             sPtr(obj4.name),
		MultipartUpload: &aws_s3.CompletedMultipartUpload{Parts: parts},
		UploadId:        uploadID.UploadId,
	})
	r.NoError(err)

	r.Eventually(func() bool {
		switchInfo, err := apiClient.GetBucketSwitchStatus(tstCtx, replID)
		if err != nil {
			return false
		}
		return switchInfo.LastStatus == pb.GetBucketSwitchStatusResponse_Done
	}, waitInterval, retryInterval)

	for _, object := range objects {
		objData, err := proxyClient.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err := io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = f1Client.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
	}
}

func TestApi_scheduled_switch(t *testing.T) {
	const (
		waitInterval  = 15 * time.Second
		retryInterval = 100 * time.Millisecond
		bucket        = "switch-bucket-scheduled"
	)

	r := require.New(t)
	exists, err := mainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = f1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = f2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)

	// fill main bucket with init data
	err = mainClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{})
	r.NoError(err)
	defer cleanup(bucket)

	obj1 := getTestObj("obj1", bucket)
	_, err = mainClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj2 := getTestObj("photo/sept/obj2", bucket)
	_, err = mainClient.PutObject(tstCtx, obj2.bucket, obj2.name, bytes.NewReader(obj2.data), int64(len(obj2.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj3 := getTestObj("photo/obj3", bucket)
	_, err = mainClient.PutObject(tstCtx, obj3.bucket, obj3.name, bytes.NewReader(obj3.data), int64(len(obj3.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj4 := getTestObj("obj4", bucket)
	_, err = mainClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)

	exists, err = mainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(exists)
	exists, err = f1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = f2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)

	_, err = apiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		User:            user,
		From:            "main",
		To:              "f1",
		Buckets:         []string{bucket},
		IsForAllBuckets: false,
	})
	r.NoError(err)
	t.Cleanup(func() {
		apiClient.DeleteReplication(tstCtx, &pb.ReplicationRequest{
			User:   user,
			Bucket: bucket,
			From:   "main",
			To:     "f1",
		})
	})
	var repl *pb.Replication
	repls, err := apiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(repls.Replications, 1)
	repl = repls.Replications[0]
	r.False(repl.HasSwitch)
	r.False(repl.IsArchived)

	// wait until repl started
	r.Eventually(func() bool {
		repls, err := apiClient.ListReplications(tstCtx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		started := false
		for _, repl := range repls.Replications {
			if repl.To == "f1" {
				started = started || (repl.InitObjListed > 0)
			}
		}
		return started
	}, waitInterval, retryInterval)

	// perform updates after migration started
	obj7 := getTestObj("photo/sept/obj7", bucket)
	_, err = proxyClient.PutObject(tstCtx, obj7.bucket, obj7.name, bytes.NewReader(obj7.data), int64(len(obj7.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	obj4 = getTestObj(obj4.name, obj4.bucket)
	_, err = proxyClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	// start scheduled switch on init done:
	replID := &pb.ReplicationRequest{
		User:   user,
		Bucket: bucket,
		From:   "main",
		To:     "f1",
	}
	_, err = apiClient.SwitchBucket(tstCtx, &pb.SwitchBucketRequest{
		ReplicationId: replID,
		DowntimeOpts: &pb.SwitchDowntimeOpts{
			StartOnInitDone:     true,
			SkipBucketCheck:     true,
			ContinueReplication: false,
		},
	})
	r.NoError(err)
	t.Cleanup(func() {
		_, _ = apiClient.DeleteBucketSwitch(tstCtx, replID)
	})

	// check that replication policy now has linked switch
	repls, err = apiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(repls.Replications, 1)
	repl = repls.Replications[0]
	r.True(repl.HasSwitch)
	r.False(repl.IsArchived)

	// wait until switch is in progress
	r.Eventually(func() bool {
		switchInfo, err := apiClient.GetBucketSwitchStatus(tstCtx, replID)
		if err != nil {
			return false
		}
		return switchInfo.LastStatus > pb.GetBucketSwitchStatusResponse_NotStarted
	}, waitInterval, retryInterval)

	//check that bucket is blocked
	_, err = proxyClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.Error(err)

	// wait for switch to be done
	r.Eventually(func() bool {
		switchInfo, err := apiClient.GetBucketSwitchStatus(tstCtx, replID)
		if err != nil {
			return false
		}
		return switchInfo.LastStatus == pb.GetBucketSwitchStatusResponse_Done
	}, waitInterval, retryInterval)
	// check that data is in sync
	diff, err := apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Bucket:    bucket,
		From:      "main",
		To:        "f1",
		ShowMatch: false,
		User:      user,
	})
	r.NoError(err)
	r.True(diff.IsMatch)

	var objects = []testObj{obj1, obj2, obj3, obj4, obj7}
	for _, object := range objects {
		// f1 equal to actual data
		objData, err := proxyClient.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err := io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = f1Client.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
	}

	//check that writes unblocked and now routed to f1
	// update obj1
	obj1 = getTestObj(obj1.name, obj1.bucket)
	_, err = proxyClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)
	objects[0] = obj1

	// now updates only in f1:
	diff, err = apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Bucket:    bucket,
		From:      "main",
		To:        "f1",
		ShowMatch: false,
		User:      user,
	})
	r.NoError(err)
	r.False(diff.IsMatch)
	// obj1 is different
	r.Len(diff.Differ, 1)
	r.Equal(obj1.name, diff.Differ[0])
	r.Empty(diff.MissTo)
	r.Empty(diff.MissFrom)
	r.Empty(diff.Error)

	for _, object := range objects {
		// check against source
		objData, err := proxyClient.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err := io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = f1Client.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		// in main obj1 is old
		objData, err = mainClient.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		if object.name == obj1.name {
			r.False(bytes.Equal(object.data, objBytes), object.name)
		} else {
			r.True(bytes.Equal(object.data, objBytes), object.name)
		}
	}

	// check that replication is archived
	repls, err = apiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(repls.Replications, 1)
	repl = repls.Replications[0]
	r.True(repl.HasSwitch)
	r.True(repl.IsInitDone)
	r.True(repl.IsArchived)
	r.EqualValues(repl.Events, repl.EventsDone)

}

func TestApi_scheduled_switch_continue_replication(t *testing.T) {
	const (
		waitInterval  = 15 * time.Second
		retryInterval = 100 * time.Millisecond
		bucket        = "switch-bucket-scheduled-continue"
	)

	r := require.New(t)
	exists, err := mainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = f1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = f2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)

	// fill main bucket with init data
	err = mainClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{})
	r.NoError(err)
	defer cleanup(bucket)

	obj1 := getTestObj("obj1", bucket)
	_, err = mainClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj2 := getTestObj("photo/sept/obj2", bucket)
	_, err = mainClient.PutObject(tstCtx, obj2.bucket, obj2.name, bytes.NewReader(obj2.data), int64(len(obj2.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj3 := getTestObj("photo/obj3", bucket)
	_, err = mainClient.PutObject(tstCtx, obj3.bucket, obj3.name, bytes.NewReader(obj3.data), int64(len(obj3.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj4 := getTestObj("obj4", bucket)
	_, err = mainClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)

	exists, err = mainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(exists)
	exists, err = f1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = f2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)

	_, err = apiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		User:            user,
		From:            "main",
		To:              "f1",
		Buckets:         []string{bucket},
		IsForAllBuckets: false,
	})
	r.NoError(err)
	t.Cleanup(func() {
		apiClient.DeleteReplication(tstCtx, &pb.ReplicationRequest{
			User:   user,
			Bucket: bucket,
			From:   "main",
			To:     "f1",
		})
	})
	var repl *pb.Replication
	repls, err := apiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(repls.Replications, 1)
	repl = repls.Replications[0]
	r.False(repl.HasSwitch)
	r.False(repl.IsArchived)

	// wait until repl started
	r.Eventually(func() bool {
		repls, err := apiClient.ListReplications(tstCtx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		started := false
		for _, repl := range repls.Replications {
			if repl.To == "f1" {
				started = started || (repl.InitObjListed > 0)
			}
		}
		return started
	}, waitInterval, retryInterval)

	// perform updates after migration started
	obj7 := getTestObj("photo/sept/obj7", bucket)
	_, err = proxyClient.PutObject(tstCtx, obj7.bucket, obj7.name, bytes.NewReader(obj7.data), int64(len(obj7.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	obj4 = getTestObj(obj4.name, obj4.bucket)
	_, err = proxyClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	// start scheduled switch on init done with continue replication:
	replID := &pb.ReplicationRequest{
		User:   user,
		Bucket: bucket,
		From:   "main",
		To:     "f1",
	}
	_, err = apiClient.SwitchBucket(tstCtx, &pb.SwitchBucketRequest{
		ReplicationId: replID,
		DowntimeOpts: &pb.SwitchDowntimeOpts{
			StartOnInitDone:     true,
			SkipBucketCheck:     true,
			ContinueReplication: true, // continue replication after switch
		},
	})
	r.NoError(err)
	t.Cleanup(func() {
		_, _ = apiClient.DeleteBucketSwitch(tstCtx, replID)
	})

	// check that replication policy now has linked switch
	repls, err = apiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(repls.Replications, 1)
	repl = repls.Replications[0]
	r.True(repl.HasSwitch)
	r.False(repl.IsArchived)

	// wait until switch is in progress
	r.Eventually(func() bool {
		switchInfo, err := apiClient.GetBucketSwitchStatus(tstCtx, replID)
		if err != nil {
			return false
		}
		return switchInfo.LastStatus > pb.GetBucketSwitchStatusResponse_NotStarted
	}, waitInterval, retryInterval)

	//check that bucket is blocked
	_, err = proxyClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.Error(err)

	// wait for switch to be done
	r.Eventually(func() bool {
		switchInfo, err := apiClient.GetBucketSwitchStatus(tstCtx, replID)
		if err != nil {
			return false
		}
		return switchInfo.LastStatus == pb.GetBucketSwitchStatusResponse_Done
	}, waitInterval, retryInterval)
	t.Cleanup(func() {
		apiClient.DeleteReplication(tstCtx, &pb.ReplicationRequest{
			User:   user,
			Bucket: bucket,
			From:   "f1",
			To:     "main",
		})
	})
	// check that data is in sync
	diff, err := apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Bucket:    bucket,
		From:      "main",
		To:        "f1",
		ShowMatch: false,
		User:      user,
	})
	r.NoError(err)
	r.True(diff.IsMatch)

	var objects = []testObj{obj1, obj2, obj3, obj4, obj7}
	for _, object := range objects {
		// f1 equal to actual data
		objData, err := proxyClient.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err := io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = f1Client.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
	}

	//check that writes unblocked and now routed to f1
	// update obj1
	obj1 = getTestObj(obj1.name, obj1.bucket)
	_, err = proxyClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)
	objects[0] = obj1

	// check that there is new replication to previous main
	repls, err = apiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(repls.Replications, 2)

	replPrev, replNew := repls.Replications[0], repls.Replications[1]
	if !replPrev.IsArchived {
		replPrev, replNew = replNew, replPrev
	}
	// validate prev replication
	r.True(replPrev.IsArchived)
	r.NotNil(replPrev.ArchivedAt)
	r.True(replPrev.HasSwitch)
	r.True(replPrev.IsInitDone)
	r.EqualValues(replPrev.Events, replPrev.EventsDone)
	r.EqualValues(replPrev.From, "main")
	r.EqualValues(replPrev.To, "f1")
	r.EqualValues(replPrev.Bucket, bucket)
	r.EqualValues(replPrev.User, user)
	// validate new replication
	r.False(replNew.IsArchived)
	r.True(replNew.IsInitDone)
	r.False(replNew.HasSwitch)
	r.EqualValues(replNew.From, "f1")
	r.EqualValues(replNew.To, "main")
	r.EqualValues(replNew.Bucket, bucket)
	r.EqualValues(replNew.User, user)

	// wait until new replication catch up
	r.Eventually(func() bool {
		repls, err := apiClient.ListReplications(tstCtx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		for _, rr := range repls.Replications {
			if rr.IsArchived {
				continue
			}
			return rr.Events == rr.EventsDone && rr.Events > 0
		}
		return false
	}, waitInterval, retryInterval)

	// now f1 and main are in sync again
	diff, err = apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Bucket:    bucket,
		From:      "main",
		To:        "f1",
		ShowMatch: false,
		User:      user,
	})
	r.NoError(err)
	r.True(diff.IsMatch)

	for _, object := range objects {
		// check against source
		objData, err := proxyClient.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err := io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = f1Client.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		// in main obj1 is old
		objData, err = mainClient.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
	}
	// get switch info with List api method
	switches, err := apiClient.ListReplicationSwitches(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(switches.Switches, 1)
	info := switches.Switches[0]
	r.True(proto.Equal(replID, info.ReplicationId))
	r.EqualValues(pb.GetBucketSwitchStatusResponse_Done, info.LastStatus)

	switchInfo, err := apiClient.GetBucketSwitchStatus(tstCtx, replID)
	r.NoError(err)
	r.True(proto.Equal(switchInfo, info))
}

func sPtr(in string) *string {
	return &in
}
func iPtr(in int64) *int64 {
	return &in
}
