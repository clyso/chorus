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

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/test/app"
)

func TestApi_ZeroDowntimeSwitch(t *testing.T) {
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	var (
		bucket = "switch-bucket"
	)

	r := require.New(t)
	exists, err := e.MainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)

	// fill main bucket with init data
	err = e.MainClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{})
	r.NoError(err)

	obj1 := getTestObj("obj1", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj2 := getTestObj("photo/sept/obj2", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj2.bucket, obj2.name, bytes.NewReader(obj2.data), int64(len(obj2.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj3 := getTestObj("photo/obj3", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj3.bucket, obj3.name, bytes.NewReader(obj3.data), int64(len(obj3.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj4 := getTestObj("obj4", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj5 := getTestObj("obj5", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj5.bucket, obj5.name, bytes.NewReader(obj5.data), int64(len(obj5.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj6 := getTestObj("obj6", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj6.bucket, obj6.name, bytes.NewReader(obj6.data), int64(len(obj6.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)

	exists, err = e.MainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(exists)
	exists, err = e.F1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)

	replID := &pb.ReplicationID{
		User:        user,
		FromStorage: "main",
		ToStorage:   "f1",
		FromBucket:  &bucket,
		ToBucket:    &bucket,
	}
	_, err = e.PolicyClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		Id: replID,
	})
	r.NoError(err)

	// wait until repl started
	r.Eventually(func() bool {
		repl, err := e.PolicyClient.GetReplication(tstCtx, replID)
		if err != nil {
			return false
		}
		started := repl.InitObjListed > 0
		return started
	}, e.WaitLong, e.RetryLong)

	// perform updates after migration started
	obj7 := getTestObj("photo/sept/obj7", bucket)
	_, err = e.ProxyClient.PutObject(tstCtx, obj7.bucket, obj7.name, bytes.NewReader(obj7.data), int64(len(obj7.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	obj8 := getTestObj("obj8", bucket)
	_, err = e.ProxyClient.PutObject(tstCtx, obj8.bucket, obj8.name, bytes.NewReader(obj8.data), int64(len(obj8.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	obj1 = getTestObj(obj1.name, obj1.bucket)
	_, err = e.ProxyClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	obj4 = getTestObj(obj4.name, obj4.bucket)
	_, err = e.ProxyClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	var objects = []testObj{obj1, obj2, obj3, obj4, obj5, obj6, obj7, obj8}

	// check that storages are in sync
	r.Eventually(func() bool {
		f1e, _ := e.F1Client.BucketExists(tstCtx, bucket)
		return f1e
	}, e.WaitLong, e.RetryLong)

	r.Eventually(func() bool {
		obsf1, _ := listObjects(e.F1Client, bucket, "")
		return len(objects) == len(obsf1)
	}, e.WaitLong, e.RetryLong)

	// wait init replication is done
	r.Eventually(func() bool {
		repl, err := e.PolicyClient.GetReplication(tstCtx, replID)
		if err != nil {
			return false
		}
		return repl.IsInitDone
	}, e.WaitLong, e.RetryLong)

	r.Eventually(func() bool {
		diff, err := e.PolicyClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Target: replID,
		})
		if err != nil {
			return false
		}
		return diff.IsMatch
	}, e.WaitLong, e.RetryLong)

	repl, err := e.PolicyClient.GetReplication(tstCtx, replID)
	r.NoError(err)
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
				_ = e.ProxyClient.RemoveObject(writeCtx, bucket, objects[i].name, mclient.RemoveObjectOptions{})
				objects[i].data = nil
			case 1:
				// upload
				objects[i] = getTestObj(objects[i].name, objects[i].bucket)
				_, err := e.ProxyClient.PutObject(writeCtx, objects[i].bucket, objects[i].name, bytes.NewReader(objects[i].data), int64(len(objects[i].data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
				r.NoError(err)
			case 2:
				// multipart upload
				objects[i] = getTestObj(objects[i].name, objects[i].bucket)

				uploadID, err := e.ProxyAwsClient.CreateMultipartUpload(&aws_s3.CreateMultipartUploadInput{
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
					uplID := *uploadID.UploadId
					g.Go(func() error {
						partReader := bytes.NewReader(dd)
						part, err := e.ProxyAwsClient.UploadPart(&aws_s3.UploadPartInput{
							Body:          partReader,
							Bucket:        sPtr(bucket),
							ContentLength: iPtr(int64(len(dd))),
							Key:           sPtr(objects[i].name),
							PartNumber:    iPtr(int64(partIDCopy)),
							UploadId:      &uplID,
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
				_, err = e.ProxyAwsClient.CompleteMultipartUpload(&aws_s3.CompleteMultipartUploadInput{
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
	_, err = e.PolicyClient.SwitchWithZeroDowntime(tstCtx, &pb.SwitchZeroDowntimeRequest{
		ReplicationId: replID,
		MultipartTtl:  durationpb.New(time.Minute),
	})
	r.NoError(err)
	t.Log("switch created", time.Now())

	repl = nil
	repls, err := e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	for i, rr := range repls.Replications {
		if *rr.Id.FromBucket == bucket && rr.Id.FromStorage == "main" && rr.Id.ToStorage == "f1" {
			repl = repls.Replications[i]
			break
		}
	}
	r.NotNil(repl)
	r.True(repl.HasSwitch)

	switchInfo, err := e.PolicyClient.GetSwitchStatus(tstCtx, replID)
	r.NoError(err)
	r.EqualValues(pb.ReplicationSwitch_IN_PROGRESS, switchInfo.LastStatus)
	r.Nil(switchInfo.DowntimeOpts)
	r.NotNil(switchInfo.LastStartedAt)
	r.EqualValues(time.Minute, switchInfo.MultipartTtl.AsDuration())
	r.True(proto.Equal(replID, switchInfo.ReplicationId))
	r.True(switchInfo.ZeroDowntime)
	<-writeCtx.Done()

	r.Eventually(func() bool {
		switchInfo, err = e.PolicyClient.GetSwitchStatus(tstCtx, replID)
		if err != nil {
			return false
		}
		return switchInfo.LastStatus == pb.ReplicationSwitch_DONE
	}, e.WaitLong*2, e.RetryLong)

	r.Eventually(func() bool {
		// wait for completion of multipart uploads to old storage started before switch.
		repl, err = e.PolicyClient.GetReplication(tstCtx, replID)
		if err != nil {
			return false
		}
		return repl.Events == repl.EventsDone
	}, e.WaitLong, e.RetryLong)

	r.NotNil(repl)
	r.True(repl.HasSwitch)
	r.True(repl.IsInitDone)
	r.EqualValues(repl.Events, repl.EventsDone)
	t.Log("switch done", repl.Events, repl.EventsDone)

	for _, object := range objects {
		if object.data == nil {
			_, err = e.ProxyClient.StatObject(tstCtx, bucket, object.name, mclient.StatObjectOptions{})
			r.Error(err, object.name)
			_, err = e.F1Client.StatObject(tstCtx, bucket, object.name, mclient.StatObjectOptions{})
			r.Error(err, object.name)
			continue
		}
		objData, err := e.ProxyClient.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err := io.ReadAll(objData)
		r.NoError(err, object.name)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = e.F1Client.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err, object.name)
		r.True(bytes.Equal(object.data, objBytes), object.name)
	}

	switchInfo, err = e.PolicyClient.GetSwitchStatus(tstCtx, replID)
	r.NoError(err)
	r.EqualValues(pb.ReplicationSwitch_DONE, switchInfo.LastStatus)
	r.Nil(switchInfo.DowntimeOpts)
	r.NotNil(switchInfo.LastStartedAt)
	r.NotNil(switchInfo.DoneAt)
	r.EqualValues(time.Minute, switchInfo.MultipartTtl.AsDuration())
	r.True(proto.Equal(replID, switchInfo.ReplicationId))
	r.True(switchInfo.ZeroDowntime)
	r.NotEmpty(switchInfo.History)
}

func TestApi_switch_multipart(t *testing.T) {
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	var (
		bucket = "switch-bucket-multipart"
	)

	r := require.New(t)
	exists, err := e.MainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)

	// fill main bucket with init data
	err = e.MainClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{})
	r.NoError(err)

	obj1 := getTestObj("obj1", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj2 := getTestObj("photo/sept/obj2", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj2.bucket, obj2.name, bytes.NewReader(obj2.data), int64(len(obj2.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj3 := getTestObj("photo/obj3", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj3.bucket, obj3.name, bytes.NewReader(obj3.data), int64(len(obj3.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj4 := getTestObj("obj4", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)

	exists, err = e.MainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(exists)
	exists, err = e.F1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)

	replID := &pb.ReplicationID{
		User:        user,
		FromBucket:  &bucket,
		FromStorage: "main",
		ToStorage:   "f1",
		ToBucket:    &bucket,
	}
	_, err = e.PolicyClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		Id: replID,
	})
	r.NoError(err)

	// wait until repl started
	r.Eventually(func() bool {
		repl, err := e.PolicyClient.GetReplication(tstCtx, replID)
		if err != nil {
			return false
		}
		started := repl.InitObjListed > 0
		return started
	}, e.WaitLong, e.RetryLong)

	// perform updates after migration started
	obj7 := getTestObj("photo/sept/obj7", bucket)
	_, err = e.ProxyClient.PutObject(tstCtx, obj7.bucket, obj7.name, bytes.NewReader(obj7.data), int64(len(obj7.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	obj4 = getTestObj(obj4.name, obj4.bucket)
	_, err = e.ProxyClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	var objects = []testObj{obj1, obj2, obj3, obj4, obj7}

	// check that storages are in sync
	r.Eventually(func() bool {
		f1e, _ := e.F1Client.BucketExists(tstCtx, bucket)
		return f1e
	}, e.WaitLong, e.RetryLong)

	f1Stream, err := e.PolicyClient.StreamReplication(tstCtx, replID)
	r.NoError(err)
	defer f1Stream.CloseSend()
	r.Eventually(func() bool {
		m, err := f1Stream.Recv()
		if err != nil {
			return false
		}
		return m.IsInitDone
	}, e.WaitLong, e.RetryLong)

	r.Eventually(func() bool {
		diff, err := e.PolicyClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Target: replID,
		})
		if err != nil {
			return false
		}
		return diff.IsMatch
	}, e.WaitLong, e.RetryLong)

	// start multipart upload
	// multipart upload
	obj4 = getTestObj(obj4.name, obj4.bucket)
	// !!! don't forget to update list of objects for final check
	objects = []testObj{obj1, obj2, obj3, obj4, obj7}

	uploadID, err := e.ProxyAwsClient.CreateMultipartUpload(&aws_s3.CreateMultipartUploadInput{
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

	parts := make([]*aws_s3.CompletedPart, pn)
	for n := len(partBuf); n >= len(partBuf); {
		if (partID - 1) == pn/2 {
			_, err = e.PolicyClient.SwitchWithZeroDowntime(tstCtx, &pb.SwitchZeroDowntimeRequest{
				ReplicationId: replID,
				MultipartTtl:  durationpb.New(time.Minute),
			})
			r.NoError(err)
			t.Log("switch started", partID)
		}

		n, _ = objReader.Read(partBuf)
		if n == 0 {
			break
		}
		partReader := bytes.NewReader(partBuf[:n])
		part, err := e.ProxyAwsClient.UploadPart(&aws_s3.UploadPartInput{
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

	_, err = e.ProxyAwsClient.CompleteMultipartUpload(&aws_s3.CompleteMultipartUploadInput{
		Bucket:          sPtr(bucket),
		Key:             sPtr(obj4.name),
		MultipartUpload: &aws_s3.CompletedMultipartUpload{Parts: parts},
		UploadId:        uploadID.UploadId,
	})
	r.NoError(err)

	r.Eventually(func() bool {
		switchInfo, err := e.PolicyClient.GetSwitchStatus(tstCtx, replID)
		if err != nil {
			return false
		}
		return switchInfo.LastStatus == pb.ReplicationSwitch_DONE
	}, e.WaitLong*2, e.RetryLong)

	r.Eventually(func() bool {
		// wait for completion of multipart uploads to old storage started before switch.
		repl, err := e.PolicyClient.GetReplication(tstCtx, replID)
		if err != nil {
			return false
		}
		return repl.Events == repl.EventsDone
	}, e.WaitLong, e.RetryLong)

	for _, object := range objects {
		objData, err := e.ProxyClient.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err := io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = e.F1Client.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
	}
}

func TestApi_scheduled_switch(t *testing.T) {
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	var (
		bucket = "switch-bucket-scheduled"
	)

	r := require.New(t)
	exists, err := e.MainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)

	// fill main bucket with init data
	err = e.MainClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{})
	r.NoError(err)

	obj1 := getTestObj("obj1", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj2 := getTestObj("photo/sept/obj2", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj2.bucket, obj2.name, bytes.NewReader(obj2.data), int64(len(obj2.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj3 := getTestObj("photo/obj3", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj3.bucket, obj3.name, bytes.NewReader(obj3.data), int64(len(obj3.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj4 := getTestObj("obj4", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)

	exists, err = e.MainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(exists)
	exists, err = e.F1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)

	replID := &pb.ReplicationID{
		User:        user,
		FromBucket:  &bucket,
		ToBucket:    &bucket,
		FromStorage: "main",
		ToStorage:   "f1",
	}
	_, err = e.PolicyClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		Id: replID,
	})
	r.NoError(err)
	var repl *pb.Replication
	repls, err := e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	r.Len(repls.Replications, 1)
	repl = repls.Replications[0]
	r.False(repl.HasSwitch)
	r.False(repl.IsArchived)

	// wait until repl started
	r.Eventually(func() bool {
		repls, err := e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
		if err != nil {
			return false
		}
		started := false
		for _, repl := range repls.Replications {
			if repl.Id.ToStorage == "f1" {
				started = started || (repl.InitObjListed > 0)
			}
		}
		return started
	}, e.WaitLong, e.RetryLong)

	// perform updates after migration started
	obj7 := getTestObj("photo/sept/obj7", bucket)
	_, err = e.ProxyClient.PutObject(tstCtx, obj7.bucket, obj7.name, bytes.NewReader(obj7.data), int64(len(obj7.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	obj4 = getTestObj(obj4.name, obj4.bucket)
	_, err = e.ProxyClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	// start scheduled switch on init done:
	_, err = e.PolicyClient.SwitchWithDowntime(tstCtx, &pb.SwitchDowntimeRequest{
		ReplicationId: replID,
		DowntimeOpts: &pb.SwitchDowntimeOpts{
			StartOnInitDone:     true,
			SkipBucketCheck:     true,
			ContinueReplication: false,
		},
	})
	r.NoError(err)

	// check that replication policy now has linked switch
	repls, err = e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	r.Len(repls.Replications, 1)
	repl = repls.Replications[0]
	r.True(repl.HasSwitch)
	r.False(repl.IsArchived)

	// wait until switch is in progress
	r.Eventually(func() bool {
		switchInfo, err := e.PolicyClient.GetSwitchStatus(tstCtx, replID)
		if err != nil {
			return false
		}
		return switchInfo.LastStatus > pb.ReplicationSwitch_NOT_STARTED
	}, e.WaitLong, e.RetryLong)

	//check that bucket is blocked
	_, err = e.ProxyClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.Error(err)

	// wait for switch to be done
	r.Eventually(func() bool {
		switchInfo, err := e.PolicyClient.GetSwitchStatus(tstCtx, replID)
		if err != nil {
			return false
		}
		return switchInfo.LastStatus == pb.ReplicationSwitch_DONE
	}, e.WaitLong*2, e.RetryLong)
	// check that data is in sync
	diff, err := e.PolicyClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Target: replID,
	})
	r.NoError(err)
	r.True(diff.IsMatch)

	var objects = []testObj{obj1, obj2, obj3, obj4, obj7}
	for _, object := range objects {
		// f1 equal to actual data
		objData, err := e.ProxyClient.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err := io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = e.F1Client.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
	}

	//check that writes unblocked and now routed to f1
	// update obj1
	obj1 = getTestObj(obj1.name, obj1.bucket)
	_, err = e.ProxyClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)
	objects[0] = obj1

	// now updates only in f1:
	diff, err = e.PolicyClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Target: replID,
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
		objData, err := e.ProxyClient.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err := io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = e.F1Client.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		// in main obj1 is old
		objData, err = e.MainClient.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
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
	repls, err = e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	r.Len(repls.Replications, 1)
	repl = repls.Replications[0]
	r.True(repl.HasSwitch)
	r.True(repl.IsInitDone)
	r.True(repl.IsArchived)
	r.EqualValues(repl.Events, repl.EventsDone)

}

func TestApi_scheduled_switch_continue_replication(t *testing.T) {
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	var (
		bucket = "switch-bucket-scheduled-continue"
	)

	r := require.New(t)
	exists, err := e.MainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)

	// fill main bucket with init data
	err = e.MainClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{})
	r.NoError(err)

	obj1 := getTestObj("obj1", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj2 := getTestObj("photo/sept/obj2", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj2.bucket, obj2.name, bytes.NewReader(obj2.data), int64(len(obj2.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj3 := getTestObj("photo/obj3", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj3.bucket, obj3.name, bytes.NewReader(obj3.data), int64(len(obj3.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj4 := getTestObj("obj4", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)

	exists, err = e.MainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(exists)
	exists, err = e.F1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F2Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(exists)

	replID := &pb.ReplicationID{
		User:        user,
		FromBucket:  &bucket,
		ToBucket:    &bucket,
		FromStorage: "main",
		ToStorage:   "f1",
	}
	_, err = e.PolicyClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		Id: replID,
	})
	r.NoError(err)
	var repl *pb.Replication
	repls, err := e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	r.Len(repls.Replications, 1)
	repl = repls.Replications[0]
	r.False(repl.HasSwitch)
	r.False(repl.IsArchived)

	// wait until repl started
	r.Eventually(func() bool {
		repls, err := e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
		if err != nil {
			return false
		}
		started := false
		for _, repl := range repls.Replications {
			if repl.Id.ToStorage == "f1" {
				started = started || (repl.InitObjListed > 0)
			}
		}
		return started
	}, e.WaitLong, e.RetryLong)

	// perform updates after migration started
	obj7 := getTestObj("photo/sept/obj7", bucket)
	_, err = e.ProxyClient.PutObject(tstCtx, obj7.bucket, obj7.name, bytes.NewReader(obj7.data), int64(len(obj7.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	obj4 = getTestObj(obj4.name, obj4.bucket)
	_, err = e.ProxyClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	// start scheduled switch on init done with continue replication:
	_, err = e.PolicyClient.SwitchWithDowntime(tstCtx, &pb.SwitchDowntimeRequest{
		ReplicationId: replID,
		DowntimeOpts: &pb.SwitchDowntimeOpts{
			StartOnInitDone:     true,
			SkipBucketCheck:     true,
			ContinueReplication: true, // continue replication after switch
		},
	})
	r.NoError(err)

	// check that replication policy now has linked switch
	repls, err = e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	r.Len(repls.Replications, 1)
	repl = repls.Replications[0]
	r.True(repl.HasSwitch)
	r.False(repl.IsArchived)

	// wait until switch is in progress
	r.Eventually(func() bool {
		switchInfo, err := e.PolicyClient.GetSwitchStatus(tstCtx, replID)
		if err != nil {
			return false
		}
		return switchInfo.LastStatus > pb.ReplicationSwitch_NOT_STARTED
	}, e.WaitLong, e.RetryLong)

	//check that bucket is blocked
	_, err = e.ProxyClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.Error(err)

	// wait for switch to be done
	r.Eventually(func() bool {
		switchInfo, err := e.PolicyClient.GetSwitchStatus(tstCtx, replID)
		if err != nil {
			return false
		}
		return switchInfo.LastStatus == pb.ReplicationSwitch_DONE
	}, e.WaitLong*2, e.RetryLong)
	// check that data is in sync
	diff, err := e.PolicyClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Target: replID,
	})
	r.NoError(err)
	r.True(diff.IsMatch)

	var objects = []testObj{obj1, obj2, obj3, obj4, obj7}
	for _, object := range objects {
		// f1 equal to actual data
		objData, err := e.ProxyClient.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err := io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = e.F1Client.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
	}

	//check that writes unblocked and now routed to f1
	// update obj1
	obj1 = getTestObj(obj1.name, obj1.bucket)
	_, err = e.ProxyClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)
	objects[0] = obj1

	// check that there is new replication to previous main
	repls, err = e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
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
	r.EqualValues(replPrev.Id.FromStorage, "main")
	r.EqualValues(replPrev.Id.ToStorage, "f1")
	r.EqualValues(*replPrev.Id.FromBucket, bucket)
	r.EqualValues(replPrev.Id.User, user)
	// validate new replication
	r.False(replNew.IsArchived)
	r.True(replNew.IsInitDone)
	r.True(replNew.HasSwitch)
	r.EqualValues(replNew.Id.FromStorage, "f1")
	r.EqualValues(replNew.Id.ToStorage, "main")
	r.EqualValues(*replNew.Id.FromBucket, bucket)
	r.EqualValues(replNew.Id.User, user)

	// wait until new replication catch up
	r.Eventually(func() bool {
		repls, err := e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
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
	}, e.WaitLong, e.RetryLong)

	// now f1 and main are in sync again
	diff, err = e.PolicyClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Target: replID,
	})
	r.NoError(err)
	r.True(diff.IsMatch)

	for _, object := range objects {
		// check against source
		objData, err := e.ProxyClient.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err := io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = e.F1Client.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		// in main obj1 is old
		objData, err = e.MainClient.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
	}
	switchInfo, err := e.PolicyClient.GetSwitchStatus(tstCtx, replID)
	r.NoError(err)
	r.True(proto.Equal(replID, switchInfo.ReplicationId))
	r.EqualValues(pb.ReplicationSwitch_DONE, switchInfo.LastStatus)
}

func Test_User_switch(t *testing.T) {
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	var (
		bucket1 = "user-switch-1"
		bucket2 = "user-switch-2"
	)

	r := require.New(t)
	exists, err := e.MainClient.BucketExists(tstCtx, bucket1)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F1Client.BucketExists(tstCtx, bucket1)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F2Client.BucketExists(tstCtx, bucket1)
	r.NoError(err)
	r.False(exists)
	exists, err = e.MainClient.BucketExists(tstCtx, bucket2)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F1Client.BucketExists(tstCtx, bucket2)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F2Client.BucketExists(tstCtx, bucket2)
	r.NoError(err)
	r.False(exists)

	replID := &pb.ReplicationID{
		User:        user,
		FromStorage: "main",
		ToStorage:   "f1",
	}
	_, err = e.PolicyClient.GetReplication(tstCtx, replID)
	r.Error(err)

	// fill main buckets with init data
	err = e.MainClient.MakeBucket(tstCtx, bucket1, mclient.MakeBucketOptions{})
	r.NoError(err)
	// bucket1: obj1, obj2, obj3
	obj1 := getTestObj("obj1", bucket1)
	_, err = e.MainClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj2 := getTestObj("photo/sept/obj2", bucket1)
	_, err = e.MainClient.PutObject(tstCtx, obj2.bucket, obj2.name, bytes.NewReader(obj2.data), int64(len(obj2.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj3 := getTestObj("photo/obj3", bucket1)
	_, err = e.MainClient.PutObject(tstCtx, obj3.bucket, obj3.name, bytes.NewReader(obj3.data), int64(len(obj3.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)

	err = e.MainClient.MakeBucket(tstCtx, bucket2, mclient.MakeBucketOptions{})
	r.NoError(err)
	// bucket2: obj1 (same as in bucket1),  obj4
	_, err = e.MainClient.PutObject(tstCtx, bucket2, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj4 := getTestObj("photo/obj3", bucket2)
	_, err = e.MainClient.PutObject(tstCtx, obj4.bucket, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)

	// check buckets exists only in main
	exists, err = e.MainClient.BucketExists(tstCtx, bucket1)
	r.NoError(err)
	r.True(exists)
	exists, err = e.F1Client.BucketExists(tstCtx, bucket1)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F2Client.BucketExists(tstCtx, bucket1)
	r.NoError(err)
	r.False(exists)
	exists, err = e.MainClient.BucketExists(tstCtx, bucket2)
	r.NoError(err)
	r.True(exists)
	exists, err = e.F1Client.BucketExists(tstCtx, bucket2)
	r.NoError(err)
	r.False(exists)
	exists, err = e.F2Client.BucketExists(tstCtx, bucket2)
	r.NoError(err)
	r.False(exists)

	// create replication for user
	_, err = e.PolicyClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		Id: replID,
	})
	r.NoError(err)
	// get replication from list
	repls, err := e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	r.Len(repls.Replications, 1)
	userRepl := repls.Replications[0]
	r.False(userRepl.HasSwitch)
	r.False(userRepl.IsArchived)

	repls, err = e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{
		HideUserReplications:   false,
		HideBucketReplications: true,
	})
	r.NoError(err)
	r.Len(repls.Replications, 1)
	repls, err = e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{
		HideUserReplications:   true,
		HideBucketReplications: false,
	})
	r.NoError(err)
	r.Len(repls.Replications, 0)

	// get replication by id
	repl, err := e.PolicyClient.GetReplication(tstCtx, replID)
	r.NoError(err)
	r.EqualValues(user, repl.Id.User)
	r.EqualValues("main", repl.Id.FromStorage)
	r.EqualValues("f1", repl.Id.ToStorage)
	r.Nil(repl.Id.FromBucket)
	r.Nil(repl.Id.ToBucket)
	r.False(repl.HasSwitch)
	r.False(repl.IsArchived)
	r.NotNil(repl.CreatedAt)

	// wait until repl started
	r.Eventually(func() bool {
		repl, err = e.PolicyClient.GetReplication(tstCtx, replID)
		if err != nil {
			return false
		}
		return repl.InitObjListed > 0
	}, e.WaitLong, e.RetryLong)

	// perform updates after migration started

	// bucket1: obj1, obj2, obj3  + obj5 (new) + obj3 (updated) - obj2 (deleted)
	obj5 := getTestObj("photo/sept/obj5", bucket1)
	_, err = e.ProxyClient.PutObject(tstCtx, obj5.bucket, obj5.name, bytes.NewReader(obj5.data), int64(len(obj5.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)
	obj3 = getTestObj("photo/obj3", bucket1)
	_, err = e.ProxyClient.PutObject(tstCtx, obj3.bucket, obj3.name, bytes.NewReader(obj3.data), int64(len(obj3.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	err = e.ProxyClient.RemoveObject(tstCtx, bucket1, obj2.name, mclient.RemoveObjectOptions{})
	r.NoError(err)

	// bucket2: obj1 (same as in bucket1),  obj4  + obj6 (new)
	obj6 := getTestObj("photo/sept/obj6", bucket2)
	_, err = e.ProxyClient.PutObject(tstCtx, obj6.bucket, obj6.name, bytes.NewReader(obj6.data), int64(len(obj6.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)
	r.NoError(err)

	// start scheduled switch on init done:
	_, err = e.PolicyClient.SwitchWithDowntime(tstCtx, &pb.SwitchDowntimeRequest{
		ReplicationId: replID,
		DowntimeOpts: &pb.SwitchDowntimeOpts{
			StartOnInitDone:     true,
			SkipBucketCheck:     true,
			ContinueReplication: false,
		},
	})
	r.NoError(err)

	// check that replication policy now has linked switch
	repl, err = e.PolicyClient.GetReplication(tstCtx, replID)
	r.NoError(err)
	r.True(repl.HasSwitch)
	r.False(repl.IsArchived)

	// wait until switch is in progress
	r.Eventually(func() bool {
		switchInfo, err := e.PolicyClient.GetSwitchStatus(tstCtx, replID)
		if err != nil {
			return false
		}
		return switchInfo.LastStatus > pb.ReplicationSwitch_NOT_STARTED
	}, e.WaitLong, e.RetryLong)

	//check that bucket1 is blocked
	_, err = e.ProxyClient.PutObject(tstCtx, bucket1, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.Error(err)
	//check that bucket2 is blocked
	_, err = e.ProxyClient.PutObject(tstCtx, bucket2, obj4.name, bytes.NewReader(obj4.data), int64(len(obj4.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.Error(err)

	// wait for switch to be done
	r.Eventually(func() bool {
		switchInfo, err := e.PolicyClient.GetSwitchStatus(tstCtx, replID)
		if err != nil {
			return false
		}
		return switchInfo.LastStatus == pb.ReplicationSwitch_DONE
	}, e.WaitLong*2, e.RetryLong)

	// check that all events are processed
	repl, err = e.PolicyClient.GetReplication(tstCtx, replID)
	r.NoError(err)
	r.True(repl.HasSwitch)
	r.True(repl.IsInitDone)
	r.EqualValues(repl.Events, repl.EventsDone)

	// check that data is in sync
	b1ID := proto.Clone(replID).(*pb.ReplicationID)
	b1ID.ToBucket = &bucket1
	b1ID.FromBucket = &bucket1
	diff, err := e.PolicyClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Target: b1ID,
	})
	r.NoError(err)
	r.True(diff.IsMatch)
	b2ID := proto.Clone(replID).(*pb.ReplicationID)
	b2ID.ToBucket = &bucket2
	b2ID.FromBucket = &bucket2
	diff, err = e.PolicyClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
		Target: b2ID,
	})
	r.NoError(err)
	r.True(diff.IsMatch)

	// bucket1: obj1,  obj3  obj5
	var objects = []testObj{obj1, obj3, obj5}
	for _, object := range objects {
		// f1 equal to actual data
		objData, err := e.ProxyClient.GetObject(tstCtx, bucket1, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err := io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = e.F1Client.GetObject(tstCtx, bucket1, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = e.MainClient.GetObject(tstCtx, bucket1, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
	}
	b1ObjF1, err := listObjects(e.F1Client, bucket1, "")
	r.NoError(err)
	r.Len(b1ObjF1, 3)
	b1ObjM, err := listObjects(e.MainClient, bucket1, "")
	r.NoError(err)
	r.Len(b1ObjM, 3)
	b1ObjP, err := listObjects(e.ProxyClient, bucket1, "")
	r.NoError(err)
	r.Len(b1ObjP, 3)
	r.ElementsMatch(b1ObjF1, b1ObjM)

	// bucket2: obj1 (same as in bucket1),  obj4  + obj6 (new)
	objects = []testObj{obj1, obj4, obj6}
	for _, object := range objects {
		// f1 equal to actual data
		objData, err := e.ProxyClient.GetObject(tstCtx, bucket2, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err := io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = e.F1Client.GetObject(tstCtx, bucket2, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = e.MainClient.GetObject(tstCtx, bucket2, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
	}
	b2ObjF1, err := listObjects(e.F1Client, bucket2, "")
	r.NoError(err)
	r.Len(b2ObjF1, 3)
	b2ObjM, err := listObjects(e.MainClient, bucket2, "")
	r.NoError(err)
	r.Len(b2ObjM, 3)
	b2ObjP, err := listObjects(e.ProxyClient, bucket2, "")
	r.NoError(err)
	r.Len(b2ObjP, 3)
	r.ElementsMatch(b2ObjF1, b2ObjM)

	//check that writes unblocked and now routed to f1
	obj7 := getTestObj("obj7", bucket2)
	_, err = e.ProxyClient.PutObject(tstCtx, obj7.bucket, obj7.name, bytes.NewReader(obj7.data), int64(len(obj7.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)
	// obj only in f1
	objData, err := e.ProxyClient.GetObject(tstCtx, bucket2, obj7.name, mclient.GetObjectOptions{})
	r.NoError(err)
	objBytes, err := io.ReadAll(objData)
	r.NoError(err)
	r.True(bytes.Equal(obj7.data, objBytes))
	objData, err = e.F1Client.GetObject(tstCtx, bucket2, obj7.name, mclient.GetObjectOptions{})
	r.NoError(err)
	objBytes, err = io.ReadAll(objData)
	r.NoError(err)
	r.True(bytes.Equal(obj7.data, objBytes))
	// not in main
	objData, err = e.MainClient.GetObject(tstCtx, bucket2, obj7.name, mclient.GetObjectOptions{})
	r.NoError(err)
	objBytes, err = io.ReadAll(objData)
	r.Error(err)

	// check that replication is archived
	repl, err = e.PolicyClient.GetReplication(tstCtx, replID)
	r.NoError(err)
	r.True(repl.HasSwitch)
	r.True(repl.IsInitDone)
	r.True(repl.IsArchived)
	r.EqualValues(repl.Events, repl.EventsDone)
}

func sPtr(in string) *string {
	return &in
}
func iPtr(in int64) *int64 {
	return &in
}
