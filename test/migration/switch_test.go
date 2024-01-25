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
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestApi_switch_e2e(t *testing.T) {
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

	// cleanup
	_, _ = apiClient.DeleteUserReplication(tstCtx, &pb.DeleteUserReplicationRequest{
		User:                     user,
		From:                     "main",
		To:                       "f1",
		DeleteBucketReplications: true,
	})
	_, _ = apiClient.DeleteUserReplication(tstCtx, &pb.DeleteUserReplicationRequest{
		User:                     user,
		From:                     "main",
		To:                       "f2",
		DeleteBucketReplications: true,
	})
	_, _ = apiClient.DeleteUserReplication(tstCtx, &pb.DeleteUserReplicationRequest{
		User:                     user,
		From:                     "f1",
		To:                       "f2",
		DeleteBucketReplications: true,
	})
	_, _ = apiClient.DeleteUserReplication(tstCtx, &pb.DeleteUserReplicationRequest{
		User:                     user,
		From:                     "f1",
		To:                       "main",
		DeleteBucketReplications: true,
	})

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

	_, err = apiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		User:            user,
		From:            "main",
		To:              "f2",
		Buckets:         []string{bucket},
		IsForAllBuckets: false,
	})
	r.NoError(err)

	// wait until repl started
	r.Eventually(func() bool {
		repls, err := apiClient.ListReplications(tstCtx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		started := false
		for _, repl := range repls.Replications {
			if repl.To == "f1" || repl.To == "f2" {
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
		f2e, _ := f2Client.BucketExists(tstCtx, bucket)
		return f1e && f2e
	}, waitInterval, retryInterval)

	r.Eventually(func() bool {
		obsf1, _ := listObjects(f1Client, bucket, "")
		obsf2, _ := listObjects(f2Client, bucket, "")
		return len(obsf1) == len(obsf2) && len(objects) == len(obsf2)
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
		if !diff.IsMatch {
			return false
		}
		diff, err = apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    bucket,
			From:      "main",
			To:        "f2",
			ShowMatch: false,
			User:      user,
		})
		if err != nil {
			return false
		}
		return diff.IsMatch
	}, waitInterval, retryInterval)

	writeCtx, cancel := context.WithCancel(tstCtx)
	defer cancel()
	go func() {
		defer cancel()
		t.Log("start write")
		for n := 0; n < 666; n++ {
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
				wg := sync.WaitGroup{}
				for nn := len(partBuf); nn >= len(partBuf); {
					nn, _ = objReader.Read(partBuf)
					dd := make([]byte, nn)
					copy(dd, partBuf[:nn])
					wg.Add(1)
					go func(data []byte, partId int) {
						defer wg.Done()
						partReader := bytes.NewReader(data)

						part, err := proxyAwsClient.UploadPart(&aws_s3.UploadPartInput{
							Body:          partReader,
							Bucket:        sPtr(bucket),
							ContentLength: iPtr(int64(len(data))),
							Key:           sPtr(objects[i].name),
							PartNumber:    iPtr(int64(partId)),
							UploadId:      uploadID.UploadId,
						})

						r.NoError(err)
						parts[partId-1] = &aws_s3.CompletedPart{
							PartNumber: iPtr(int64(partId)),
							ETag:       part.ETag,
						}
					}(dd, partID)
					partID++
				}
				wg.Wait()
				_, err = proxyAwsClient.CompleteMultipartUpload(&aws_s3.CompleteMultipartUploadInput{
					Bucket:          sPtr(bucket),
					Key:             sPtr(objects[i].name),
					MultipartUpload: &aws_s3.CompletedMultipartUpload{Parts: parts},
					UploadId:        uploadID.UploadId,
				})
				r.NoError(err)
			}
		}
		t.Log("end write")
	}()
	time.Sleep(time.Second)
	_, err = apiClient.SwitchMainBucket(tstCtx, &pb.SwitchMainBucketRequest{
		User:    user,
		Bucket:  bucket,
		NewMain: "f1",
	})
	r.NoError(err)
	t.Log("switch done")
	<-writeCtx.Done()

	r.Eventually(func() bool {
		diff, err := apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    bucket,
			From:      "f1",
			To:        "f2",
			ShowMatch: true,
			User:      user,
		})
		if err != nil {
			return false
		}
		return diff.IsMatch
	}, waitInterval, retryInterval)

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
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
		objData, err = f1Client.GetObject(tstCtx, bucket, object.name, mclient.GetObjectOptions{})
		r.NoError(err, object.name)
		objBytes, err = io.ReadAll(objData)
		r.NoError(err)
		r.True(bytes.Equal(object.data, objBytes), object.name)
	}
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

	// cleanup
	_, _ = apiClient.DeleteUserReplication(tstCtx, &pb.DeleteUserReplicationRequest{
		User:                     user,
		From:                     "main",
		To:                       "f1",
		DeleteBucketReplications: true,
	})
	_, _ = apiClient.DeleteUserReplication(tstCtx, &pb.DeleteUserReplicationRequest{
		User:                     user,
		From:                     "main",
		To:                       "f2",
		DeleteBucketReplications: true,
	})
	_, _ = apiClient.DeleteUserReplication(tstCtx, &pb.DeleteUserReplicationRequest{
		User:                     user,
		From:                     "f1",
		To:                       "f2",
		DeleteBucketReplications: true,
	})
	_, _ = apiClient.DeleteUserReplication(tstCtx, &pb.DeleteUserReplicationRequest{
		User:                     user,
		From:                     "f1",
		To:                       "main",
		DeleteBucketReplications: true,
	})

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

	_, err = apiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		User:            user,
		From:            "main",
		To:              "f2",
		Buckets:         []string{bucket},
		IsForAllBuckets: false,
	})
	r.NoError(err)

	// wait until repl started
	r.Eventually(func() bool {
		repls, err := apiClient.ListReplications(tstCtx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		started := false
		for _, repl := range repls.Replications {
			if repl.To == "f1" || repl.To == "f2" {
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
		f2e, _ := f2Client.BucketExists(tstCtx, bucket)
		return f1e && f2e
	}, waitInterval, retryInterval)

	r.Eventually(func() bool {
		obsf1, _ := listObjects(f1Client, bucket, "")
		obsf2, _ := listObjects(f2Client, bucket, "")
		return len(obsf1) == len(obsf2) && len(objects) == len(obsf2)
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
		if !diff.IsMatch {
			return false
		}
		diff, err = apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    bucket,
			From:      "main",
			To:        "f2",
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

	parts := make([]*aws_s3.CompletedPart, pn)
	for n := len(partBuf); n >= len(partBuf); {
		if (partID - 1) == pn/2 {
			_, err = apiClient.SwitchMainBucket(tstCtx, &pb.SwitchMainBucketRequest{
				User:    user,
				Bucket:  bucket,
				NewMain: "f1",
			})
			r.NoError(err)
			t.Log("switch done", partID)
		}

		n, _ = objReader.Read(partBuf)
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
		diff, err := apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    bucket,
			From:      "f1",
			To:        "f2",
			ShowMatch: true,
			User:      user,
		})
		if err != nil {
			return false
		}
		return diff.IsMatch
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

func sPtr(in string) *string {
	return &in
}
func iPtr(in int64) *int64 {
	return &in
}