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

package test

import (
	"bytes"
	"io"
	"math/rand"
	"testing"
	"time"

	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
)

func TestApi_Object_CRUD(t *testing.T) {
	t.Parallel()
	bucket := "object-crud"
	r := require.New(t)

	err := proxyClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)
	t.Cleanup(func() {
		cleanup(t, false, bucket)
	})
	ok, err := proxyClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(ok)

	r.Eventually(func() bool {
		ok, err = mainClient.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = f1Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = f2Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		return true
	}, time.Second*3, time.Millisecond*100)

	objName := "obj-crud"
	_, err = mainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = f1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = f2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = proxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)

	source := bytes.Repeat([]byte("3"), rand.Intn(1<<20)+32*1024)

	putInfo, err := proxyClient.PutObject(tstCtx, bucket, objName, bytes.NewReader(source), int64(len(source)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)
	r.EqualValues(objName, putInfo.Key)
	r.EqualValues(bucket, putInfo.Bucket)

	obj, err := proxyClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)

	objBytes, err := io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(source, objBytes)

	_, err = proxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.NoError(err)

	r.Eventually(func() bool {
		_, err = mainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = f1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = f2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		return true
	}, time.Second*3, time.Millisecond*100)

	obj, err = mainClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)
	objBytes, err = io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(source, objBytes)

	obj, err = f1Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)
	objBytes, err = io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(source, objBytes)

	obj, err = f2Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)
	objBytes, err = io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(source, objBytes)

	updated := bytes.Repeat([]byte("3"), rand.Intn(1<<20)+32*1024)
	r.NotEqualValues(source, updated)

	putInfo, err = proxyClient.PutObject(tstCtx, bucket, objName, bytes.NewReader(updated), int64(len(updated)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)
	r.EqualValues(objName, putInfo.Key)
	r.EqualValues(bucket, putInfo.Bucket)

	obj, err = proxyClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)

	objBytes, err = io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(updated, objBytes)

	_, err = proxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.NoError(err)

	r.Eventually(func() bool {
		obj, err = mainClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
		if err != nil {
			return false
		}
		objBytes, err = io.ReadAll(obj)
		if err != nil {
			return false
		}
		if !bytes.Equal(updated, objBytes) {
			return false
		}

		obj, err = f1Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
		if err != nil {
			return false
		}
		objBytes, err = io.ReadAll(obj)
		if err != nil {
			return false
		}
		if !bytes.Equal(updated, objBytes) {
			return false
		}

		obj, err = f2Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
		if err != nil {
			return false
		}
		objBytes, err = io.ReadAll(obj)
		if err != nil {
			return false
		}
		if !bytes.Equal(updated, objBytes) {
			return false
		}

		return true
	}, time.Second*3, time.Millisecond*100)

	err = proxyClient.RemoveObject(tstCtx, bucket, objName, mclient.RemoveObjectOptions{})
	r.NoError(err)

	_, err = proxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)

	r.Eventually(func() bool {
		_, err = mainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err == nil {
			return false
		}
		_, err = f1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err == nil {
			return false
		}
		_, err = f2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err == nil {
			return false
		}
		return true
	}, time.Second*3, time.Millisecond*100)

}

func TestApi_Object_Folder(t *testing.T) {
	t.Parallel()
	bucket := "object-folder"
	r := require.New(t)

	err := proxyClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)
	t.Cleanup(func() {
		cleanup(t, false, bucket)
	})
	ok, err := proxyClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(ok)

	r.Eventually(func() bool {
		ok, err = mainClient.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = f1Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = f2Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		return true
	}, time.Second*3, time.Millisecond*100)

	objName := "folder/"
	_, err = mainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = f1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = f2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = proxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)

	source := []byte{}

	putInfo, err := proxyClient.PutObject(tstCtx, bucket, objName, bytes.NewReader(source), int64(len(source)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)
	r.EqualValues(objName, putInfo.Key)
	r.EqualValues(bucket, putInfo.Bucket)

	obj, err := proxyClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)

	objBytes, err := io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(source, objBytes)

	_, err = proxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.NoError(err)

	r.Eventually(func() bool {
		_, err = mainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = f1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = f2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		return true
	}, time.Second*3, time.Millisecond*100)
}
