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

	"github.com/clyso/chorus/test/env"
	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
)

func TestApi_Object_CRUD(t *testing.T) {
	e := env.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	bucket := "object-crud"
	r := require.New(t)

	err := e.ProxyClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)
	ok, err := e.ProxyClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(ok)

	r.Eventually(func() bool {
		ok, err = e.MainClient.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = e.F1Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = e.F2Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		return true
	}, time.Second*3, time.Millisecond*100)

	objName := "obj-crud"
	_, err = e.MainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = e.F1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = e.F2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = e.ProxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)

	source := bytes.Repeat([]byte("3"), rand.Intn(1<<20)+32*1024)

	putInfo, err := e.ProxyClient.PutObject(tstCtx, bucket, objName, bytes.NewReader(source), int64(len(source)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)
	r.EqualValues(objName, putInfo.Key)
	r.EqualValues(bucket, putInfo.Bucket)

	obj, err := e.ProxyClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)

	objBytes, err := io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(source, objBytes)

	_, err = e.ProxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.NoError(err)

	r.Eventually(func() bool {
		_, err = e.MainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = e.F1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = e.F2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		return true
	}, time.Second*3, time.Millisecond*100)

	obj, err = e.MainClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)
	objBytes, err = io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(source, objBytes)

	obj, err = e.F1Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)
	objBytes, err = io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(source, objBytes)

	obj, err = e.F2Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)
	objBytes, err = io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(source, objBytes)

	updated := bytes.Repeat([]byte("3"), rand.Intn(1<<20)+32*1024)
	r.NotEqualValues(source, updated)

	putInfo, err = e.ProxyClient.PutObject(tstCtx, bucket, objName, bytes.NewReader(updated), int64(len(updated)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)
	r.EqualValues(objName, putInfo.Key)
	r.EqualValues(bucket, putInfo.Bucket)

	obj, err = e.ProxyClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)

	objBytes, err = io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(updated, objBytes)

	_, err = e.ProxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.NoError(err)

	r.Eventually(func() bool {
		obj, err = e.MainClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
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

		obj, err = e.F1Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
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

		obj, err = e.F2Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
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

	err = e.ProxyClient.RemoveObject(tstCtx, bucket, objName, mclient.RemoveObjectOptions{})
	r.NoError(err)

	_, err = e.ProxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)

	r.Eventually(func() bool {
		_, err = e.MainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err == nil {
			return false
		}
		_, err = e.F1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err == nil {
			return false
		}
		_, err = e.F2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err == nil {
			return false
		}
		return true
	}, time.Second*3, time.Millisecond*100)

}

func TestApi_Object_Folder(t *testing.T) {
	e := env.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	bucket := "object-folder"
	r := require.New(t)

	err := e.ProxyClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)
	ok, err := e.ProxyClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(ok)

	r.Eventually(func() bool {
		ok, err = e.MainClient.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = e.F1Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = e.F2Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		return true
	}, time.Second*20, time.Millisecond*100)

	objName := "folder/"
	_, err = e.MainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = e.F1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = e.F2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = e.ProxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)

	source := []byte{}

	putInfo, err := e.ProxyClient.PutObject(tstCtx, bucket, objName, bytes.NewReader(source), int64(len(source)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)
	r.EqualValues(objName, putInfo.Key)
	r.EqualValues(bucket, putInfo.Bucket)

	obj, err := e.ProxyClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)

	objBytes, err := io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(source, objBytes)

	_, err = e.ProxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.NoError(err)

	r.Eventually(func() bool {
		_, err = e.MainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = e.F1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = e.F2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		return true
	}, time.Second*3, time.Millisecond*100)
}
