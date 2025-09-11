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
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/clyso/chorus/test/env"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
)

const (
	user = "test"
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
	}, e.WaitShort, e.RetryShort)

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
	}, e.WaitShort, e.RetryShort)

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
	}, e.WaitShort, e.RetryShort)

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
	}, e.WaitShort, e.RetryShort)

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
	}, e.WaitLong, e.RetryLong)

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
	}, e.WaitShort, e.RetryShort)
}

// Tests for critical object names that would be problematic as file system paths
// but should be valid as S3 object keys

func TestApi_Object_CriticalObjectNames(t *testing.T) {
	t.Parallel()
	e, _, _ := env.SetupEmbedded(t, workerConf, proxyConf)
	tests := []struct {
		name        string
		description string
	}{
		{"/root/system", "Absolute path with leading slash"},
		{"file\x01\x02\x03.txt", "Control characters in name"},
		{"äöüß", "UTF8"},
		{"file\u200B\u2060name", "Unicode control characters"},
		{"..hidden", "File starting with double dots"},
		{".hidden", "File starting with single dot"},
		{" file with spaces and special chars !@#$%^&*()+={}[]|:;\"'<>,./`~", "Spaces and special characters"},
		{"very_long_object_name_that_exceeds_typical_filesystem_limits_" + string(bytes.Repeat([]byte("x"), 200)), "Very long object name"},
		{"file|name", "Pipe character in name"},
		{"file?name", "Question mark in name"},
		{"file*name", "Asterisk wildcard in name"},
		{"path//with///multiple////slashes", "Multiple consecutive slashes"},
		{"a/", "Object name ending with single slash (directory-like)"},
		{"folder////", "Object name ending with multiple slashes"},
		{"file:with:colons", "Colons in filename"},
		{"file<with>angle<brackets>", "Angle brackets in filename"},
		{"file\"with'quotes", "Quotes in filename"},
		{"file\twith\nnewline", "Tab and newline characters"},
		{"/", "Object name consisting of a single slash"},
		{"////", "Object name consisting only of multiple slashes"},
	}
	tstCtx := t.Context()
	for i := range len(tests) {
		r := require.New(t)
		bucket := fmt.Sprintf("object-critical-%d", i+1)
		fmt.Println(bucket)
		err := e.MainClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
		r.NoError(err)
		err = e.F1Client.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
		r.NoError(err)
		err = e.F2Client.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
		r.NoError(err)
		ok, err := e.MainClient.BucketExists(tstCtx, bucket)
		r.NoError(err)
		r.True(ok)
		ok, err = e.F1Client.BucketExists(tstCtx, bucket)
		r.NoError(err)
		r.True(ok)
		ok, err = e.F2Client.BucketExists(tstCtx, bucket)
		r.NoError(err)
		r.True(ok)
		ok, err = e.ProxyClient.BucketExists(tstCtx, bucket)
		r.NoError(err)
		r.True(ok)
		_, err = e.ApiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
			User:            user,
			From:            "main",
			To:              "f1",
			Buckets:         []string{bucket},
			IsForAllBuckets: false,
		})
		r.NoError(err)
		_, err = e.ApiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
			User:            user,
			From:            "main",
			To:              "f2",
			Buckets:         []string{bucket},
			IsForAllBuckets: false,
		})
		r.NoError(err)
	}
	for i:= range len(tests) {
		bucket := fmt.Sprintf("object-critical-%d", i+1)
		test := tests[i]
		objName, description := test.name, test.description
		t.Run(description, func(t *testing.T) {
			t.Parallel()
			r := require.New(t)

			// Generate test data
			source := []byte(bucket)

			// Test object creation
			putInfo, err := e.ProxyClient.PutObject(tstCtx, bucket, objName, bytes.NewReader(source), int64(len(source)), mclient.PutObjectOptions{
				ContentType: "binary/octet-stream", DisableContentSha256: true,
			})
			r.NoError(err, "Failed to create object with name: %s (%s)", objName, description)
			r.EqualValues(objName, putInfo.Key)
			r.EqualValues(bucket, putInfo.Bucket)

			// Test object retrieval
			obj, err := e.ProxyClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
			r.NoError(err, "Failed to retrieve object with name: %s (%s)", objName, description)
			objBytes, err := io.ReadAll(obj)
			r.NoError(err, "Failed to read object data: %s (%s)", objName, description)
			r.EqualValues(source, objBytes, "Object data mismatch for: %s (%s)", objName, description)

			// Test object stat
			_, err = e.ProxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
			r.NoError(err, "Failed to stat object: %s (%s)", objName, description)
			_, err = e.MainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
			r.NoError(err, "Failed to stat object: %s (%s)", objName, description)

			// Verify replication to all backends
			r.Eventually(func() bool {
				_, err = e.F1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
				if err != nil {
					return false
				}
				_, err = e.F2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
				if err != nil {
					return false
				}
				return true
			}, e.WaitLong, e.RetryLong, "Object not replicated to all backends: %s (%s)", objName, description)

			// Verify data consistency across all backends
			mainObj, err := e.MainClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
			r.NoError(err, "Failed to get object from main backend: %s", objName)
			mainBytes, err := io.ReadAll(mainObj)
			r.NoError(err)
			r.EqualValues(source, mainBytes, "Data mismatch in main backend for: %s", objName)

			f1Obj, err := e.F1Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
			r.NoError(err, "Failed to get object from f1 backend: %s", objName)
			f1Bytes, err := io.ReadAll(f1Obj)
			r.NoError(err)
			r.EqualValues(source, f1Bytes, "Data mismatch in f1 backend for: %s", objName)

			f2Obj, err := e.F2Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
			r.NoError(err, "Failed to get object from f2 backend: %s", objName)
			f2Bytes, err := io.ReadAll(f2Obj)
			r.NoError(err)
			r.EqualValues(source, f2Bytes, "Data mismatch in f2 backend for: %s", objName)

			// Test object deletion
			err = e.ProxyClient.RemoveObject(tstCtx, bucket, objName, mclient.RemoveObjectOptions{})
			r.NoError(err, "Failed to delete object: %s (%s)", objName, description)

			// Verify deletion propagated (with longer timeout)
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
			}, e.WaitLong, e.RetryLong, "Object deletion not propagated: %s (%s)", objName, description)
		})
	}
}

// The following tests are handled separately because the proxy handles these objecte correctly,
// but the replication backend (rclone) does not.
// Fixing this would either need a fix in rclone, or the switch to a different (possibly newly created)
// replication backend. The correct proxy bevaviour is nevertheless tested here.

func TestApi_Object_CriticalObjectNamesNotSynced(t *testing.T) {
	t.Parallel()
	e, _, _ := env.SetupEmbedded(t, workerConf, proxyConf)
	tests := []struct {
		name        string
		description string
	}{
		{".", "Object name consisting only of a single dot"},
		{"./", "Object name consisting only of a single dot and a slash"},
		{"./.", "Object name consisting of two dots, separatd by a slash"},
		{"././././", "Object name consisting of several dots and slashes"},
		{"./name", "Object name consisting of a dot, a slash and a word"},
		{"word/..", "Object name consisting of a word, a slash and two dots"},
		{"word/../another", "Object name consisting of a word, a slash, two dots, a slash and a word"},
	}
	var cnt uint64 = 0
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			objName, description := test.name, test.description
			tstCtx := t.Context()
			bucket := fmt.Sprintf("object-critical-nosync-%d", atomic.AddUint64(&cnt, 1))
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
			}, e.WaitLong, e.RetryLong)

			// Generate random test data
			source := []byte("test-data")

			// Test object creation
			putInfo, err := e.ProxyClient.PutObject(tstCtx, bucket, objName, bytes.NewReader(source), int64(len(source)), mclient.PutObjectOptions{
				ContentType: "binary/octet-stream", DisableContentSha256: true,
			})
			r.NoError(err, "Failed to create object with name: %s (%s)", objName, description)
			r.EqualValues(objName, putInfo.Key)
			r.EqualValues(bucket, putInfo.Bucket)

			// Test object retrieval
			obj, err := e.ProxyClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
			r.NoError(err, "Failed to retrieve object with name: %s (%s) from proxy", objName, description)
			objBytes, err := io.ReadAll(obj)
			r.NoError(err, "Failed to read object data: %s (%s)", objName, description)
			r.EqualValues(source, objBytes, "Object data mismatch for: %s (%s) from proxy", objName, description)
			obj, err = e.MainClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
			r.NoError(err, "Failed to retrieve object with name: %s (%s) from main", objName, description)
			objBytes, err = io.ReadAll(obj)
			r.NoError(err, "Failed to read object data: %s (%s)", objName, description)
			r.EqualValues(source, objBytes, "Object data mismatch for: %s (%s) from main", objName, description)

			// Test object stat
			_, err = e.ProxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
			r.NoError(err, "Failed to stat object: %s (%s) from proxy", objName, description)
			_, err = e.MainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
			r.NoError(err, "Failed to stat object: %s (%s) from main", objName, description)

			// Object is currently not replicated
			r.Never(func() bool {
				_, err = e.F1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
				if err != nil {
					return false
				}
				_, err = e.F2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
				if err != nil {
					return false
				}
				return true
			}, e.WaitLong, e.RetryLong, "Object not replicated to any backends: %s (%s)", objName, description)

			// Test object deletion
			err = e.ProxyClient.RemoveObject(tstCtx, bucket, objName, mclient.RemoveObjectOptions{})
			r.NoError(err, "Failed to delete object: %s (%s)", objName, description)

			// Verify deletion propagated (with longer timeout)
			r.Eventually(func() bool {
				_, err = e.MainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
				if err == nil {
					return false
				}
				return true
			}, e.WaitLong, e.RetryLong, "Object deletion not propagated: %s (%s)", objName, description)

		})
	}
}
