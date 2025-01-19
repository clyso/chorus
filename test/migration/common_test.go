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
	"encoding/base64"
	"fmt"
	"math/rand"
	"strings"

	mclient "github.com/minio/minio-go/v7"

	"github.com/clyso/chorus/pkg/s3"
)

type testObj struct {
	name   string
	data   []byte
	bucket string
}

const tstObjSize = 4096

func getTestObj(name, bucket string) testObj {
	rndLen := rand.Intn(200) - 100
	to := testObj{
		name:   name,
		data:   make([]byte, tstObjSize+rndLen),
		bucket: bucket,
	}
	_, _ = rand.Read(to.data)
	return to
}

func listObjects(c *mclient.Client, bucket string, prefix string) ([]string, error) {
	var res []string
	objCh := c.ListObjects(tstCtx, bucket, mclient.ListObjectsOptions{Prefix: prefix})
	for obj := range objCh {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if obj.Size == 0 {
			subRes, err := listObjects(c, bucket, obj.Key)
			if err != nil {
				return nil, err
			}
			res = append(res, subRes...)
		} else {
			res = append(res, obj.Key)
		}
	}
	return res, nil
}

//func startMigration(from, to string) error {
//	task, err := tasks.NewTask(tasks.MigrationStartPayload{
//		FromStorage: from,
//		ToStorage:   to,
//	})
//	if err != nil {
//		return err
//	}
//	_, err = taskClient.EnqueueContext(tstCtx, task)
//	return err
//}

func generateCredentials() s3.CredentialsV4 {
	res := s3.CredentialsV4{}
	const (
		// Minimum length for MinIO access key.
		accessKeyMinLen = 3

		// Maximum length for MinIO access key.
		// There is no max length enforcement for access keys
		accessKeyMaxLen = 20

		// Minimum length for MinIO secret key for both server
		secretKeyMinLen = 8

		// Maximum secret key length for MinIO, this
		// is used when autogenerating new credentials.
		// There is no max length enforcement for secret keys
		secretKeyMaxLen = 40

		// Alpha numeric table used for generating access keys.
		alphaNumericTable = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"

		// Total length of the alpha numeric table.
		alphaNumericTableLen = byte(len(alphaNumericTable))
	)
	readBytes := func(size int) (data []byte, err error) {
		data = make([]byte, size)
		var n int
		if n, err = rand.Read(data); err != nil {
			return nil, err
		} else if n != size {
			panic(fmt.Errorf("Not enough data. Expected to read: %v bytes, got: %v bytes", size, n))
		}
		return data, nil
	}

	// Generate access key.
	keyBytes, err := readBytes(accessKeyMaxLen)
	if err != nil {
		panic(err)
	}
	for i := 0; i < accessKeyMaxLen; i++ {
		keyBytes[i] = alphaNumericTable[keyBytes[i]%alphaNumericTableLen]
	}
	res.AccessKeyID = string(keyBytes)

	// Generate secret key.
	keyBytes, err = readBytes(secretKeyMaxLen)
	if err != nil {
		panic(err)
	}

	res.SecretAccessKey = strings.ReplaceAll(string([]byte(base64.StdEncoding.EncodeToString(keyBytes))[:secretKeyMaxLen]),
		"/", "+")

	return res
}

func cleanup(buckets ...string) {
	for _, bucket := range buckets {
		objCh, _ := listObjects(mainClient, bucket, "")
		for _, obj := range objCh {
			_ = mainClient.RemoveObject(tstCtx, bucket, obj, mclient.RemoveObjectOptions{ForceDelete: true})
		}
		_ = mainClient.RemoveBucket(tstCtx, bucket)

		objCh, _ = listObjects(f1Client, bucket, "")
		for _, obj := range objCh {
			_ = f1Client.RemoveObject(tstCtx, bucket, obj, mclient.RemoveObjectOptions{ForceDelete: true})
		}
		_ = f1Client.RemoveBucket(tstCtx, bucket)

		objCh, _ = listObjects(f2Client, bucket, "")
		for _, obj := range objCh {
			_ = f2Client.RemoveObject(tstCtx, bucket, obj, mclient.RemoveObjectOptions{ForceDelete: true})
		}
		_ = f2Client.RemoveBucket(tstCtx, bucket)
	}
}

func rmBucket(client *mclient.Client, bucket string) error {
	objs, _ := listObjects(client, bucket, "")
	for _, obj := range objs {
		_ = client.RemoveObject(tstCtx, bucket, obj, mclient.RemoveObjectOptions{ForceDelete: true})
	}
	return client.RemoveBucket(tstCtx, bucket)
}
