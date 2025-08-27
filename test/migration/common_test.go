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
	"context"
	"math/rand"

	mclient "github.com/minio/minio-go/v7"
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
	objCh := c.ListObjects(context.Background(), bucket, mclient.ListObjectsOptions{Prefix: prefix, Recursive: true})
	for obj := range objCh {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if obj.Size == 0 && obj.Key != prefix {
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

func rmBucket(client *mclient.Client, bucket string) error {
	objs, _ := listObjects(client, bucket, "")
	for _, obj := range objs {
		_ = client.RemoveObject(context.Background(), bucket, obj, mclient.RemoveObjectOptions{ForceDelete: true})
	}
	return client.RemoveBucket(context.Background(), bucket)
}
