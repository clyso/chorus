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
	"strings"
	"testing"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/test/app"
	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
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

type ReplicationDiffResponse struct {
	IsMatch  bool
	MissFrom []string
	MissTo   []string
	Differ   []string
}

func replicationDiff(t *testing.T, e app.EmbeddedEnv, id *pb.ReplicationID) ReplicationDiffResponse {
	t.Helper()
	r := require.New(t)
	r.NotNil(id.FromBucket)
	r.NotNil(id.ToBucket)
	loc := []*pb.MigrateLocation{
		{Bucket: *id.FromBucket, Storage: id.FromStorage},
		{Bucket: *id.ToBucket, Storage: id.ToStorage},
	}
	// delete any existing diff
	_, err := e.DiffClient.DeleteReport(t.Context(), &pb.ConsistencyCheckRequest{
		Locations: loc,
	})
	r.NoError(err)

	// create diff
	_, err = e.DiffClient.Start(t.Context(), &pb.StartConsistencyCheckRequest{
		Locations:             loc,
		User:                  id.User,
		CheckOnlyLastVersions: true,
	})
	r.NoError(err)

	var res *pb.GetConsistencyCheckReportResponse
	r.Eventually(func() bool {
		//poll until ready
		res, err = e.DiffClient.GetReport(t.Context(), &pb.ConsistencyCheckRequest{
			Locations: loc,
		})
		r.NoError(err)
		return res.Check.Ready
	}, e.WaitLong, e.RetryShort)
	r.True(res.Check.Ready)

	if res.Check.Consistent {
		// match
		return ReplicationDiffResponse{IsMatch: true}
	}
	fromMap := make(map[string]struct{})
	toMap := make(map[string]struct{})
	// not match, request diff entries
	diff := ReplicationDiffResponse{IsMatch: false}
	cursor := uint64(0)
	for {
		batch, err := e.DiffClient.GetReportEntries(t.Context(), &pb.GetConsistencyCheckReportEntriesRequest{
			Locations: loc,
			Cursor:    0,
			PageSize:  100,
		})
		r.NoError(err)
		cursor = batch.Cursor
		for _, entry := range batch.Entries {
			if len(entry.StorageEntries) == 1 {
				stor := entry.StorageEntries[0]
				if strings.HasSuffix(entry.Object, "/") {
					//TODO: fix consistency check to
					// skip folder placehoders
					continue
				}
				if stor.Storage == id.FromStorage && stor.Bucket == *id.FromBucket {
					fromMap[entry.Object] = struct{}{}
				} else if stor.Storage == id.ToStorage && stor.Bucket == *id.ToBucket {
					toMap[entry.Object] = struct{}{}
				} else {
					r.Fail("entry with unexpected location found")
				}
			} else {
				r.Fail("entry with unexpected number of locations found")
			}
		}
		if cursor == 0 {
			break
		}
	}

	// find missing objects
	for fromObj := range fromMap {
		if _, found := toMap[fromObj]; !found {
			diff.MissTo = append(diff.MissTo, fromObj)
		} else {
			delete(toMap, fromObj)
			diff.Differ = append(diff.Differ, fromObj)
		}
	}
	for toObj := range toMap {
		diff.MissFrom = append(diff.MissFrom, toObj)
	}
	return diff
}
