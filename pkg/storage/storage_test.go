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

package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/testutil"
)

func Test_svc_GetLastListedObj(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)

	storage := New(c)
	ctx := context.Background()
	s1 := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		FromStorage: "f1",
		ToStorage:   "f2",
		FromBucket:  "b",
		ToBucket:    "b",
	},
	)
	s2 := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		FromStorage: "f2",
		ToStorage:   "f1",
		FromBucket:  "b",
		ToBucket:    "b2",
	},
	)
	p1, p2 := "", "pref/"

	stors := []entity.UniversalReplicationID{s1, s2}
	bucks := []string{"b"}
	prefs := []string{p1, p2}

	for _, stor := range stors {
		for _, buck := range bucks {
			for _, pref := range prefs {
				task := tasks.MigrateBucketListObjectsPayload{
					Bucket: buck,
					Prefix: pref,
				}
				task.SetReplicationID(stor)
				res, err := storage.GetLastListedObj(ctx, task)
				r.NoError(err)
				r.Empty(res)
			}
		}
	}

	for i, stor := range stors {
		for j, buck := range bucks {
			for k, pref := range prefs {
				task := tasks.MigrateBucketListObjectsPayload{
					Bucket: buck,
					Prefix: pref,
				}
				task.SetReplicationID(stor)
				err := storage.SetLastListedObj(ctx, task, fmt.Sprintf("%d-%d-%d", i, j, k))
				r.NoError(err)
			}
		}
	}

	for storIdx, stor := range stors {
		for buckIdx, buck := range bucks {
			for prefIdx, pref := range prefs {
				task := tasks.MigrateBucketListObjectsPayload{
					Bucket: buck,
					Prefix: pref,
				}
				task.SetReplicationID(stor)
				res, err := storage.GetLastListedObj(ctx, task)
				r.NoError(err)
				r.EqualValues(fmt.Sprintf("%d-%d-%d", storIdx, buckIdx, prefIdx), res)
			}
		}
	}
	r.NoError(storage.CleanLastListedObj(ctx, s2.FromStorage(), s2.ToStorage(), "b", "b2"))
	for storIdx, stor := range stors {
		for buckIdx, buck := range bucks {
			for prefIdx, pref := range prefs {
				task := tasks.MigrateBucketListObjectsPayload{
					Bucket: buck,
					Prefix: pref,
				}
				task.SetReplicationID(stor)
				res, err := storage.GetLastListedObj(ctx, task)
				r.NoError(err)
				fromBuck, toBuck := stor.FromToBuckets(buck)
				if fromBuck == "b" && toBuck == "b2" && stor.ToStorage() == s2.ToStorage() && stor.FromStorage() == s2.FromStorage() {
					r.Empty(res)
					continue
				}
				r.EqualValues(fmt.Sprintf("%d-%d-%d", storIdx, buckIdx, prefIdx), res)
			}
		}
	}

}

func Test_svc_StoreUploadID(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)

	storage := New(c)
	ctx := context.Background()

	users := []string{"u1", "u2"}
	buckets := []string{"b1", "b2"}
	uploads := []string{"id1", "id2"}

	for _, user := range users {
		exists, err := storage.ExistsUploadsForUser(ctx, user)
		r.NoError(err)
		r.False(exists)
		for _, bucket := range buckets {
			exists, err := storage.ExistsUploads(ctx, user, bucket)
			r.NoError(err)
			r.False(exists)
			for _, upload := range uploads {
				exists, err := storage.ExistsUploadID(ctx, user, bucket, upload, upload)
				r.NoError(err)
				r.False(exists)
			}
		}
	}

	for _, user := range users {
		for _, bucket := range buckets {
			for _, upload := range uploads {
				err := storage.StoreUploadID(ctx, user, bucket, upload, upload, time.Minute)
				r.NoError(err)
			}
		}
	}

	for _, user := range users {
		exists, err := storage.ExistsUploadsForUser(ctx, user)
		r.NoError(err)
		r.True(exists)
		for _, bucket := range buckets {
			exists, err := storage.ExistsUploads(ctx, user, bucket)
			r.NoError(err)
			r.True(exists)
			for _, upload := range uploads {
				exists, err := storage.ExistsUploadID(ctx, user, bucket, upload, upload)
				r.NoError(err)
				r.True(exists)
			}
		}
	}
	exists, err := storage.ExistsUploadID(ctx, users[0], buckets[0], uploads[0], uploads[0])
	r.NoError(err)
	r.True(exists)
	err = storage.DeleteUploadID(ctx, users[0], buckets[0], uploads[0], uploads[0])
	r.NoError(err)
	exists, err = storage.ExistsUploadID(ctx, users[0], buckets[0], uploads[0], uploads[0])
	r.NoError(err)
	r.False(exists)

	err = storage.DeleteUploadID(ctx, "missing", "keys", "valid", "args")
	r.NoError(err)

}

func Test_StoreUploadID(t *testing.T) {
	c := testutil.SetupRedis(t)
	r := require.New(t)
	ctx := t.Context()
	storage := New(c)

	user := "u1"
	bucket1, bucket2 := "b1", "b2"
	obj := "o1"
	upload := "id1"

	// not exists
	exists, err := storage.ExistsUploadsForUser(ctx, user)
	r.NoError(err)
	r.False(exists)
	exists, err = storage.ExistsUploads(ctx, user, bucket1)
	r.NoError(err)
	r.False(exists)
	exists, err = storage.ExistsUploadID(ctx, user, bucket1, obj, upload)
	r.NoError(err)
	r.False(exists)

	// store to user bucket1
	err = storage.StoreUploadID(ctx, user, bucket1, obj, upload, time.Minute)
	r.NoError(err)
	// exists for user and bucket1
	exists, err = storage.ExistsUploadsForUser(ctx, user)
	r.NoError(err)
	r.True(exists)
	exists, err = storage.ExistsUploads(ctx, user, bucket1)
	r.NoError(err)
	r.True(exists)
	exists, err = storage.ExistsUploadID(ctx, user, bucket1, obj, upload)
	r.NoError(err)
	r.True(exists)

	// not exists for bucket2
	exists, err = storage.ExistsUploads(ctx, user, bucket2)
	r.NoError(err)
	r.False(exists)
	// not exists for other user
	exists, err = storage.ExistsUploadsForUser(ctx, "u2")
	r.NoError(err)
	r.False(exists)

	// delete upload ID
	err = storage.DeleteUploadID(ctx, user, bucket1, obj, upload)
	r.NoError(err)

	// not exists
	exists, err = storage.ExistsUploadsForUser(ctx, user)
	r.NoError(err)
	r.False(exists)
	exists, err = storage.ExistsUploads(ctx, user, bucket1)
	r.NoError(err)
	r.False(exists)
	exists, err = storage.ExistsUploadID(ctx, user, bucket1, obj, upload)
	r.NoError(err)
	r.False(exists)
}
