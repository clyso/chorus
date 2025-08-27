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

	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/testutil"
)

func Test_svc_GetLastListedObj(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)

	storage := New(c)
	ctx := context.Background()
	s1 := tasks.Sync{
		FromStorage: "a",
		ToStorage:   "b",
	}
	s2 := tasks.Sync{
		FromStorage: "b",
		ToStorage:   "c",
	}
	s3 := tasks.Sync{
		FromStorage: "asdf",
		ToStorage:   "asdf",
	}
	b1, b2 := "b1", "b2"
	p1, p2 := "", "pref/"

	stors := []tasks.Sync{s1, s2, s3}
	bucks := []string{b1, b2}
	prefs := []string{p1, p2}

	for _, stor := range stors {
		for _, buck := range bucks {
			for _, pref := range prefs {
				res, err := storage.GetLastListedObj(ctx, tasks.MigrateBucketListObjectsPayload{
					Sync:   stor,
					Bucket: buck,
					Prefix: pref,
				})
				r.NoError(err)
				r.Empty(res)
			}
		}
	}

	for i, stor := range stors {
		for j, buck := range bucks {
			for k, pref := range prefs {
				err := storage.SetLastListedObj(ctx, tasks.MigrateBucketListObjectsPayload{
					Sync:   stor,
					Bucket: buck,
					Prefix: pref,
				}, fmt.Sprintf("%d-%d-%d", i, j, k))
				r.NoError(err)
			}
		}
	}

	for storIdx, stor := range stors {
		for buckIdx, buck := range bucks {
			for prefIdx, pref := range prefs {
				res, err := storage.GetLastListedObj(ctx, tasks.MigrateBucketListObjectsPayload{
					Sync:   stor,
					Bucket: buck,
					Prefix: pref,
				})
				r.NoError(err)
				r.EqualValues(fmt.Sprintf("%d-%d-%d", storIdx, buckIdx, prefIdx), res)
			}
		}
	}
	r.NoError(storage.CleanLastListedObj(ctx, s1.FromStorage, s1.ToStorage, b1, ""))
	for storIdx, stor := range stors {
		for buckIdx, buck := range bucks {
			for prefIdx, pref := range prefs {
				res, err := storage.GetLastListedObj(ctx, tasks.MigrateBucketListObjectsPayload{
					Sync:   stor,
					Bucket: buck,
					Prefix: pref,
				})
				r.NoError(err)
				if buck == b1 && stor.ToStorage == s1.ToStorage && stor.FromStorage == s1.FromStorage {
					r.Empty(res)
					break
				}
				r.EqualValues(fmt.Sprintf("%d-%d-%d", storIdx, buckIdx, prefIdx), res)
			}
		}
	}

}

func Test_GetLastListedObjWithCustomDestBucket(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)

	storage := New(c)
	ctx := context.Background()
	destBuck := "bucket"

	noDestBuckNoPrefix := tasks.MigrateBucketListObjectsPayload{
		Sync: tasks.Sync{
			FromStorage: "a",
			ToStorage:   "b",
			ToBucket:    "c",
		},
		Bucket: "c",
		Prefix: "",
	}
	r.NoError(storage.SetLastListedObj(ctx, noDestBuckNoPrefix, "nbnp"))
	noDestBuckWithPrefix := tasks.MigrateBucketListObjectsPayload{
		Sync: tasks.Sync{
			FromStorage: "a",
			ToStorage:   "b",
			ToBucket:    "c",
		},
		Bucket: "c",
		Prefix: "d",
	}
	r.NoError(storage.SetLastListedObj(ctx, noDestBuckWithPrefix, "nbwp"))
	withDestBuckNoPrefix := tasks.MigrateBucketListObjectsPayload{
		Sync: tasks.Sync{
			FromStorage: "a",
			ToStorage:   "b",
			ToBucket:    destBuck,
		},
		Bucket: "c",
		Prefix: "",
	}
	r.NoError(storage.SetLastListedObj(ctx, withDestBuckNoPrefix, "wbnp"))
	withDestBuckWithPrefix := tasks.MigrateBucketListObjectsPayload{
		Sync: tasks.Sync{
			FromStorage: "a",
			ToStorage:   "b",
			ToBucket:    destBuck,
		},
		Bucket: "c",
		Prefix: "d",
	}
	r.NoError(storage.SetLastListedObj(ctx, withDestBuckWithPrefix, "wbwp"))

	res, err := storage.GetLastListedObj(ctx, noDestBuckNoPrefix)
	r.NoError(err)
	r.EqualValues("nbnp", res)

	res, err = storage.GetLastListedObj(ctx, noDestBuckWithPrefix)
	r.NoError(err)
	r.EqualValues("nbwp", res)

	res, err = storage.GetLastListedObj(ctx, withDestBuckNoPrefix)
	r.NoError(err)
	r.EqualValues("wbnp", res)

	res, err = storage.GetLastListedObj(ctx, withDestBuckWithPrefix)
	r.NoError(err)
	r.EqualValues("wbwp", res)

	r.NoError(storage.DelLastListedObj(ctx, noDestBuckNoPrefix))
	res, _ = storage.GetLastListedObj(ctx, noDestBuckNoPrefix)
	r.Empty(res)

	r.NoError(storage.DelLastListedObj(ctx, noDestBuckWithPrefix))
	res, _ = storage.GetLastListedObj(ctx, noDestBuckWithPrefix)
	r.Empty(res)

	r.NoError(storage.DelLastListedObj(ctx, withDestBuckNoPrefix))
	res, _ = storage.GetLastListedObj(ctx, withDestBuckNoPrefix)
	r.Empty(res)

	r.NoError(storage.DelLastListedObj(ctx, withDestBuckWithPrefix))
	res, _ = storage.GetLastListedObj(ctx, withDestBuckWithPrefix)
	r.Empty(res)
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
