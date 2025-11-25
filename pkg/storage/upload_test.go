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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/testutil"
)

func Test_svc_StoreUploadID(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)

	storage := NewUploadSvc(c)
	ctx := context.Background()

	users := []string{"u1", "u2"}
	buckets := []string{"b1", "b2"}
	uploads := []string{"id1", "id2"}

	for _, user := range users {
		exists, err := storage.UploadsExistForUser(ctx, user)
		r.NoError(err)
		r.False(exists)
		for _, bucket := range buckets {
			exists, err := storage.UploadsExistForUserBucket(ctx, entity.NewUserUploadObjectID(user, bucket))
			r.NoError(err)
			r.False(exists)
			for _, upload := range uploads {
				exists, err := storage.UploadExists(ctx, entity.NewUserUploadObjectID(user, bucket), entity.NewUserUploadObject(upload, upload))
				r.NoError(err)
				r.False(exists)
			}
		}
	}

	for _, user := range users {
		for _, bucket := range buckets {
			for _, upload := range uploads {
				err := storage.StoreUpload(ctx, entity.NewUserUploadObjectID(user, bucket), entity.NewUserUploadObject(upload, upload), time.Minute)
				r.NoError(err)
			}
		}
	}

	for _, user := range users {
		exists, err := storage.UploadsExistForUser(ctx, user)
		r.NoError(err)
		r.True(exists)
		for _, bucket := range buckets {
			exists, err := storage.UploadsExistForUserBucket(ctx, entity.NewUserUploadObjectID(user, bucket))
			r.NoError(err)
			r.True(exists)
			for _, upload := range uploads {
				exists, err := storage.UploadExists(ctx, entity.NewUserUploadObjectID(user, bucket), entity.NewUserUploadObject(upload, upload))
				r.NoError(err)
				r.True(exists)
			}
		}
	}
	exists, err := storage.UploadExists(ctx, entity.NewUserUploadObjectID(users[0], buckets[0]), entity.NewUserUploadObject(uploads[0], uploads[0]))
	r.NoError(err)
	r.True(exists)
	err = storage.DeleteUpload(ctx, entity.NewUserUploadObjectID(users[0], buckets[0]), entity.NewUserUploadObject(uploads[0], uploads[0]))
	r.NoError(err)
	exists, err = storage.UploadExists(ctx, entity.NewUserUploadObjectID(users[0], buckets[0]), entity.NewUserUploadObject(uploads[0], uploads[0]))
	r.NoError(err)
	r.False(exists)

	err = storage.DeleteUpload(ctx, entity.NewUserUploadObjectID("missing", "keys"), entity.NewUserUploadObject("valid", "args"))
	r.NoError(err)

}

func Test_StoreUploadID(t *testing.T) {
	c := testutil.SetupRedis(t)
	r := require.New(t)
	ctx := t.Context()
	// storage := New(c)
	storage := NewUploadSvc(c)

	user := "u1"
	bucket1, bucket2 := "b1", "b2"
	obj := "o1"
	upload := "id1"

	// not exists
	exists, err := storage.UploadsExistForUser(ctx, user)
	r.NoError(err)
	r.False(exists)
	exists, err = storage.UploadsExistForUserBucket(ctx, entity.NewUserUploadObjectID(user, bucket1))
	r.NoError(err)
	r.False(exists)
	exists, err = storage.UploadExists(ctx, entity.NewUserUploadObjectID(user, bucket1), entity.NewUserUploadObject(obj, upload))
	r.NoError(err)
	r.False(exists)

	// store to user bucket1
	err = storage.StoreUpload(ctx, entity.NewUserUploadObjectID(user, bucket1), entity.NewUserUploadObject(obj, upload), time.Minute)
	r.NoError(err)
	// exists for user and bucket1
	exists, err = storage.UploadsExistForUser(ctx, user)
	r.NoError(err)
	r.True(exists)
	exists, err = storage.UploadsExistForUserBucket(ctx, entity.NewUserUploadObjectID(user, bucket1))
	r.NoError(err)
	r.True(exists)
	exists, err = storage.UploadExists(ctx, entity.NewUserUploadObjectID(user, bucket1), entity.NewUserUploadObject(obj, upload))
	r.NoError(err)
	r.True(exists)

	// not exists for bucket2
	exists, err = storage.UploadsExistForUserBucket(ctx, entity.NewUserUploadObjectID(user, bucket2))
	r.NoError(err)
	r.False(exists)
	// not exists for other user
	exists, err = storage.UploadsExistForUser(ctx, "u2")
	r.NoError(err)
	r.False(exists)

	// delete upload ID
	err = storage.DeleteUpload(ctx, entity.NewUserUploadObjectID(user, bucket1), entity.NewUserUploadObject(obj, upload))
	r.NoError(err)

	// not exists
	exists, err = storage.UploadsExistForUser(ctx, user)
	r.NoError(err)
	r.False(exists)
	exists, err = storage.UploadsExistForUserBucket(ctx, entity.NewUserUploadObjectID(user, bucket1))
	r.NoError(err)
	r.False(exists)
	exists, err = storage.UploadExists(ctx, entity.NewUserUploadObjectID(user, bucket1), entity.NewUserUploadObject(obj, upload))
	r.NoError(err)
	r.False(exists)
}
