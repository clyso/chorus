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

package meta

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_Version_svc(t *testing.T) {
	r := require.New(t)
	red := miniredis.RunT(t)

	c := redis.NewClient(&redis.Options{
		Addr: red.Addr(),
	})

	s := NewVersionService(c)

	obj := dom.Object{
		Bucket:  "bucket",
		Name:    "obj",
		Version: "",
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	vers, err := s.GetObj(ctx, obj)
	r.NoError(err)
	r.Empty(vers)
	vers, err = s.GetTags(ctx, obj)
	r.NoError(err)
	r.Empty(vers)
	vers, err = s.GetACL(ctx, obj)
	r.NoError(err)
	r.Empty(vers)

	stor := "stor1"
	err = s.UpdateIfGreater(ctx, obj, stor, -1)
	r.ErrorIs(err, dom.ErrInvalidArg)
	err = s.UpdateIfGreater(ctx, obj, stor, 0)
	r.ErrorIs(err, dom.ErrInvalidArg)
	err = s.UpdateIfGreater(ctx, obj, stor, 69)
	r.ErrorIs(err, dom.ErrNotFound)
	vers, err = s.GetObj(ctx, obj)
	r.NoError(err)
	r.Empty(vers)
	incVer, err := s.IncrementObj(ctx, obj, stor)
	r.NoError(err)
	r.EqualValues(1, incVer)
	err = s.UpdateIfGreater(ctx, obj, stor, 69)
	r.NoError(err)
	vers, err = s.GetObj(ctx, obj)
	r.NoError(err)
	r.EqualValues(69, vers[stor])

	err = s.UpdateIfGreater(ctx, obj, stor, 67)
	r.ErrorIs(err, dom.ErrAlreadyExists)
	vers, err = s.GetObj(ctx, obj)
	r.NoError(err)
	r.EqualValues(69, vers[stor])
	err = s.UpdateIfGreater(ctx, obj, stor, 0)
	r.ErrorIs(err, dom.ErrInvalidArg)
	err = s.UpdateIfGreater(ctx, obj, stor, 420)
	r.NoError(err)
	vers, err = s.GetObj(ctx, obj)
	r.NoError(err)
	r.EqualValues(420, vers[stor])

	incVer, err = s.IncrementObj(ctx, obj, stor)
	r.NoError(err)
	r.EqualValues(421, incVer)
	vers, err = s.GetObj(ctx, obj)
	r.NoError(err)
	r.EqualValues(421, vers[stor])

	stor2 := "stor2"
	incVer2, err := s.IncrementObj(ctx, obj, stor2)
	r.NoError(err)
	r.EqualValues(422, incVer2)
	vers, err = s.GetObj(ctx, obj)
	r.NoError(err)
	r.Len(vers, 2)
	r.EqualValues(421, vers[stor])
	r.EqualValues(422, vers[stor2])

	vers, err = s.GetTags(ctx, obj)
	r.NoError(err)
	r.Empty(vers)
	vers, err = s.GetACL(ctx, obj)
	r.NoError(err)
	r.Empty(vers)

	err = s.DeleteObjAll(ctx, obj)
	r.NoError(err)
	vers, err = s.GetObj(ctx, obj)
	r.NoError(err)
	r.Empty(vers)
	vers, err = s.GetTags(ctx, obj)
	r.NoError(err)
	r.Empty(vers)
	vers, err = s.GetACL(ctx, obj)
	r.NoError(err)
	r.Empty(vers)

	err = s.UpdateACLIfGreater(ctx, obj, stor, -1)
	r.ErrorIs(err, dom.ErrInvalidArg)
	err = s.UpdateACLIfGreater(ctx, obj, stor, 0)
	r.ErrorIs(err, dom.ErrInvalidArg)
	err = s.UpdateACLIfGreater(ctx, obj, stor, 33)
	r.ErrorIs(err, dom.ErrNotFound)
	vers, err = s.GetACL(ctx, obj)
	r.NoError(err)
	r.Empty(vers)

	aclVer, err := s.IncrementACL(ctx, obj, stor)
	r.NoError(err)
	r.EqualValues(1, aclVer)
	err = s.UpdateACLIfGreater(ctx, obj, stor, 33)
	r.NoError(err)
	vers, err = s.GetACL(ctx, obj)
	r.NoError(err)
	r.EqualValues(33, vers[stor])

	err = s.UpdateACLIfGreater(ctx, obj, stor, 32)
	r.ErrorIs(err, dom.ErrAlreadyExists)
	vers, err = s.GetACL(ctx, obj)
	r.NoError(err)
	r.EqualValues(33, vers[stor])
	err = s.UpdateACLIfGreater(ctx, obj, stor, 0)
	r.ErrorIs(err, dom.ErrInvalidArg)
	err = s.UpdateACLIfGreater(ctx, obj, stor, 55)
	r.NoError(err)
	vers, err = s.GetACL(ctx, obj)
	r.NoError(err)
	r.EqualValues(55, vers[stor])

	aclVer, err = s.IncrementACL(ctx, obj, stor)
	r.NoError(err)
	r.EqualValues(56, aclVer)
	vers, err = s.GetACL(ctx, obj)
	r.NoError(err)
	r.Len(vers, 1)
	r.EqualValues(56, vers[stor])

	aclVer2, err := s.IncrementACL(ctx, obj, stor2)
	r.NoError(err)
	r.EqualValues(57, aclVer2)
	vers, err = s.GetACL(ctx, obj)
	r.NoError(err)
	r.Len(vers, 2)
	r.EqualValues(57, vers[stor2])

	err = s.UpdateTagsIfGreater(ctx, obj, stor, -1)
	r.ErrorIs(err, dom.ErrInvalidArg)
	err = s.UpdateTagsIfGreater(ctx, obj, stor, 0)
	r.ErrorIs(err, dom.ErrInvalidArg)
	err = s.UpdateTagsIfGreater(ctx, obj, stor, 33)
	r.ErrorIs(err, dom.ErrNotFound)
	vers, err = s.GetTags(ctx, obj)
	r.NoError(err)
	r.Empty(vers)

	tagVer, err := s.IncrementTags(ctx, obj, stor)
	r.NoError(err)
	r.EqualValues(1, tagVer)
	err = s.UpdateTagsIfGreater(ctx, obj, stor, 33)
	r.NoError(err)
	vers, err = s.GetTags(ctx, obj)
	r.NoError(err)
	r.EqualValues(33, vers[stor])

	err = s.UpdateTagsIfGreater(ctx, obj, stor, 32)
	r.ErrorIs(err, dom.ErrAlreadyExists)
	vers, err = s.GetTags(ctx, obj)
	r.NoError(err)
	r.EqualValues(33, vers[stor])
	err = s.UpdateTagsIfGreater(ctx, obj, stor, 0)
	r.Error(err)
	err = s.UpdateTagsIfGreater(ctx, obj, stor, 55)
	r.NoError(err)
	vers, err = s.GetTags(ctx, obj)
	r.NoError(err)
	r.EqualValues(55, vers[stor])

	tagVer, err = s.IncrementTags(ctx, obj, stor)
	r.NoError(err)
	r.EqualValues(56, tagVer)
	vers, err = s.GetTags(ctx, obj)
	r.NoError(err)
	r.Len(vers, 1)
	r.EqualValues(56, vers[stor])

	tagVer2, err := s.IncrementTags(ctx, obj, stor2)
	r.NoError(err)
	r.EqualValues(57, tagVer2)
	vers, err = s.GetTags(ctx, obj)
	r.NoError(err)
	r.Len(vers, 2)
	r.EqualValues(57, vers[stor2])

	err = s.DeleteObjAll(ctx, obj)
	r.NoError(err)
	vers, err = s.GetObj(ctx, obj)
	r.NoError(err)
	r.Empty(vers)
	vers, err = s.GetTags(ctx, obj)
	r.NoError(err)
	r.Empty(vers)
	vers, err = s.GetACL(ctx, obj)
	r.NoError(err)
	r.Empty(vers)
}

func Test_inc_version_during_switch(t *testing.T) {
	var (
		stor1   = "stor1"
		stor2   = "stor2"
		stor3   = "stor3"
		buck    = "buck"
		objName = "object"
		obj     = dom.Object{
			Bucket:  buck,
			Name:    objName,
			Version: "",
		}
		ctx = context.Background()
	)
	r := require.New(t)
	red := miniredis.RunT(t)

	c := redis.NewClient(&redis.Options{
		Addr: red.Addr(),
	})
	s := NewVersionService(c)

	vers, err := s.GetObj(ctx, obj)
	r.NoError(err)
	r.Empty(vers)

	ver, err := s.IncrementObj(ctx, obj, stor1)
	r.NoError(err)
	r.EqualValues(1, ver)

	ver, err = s.IncrementObj(ctx, obj, stor2)
	r.NoError(err)
	r.EqualValues(2, ver)
	ver, err = s.IncrementObj(ctx, obj, stor2)
	r.NoError(err)
	r.EqualValues(3, ver)

	ver, err = s.IncrementObj(ctx, obj, stor3)
	r.NoError(err)
	r.EqualValues(4, ver)
	ver, err = s.IncrementObj(ctx, obj, stor3)
	r.NoError(err)
	r.EqualValues(5, ver)
	ver, err = s.IncrementObj(ctx, obj, stor3)
	r.NoError(err)
	r.EqualValues(6, ver)

	vers, err = s.GetObj(ctx, obj)
	r.NoError(err)
	r.Len(vers, 3)
	r.EqualValues(1, vers[stor1])
	r.EqualValues(3, vers[stor2])
	r.EqualValues(6, vers[stor3])

	ver, err = s.IncrementObj(ctx, obj, stor1)
	r.NoError(err)
	r.EqualValues(7, ver)

	vers, err = s.GetObj(ctx, obj)
	r.NoError(err)
	r.Len(vers, 3)
	r.EqualValues(7, vers[stor1])
	r.EqualValues(3, vers[stor2])
	r.EqualValues(6, vers[stor3])

	ver, err = s.IncrementObj(ctx, obj, stor2)
	r.NoError(err)
	r.EqualValues(8, ver)

	vers, err = s.GetObj(ctx, obj)
	r.NoError(err)
	r.Len(vers, 3)
	r.EqualValues(7, vers[stor1])
	r.EqualValues(8, vers[stor2])
	r.EqualValues(6, vers[stor3])

	ver, err = s.IncrementObj(ctx, obj, stor3)
	r.NoError(err)
	r.EqualValues(9, ver)

	vers, err = s.GetObj(ctx, obj)
	r.NoError(err)
	r.Len(vers, 3)
	r.EqualValues(7, vers[stor1])
	r.EqualValues(8, vers[stor2])
	r.EqualValues(9, vers[stor3])
}

func Test_DeleteBucketMeta(t *testing.T) {
	// setup
	r := require.New(t)
	red := miniredis.RunT(t)

	c := redis.NewClient(&redis.Options{
		Addr: red.Addr(),
	})

	s := NewVersionService(c)

	s1, s2 := "stor1", "stor2"
	b1, b2 := "buck1", "buck2"
	o1, o2, o3, o4 := dom.Object{
		Bucket: b1,
		Name:   "obj1",
	}, dom.Object{
		Bucket: b1,
		Name:   "obj2",
	}, dom.Object{
		Bucket: b2,
		Name:   "obj1",
	}, dom.Object{
		Bucket: b2,
		Name:   "obj2",
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// add version metadata for all storage/bucket/obj combinations
	s.IncrementBucket(ctx, b1, s1)
	s.IncrementBucket(ctx, b1, s2)
	s.IncrementBucket(ctx, b2, s1)
	s.IncrementBucket(ctx, b2, s2)

	s.IncrementBucketACL(ctx, b1, s1)
	s.IncrementBucketACL(ctx, b1, s2)
	s.IncrementBucketACL(ctx, b2, s1)
	s.IncrementBucketACL(ctx, b2, s2)

	s.IncrementBucketTags(ctx, b1, s1)
	s.IncrementBucketTags(ctx, b1, s2)
	s.IncrementBucketTags(ctx, b2, s1)
	s.IncrementBucketTags(ctx, b2, s2)

	s.IncrementObj(ctx, o1, s1)
	s.IncrementObj(ctx, o1, s2)
	s.IncrementObj(ctx, o2, s1)
	s.IncrementObj(ctx, o2, s2)
	s.IncrementObj(ctx, o3, s1)
	s.IncrementObj(ctx, o3, s2)
	s.IncrementObj(ctx, o4, s1)
	s.IncrementObj(ctx, o4, s2)

	s.IncrementACL(ctx, o1, s1)
	s.IncrementACL(ctx, o1, s2)
	s.IncrementACL(ctx, o2, s1)
	s.IncrementACL(ctx, o2, s2)
	s.IncrementACL(ctx, o3, s1)
	s.IncrementACL(ctx, o3, s2)
	s.IncrementACL(ctx, o4, s1)
	s.IncrementACL(ctx, o4, s2)

	s.IncrementTags(ctx, o1, s1)
	s.IncrementTags(ctx, o1, s2)
	s.IncrementTags(ctx, o2, s1)
	s.IncrementTags(ctx, o2, s2)
	s.IncrementTags(ctx, o3, s1)
	s.IncrementTags(ctx, o3, s2)
	s.IncrementTags(ctx, o4, s1)
	s.IncrementTags(ctx, o4, s2)

	// delete bucket meta only for storage s1 and bucket b1
	s.DeleteBucketMeta(ctx, s1, b1)

	// check that that version metadata only for s1&b1 combiantion was removed (version is zero)
	// and the rest of metadata remained the same
	ver, err := s.GetObj(ctx, o1)
	r.NoError(err)
	r.Zero(ver[s1])
	r.EqualValues(2, ver[s2])

	ver, err = s.GetObj(ctx, o2)
	r.NoError(err)
	r.Zero(ver[s1])
	r.EqualValues(2, ver[s2])

	ver, err = s.GetObj(ctx, o3)
	r.NoError(err)
	r.EqualValues(1, ver[s1])
	r.EqualValues(2, ver[s2])

	ver, err = s.GetObj(ctx, o4)
	r.NoError(err)
	r.EqualValues(1, ver[s1])
	r.EqualValues(2, ver[s2])

	ver, err = s.GetACL(ctx, o1)
	r.NoError(err)
	r.Zero(ver[s1])
	r.EqualValues(2, ver[s2])

	ver, err = s.GetACL(ctx, o2)
	r.NoError(err)
	r.Zero(ver[s1])
	r.EqualValues(2, ver[s2])

	ver, err = s.GetACL(ctx, o3)
	r.NoError(err)
	r.EqualValues(1, ver[s1])
	r.EqualValues(2, ver[s2])

	ver, err = s.GetACL(ctx, o4)
	r.NoError(err)
	r.EqualValues(1, ver[s1])
	r.EqualValues(2, ver[s2])

	ver, err = s.GetTags(ctx, o1)
	r.NoError(err)
	r.Zero(ver[s1])
	r.EqualValues(2, ver[s2])

	ver, err = s.GetTags(ctx, o2)
	r.NoError(err)
	r.Zero(ver[s1])
	r.EqualValues(2, ver[s2])

	ver, err = s.GetTags(ctx, o3)
	r.NoError(err)
	r.EqualValues(1, ver[s1])
	r.EqualValues(2, ver[s2])

	ver, err = s.GetTags(ctx, o4)
	r.NoError(err)
	r.EqualValues(1, ver[s1])
	r.EqualValues(2, ver[s2])

	ver, err = s.GetBucket(ctx, b1)
	r.NoError(err)
	r.Zero(ver[s1])
	r.EqualValues(2, ver[s2])

	ver, err = s.GetBucket(ctx, b2)
	r.NoError(err)
	r.EqualValues(1, ver[s1])
	r.EqualValues(2, ver[s2])

	ver, err = s.GetBucketACL(ctx, b1)
	r.NoError(err)
	r.Zero(ver[s1])
	r.EqualValues(2, ver[s2])

	ver, err = s.GetBucketACL(ctx, b2)
	r.NoError(err)
	r.EqualValues(1, ver[s1])
	r.EqualValues(2, ver[s2])

	ver, err = s.GetBucketTags(ctx, b1)
	r.NoError(err)
	r.Zero(ver[s1])
	r.EqualValues(2, ver[s2])

	ver, err = s.GetBucketTags(ctx, b2)
	r.NoError(err)
	r.EqualValues(1, ver[s1])
	r.EqualValues(2, ver[s2])
}
