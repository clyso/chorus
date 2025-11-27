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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/testutil"
)

func Test_Version_e2e(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	s := NewVersionService(c)
	ctx := t.Context()

	var (
		replB1 = entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
			User:        "user1",
			FromStorage: "stor1",
			FromBucket:  "bucket",
			ToStorage:   "stor2",
			ToBucket:    "bucket",
		})

		replB2 = entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
			User:        "user1",
			FromStorage: "stor2",
			FromBucket:  "bucket",
			ToStorage:   "stor1",
			ToBucket:    "bucket2",
		})

		replU1 = entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
			User:        "user2",
			FromStorage: "stor1",
			ToStorage:   "stor2",
		})

		replU2 = entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
			User:        "user3",
			FromStorage: "stor1",
			ToStorage:   "stor2",
		})

		obj1 = dom.Object{
			Bucket:  "bucket",
			Name:    "obj1",
			Version: "",
		}

		obj2 = dom.Object{
			Bucket:  "bucket",
			Name:    "obj2",
			Version: "",
		}

		replsU = []entity.UniversalReplicationID{replU1, replU2}
		replsB = []entity.UniversalReplicationID{replB1, replB2}
		repls  = append(replsU, replsB...)
		objs   = []dom.Object{obj1, obj2}

		bucket = "bucket"

		emptyVer Version
	)

	// check that all versions are zero initially
	for _, rID := range repls {
		for _, obj := range objs {
			ver, err := s.GetObj(ctx, rID, obj)
			r.NoError(err)
			r.Equal(emptyVer, ver)

			ver, err = s.GetTags(ctx, rID, obj)
			r.NoError(err)
			r.Equal(emptyVer, ver)

			ver, err = s.GetACL(ctx, rID, obj)
			r.NoError(err)
			r.Equal(emptyVer, ver)
		}
		ver, err := s.GetBucket(ctx, rID, bucket)
		r.NoError(err)
		r.Equal(emptyVer, ver)

		ver, err = s.GetBucketTags(ctx, rID, bucket)
		r.NoError(err)
		r.Equal(emptyVer, ver)

		ver, err = s.GetBucketACL(ctx, rID, bucket)
		r.NoError(err)
		r.Equal(emptyVer, ver)
	}

	// increment every version different amount of times
	n := 0
	for _, rID := range repls {
		for _, obj := range objs {
			dest := Destination{
				Storage: rID.FromStorage(),
				Bucket:  obj.Bucket,
			}

			n++
			for i := 1; i <= n; i++ {
				incVer, err := s.IncrementObj(ctx, rID, obj, dest)
				r.NoError(err)
				r.EqualValues(i, incVer)
			}
			n++
			for i := 1; i <= n; i++ {
				incVer, err := s.IncrementTags(ctx, rID, obj, dest)
				r.NoError(err)
				r.EqualValues(i, incVer)
			}
			n++
			for i := 1; i <= n; i++ {
				incVer, err := s.IncrementACL(ctx, rID, obj, dest)
				r.NoError(err)
				r.EqualValues(i, incVer)
			}
		}
		dest := Destination{
			Storage: rID.FromStorage(),
			Bucket:  bucket,
		}

		n++
		for i := 1; i <= n; i++ {
			incVer, err := s.IncrementBucket(ctx, rID, bucket, dest)
			r.NoError(err)
			r.EqualValues(i, incVer)
		}
		n++
		for i := 1; i <= n; i++ {
			incVer, err := s.IncrementBucketTags(ctx, rID, bucket, dest)
			r.NoError(err)
			r.EqualValues(i, incVer)
		}
		n++
		for i := 1; i <= n; i++ {
			incVer, err := s.IncrementBucketACL(ctx, rID, bucket, dest)
			r.NoError(err)
			r.EqualValues(i, incVer)
		}
	}

	// check that all versions are as expected
	// and set to version = from version
	n = 0
	for _, rID := range repls {
		for _, obj := range objs {
			_, toBucket := rID.FromToBuckets(obj.Bucket)
			dest := Destination{
				Storage: rID.ToStorage(),
				Bucket:  toBucket,
			}

			n++
			ver, err := s.GetObj(ctx, rID, obj)
			r.NoError(err)
			r.EqualValues(n, ver.From)
			r.Zero(ver.To)
			// set to version = from
			err = s.UpdateIfGreater(ctx, rID, obj, dest, ver.From)
			r.NoError(err)

			n++
			ver, err = s.GetTags(ctx, rID, obj)
			r.NoError(err)
			r.EqualValues(n, ver.From)
			r.Zero(ver.To)
			// set to version = from
			err = s.UpdateTagsIfGreater(ctx, rID, obj, dest, ver.From)
			r.NoError(err)

			n++
			ver, err = s.GetACL(ctx, rID, obj)
			r.NoError(err)
			r.EqualValues(n, ver.From)
			r.Zero(ver.To)
			// set to version = from
			err = s.UpdateACLIfGreater(ctx, rID, obj, dest, ver.From)
			r.NoError(err)
		}
		_, toBucket := rID.FromToBuckets(bucket)
		dest := Destination{
			Storage: rID.ToStorage(),
			Bucket:  toBucket,
		}

		n++
		ver, err := s.GetBucket(ctx, rID, bucket)
		r.NoError(err)
		r.EqualValues(n, ver.From)
		r.Zero(ver.To)
		// set to version = from
		err = s.UpdateBucketIfGreater(ctx, rID, bucket, dest, ver.From)
		r.NoError(err)

		n++
		ver, err = s.GetBucketTags(ctx, rID, bucket)
		r.NoError(err)
		r.EqualValues(n, ver.From)
		r.Zero(ver.To)
		// set to version = from
		err = s.UpdateBucketTagsIfGreater(ctx, rID, bucket, dest, ver.From)
		r.NoError(err)

		n++
		ver, err = s.GetBucketACL(ctx, rID, bucket)
		r.NoError(err)
		r.EqualValues(n, ver.From)
		r.Zero(ver.To)
		// set to version = from
		err = s.UpdateBucketACLIfGreater(ctx, rID, bucket, dest, ver.From)
		r.NoError(err)
	}

	// check from/to versions
	n = 0
	for _, rID := range repls {
		for _, obj := range objs {
			n++
			ver, err := s.GetObj(ctx, rID, obj)
			r.NoError(err)
			r.EqualValues(n, ver.From)
			r.EqualValues(n, ver.To)

			n++
			ver, err = s.GetTags(ctx, rID, obj)
			r.NoError(err)
			r.EqualValues(n, ver.From)
			r.EqualValues(n, ver.To)

			n++
			ver, err = s.GetACL(ctx, rID, obj)
			r.NoError(err)
			r.EqualValues(n, ver.From)
			r.EqualValues(n, ver.To)
		}

		n++
		ver, err := s.GetBucket(ctx, rID, bucket)
		r.NoError(err)
		r.EqualValues(n, ver.From)
		r.EqualValues(n, ver.To)

		n++
		ver, err = s.GetBucketTags(ctx, rID, bucket)
		r.NoError(err)
		r.EqualValues(n, ver.From)
		r.EqualValues(n, ver.To)

		n++
		ver, err = s.GetBucketACL(ctx, rID, bucket)
		r.NoError(err)
		r.EqualValues(n, ver.From)
	}

	// delete replciation versions one by one and check that the rest remain unchanged
	n = 0
	for i, rID := range repls {
		// delete repl meta
		err := s.Cleanup(ctx, rID)
		r.NoError(err)

		n = 0
		// check deleted repl version are empty now
		for _, obj := range objs {
			n++
			ver, err := s.GetObj(ctx, rID, obj)
			r.NoError(err)
			r.Equal(emptyVer, ver)

			n++
			ver, err = s.GetTags(ctx, rID, obj)
			r.NoError(err)
			r.Equal(emptyVer, ver)

			n++
			ver, err = s.GetACL(ctx, rID, obj)
			r.NoError(err)
			r.Equal(emptyVer, ver)
		}
		n++
		ver, err := s.GetBucket(ctx, rID, bucket)
		r.NoError(err)
		r.Equal(emptyVer, ver)

		n++
		ver, err = s.GetBucketTags(ctx, rID, bucket)
		r.NoError(err)
		r.Equal(emptyVer, ver)

		n++
		ver, err = s.GetBucketACL(ctx, rID, bucket)
		r.NoError(err)
		r.Equal(emptyVer, ver)

		n *= i + 1
		// check remaining repls unchanged
		n = (i + 1) * (len(objs)*3 + 3)
		for _, rID := range repls[i+1:] {
			for _, obj := range objs {
				n++
				ver, err := s.GetObj(ctx, rID, obj)
				r.NoError(err)
				r.EqualValues(n, ver.From)
				r.EqualValues(n, ver.To)

				n++
				ver, err = s.GetTags(ctx, rID, obj)
				r.NoError(err)
				r.EqualValues(n, ver.From)
				r.EqualValues(n, ver.To)

				n++
				ver, err = s.GetACL(ctx, rID, obj)
				r.NoError(err)
				r.EqualValues(n, ver.From)
				r.EqualValues(n, ver.To)
			}

			n++
			ver, err := s.GetBucket(ctx, rID, bucket)
			r.NoError(err)
			r.EqualValues(n, ver.From)
			r.EqualValues(n, ver.To)

			n++
			ver, err = s.GetBucketTags(ctx, rID, bucket)
			r.NoError(err)
			r.EqualValues(n, ver.From)
			r.EqualValues(n, ver.To)

			n++
			ver, err = s.GetBucketACL(ctx, rID, bucket)
			r.NoError(err)
			r.EqualValues(n, ver.From)
		}
	}
}

func Test_Version(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	s := NewVersionService(c)
	ctx := t.Context()

	var (
		replID = entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
			User:        "user1",
			FromStorage: "stor1",
			FromBucket:  "bucket",
			ToStorage:   "stor2",
			ToBucket:    "bucket",
		})

		obj = dom.Object{
			Bucket:  "bucket",
			Name:    "obj1",
			Version: "",
		}

		fromDest = Destination{
			Storage: replID.FromStorage(),
			Bucket:  obj.Bucket,
		}

		toDest = Destination{
			Storage: replID.ToStorage(),
			Bucket:  "bucket",
		}
	)

	// check initial versions
	ver, err := s.GetObj(ctx, replID, obj)
	r.NoError(err, "no error for empty version")
	r.Equal(Version{}, ver, "initial version is zero")

	ver, err = s.GetTags(ctx, replID, obj)
	r.NoError(err, "no error for empty tags version")
	r.Equal(Version{}, ver, "initial tags version is zero")

	ver, err = s.GetACL(ctx, replID, obj)
	r.NoError(err, "no error for empty ACL version")
	r.Equal(Version{}, ver, "initial ACL version is zero")

	// test increments
	incVer, err := s.IncrementObj(ctx, replID, obj, fromDest)
	r.NoError(err, "increment from version")
	r.EqualValues(1, incVer, "from version incremented to 1")

	ver, err = s.GetObj(ctx, replID, obj)
	r.NoError(err, "get version after increment")
	r.EqualValues(1, ver.From, "from version incremented to 1")
	r.EqualValues(0, ver.To, "to version is still zero")

	incVer, err = s.IncrementObj(ctx, replID, obj, fromDest)
	r.NoError(err, "increment from version second time")
	r.EqualValues(2, incVer, "from version incremented to 2")

	ver, err = s.GetObj(ctx, replID, obj)
	r.NoError(err, "get version after second increment")
	r.EqualValues(2, ver.From, "from version incremented to 2")
	r.EqualValues(0, ver.To, "to version is still zero")

	incVer, err = s.IncrementObj(ctx, replID, obj, toDest)
	r.NoError(err, "increment to version")
	r.EqualValues(3, incVer, "to version incremented to 3 = max(from,to)+1")

	ver, err = s.GetObj(ctx, replID, obj)
	r.NoError(err, "get version after to increment")
	r.EqualValues(2, ver.From, "from version is still 2")
	r.EqualValues(3, ver.To, "to version incremented to 3")

	// test UpdateIfGreater
	wrongObj := dom.Object{Bucket: "bucket", Name: "asdfasdfasdfasdfasdfasdf", Version: ""}
	err = s.UpdateIfGreater(ctx, replID, wrongObj, fromDest, 3)
	r.ErrorIs(err, dom.ErrNotFound, "not fount error if hash not exists")

	wrongReplID := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User:        "userX",
		FromStorage: "storX",
		FromBucket:  "bucket",
		ToStorage:   "stor2",
		ToBucket:    "bucket",
	})
	err = s.UpdateIfGreater(ctx, wrongReplID, obj, toDest, 3)
	r.ErrorIs(err, dom.ErrNotFound, "error when replication ID not found")

	err = s.UpdateIfGreater(ctx, replID, obj, fromDest, 4)
	r.ErrorIs(err, dom.ErrNotFound, "error when trying to set version more than other side")

	err = s.UpdateIfGreater(ctx, replID, obj, fromDest, 1)
	r.ErrorIs(err, dom.ErrAlreadyExists, "error when trying to set version less than current")

	err = s.UpdateIfGreater(ctx, replID, obj, fromDest, 2)
	r.ErrorIs(err, dom.ErrAlreadyExists, "error when trying to set version equal to current")

	err = s.UpdateIfGreater(ctx, replID, obj, fromDest, 3)
	r.NoError(err, "set from version to 3")

	// inc versions for bucket
	incVer, err = s.IncrementBucket(ctx, replID, obj.Bucket, fromDest)
	r.NoError(err, "increment bucket from version")
	r.EqualValues(1, incVer, "bucket from version incremented to 1")
	ver, err = s.GetBucket(ctx, replID, obj.Bucket)
	r.NoError(err, "get bucket version after increment")
	r.EqualValues(1, ver.From, "bucket from version is 1")
	r.EqualValues(0, ver.To, "bucket to version is 0")

	//inc obj tags and acls
	incVer, err = s.IncrementTags(ctx, replID, obj, fromDest)
	r.NoError(err, "increment tags from version")
	r.EqualValues(1, incVer, "tags from version incremented to 1")
	ver, err = s.GetTags(ctx, replID, obj)
	r.NoError(err, "get object tags version after increment")
	r.EqualValues(1, ver.From, "tags from version is 1")
	r.EqualValues(0, ver.To, "tags to version is 0")

	incVer, err = s.IncrementACL(ctx, replID, obj, fromDest)
	r.NoError(err, "increment ACL from version")
	r.EqualValues(1, incVer, "ACL from version incremented to 1")
	ver, err = s.GetACL(ctx, replID, obj)
	r.NoError(err, "get object ACL version after increment")
	r.EqualValues(1, ver.From, "ACL from version is 1")
	r.EqualValues(0, ver.To, "ACL to version is 0")

	// delete all object versions
	err = s.DeleteObjAll(ctx, replID, obj)
	r.NoError(err, "delete all object versions")

	// check that all obj versions are zero and bucket version remains
	ver, err = s.GetObj(ctx, replID, obj)
	r.NoError(err, "get object version after deletion")
	r.Equal(Version{}, ver, "object version is zero after deletion")

	ver, err = s.GetTags(ctx, replID, obj)
	r.NoError(err, "get object tags version after deletion")
	r.Equal(Version{}, ver, "object tags version is zero after deletion")

	ver, err = s.GetACL(ctx, replID, obj)
	r.NoError(err, "get object ACL version after deletion")
	r.Equal(Version{}, ver, "object ACL version is zero after deletion")

	ver, err = s.GetBucket(ctx, replID, obj.Bucket)
	r.NoError(err, "get bucket version after object deletion")
	r.EqualValues(1, ver.From, "bucket from version remains")
	r.EqualValues(0, ver.To, "bucket to version remains zero")

	// inc obj payload tags and acls again to verify they work after deletion
	incVer, err = s.IncrementObj(ctx, replID, obj, fromDest)
	r.NoError(err, "increment object from version after deletion")
	r.EqualValues(1, incVer, "object from version incremented to 1 after deletion")
	ver, err = s.GetObj(ctx, replID, obj)
	r.NoError(err, "get object version after increment post-deletion")
	r.EqualValues(1, ver.From, "object from version is 1 after deletion")
	r.EqualValues(0, ver.To, "object to version is 0 after deletion")

	incVer, err = s.IncrementTags(ctx, replID, obj, fromDest)
	r.NoError(err, "increment tags from version after deletion")
	r.EqualValues(1, incVer, "tags from version incremented to 1 after deletion")
	ver, err = s.GetTags(ctx, replID, obj)
	r.NoError(err, "get object tags version after increment post-deletion")
	r.EqualValues(1, ver.From, "tags from version is 1 after deletion")
	r.EqualValues(0, ver.To, "tags to version is 0 after deletion")

	incVer, err = s.IncrementACL(ctx, replID, obj, fromDest)
	r.NoError(err, "increment ACL from version after deletion")
	r.EqualValues(1, incVer, "ACL from version incremented to 1 after deletion")
	ver, err = s.GetACL(ctx, replID, obj)
	r.NoError(err, "get object ACL version after increment post-deletion")
	r.EqualValues(1, ver.From, "ACL from version is 1 after deletion")
	r.EqualValues(0, ver.To, "ACL to version is 0 after deletion")

	//delete bucket versions
	err = s.DeleteBucketAll(ctx, replID, obj.Bucket)
	r.NoError(err, "delete all bucket versions")

	// check that bucket versions are zero now
	ver, err = s.GetBucket(ctx, replID, obj.Bucket)
	r.NoError(err, "get bucket version after deletion")
	r.Equal(Version{}, ver, "bucket version is zero after deletion")

	//check that all object version remains unaffected
	ver, err = s.GetObj(ctx, replID, obj)
	r.NoError(err, "get object version after bucket deletion")
	r.EqualValues(1, ver.From, "object from version remains after bucket deletion")
	r.EqualValues(0, ver.To, "object to version remains after bucket deletion")

	ver, err = s.GetTags(ctx, replID, obj)
	r.NoError(err, "get object tags version after bucket deletion")
	r.EqualValues(1, ver.From, "object tags from version remains after bucket deletion")
	r.EqualValues(0, ver.To, "object tags to version remains after bucket deletion")

	ver, err = s.GetACL(ctx, replID, obj)
	r.NoError(err, "get object ACL version after bucket deletion")
	r.EqualValues(1, ver.From, "object ACL from version remains after bucket deletion")
	r.EqualValues(0, ver.To, "object ACL to version remains after bucket deletion")

	// add second replication versions to verify cleanup of multiple replications
	replID2 := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User:        "user1",
		FromStorage: "stor1",
		FromBucket:  "bucket",
		ToStorage:   "stor3",
		ToBucket:    "bucket",
	})
	// inc all obj/bucket versions for second replication
	_, err = s.IncrementObj(ctx, replID2, obj, fromDest)
	r.NoError(err, "increment object from version for second replication")

	_, err = s.IncrementTags(ctx, replID2, obj, fromDest)
	r.NoError(err, "increment tags from version for second replication")

	_, err = s.IncrementACL(ctx, replID2, obj, fromDest)
	r.NoError(err, "increment ACL from version for second replication")

	_, err = s.IncrementBucket(ctx, replID2, obj.Bucket, fromDest)
	r.NoError(err, "increment bucket from version for second replication")

	_, err = s.IncrementBucketTags(ctx, replID2, obj.Bucket, fromDest)
	r.NoError(err, "increment bucket tags from version for second replication")

	_, err = s.IncrementBucketACL(ctx, replID2, obj.Bucket, fromDest)
	r.NoError(err, "increment bucket ACL from version for second replication")

	// check that versions for second replication are set
	ver, err = s.GetObj(ctx, replID2, obj)
	r.NoError(err, "get object version for second replication")
	r.EqualValues(1, ver.From, "object from version is 1 for second replication")
	r.EqualValues(0, ver.To, "object to version is 0 for second replication")

	ver, err = s.GetTags(ctx, replID2, obj)
	r.NoError(err, "get object tags version for second replication")
	r.EqualValues(1, ver.From, "object tags from version is 1 for second replication")
	r.EqualValues(0, ver.To, "object tags to version is 0 for second replication")

	ver, err = s.GetACL(ctx, replID2, obj)
	r.NoError(err, "get object ACL version for second replication")
	r.EqualValues(1, ver.From, "object ACL from version is 1 for second replication")
	r.EqualValues(0, ver.To, "object ACL to version is 0 for second replication")

	ver, err = s.GetBucket(ctx, replID2, obj.Bucket)
	r.NoError(err, "get bucket version for second replication")
	r.EqualValues(1, ver.From, "bucket from version is 1 for second replication")
	r.EqualValues(0, ver.To, "bucket to version is 0 for second replication")

	ver, err = s.GetBucketTags(ctx, replID2, obj.Bucket)
	r.NoError(err, "get bucket tags version for second replication")
	r.EqualValues(1, ver.From, "bucket tags from version is 1 for second replication")
	r.EqualValues(0, ver.To, "bucket tags to version is 0 for second replication")

	ver, err = s.GetBucketACL(ctx, replID2, obj.Bucket)
	r.NoError(err, "get bucket ACL version for second replication")
	r.EqualValues(1, ver.From, "bucket ACL from version is 1 for second replication")
	r.EqualValues(0, ver.To, "bucket ACL to version is 0 for second replication")

	// cleanup first replication

	err = s.Cleanup(ctx, replID)
	r.NoError(err, "cleanup replication versions")

	// check that all versions for the first replication are zero now
	ver, err = s.GetObj(ctx, replID, obj)
	r.NoError(err, "get object version after replication cleanup")
	r.Equal(Version{}, ver, "object version is zero after replication cleanup")

	ver, err = s.GetTags(ctx, replID, obj)
	r.NoError(err, "get object tags version after replication cleanup")
	r.Equal(Version{}, ver, "object tags version is zero after replication cleanup")

	ver, err = s.GetACL(ctx, replID, obj)
	r.NoError(err, "get object ACL version after replication cleanup")
	r.Equal(Version{}, ver, "object ACL version is zero after replication cleanup")

	ver, err = s.GetBucket(ctx, replID, obj.Bucket)
	r.NoError(err, "get bucket version after replication cleanup")
	r.Equal(Version{}, ver, "bucket version is zero after replication cleanup")

	// check that all versions for the second replication remain unaffected
	ver, err = s.GetObj(ctx, replID2, obj)
	r.NoError(err, "get object version for second replication after first replication cleanup")
	r.EqualValues(1, ver.From, "object from version remains for second replication after first replication cleanup")
	r.EqualValues(0, ver.To, "object to version remains for second replication after first replication cleanup")

	ver, err = s.GetTags(ctx, replID2, obj)
	r.NoError(err, "get object tags version for second replication after first replication cleanup")
	r.EqualValues(1, ver.From, "object tags from version remains for second replication after first replication cleanup")
	r.EqualValues(0, ver.To, "object tags to version remains for second replication after first replication cleanup")

	ver, err = s.GetACL(ctx, replID2, obj)
	r.NoError(err, "get object ACL version for second replication after first replication cleanup")
	r.EqualValues(1, ver.From, "object ACL from version remains for second replication after first replication cleanup")
	r.EqualValues(0, ver.To, "object ACL to version remains for second replication after first replication cleanup")

	ver, err = s.GetBucket(ctx, replID2, obj.Bucket)
	r.NoError(err, "get bucket version for second replication after first replication cleanup")
	r.EqualValues(1, ver.From, "bucket from version remains for second replication after first replication cleanup")
	r.EqualValues(0, ver.To, "bucket to version remains for second replication after first replication cleanup")

	ver, err = s.GetBucketTags(ctx, replID2, obj.Bucket)
	r.NoError(err, "get bucket tags version for second replication after first replication cleanup")
	r.EqualValues(1, ver.From, "bucket tags from version remains for second replication after first replication cleanup")
	r.EqualValues(0, ver.To, "bucket tags to version remains for second replication after first replication cleanup")

	ver, err = s.GetBucketACL(ctx, replID2, obj.Bucket)
	r.NoError(err, "get bucket ACL version for second replication after first replication cleanup")
	r.EqualValues(1, ver.From, "bucket ACL from version remains for second replication after first replication cleanup")
	r.EqualValues(0, ver.To, "bucket ACL to version remains for second replication after first replication cleanup")

	// final cleanup
	err = s.Cleanup(ctx, replID2)
	r.NoError(err, "final cleanup of second replication")

	// check that all versions for the second replication are zero now
	ver, err = s.GetObj(ctx, replID2, obj)
	r.NoError(err, "get object version after final replication cleanup")
	r.Equal(Version{}, ver, "object version is zero after final replication cleanup")

	ver, err = s.GetTags(ctx, replID2, obj)
	r.NoError(err, "get object tags version after final replication cleanup")
	r.Equal(Version{}, ver, "object tags version is zero after final replication cleanup")

	ver, err = s.GetACL(ctx, replID2, obj)
	r.NoError(err, "get object ACL version after final replication cleanup")
	r.Equal(Version{}, ver, "object ACL version is zero after final replication cleanup")

	ver, err = s.GetBucket(ctx, replID2, obj.Bucket)
	r.NoError(err, "get bucket version after final replication cleanup")
	r.Equal(Version{}, ver, "bucket version is zero after final replication cleanup")

	ver, err = s.GetBucketTags(ctx, replID2, obj.Bucket)
	r.NoError(err, "get bucket tags version after final replication cleanup")
	r.Equal(Version{}, ver, "bucket tags version is zero after final replication cleanup")

	ver, err = s.GetBucketACL(ctx, replID2, obj.Bucket)
	r.NoError(err, "get bucket ACL version after final replication cleanup")
	r.Equal(Version{}, ver, "bucket ACL version is zero after final replication cleanup")

}
