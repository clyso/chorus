// Copyright 2025 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package entity

type ObjectLockID struct {
	Storage string
	Bucket  string
	Name    string
	Version string
}

func NewObjectLockID(storage string, bucket string, name string) ObjectLockID {
	return ObjectLockID{
		Storage: storage,
		Bucket:  bucket,
		Name:    name,
	}
}

func NewVersionedObjectLockID(storage string, bucket string, name string, version string) ObjectLockID {
	return ObjectLockID{
		Storage: storage,
		Bucket:  bucket,
		Name:    name,
		Version: version,
	}
}

type BucketLockID struct {
	Storage string
	Bucket  string
}

func NewBucketLockID(storage string, bucket string) BucketLockID {
	return BucketLockID{
		Storage: storage,
		Bucket:  bucket,
	}
}

type VersionedObjectID struct {
	Storage string
	Bucket  string
	Name    string
}

func NewVersionedObjectID(storage string, bucket string, name string) VersionedObjectID {
	return VersionedObjectID{
		Storage: storage,
		Bucket:  bucket,
		Name:    name,
	}
}

type MigrationObjectID struct {
	User        string
	FromStorage string
	ToStorage   string
	FromBucket  string
	ToBucket    string
	Prefix      string
}

func NewMigrationObjectID(user string, fromStorage string, fromBucket string, toStorage string, toBucket string, prefix string) MigrationObjectID {
	return MigrationObjectID{
		User:        user,
		FromStorage: fromStorage,
		ToStorage:   toStorage,
		FromBucket:  fromBucket,
		ToBucket:    toBucket,
		Prefix:      prefix,
	}
}

func NewMigrationObjectIDFromUniversalReplicationID(replicationID UniversalReplicationID, bucket string, prefix string) MigrationObjectID {
	fromBucket, toBucket := replicationID.FromToBuckets(bucket)
	return MigrationObjectID{
		User:        replicationID.user,
		FromStorage: replicationID.fromStorage,
		FromBucket:  fromBucket,
		ToStorage:   replicationID.toStorage,
		ToBucket:    toBucket,
		Prefix:      prefix,
	}
}

func NewNonRecursiveMigrationObjectIDFromUniversalReplicationID(replicationID UniversalReplicationID, bucket string) MigrationObjectID {
	fromBucket, toBucket := replicationID.FromToBuckets(bucket)
	return MigrationObjectID{
		User:        replicationID.user,
		FromStorage: replicationID.fromStorage,
		ToStorage:   replicationID.toStorage,
		FromBucket:  fromBucket,
		ToBucket:    toBucket,
	}
}

type MigrationBucketID struct {
	User        string
	FromStorage string
	ToStorage   string
	FromBucket  string
	ToBucket    string
}

func NewMigrationBucketIDFromUniversalReplicationID(replicationID UniversalReplicationID) MigrationBucketID {
	return MigrationBucketID{
		User:        replicationID.user,
		FromStorage: replicationID.fromStorage,
		ToStorage:   replicationID.toStorage,
		FromBucket:  replicationID.fromBucket,
		ToBucket:    replicationID.toBucket,
	}
}
