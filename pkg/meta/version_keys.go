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

package meta

import (
	"fmt"
	"slices"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
)

type versionKind string

const (
	payload versionKind = "p"
	acl     versionKind = "a"
	tags    versionKind = "t"
)

var kinds = []versionKind{acl, payload, tags}

// Version meta Redis schema:
//
// Versions are stored in per-bucket Redis hashes, where:
// Key: v:<user_id>:<from_storage>:<to_storage>:<from_bucket>:<to_bucket>
// Fields:
// {f|t}:[<obj_id>]:{p|a|t} -> <int_version>
// ^from/to ^object    ^(payload|acl|tags)
//
// Q: why per-bucket hash instead of per-replicationID hash?
//
//	Pros: lower memory usage for user replication: no bucket name if hash fields for objects
//	Cons: must use SCAN prefix MATCH to delete all metadata for user replication
//
// Motivation:
//
//	avg(buckets per user) << avg(objects per bucket) => big memory savings & small scan overhead => per-bucket hash
//
// Example hash:
//
// v:user1:storage1:storage2:bucket1:bucket1 -> {
//
//	 "f::t": 5,              # from bucket1 tags version
//	 "t::t": 3,              # to   bucket1 tags version
//	 "f::a": 2,              # from bucket1 acl version
//	 "t::a": 4,              # to   bucket1 acl version
//	 "f:obj1:p": 10,         # from bucket1/obj1 payload version
//	 "t:obj1:p": 8,          # to   bucket1/obj1 payload version
//	 "f:obj1:t": 10,         # from bucket1/obj1 tags version
//	 "t:obj1:t": 8,          # to   bucket1/obj1 tags version
//	 ...
//	}
type redisKeys struct {
	hashKey   string
	fromField string
	toField   string
}

func toRedisKeys(replID entity.UniversalReplicationID, bucket, objectID string, kind versionKind) redisKeys {
	fromBucket, toBucket := replID.FromToBuckets(bucket)
	return redisKeys{
		hashKey:   fmt.Sprintf("v:%s:%s:%s:%s:%s", replID.User(), replID.FromStorage(), replID.ToStorage(), fromBucket, toBucket),
		fromField: fmt.Sprintf("f:%s:%s", objectID, kind),
		toField:   fmt.Sprintf("t:%s:%s", objectID, kind),
	}
}

// returns Redis MATCH key pattern
func redisReplicationMATCH(replID entity.UniversalReplicationID) string {
	if bucketID, ok := replID.AsBucketID(); ok {
		// exact match for single HASH of bucket replication
		return fmt.Sprintf("v:%s:%s:%s:%s:%s", replID.User(), replID.FromStorage(), replID.ToStorage(), bucketID.FromBucket, bucketID.ToBucket)
	}
	// wildcard match for all HASHES of user replication
	return fmt.Sprintf("v:%s:%s:%s:*", replID.User(), replID.FromStorage(), replID.ToStorage())
}

func toRedisBucketKeys(replID entity.UniversalReplicationID, bucket string, kind versionKind) (redisKeys, error) {
	if bucket == "" {
		return redisKeys{}, fmt.Errorf("%w: bucket name is required for bucket version", dom.ErrInvalidArg)
	}
	if !slices.Contains(kinds, kind) {
		return redisKeys{}, fmt.Errorf("%w: invalid bucket version kind %q", dom.ErrInvalidArg, kind)
	}
	return toRedisKeys(replID, bucket, "", kind), nil
}

func toRedisObjKeys(replID entity.UniversalReplicationID, object dom.Object, kind versionKind) (redisKeys, error) {
	bucket, objectID := object.Bucket, objectID(object)
	if bucket == "" {
		return redisKeys{}, fmt.Errorf("%w: bucket name is required for object version", dom.ErrInvalidArg)
	}
	if objectID == "" {
		return redisKeys{}, fmt.Errorf("%w: object ID is required for object version", dom.ErrInvalidArg)
	}
	if !slices.Contains(kinds, kind) {
		return redisKeys{}, fmt.Errorf("%w: invalid object version kind %q", dom.ErrInvalidArg, kind)
	}
	return toRedisKeys(replID, bucket, objectID, kind), nil
}

// provides unique ID for versioning objects
func objectID(obj dom.Object) string {
	if obj.Version == "" {
		return obj.Name
	}
	return obj.Name + obj.Version
}

func allRedisObjKeys(replID entity.UniversalReplicationID, object dom.Object) (key string, fields []string, err error) {
	fields = make([]string, 0, len(kinds)*2)
	for _, kind := range kinds {
		objKeys, err := toRedisObjKeys(replID, object, kind)
		if err != nil {
			return "", nil, err
		}
		key = objKeys.hashKey
		fields = append(fields, objKeys.fromField, objKeys.toField)
	}
	return key, fields, nil
}

func allRedisBucketKeys(replID entity.UniversalReplicationID, bucket string) (key string, fields []string, err error) {
	fields = make([]string, 0, len(kinds)*2)
	for _, kind := range kinds {
		objKeys, err := toRedisBucketKeys(replID, bucket, kind)
		if err != nil {
			return "", nil, err
		}
		key = objKeys.hashKey
		fields = append(fields, objKeys.fromField, objKeys.toField)
	}
	return key, fields, nil
}
