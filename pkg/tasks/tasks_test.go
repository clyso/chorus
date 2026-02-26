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

package tasks

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/entity"
)

func Test_ReplicationGetterSetter(t *testing.T) {
	r := require.New(t)
	expect := BucketCreatePayload{
		Bucket:   "fb",
		Location: "l",
	}
	r.Empty(expect.ID.AsString())
	r.True(expect.ID.IsEmpty())

	expect.SetReplicationID(entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User:        "u",
		FromStorage: "fs",
		FromBucket:  "fb",
		ToStorage:   "ts",
		ToBucket:    "tb",
	}))
	r.NotEmpty(expect.ID.AsString())
	r.False(expect.ID.IsEmpty())
	r.EqualValues("u", expect.ID.User())
	r.EqualValues("fs", expect.ID.FromStorage())
	r.EqualValues("ts", expect.ID.ToStorage())
	fromBucket, toBucket := expect.ID.FromToBuckets(expect.Bucket)
	r.EqualValues("fb", fromBucket)
	r.EqualValues("tb", toBucket)
	r.EqualValues(expect.ID, expect.GetReplicationID())
}
