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

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/clyso/chorus/pkg/dom"
)

func IDFromBucketReplication(id ReplicationStatusID) UniversalReplicationID {
	return UniversalReplicationID{
		user:        id.User,
		fromStorage: id.FromStorage,
		toStorage:   id.ToStorage,
		fromBucket:  id.FromBucket,
		toBucket:    id.ToBucket,
	}
}

type UniversalReplicationID struct {
	user        string
	fromStorage string
	toStorage   string
	fromBucket  string
	toBucket    string
}

func (r *UniversalReplicationID) FromStorage() string {
	return r.fromStorage
}

func (r *UniversalReplicationID) ToStorage() string {
	return r.toStorage
}

func (r *UniversalReplicationID) User() string {
	return r.user
}

func (r *UniversalReplicationID) FromToBuckets(taskBucket string) (from, to string) {
	if taskBucket != r.fromBucket {
		// should never happen
		panic(fmt.Sprintf("task bucket %q does not match replication from bucket %q", taskBucket, r.fromBucket))
	}
	if r.fromBucket == "" {
		// user replication
		return taskBucket, taskBucket
	}
	// bucket replication
	return r.fromBucket, r.toBucket
}

func (r *UniversalReplicationID) IsEmpty() bool {
	return r == nil || r.user == "" || r.fromStorage == "" || r.toStorage == ""
}

func (r *UniversalReplicationID) AsBucketID() (ReplicationStatusID, bool) {
	if r.IsEmpty() {
		return ReplicationStatusID{}, false
	}
	if r.fromBucket == "" {
		// user replication
		return ReplicationStatusID{}, false
	}
	// bucket replication
	return ReplicationStatusID{
		User:        r.user,
		FromStorage: r.fromStorage,
		ToStorage:   r.toStorage,
		FromBucket:  r.fromBucket,
		ToBucket:    r.toBucket,
	}, true
}

func (r *UniversalReplicationID) AsString() string {
	if r.IsEmpty() {
		return ""
	}
	if r.fromBucket == "" {
		// user replication
		return strings.Join([]string{r.user, r.fromStorage, r.toStorage}, ":")
	}
	// bucket replication
	return strings.Join([]string{r.user, r.fromStorage, r.toStorage, r.fromBucket, r.toBucket}, ":")
}

func UniversalIDFromString(s string) (UniversalReplicationID, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 3 && len(parts) != 5 {
		return UniversalReplicationID{}, fmt.Errorf("%w: invalid replication ID %q", dom.ErrInternal, s)
	}
	id := UniversalReplicationID{
		user:        parts[0],
		fromStorage: parts[1],
		toStorage:   parts[2],
	}
	if len(parts) == 5 {
		id.fromBucket = parts[3]
		id.toBucket = parts[4]
	}
	return id, nil
}

func (r UniversalReplicationID) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.AsString())
}

func (r *UniversalReplicationID) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	id, err := UniversalIDFromString(s)
	if err != nil {
		return err
	}
	*r = id
	return nil
}
