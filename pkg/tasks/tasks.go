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
	"fmt"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
)

// A list of task types.
const (
	TypeBucketCreate   = "bucket:create"
	TypeBucketDelete   = "bucket:delete"
	TypeBucketSyncTags = "bucket:sync:tags"
	TypeBucketSyncACL  = "bucket:sync:acl"

	TypeObjectSync     = "object:sync"
	TypeObjectSyncTags = "object:sync:tags"
	TypeObjectSyncACL  = "object:sync:acl"

	TypeMigrateBucketListObjects  = "migrate:bucket:list_objects"
	TypeMigrateObjCopy            = "migrate:object:copy"
	TypeMigrateObjectListVersions = "migrate:object:list_versions"
	TypeMigrateVersionedObject    = "migrate:object:copy_versioned"

	TypeConsistencyCheck          = "consistency"
	TypeConsistencyCheckList      = "consistency:list"
	TypeConsistencyCheckReadiness = "consistency:readiness"
	TypeConsistencyCheckResult    = "consistency:result"

	TypeApiZeroDowntimeSwitch = "api:switch_zero_downtime"
	TypeApiSwitchWithDowntime = "api:switch_w_downtime"
)

type TaskPayload interface {
	BucketCreatePayload |
		BucketDeletePayload |
		BucketSyncTagsPayload |
		BucketSyncACLPayload |
		ObjectSyncPayload |
		ObjSyncTagsPayload |
		ObjSyncACLPayload |
		MigrateBucketListObjectsPayload |
		MigrateObjCopyPayload |
		ListObjectVersionsPayload |
		MigrateVersionedObjectPayload |
		ZeroDowntimeReplicationSwitchPayload |
		SwitchWithDowntimePayload |
		ConsistencyCheckPayload |
		ConsistencyCheckListPayload |
		ConsistencyCheckReadinessPayload |
		ConsistencyCheckDeletePayload
}

type ReplicationTask interface {
	SetReplicationID(id entity.ReplicationStatusID)
	GetReplicationID() entity.ReplicationStatusID
	FromToBuckets(taskBucket string) (from, to string)
	validate() error
}

type ReplicationID struct {
	Replication entity.ReplicationStatusID
}

var _ ReplicationTask = (*ReplicationID)(nil)

func (t *ReplicationID) GetReplicationID() entity.ReplicationStatusID {
	return t.Replication
}

func (t *ReplicationID) SetReplicationID(id entity.ReplicationStatusID) {
	t.Replication = id
}

func (t *ReplicationID) validate() error {
	if t.Replication.FromStorage == "" {
		return fmt.Errorf("%w: invalid task replication id %+v: FromStorage required", dom.ErrInvalidArg, t.Replication)
	}
	if t.Replication.ToStorage == "" {
		return fmt.Errorf("%w: invalid task replication id %+v: ToStorage required", dom.ErrInvalidArg, t.Replication)
	}
	// FromBucket and ToBucket should be either both set or both empty
	if (t.Replication.FromBucket == "") != (t.Replication.ToBucket == "") {
		return fmt.Errorf("%w: invalid task replication id %+v: FromBucket and ToBucket should be either both set or both empty", dom.ErrInvalidArg, t.Replication)
	}
	return nil
}

func (t *ReplicationID) FromToBuckets(taskBucket string) (from, to string) {
	if t.Replication.FromBucket == "" {
		// user replication policy
		// keep bucket name the same
		return taskBucket, taskBucket
	}
	if t.Replication.FromBucket != taskBucket {
		// should not happen
		panic(fmt.Sprintf("replication task bucket name is different from source bucket name: expected %s, got %s", t.Replication.FromBucket, taskBucket))
	}
	if t.Replication.ToBucket == "" {
		// should never happen. If FromBucket is set, ToBucket must be set too.
		return taskBucket, taskBucket
	}
	return taskBucket, t.Replication.ToBucket
}

type ZeroDowntimeReplicationSwitchPayload struct {
	ReplicationID
}

type BucketSyncTagsPayload struct {
	Bucket string
	ReplicationID
}

type ObjSyncTagsPayload struct {
	Object dom.Object
	ReplicationID
}

type BucketSyncACLPayload struct {
	Bucket string
	ReplicationID
}

type ObjSyncACLPayload struct {
	Object dom.Object
	ReplicationID
}

type BucketCreatePayload struct {
	ReplicationID
	Bucket   string
	Location string
	//Storage  string
}

type ObjectSyncPayload struct {
	Object dom.Object
	ReplicationID

	//FromVersion int64
	ObjSize int64
	Deleted bool
}

type BucketDeletePayload struct {
	ReplicationID
	Bucket string
	//Storage string
}

type ObjInfo struct {
	Name      string
	VersionID string
}

type ListObjectVersionsPayload struct {
	ReplicationID
	Bucket string
	Prefix string
}

type MigrateVersionedObjectPayload struct {
	ReplicationID
	Bucket string
	Prefix string
}

type MigrateBucketListObjectsPayload struct {
	ReplicationID
	Bucket    string
	Prefix    string
	Versioned bool
}

type MigrateObjCopyPayload struct {
	ReplicationID
	Bucket string
	Obj    ObjPayload
}

type ObjPayload struct {
	Name        string
	VersionID   string
	ETag        string
	Size        int64
	ContentType string
}

type MigrateLocation struct {
	Storage string
	Bucket  string
	User    string
}

type ConsistencyCheckPayload struct {
	ID        string
	Locations []MigrateLocation
}

type ConsistencyCheckListPayload struct {
	MigrateLocation
	Prefix       string
	ID           string
	StorageCount uint8
}

type ConsistencyCheckReadinessPayload struct {
	ID string
}

type ConsistencyCheckDeletePayload struct {
	ID string
}

type SwitchWithDowntimePayload struct {
	ReplicationID
}
