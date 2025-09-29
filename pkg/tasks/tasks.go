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

	TypeConsistencyCheck             = "consistency"
	TypeConsistencyCheckListObjects  = "consistency:list_objects"
	TypeConsistencyCheckListVersions = "consistency:list_versions"

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
		ConsistencyCheckListObjectsPayload |
		ConsistencyCheckListVersionsPayload
}

type ReplicationTask interface {
	SetReplicationID(id entity.UniversalReplicationID)
	GetReplicationID() entity.UniversalReplicationID
}

type replicationID struct {
	ID entity.UniversalReplicationID
}

func (r *replicationID) GetReplicationID() entity.UniversalReplicationID {
	return r.ID
}

func (r *replicationID) SetReplicationID(id entity.UniversalReplicationID) {
	r.ID = id
}

var _ ReplicationTask = (*replicationID)(nil)

type ZeroDowntimeReplicationSwitchPayload struct {
	ID entity.UniversalReplicationID
}

type SwitchWithDowntimePayload struct {
	ID entity.UniversalReplicationID
}

type BucketSyncTagsPayload struct {
	replicationID
	Bucket string
}

type ObjSyncTagsPayload struct {
	replicationID
	Object dom.Object
}

type BucketSyncACLPayload struct {
	replicationID
	Bucket string
}

type ObjSyncACLPayload struct {
	replicationID
	Object dom.Object
}

type BucketCreatePayload struct {
	replicationID
	Bucket   string
	Location string
	//Storage  string
}

type ObjectSyncPayload struct {
	replicationID
	Object dom.Object

	//FromVersion int64
	ObjSize int64
	Deleted bool
}

type BucketDeletePayload struct {
	replicationID
	Bucket string
	//Storage string
}

type ObjInfo struct {
	Name      string
	VersionID string
}

type ListObjectVersionsPayload struct {
	replicationID
	Bucket string
	Prefix string
}

type MigrateVersionedObjectPayload struct {
	replicationID
	Bucket string
	Prefix string
}

type MigrateBucketListObjectsPayload struct {
	replicationID
	Bucket    string
	Prefix    string
	Versioned bool
}

type MigrateObjCopyPayload struct {
	replicationID
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
}

type ConsistencyCheckPayload struct {
	Locations []MigrateLocation
	User      string
}

type ConsistencyCheckListObjectsPayload struct {
	Locations []MigrateLocation
	User      string
	Index     int
	Prefix    string
	Versioned bool
}

type ConsistencyCheckListVersionsPayload struct {
	Locations []MigrateLocation
	User      string
	Index     int
	Prefix    string
}
