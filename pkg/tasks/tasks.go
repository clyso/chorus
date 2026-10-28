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

	TypeMigrateS3User             = "migrate:user"
	TypeMigrateBucketListObjects  = "migrate:bucket:list_objects"
	TypeMigrateObjCopy            = "migrate:object:copy"
	TypeMigrateObjectListVersions = "migrate:object:list_versions"
	TypeMigrateVersionedObject    = "migrate:object:copy_versioned"

	TypeDiff             = "diff"
	TypeDiffListObjects  = "diff:list_objects"
	TypeDiffListVersions = "diff:list_versions"

	TypeDiffFixCollectObjects = "diff:fix:collect_objects"
	TypeDiffFixRemoveObjects  = "diff:fix:remove_objects"
	TypeDiffFixEnsureRemove   = "diff:fix:ensure_remove"
	TypeDiffFixCopyS3         = "diff:fix:copy:s3"
	TypeDiffFixCopySwift      = "diff:fix:copy:swift"
	TypeDiffFixS3ListVersions = "diff:fix:list_versions"

	TypeApiZeroDowntimeSwitch = "api:switch_zero_downtime"
	TypeApiSwitchWithDowntime = "api:switch_w_downtime"

	// swift tasks:
	TypeSwiftAccountUpdate      = "account:update"
	TypeSwiftContainerUpdate    = "container:update"
	TypeSwiftObjUpdate          = "obj:update"
	TypeSwiftObjMetaUpdate      = "obj:meta:update"
	TypeSwiftObjDelete          = "obj:del"
	TypeSwiftAccountMigration   = "migrate:swift:account"
	TypeSwiftContainerMigration = "migrate:swift:container"
	TypeSwiftObjectMigration    = "migrate:swift:obj"
)

type TaskPayload interface {
	BucketCreatePayload |
		BucketDeletePayload |
		BucketSyncTagsPayload |
		BucketSyncACLPayload |
		ObjectSyncPayload |
		ObjSyncTagsPayload |
		ObjSyncACLPayload |
		MigrateS3UserPayload |
		MigrateBucketListObjectsPayload |
		MigrateObjCopyPayload |
		ListObjectVersionsPayload |
		MigrateVersionedObjectPayload |
		ZeroDowntimeReplicationSwitchPayload |
		SwitchWithDowntimePayload |
		SwiftAccountUpdatePayload |
		SwiftContainerUpdatePayload |
		SwiftObjectMetaUpdatePayload |
		SwiftObjectUpdatePayload |
		SwiftObjectDeletePayload |
		SwiftAccountMigrationPayload |
		SwiftContainerMigrationPayload |
		SwiftObjectMigrationPayload |
		DiffPayload |
		DiffListObjectsPayload |
		DiffListVersionsPayload |
		DiffFixCollectObjectsPayload |
		DiffFixRemoveObjectsPayload |
		DiffFixEnsureObjectsRemovedPayload |
		DiffFixSwiftCopyPayload |
		DiffFixS3CopyPayload |
		DiffFixS3ListVersionsPayload
}

type ReplicationTask interface {
	SetReplicationID(id entity.UniversalReplicationID, generation int64)
	GetReplicationID() entity.UniversalReplicationID
	SetInheritableQueue(queue string)
	GetInheritableQueue() string
}

type replicationID struct {
	ID         entity.UniversalReplicationID
	Queue      string
	Generation int64
}

func (r *replicationID) GetReplicationID() entity.UniversalReplicationID {
	return r.ID
}

func (r *replicationID) SetReplicationID(id entity.UniversalReplicationID, generation int64) {
	r.ID = id
	r.Generation = generation
}

func (r *replicationID) SetInheritableQueue(queue string) {
	r.Queue = queue
}

func (r *replicationID) GetInheritableQueue() string {
	return r.Queue
}

var _ ReplicationTask = (*replicationID)(nil)

type ZeroDowntimeReplicationSwitchPayload struct {
	ID entity.UniversalReplicationID
}

type SwitchWithDowntimePayload struct {
	ID entity.UniversalReplicationID
}

type BucketSyncTagsPayload struct {
	Bucket string
	replicationID
}

type ObjSyncTagsPayload struct {
	Object dom.Object
	replicationID
}

type BucketSyncACLPayload struct {
	Bucket string
	replicationID
}

type ObjSyncACLPayload struct {
	Object dom.Object
	replicationID
}

type BucketCreatePayload struct {
	Bucket   string
	Location string
	//Storage  string
	replicationID
}

type ObjectSyncPayload struct {
	Object dom.Object
	replicationID

	//FromVersion int64
	ObjSize int64
	Deleted bool
}

type BucketDeletePayload struct {
	Bucket string
	//Storage string
	replicationID
}

type ObjInfo struct {
	Name      string
	VersionID string
}

type ListObjectVersionsPayload struct {
	Bucket string
	Prefix string
	replicationID
}

type MigrateVersionedObjectPayload struct {
	Bucket string
	Prefix string
	replicationID
}

type MigrateS3UserPayload struct {
	replicationID
}

type MigrateBucketListObjectsPayload struct {
	Bucket string
	Prefix string
	replicationID
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
	ContentType string
	Size        int64
}

type MigrateLocation struct {
	Storage string
	Bucket  string
}

type DiffPayload struct {
	User        string
	Locations   []MigrateLocation
	Versioned   bool
	IgnoreEtags bool
	IgnoreSizes bool
}

type DiffListObjectsPayload struct {
	User        string
	Prefix      string
	Locations   []MigrateLocation
	Index       int
	Versioned   bool
	IgnoreEtags bool
	IgnoreSizes bool
}

type DiffListVersionsPayload struct {
	User        string
	Prefix      string
	Locations   []MigrateLocation
	Index       int
	IgnoreEtags bool
	IgnoreSizes bool
}

type DiffFixCollectObjectsPayload struct {
	StorageType dom.StorageType
	User        string
	Locations   []MigrateLocation
	SourceIndex int
	Versioned   bool
}

type DiffFixRemoveObjectsPayload struct {
	StorageType dom.StorageType
	User        string
	Locations   []MigrateLocation
	SourceIndex int
	RemoveIndex int
	Versioned   bool
}

type DiffFixEnsureObjectsRemovedPayload struct {
	StorageType dom.StorageType
	User        string
	Locations   []MigrateLocation
	SourceIndex int
	RemoveIndex int
	Versioned   bool
}

type DiffFixSwiftCopyPayload struct {
	Locations []MigrateLocation
	SwiftObjectMigrationPayload
}

type DiffFixS3CopyPayload struct {
	MigrateObjCopyPayload
	Locations []MigrateLocation
}

type DiffFixS3ListVersionsPayload struct {
	ListObjectVersionsPayload
	Locations []MigrateLocation
}

type SwiftAccountUpdatePayload struct {
	// Date of the server response. Not Account modification date, so it cannot be
	// compared with Last-modified directly, but can be used as a reference
	// because Openstack Swift does not return Last-Modified for Account Updates
	Date string
	replicationID
}

type SwiftContainerUpdatePayload struct {
	Bucket string
	// Date of the server response. Not Container modification date, so it cannot be
	// compared with Last-modified directly, but can be used as a reference
	// because Openstack Swift does not return Last-Modified for Container Updates
	Date string
	replicationID
}

type SwiftObjectMetaUpdatePayload struct {
	Bucket string
	Object string
	// Date of the server response. Not Object modification date, so it cannot be
	// compared with Last-modified directly, but can be used as a reference
	// because Openstack Swift does not return Last-Modified for Object Meta Updates
	Date string
	replicationID
}

type SwiftObjectUpdatePayload struct {
	Bucket       string
	Object       string
	VersionID    string
	Etag         string
	LastModified string
	replicationID
}

type SwiftObjectDeletePayload struct {
	Bucket    string
	Object    string
	VersionID string
	// Date of the server response. Not Object deletion date, so it cannot be
	// compared with Last-modified directly, but can be used as a reference
	// because Openstack Swift does not return Last-Modified for Object delete
	Date string
	replicationID
	DeleteMultipart bool
}

type SwiftAccountMigrationPayload struct {
	replicationID
}

type SwiftContainerMigrationPayload struct {
	Bucket string
	replicationID
}

type SwiftObjectMigrationPayload struct {
	Bucket          string
	ObjName         string
	ObjVersion      string
	ObjEtag         string
	ObjLastModified string
	replicationID
	ObjSize int64
}
