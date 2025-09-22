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

	TypeConsistencyCheck          = "consistency"
	TypeConsistencyCheckList      = "consistency:list"
	TypeConsistencyCheckReadiness = "consistency:readiness"
	TypeConsistencyCheckResult    = "consistency:result"

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
		ConsistencyCheckPayload |
		ConsistencyCheckListPayload |
		ConsistencyCheckReadinessPayload |
		ConsistencyCheckDeletePayload
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

type SwiftAccountUpdatePayload struct {
	replicationID

	// Date of the server response. Not Account modification date, so it cannot be
	// compared with Last-modified directly, but can be used as a reference
	// because Openstack Swift does not return Last-Modified for Account Updates
	Date string
}

type SwiftContainerUpdatePayload struct {
	replicationID

	Bucket string
	// Date of the server response. Not Container modification date, so it cannot be
	// compared with Last-modified directly, but can be used as a reference
	// because Openstack Swift does not return Last-Modified for Container Updates
	Date string
}

type SwiftObjectMetaUpdatePayload struct {
	replicationID

	Bucket string
	Object string
	// Date of the server response. Not Object modification date, so it cannot be
	// compared with Last-modified directly, but can be used as a reference
	// because Openstack Swift does not return Last-Modified for Object Meta Updates
	Date string
}

type SwiftObjectUpdatePayload struct {
	replicationID

	Bucket       string
	Object       string
	VersionID    string
	Etag         string
	LastModified string
}

type SwiftObjectDeletePayload struct {
	replicationID

	Bucket    string
	Object    string
	VersionID string
	// Date of the server response. Not Object deletion date, so it cannot be
	// compared with Last-modified directly, but can be used as a reference
	// because Openstack Swift does not return Last-Modified for Object delete
	Date            string
	DeleteMultipart bool
}

type SwiftAccountMigrationPayload struct {
	replicationID
}

type SwiftContainerMigrationPayload struct {
	replicationID

	Bucket string
}

type SwiftObjectMigrationPayload struct {
	replicationID

	Bucket          string
	ObjName         string
	ObjVersion      string
	ObjEtag         string
	ObjSize         int64
	ObjLastModified string
}

// case AccountUpdatePayload:
// 	queue := replicationQueueName(QueueEventsPrefix, replicationID)
// 	optionList = []asynq.Option{asynq.Queue(queue)}
// 	taskType = TypeAccountUpdate
// case ContainerUpdatePayload:
// 	queue := replicationQueueName(QueueEventsPrefix, replicationID)
// 	optionList = []asynq.Option{asynq.Queue(queue)}
// 	taskType = TypeContainerUpdate
// case ObjectUpdatePayload:
// 	queue := replicationQueueName(QueueEventsPrefix, replicationID)
// 	optionList = []asynq.Option{asynq.Queue(queue)}
// 	taskType = TypeObjUpdate
// case ObjectMetaUpdatePayload:
// 	queue := replicationQueueName(QueueEventsPrefix, replicationID)
// 	optionList = []asynq.Option{asynq.Queue(queue)}
// 	taskType = TypeObjMetaUpdate
// case ObjectDeletePayload:
// 	queue := replicationQueueName(QueueEventsPrefix, replicationID)
// 	optionList = []asynq.Option{asynq.Queue(queue)}
// 	taskType = TypeObjDelete
// case SwiftAccountMigrationPayload:
// 	id := fmt.Sprintf("mgr:swift:a:%s:%s:%s:%s", p.FromStorage, p.ToStorage, p.FromAccount, p.ToAccount)
// 	queue := replicationQueueName(QueueMigrateListObjectsPrefix, replicationID)
// 	optionList = []asynq.Option{asynq.Queue(queue), asynq.TaskID(id)}
// 	taskType = TypeSwiftAccountMigration
// case SwiftContainerMigrationPayload:
// 	id := fmt.Sprintf("mgr:swift:c:%s:%s:%s:%s:%s:%s", p.FromStorage, p.ToStorage, p.FromAccount, p.ToAccount, p.FromContaier, p.ToContaier)
// 	queue := replicationQueueName(QueueMigrateListObjectsPrefix, replicationID)
// 	optionList = []asynq.Option{asynq.Queue(queue), asynq.TaskID(id)}
// 	taskType = TypeSwiftContainerMigration
// case SwiftObjectMigrationPayload:
// 	id := fmt.Sprintf("mgr:swift:o:%s:%s:%s:%s:%s:%s:%s:%s", p.FromStorage, p.ToStorage, p.FromAccount, p.ToAccount, p.FromContaier, p.ToContaier, p.ObjName, p.ObjVersion)
// 	queue := replicationQueueName(QueueMigrateCopyObjectPrefix, replicationID)
// 	optionList = []asynq.Option{asynq.Queue(queue), asynq.TaskID(id)}
// 	taskType = TypeSwiftObjectMigration
