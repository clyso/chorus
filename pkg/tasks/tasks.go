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
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/buger/jsonparser"
	"github.com/hibiken/asynq"

	xctx "github.com/clyso/chorus/pkg/ctx"
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
	TypeAccountUpdate           = "account:update"
	TypeContainerUpdate         = "container:update"
	TypeObjUpdate               = "obj:update"
	TypeObjMetaUpdate           = "obj:meta:update"
	TypeObjDelete               = "obj:del"
	TypeSwiftAccountMigration   = "migrate:swift:account"
	TypeSwiftContainerMigration = "migrate:swift:container"
	TypeSwiftObjectMigration    = "migrate:swift:obj"
)

type Queue string

const (
	QueueAPI                      Queue = "api"
	QueueMigrateListObjectsPrefix Queue = "migr_list_obj"
	QueueConsistencyCheck         Queue = "consistency_check"
	QueueMigrateCopyObjectPrefix  Queue = "migr_copy_obj"
	QueueEventsPrefix             Queue = "event"
)

// Priority defines the priority of the queues from highest to lowest.
var Priority = map[string]int{
	string(QueueAPI): 200, // highest priority
	string(QueueMigrateListObjectsPrefix) + ":*": 100,
	string(QueueConsistencyCheck):                50,
	string(QueueMigrateCopyObjectPrefix) + ":*":  10,
	string(QueueEventsPrefix) + ":*":             5, // lowest priority
	"*":                                          1, // fallback for legacy queues
}

func replicationQueueName(queuePrefix Queue, id entity.ReplicationStatusID) string {
	switch queuePrefix {
	case QueueMigrateCopyObjectPrefix,
		QueueMigrateListObjectsPrefix,
		QueueEventsPrefix:
		return fmt.Sprintf("%s:%s:%s:%s:%s", queuePrefix, id.FromStorage, id.FromBucket, id.ToStorage, id.ToBucket)
	default:
		panic(fmt.Sprintf("%s is not a replication queue prefix", queuePrefix))
	}
}

func InitMigrationQueues(id entity.ReplicationStatusID) []string {
	return []string{
		replicationQueueName(QueueMigrateListObjectsPrefix, id),
		replicationQueueName(QueueMigrateCopyObjectPrefix, id),
	}
}

func EventMigrationQueues(id entity.ReplicationStatusID) []string {
	return []string{
		replicationQueueName(QueueEventsPrefix, id),
	}
}

func AllReplicationQueues(id entity.ReplicationStatusID) []string {
	return append(InitMigrationQueues(id), EventMigrationQueues(id)...)
}

type SyncTask interface {
	GetFromStorage() string
	GetToStorage() string
	GetToBucket() *string
	SetFrom(storage, account string)
	SetTo(storage, account string, bucket string)
	InitDate()
	GetDate() time.Time
	GetFromAccount() string
	GetToAccount() string
}

type Sync struct {
	FromStorage string
	FromAccount string
	ToStorage   string
	ToAccount   string
	ToBucket    string
	CreatedAt   time.Time
}

func (t *Sync) GetFromAccount() string {
	return t.FromAccount
}

func (t *Sync) GetToAccount() string {
	return t.ToAccount
}

func (t *Sync) GetFromStorage() string {
	return t.FromStorage
}

func (t *Sync) GetToStorage() string {
	return t.ToStorage
}
func (t *Sync) GetToBucket() string {
	return t.ToBucket
}

func (t *Sync) SetFrom(storage, account string) {
	t.FromStorage, t.FromAccount = storage, account
}

func (t *Sync) SetTo(storage, account string, bucket string) {
	t.ToStorage = storage
	t.ToAccount = account
	t.ToBucket = bucket
}

func (t *Sync) InitDate() {
	t.CreatedAt = time.Now().UTC()
}
func (t *Sync) GetDate() time.Time {
	return t.CreatedAt
}

type ZeroDowntimeReplicationSwitchPayload struct {
	Sync
	Bucket string
	User   string
}

type BucketSyncTagsPayload struct {
	Bucket string
	Sync
}

type ObjSyncTagsPayload struct {
	Object dom.Object
	Sync
}

type BucketSyncACLPayload struct {
	Bucket string
	Sync
}

type ObjSyncACLPayload struct {
	Object dom.Object
	Sync
}

type BucketCreatePayload struct {
	Sync
	Bucket   string
	Location string
	//Storage  string
}

type ObjectSyncPayload struct {
	Object dom.Object
	Sync

	//FromVersion int64
	ObjSize int64
	Deleted bool
}

type BucketDeletePayload struct {
	Sync
	Bucket string
	//Storage string
}

type ObjInfo struct {
	Name      string
	VersionID string
}

type ListObjectVersionsPayload struct {
	Sync
	Bucket string
	Prefix string
}

type MigrateVersionedObjectPayload struct {
	Sync
	Bucket string
	Prefix string
}

type MigrateBucketListObjectsPayload struct {
	Sync
	Bucket    string
	Prefix    string
	Versioned bool
}

type MigrateObjCopyPayload struct {
	Sync
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
	FromStorage string
	ToStorage   string
	User        string
	Bucket      string
	CreatedAt   time.Time
}

type AccountUpdatePayload struct {
	Sync
	// Date of the server response. Not Account modification date, so it cannot be
	// compared with Last-modified directly, but can be used as a reference
	// because Openstack Swift does not return Last-Modified for Account Updates
	Date string
}

type ContainerUpdatePayload struct {
	Sync
	Bucket string
	// Date of the server response. Not Container modification date, so it cannot be
	// compared with Last-modified directly, but can be used as a reference
	// because Openstack Swift does not return Last-Modified for Container Updates
	Date string
}

type ObjectMetaUpdatePayload struct {
	Sync
	Bucket string
	Object string
	// Date of the server response. Not Object modification date, so it cannot be
	// compared with Last-modified directly, but can be used as a reference
	// because Openstack Swift does not return Last-Modified for Object Meta Updates
	Date string
}

type ObjectUpdatePayload struct {
	Sync
	Bucket       string
	Object       string
	VersionID    string
	Etag         string
	LastModified string
}

type ObjectDeletePayload struct {
	Sync
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
	FromStorage string
	ToStorage   string
	FromAccount string
	ToAccount   string
}

type SwiftContainerMigrationPayload struct {
	FromStorage  string
	FromAccount  string
	FromContaier string
	ToStorage    string
	ToAccount    string
	ToContaier   string
}

type SwiftObjectMigrationPayload struct {
	FromStorage  string
	FromAccount  string
	FromContaier string
	ToStorage    string
	ToAccount    string
	ToContaier   string

	ObjName         string
	ObjVersion      string
	ObjEtag         string
	ObjSize         int64
	ObjLastModified string
}

type ReplicationTask interface {
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
		MigrateVersionedObjectPayload
}

func NewReplicationTask[T ReplicationTask](ctx context.Context, replicationID entity.ReplicationStatusID, payload T) (*asynq.Task, error) {
	bytes, err := json.Marshal(&payload)
	if err != nil {
		return nil, err
	}
	if xctx.GetUser(ctx) != "" {
		bytes, err = jsonparser.Set(bytes, []byte(`"`+xctx.GetUser(ctx)+`"`), "User")
		if err != nil {
			return nil, fmt.Errorf("%w: unable to add User to payload", err)
		}
	}
	taskType := ""
	var optionList []asynq.Option
	switch p := any(payload).(type) {
	case BucketCreatePayload:
		id := fmt.Sprintf("cb:%s:%s:%s:%s", p.FromStorage, p.ToStorage, p.Bucket, p.ToBucket)
		queue := replicationQueueName(QueueMigrateListObjectsPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue), asynq.TaskID(id)}
		taskType = TypeBucketCreate
	case BucketDeletePayload:
		queue := replicationQueueName(QueueEventsPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue)}
		taskType = TypeBucketDelete
	case ObjectSyncPayload:
		queue := replicationQueueName(QueueEventsPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue)}
		taskType = TypeObjectSync
	case BucketSyncTagsPayload:
		queue := replicationQueueName(QueueEventsPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue)}
		taskType = TypeBucketSyncTags
	case BucketSyncACLPayload:
		queue := replicationQueueName(QueueEventsPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue)}
		taskType = TypeBucketSyncACL
	case ObjSyncTagsPayload:
		queue := replicationQueueName(QueueEventsPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue)}
		taskType = TypeObjectSyncTags
	case ObjSyncACLPayload:
		queue := replicationQueueName(QueueEventsPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue)}
		taskType = TypeObjectSyncACL
	case AccountUpdatePayload:
		optionList = []asynq.Option{asynq.Queue(taskOpts.priority.EventQueue())}
		taskType = TypeAccountUpdate
	case ContainerUpdatePayload:
		optionList = []asynq.Option{asynq.Queue(taskOpts.priority.EventQueue())}
		taskType = TypeContainerUpdate
	case ObjectUpdatePayload:
		optionList = []asynq.Option{asynq.Queue(taskOpts.priority.EventQueue())}
		taskType = TypeObjUpdate
	case ObjectMetaUpdatePayload:
		optionList = []asynq.Option{asynq.Queue(taskOpts.priority.EventQueue())}
		taskType = TypeObjMetaUpdate
	case ObjectDeletePayload:
		optionList = []asynq.Option{asynq.Queue(taskOpts.priority.EventQueue())}
		taskType = TypeObjDelete
	case SwiftAccountMigrationPayload:
		id := fmt.Sprintf("mgr:swift:a:%s:%s:%s:%s", p.FromStorage, p.ToStorage, p.FromAccount, p.ToAccount)
		optionList = []asynq.Option{asynq.Queue(taskOpts.priority.MigrationQueue()), asynq.TaskID(id)}
		taskType = TypeSwiftAccountMigration
	case SwiftContainerMigrationPayload:
		id := fmt.Sprintf("mgr:swift:c:%s:%s:%s:%s:%s:%s", p.FromStorage, p.ToStorage, p.FromAccount, p.ToAccount, p.FromContaier, p.ToContaier)
		optionList = []asynq.Option{asynq.Queue(taskOpts.priority.MigrationQueue()), asynq.TaskID(id)}
		taskType = TypeSwiftContainerMigration
	case SwiftObjectMigrationPayload:
		id := fmt.Sprintf("mgr:swift:o:%s:%s:%s:%s:%s:%s:%s:%s", p.FromStorage, p.ToStorage, p.FromAccount, p.ToAccount, p.FromContaier, p.ToContaier, p.ObjName, p.ObjVersion)
		optionList = []asynq.Option{asynq.Queue(taskOpts.priority.MigrationQueue()), asynq.TaskID(id)}
		taskType = TypeSwiftObjectMigration
	case MigrateBucketListObjectsPayload:
		id := fmt.Sprintf("mgr:lo:%s:%s:%s:%s", p.FromStorage, p.ToStorage, p.Bucket, p.ToBucket)
		if p.Prefix != "" {
			id += ":" + p.Prefix
		}
		queue := replicationQueueName(QueueMigrateListObjectsPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue), asynq.TaskID(id)}
		taskType = TypeMigrateBucketListObjects
	case MigrateObjCopyPayload:
		id := fmt.Sprintf("mgr:co:%s:%s:%s:%s:%s", p.FromStorage, p.ToStorage, p.Bucket, p.ToBucket, p.Obj.Name)
		if p.Obj.VersionID != "" {
			id += ":" + p.Obj.VersionID
		}
		queue := replicationQueueName(QueueMigrateCopyObjectPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue), asynq.TaskID(id)}
		taskType = TypeMigrateObjCopy
	case MigrateVersionedObjectPayload:
		id := fmt.Sprintf("mgr:cov:%s:%s:%s:%s", p.FromStorage, p.ToStorage, p.Bucket, p.Prefix)
		queue := replicationQueueName(QueueMigrateCopyObjectPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue), asynq.TaskID(id)}
		taskType = TypeMigrateVersionedObject
	case ListObjectVersionsPayload:
		id := fmt.Sprintf("mgr:lov:%s:%s:%s:%s", p.FromStorage, p.ToStorage, p.Bucket, p.Prefix)
		queue := replicationQueueName(QueueMigrateListObjectsPrefix, replicationID)
		optionList = []asynq.Option{asynq.Queue(queue), asynq.TaskID(id)}
		taskType = TypeMigrateObjectListVersions
	default:
		return nil, fmt.Errorf("%w: unknown task type %T", dom.ErrInvalidArg, p)
	}

	return asynq.NewTask(taskType, bytes, optionList...), nil
}

type ApiTask interface {
	ZeroDowntimeReplicationSwitchPayload |
		SwitchWithDowntimePayload |
		ConsistencyCheckPayload |
		ConsistencyCheckListPayload |
		ConsistencyCheckReadinessPayload |
		ConsistencyCheckDeletePayload
}

func NewTask[T ApiTask](ctx context.Context, payload T) (*asynq.Task, error) {
	bytes, err := json.Marshal(&payload)
	if err != nil {
		return nil, err
	}
	if xctx.GetUser(ctx) != "" {
		bytes, err = jsonparser.Set(bytes, []byte(`"`+xctx.GetUser(ctx)+`"`), "User")
		if err != nil {
			return nil, fmt.Errorf("%w: unable to add User to payload", err)
		}
	}
	taskType := ""
	var optionList []asynq.Option
	switch p := any(payload).(type) {
	case ZeroDowntimeReplicationSwitchPayload:
		optionList = []asynq.Option{asynq.Queue(string(QueueAPI)), asynq.TaskID(fmt.Sprintf("api:zdrs:%s:%s:%s:%s", p.FromStorage, p.ToStorage, p.User, p.Bucket))}
		taskType = TypeApiZeroDowntimeSwitch
	case SwitchWithDowntimePayload:
		id := fmt.Sprintf("api:sd:%s:%s:%s:%s", p.FromStorage, p.ToStorage, p.User, p.Bucket)
		optionList = []asynq.Option{asynq.Queue(string(QueueAPI)), asynq.TaskID(id)}
		taskType = TypeApiSwitchWithDowntime
	case ConsistencyCheckPayload:
		optionList = []asynq.Option{asynq.Queue(string(QueueConsistencyCheck))}
		taskType = TypeConsistencyCheck
	case ConsistencyCheckListPayload:
		id := fmt.Sprintf("cc:l:%s:%s:%s:%s", p.ID, p.Storage, p.Bucket, p.Prefix)
		optionList = []asynq.Option{asynq.Queue(string(QueueConsistencyCheck)), asynq.TaskID(id)}
		taskType = TypeConsistencyCheckList
	case ConsistencyCheckReadinessPayload:
		id := fmt.Sprintf("cc:r:%s", p.ID)
		optionList = []asynq.Option{asynq.Queue(string(QueueConsistencyCheck)), asynq.TaskID(id)}
		taskType = TypeConsistencyCheckReadiness
	case ConsistencyCheckDeletePayload:
		id := fmt.Sprintf("cc:d:%s", p.ID)
		optionList = []asynq.Option{asynq.Queue(string(QueueConsistencyCheck)), asynq.TaskID(id)}
		taskType = TypeConsistencyCheckResult
	default:
		return nil, fmt.Errorf("%w: unknown task type %T", dom.ErrInvalidArg, p)
	}

	return asynq.NewTask(taskType, bytes, optionList...), nil
}
