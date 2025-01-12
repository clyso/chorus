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
	"github.com/buger/jsonparser"
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/hibiken/asynq"
	"time"
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

	TypeMigrateBucketListObjects = "migrate:bucket:list_objects"
	TypeMigrateObjCopy           = "migrate:object:copy"

	TypeApiCostEstimation     = "api:cost_estimation"
	TypeApiCostEstimationList = "api:cost_estimation:list"
	TypeApiReplicationSwitch  = "api:replication_switch"
)

type Priority uint8

func (p Priority) EventQueue() string {
	switch p {
	case PriorityDefault1:
		return QueueEventsDefault1
	case Priority2:
		return QueueEvents2
	case Priority3:
		return QueueEvents3
	case Priority4:
		return QueueEvents4
	case PriorityHighest5:
		return QueueEventsHighest5
	}
	return QueueEventsDefault1
}

func (p Priority) MigrationQueue() string {
	switch p {
	case PriorityDefault1:
		return QueueMigrateObjCopyDefault1
	case Priority2:
		return QueueMigrateObjCopy2
	case Priority3:
		return QueueMigrateObjCopy3
	case Priority4:
		return QueueMigrateObjCopy4
	case PriorityHighest5:
		return QueueMigrateObjCopyHighest5
	}
	return QueueMigrateObjCopyDefault1
}

const (
	PriorityDefault1 Priority = iota
	Priority2        Priority = iota
	Priority3        Priority = iota
	Priority4        Priority = iota
	PriorityHighest5 Priority = iota
)

const (
	QueueEventsDefault1           = "events1"
	QueueEvents2                  = "events2"
	QueueEvents3                  = "events3"
	QueueEvents4                  = "events4"
	QueueEventsHighest5           = "events5"
	QueueMigrateBucketListObjects = "migrate_bucket_list_obj"
	QueueMigrateObjCopyDefault1   = "migrate_obj_copy1"
	QueueMigrateObjCopy2          = "migrate_obj_copy2"
	QueueMigrateObjCopy3          = "migrate_obj_copy3"
	QueueMigrateObjCopy4          = "migrate_obj_copy4"
	QueueMigrateObjCopyHighest5   = "migrate_obj_copy5"
	QueueAPI                      = "api"
)

const (
	costEstimationTaskRetention = time.Second
)

type SyncTask interface {
	GetFrom() string
	GetToStorage() string
	GetToBucket() *string
	SetFrom(from string)
	SetTo(storage string, bucket *string)
	InitDate()
	GetDate() time.Time
}

type Sync struct {
	FromStorage string
	ToStorage   string
	ToBucket    *string
	CreatedAt   time.Time
}

func (t *Sync) GetFrom() string {
	return t.FromStorage
}
func (t *Sync) GetToStorage() string {
	return t.ToStorage
}
func (t *Sync) GetToBucket() *string {
	return t.ToBucket
}
func (t *Sync) SetFrom(from string) {
	t.FromStorage = from
}
func (t *Sync) SetTo(storage string, bucket *string) {
	t.ToStorage = storage
	t.ToBucket = bucket
}
func (t *Sync) InitDate() {
	t.CreatedAt = time.Now().UTC()
}
func (t *Sync) GetDate() time.Time {
	return t.CreatedAt
}

type CostEstimationPayload struct {
	Sync
}

type FinishReplicationSwitchPayload struct {
	User   string
	Bucket string
}

type CostEstimationListPayload struct {
	FromStorage string
	ToStorage   string
	Bucket      string
	Prefix      string
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

type MigrateBucketListObjectsPayload struct {
	Sync
	Bucket string
	Prefix string
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

func NewTask[T BucketCreatePayload | BucketDeletePayload |
	BucketSyncTagsPayload | BucketSyncACLPayload |
	ObjectSyncPayload | ObjSyncTagsPayload | ObjSyncACLPayload |
	MigrateBucketListObjectsPayload | MigrateObjCopyPayload |
	CostEstimationPayload | CostEstimationListPayload | FinishReplicationSwitchPayload](ctx context.Context, payload T, opts ...Opt) (*asynq.Task, error) {
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
	taskOpts := options{}
	for _, o := range opts {
		o.apply(&taskOpts)
	}
	taskType := ""
	var optionList []asynq.Option
	switch p := any(payload).(type) {
	case FinishReplicationSwitchPayload:
		optionList = []asynq.Option{asynq.Queue(QueueAPI), asynq.TaskID(fmt.Sprintf("api:rs:%s:%s", p.User, p.Bucket))}
		taskType = TypeApiReplicationSwitch
	case CostEstimationPayload:
		optionList = []asynq.Option{asynq.Queue(QueueAPI), asynq.Retention(costEstimationTaskRetention), asynq.TaskID(fmt.Sprintf("api:ce:%s:%s", p.FromStorage, p.ToStorage))}
		taskType = TypeApiCostEstimation
	case CostEstimationListPayload:
		id := fmt.Sprintf("api:cel:%s:%s:%s", p.FromStorage, p.ToStorage, p.Bucket)
		if p.Prefix != "" {
			id = fmt.Sprintf("api:cel:%s:%s:%s:%s", p.FromStorage, p.ToStorage, p.Bucket, p.Prefix)
		}
		optionList = []asynq.Option{asynq.Queue(QueueAPI), asynq.TaskID(id)}
		taskType = TypeApiCostEstimationList
	case BucketCreatePayload:
		id := fmt.Sprintf("cb:%s:%s:%s", p.FromStorage, p.ToStorage, p.Bucket)
		if p.ToBucket != nil {
			id += ":" + *p.ToBucket
		}
		optionList = []asynq.Option{asynq.Queue(taskOpts.priority.EventQueue()), asynq.TaskID(id)}
		taskType = TypeBucketCreate
	case BucketDeletePayload:
		optionList = []asynq.Option{asynq.Queue(taskOpts.priority.EventQueue())}
		taskType = TypeBucketDelete
	case ObjectSyncPayload:
		optionList = []asynq.Option{asynq.Queue(taskOpts.priority.EventQueue())}
		taskType = TypeObjectSync
	case BucketSyncTagsPayload:
		optionList = []asynq.Option{asynq.Queue(taskOpts.priority.EventQueue())}
		taskType = TypeBucketSyncTags
	case BucketSyncACLPayload:
		optionList = []asynq.Option{asynq.Queue(taskOpts.priority.EventQueue())}
		taskType = TypeBucketSyncACL
	case ObjSyncTagsPayload:
		optionList = []asynq.Option{asynq.Queue(taskOpts.priority.EventQueue())}
		taskType = TypeObjectSyncTags
	case ObjSyncACLPayload:
		optionList = []asynq.Option{asynq.Queue(taskOpts.priority.EventQueue())}
		taskType = TypeObjectSyncACL
	case MigrateBucketListObjectsPayload:
		id := fmt.Sprintf("mgr:lo:%s:%s:%s", p.FromStorage, p.ToStorage, p.Bucket)
		if p.ToBucket != nil {
			id += ":" + *p.ToBucket
		}
		if p.Prefix != "" {
			id += ":" + p.Prefix
		}
		optionList = []asynq.Option{asynq.Queue(QueueMigrateBucketListObjects), asynq.TaskID(id)}
		taskType = TypeMigrateBucketListObjects
	case MigrateObjCopyPayload:
		id := fmt.Sprintf("mgr:co:%s:%s:%s:%s", p.FromStorage, p.ToStorage, p.Bucket, p.Obj.Name)
		if p.ToBucket != nil {
			id += ":" + *p.ToBucket
		}
		if p.Obj.VersionID != "" {
			id += ":" + p.Obj.VersionID
		}
		optionList = []asynq.Option{asynq.Queue(taskOpts.priority.MigrationQueue()), asynq.TaskID(id)}
		taskType = TypeMigrateObjCopy
	default:
		return nil, fmt.Errorf("%w: unknown task type %+v", dom.ErrInvalidArg, payload)
	}

	return asynq.NewTask(taskType, bytes, optionList...), nil
}

type options struct {
	priority Priority
}

type Opt interface {
	apply(*options)
}

type priorityOption Priority

func (o priorityOption) apply(opts *options) {
	opts.priority = Priority(o)
}

func WithPriority(p Priority) Opt {
	return priorityOption(p)
}
