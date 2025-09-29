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

package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/hibiken/asynq"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
)

// encoder contains metadata for task payload.
type encoder[T TaskPayload] struct {
	// generates unique task ID based on payload. Used for deduplication.
	// If nil, no task ID is set and no deduplication is performed.
	taskID func(p T) string
	// calculates queue name based on payload.
	queue func(p T) string
	// Uniq
	taskType string
}

func (e encoder[T]) Encode(ctx context.Context, payload T) (*asynq.Task, error) {
	// Ensure that replication ID is set for replication tasks.
	if replTask, ok := any(&payload).(ReplicationTask); ok {
		id := replTask.GetReplicationID()
		if id.IsEmpty() {
			return nil, fmt.Errorf("%w: replicationID is not set to replication task: %#v. Use task.SetReplicationID() method", dom.ErrInternal, payload)
		}
	}
	bytes, err := json.Marshal(&payload)
	if err != nil {
		return nil, err
	}
	queue := e.queue(payload)
	optionList := []asynq.Option{asynq.Queue(queue)}
	if e.taskID != nil {
		id := e.taskID(payload)
		if id == "" {
			return nil, fmt.Errorf("%w: task ID generator returned empty task ID", dom.ErrInternal)
		}
		optionList = append(optionList, asynq.TaskID(id))
	}
	return asynq.NewTask(e.taskType, bytes, optionList...), nil
}

func (e encoder[T]) Enqueue(ctx context.Context, taskClient *asynq.Client, payload T) error {
	task, err := e.Encode(ctx, payload)
	if err != nil {
		return err
	}
	_, err = taskClient.EnqueueContext(ctx, task)
	if err != nil && !errors.Is(err, asynq.ErrDuplicateTask) && !errors.Is(err, asynq.ErrTaskIDConflict) {
		return err
	}
	return nil
}

func enqueueAny(ctx context.Context, taskClient *asynq.Client, payload any) error {
	// TODO: think about code generation to avoid this boilerplate code
	switch p := payload.(type) {
	case *BucketCreatePayload:
		return bucketCreate.Enqueue(ctx, taskClient, *p)
	case BucketCreatePayload:
		return bucketCreate.Enqueue(ctx, taskClient, p)
	case *BucketDeletePayload:
		return bucketDelete.Enqueue(ctx, taskClient, *p)
	case BucketDeletePayload:
		return bucketDelete.Enqueue(ctx, taskClient, p)
	case *BucketSyncTagsPayload:
		return bucketSyncTags.Enqueue(ctx, taskClient, *p)
	case BucketSyncTagsPayload:
		return bucketSyncTags.Enqueue(ctx, taskClient, p)
	case *BucketSyncACLPayload:
		return bucketSyncACL.Enqueue(ctx, taskClient, *p)
	case BucketSyncACLPayload:
		return bucketSyncACL.Enqueue(ctx, taskClient, p)
	case *ObjectSyncPayload:
		return objectSync.Enqueue(ctx, taskClient, *p)
	case ObjectSyncPayload:
		return objectSync.Enqueue(ctx, taskClient, p)
	case *ObjSyncTagsPayload:
		return objSyncTags.Enqueue(ctx, taskClient, *p)
	case ObjSyncTagsPayload:
		return objSyncTags.Enqueue(ctx, taskClient, p)
	case *ObjSyncACLPayload:
		return objSyncACL.Enqueue(ctx, taskClient, *p)
	case ObjSyncACLPayload:
		return objSyncACL.Enqueue(ctx, taskClient, p)
	case *MigrateBucketListObjectsPayload:
		return migrateBucketListObjects.Enqueue(ctx, taskClient, *p)
	case MigrateBucketListObjectsPayload:
		return migrateBucketListObjects.Enqueue(ctx, taskClient, p)
	case *MigrateObjCopyPayload:
		return migrateObjCopy.Enqueue(ctx, taskClient, *p)
	case MigrateObjCopyPayload:
		return migrateObjCopy.Enqueue(ctx, taskClient, p)
	case *ListObjectVersionsPayload:
		return listObjectVersions.Enqueue(ctx, taskClient, *p)
	case ListObjectVersionsPayload:
		return listObjectVersions.Enqueue(ctx, taskClient, p)
	case *MigrateVersionedObjectPayload:
		return migrateVersionedObject.Enqueue(ctx, taskClient, *p)
	case MigrateVersionedObjectPayload:
		return migrateVersionedObject.Enqueue(ctx, taskClient, p)
	case *ZeroDowntimeReplicationSwitchPayload:
		return zeroDowntimeReplicationSwitch.Enqueue(ctx, taskClient, *p)
	case ZeroDowntimeReplicationSwitchPayload:
		return zeroDowntimeReplicationSwitch.Enqueue(ctx, taskClient, p)
	case *SwitchWithDowntimePayload:
		return switchWithDowntime.Enqueue(ctx, taskClient, *p)
	case SwitchWithDowntimePayload:
		return switchWithDowntime.Enqueue(ctx, taskClient, p)
	case *ConsistencyCheckPayload:
		return consistencyCheck.Enqueue(ctx, taskClient, *p)
	case ConsistencyCheckPayload:
		return consistencyCheck.Enqueue(ctx, taskClient, p)
	case *ConsistencyCheckListObjectsPayload:
		return consistencyCheckListObjects.Enqueue(ctx, taskClient, *p)
	case ConsistencyCheckListObjectsPayload:
		return consistencyCheckListObjects.Enqueue(ctx, taskClient, p)
	case *ConsistencyCheckListVersionsPayload:
		return consistencyCheckListVersions.Enqueue(ctx, taskClient, *p)
	case ConsistencyCheckListVersionsPayload:
		return consistencyCheckListVersions.Enqueue(ctx, taskClient, p)
	default:
		return fmt.Errorf("%w: unsupported payload type %T. Define encoder[%T] instance in pkg/tasks/encoder.go and add it to encodeAny switch statement", dom.ErrNotImplemented, payload, payload)
	}
}

var (
	bucketCreate = encoder[BucketCreatePayload]{
		taskID: func(p BucketCreatePayload) string {
			return toTaskID("cb", p.ID.AsString(), p.Bucket)
		},
		queue: func(p BucketCreatePayload) string {
			return initMigrationListQueue(&p)
		},
		taskType: TypeBucketCreate,
	}
	bucketDelete = encoder[BucketDeletePayload]{
		taskID: nil,
		queue: func(p BucketDeletePayload) string {
			return eventQueue(&p)
		},
		taskType: TypeBucketDelete,
	}
	bucketSyncTags = encoder[BucketSyncTagsPayload]{
		taskID: nil,
		queue: func(p BucketSyncTagsPayload) string {
			return eventQueue(&p)

		},
		taskType: TypeBucketSyncTags,
	}
	bucketSyncACL = encoder[BucketSyncACLPayload]{
		taskID: nil,
		queue: func(p BucketSyncACLPayload) string {
			return eventQueue(&p)
		},
		taskType: TypeBucketSyncACL,
	}
	objectSync = encoder[ObjectSyncPayload]{
		taskID: nil,
		queue: func(p ObjectSyncPayload) string {
			return eventQueue(&p)
		},
		taskType: TypeObjectSync,
	}
	objSyncTags = encoder[ObjSyncTagsPayload]{
		taskID: nil,
		queue: func(p ObjSyncTagsPayload) string {
			return eventQueue(&p)
		},
		taskType: TypeObjectSyncTags,
	}
	objSyncACL = encoder[ObjSyncACLPayload]{
		taskID: nil,
		queue: func(p ObjSyncACLPayload) string {
			return eventQueue(&p)
		},
		taskType: TypeObjectSyncACL,
	}
	migrateBucketListObjects = encoder[MigrateBucketListObjectsPayload]{
		taskID: func(p MigrateBucketListObjectsPayload) string {
			return toTaskID("mgr:lo", p.ID.AsString(), p.Bucket, p.Prefix)
		},
		queue: func(p MigrateBucketListObjectsPayload) string {
			return initMigrationListQueue(&p)
		},
		taskType: TypeMigrateBucketListObjects,
	}
	migrateObjCopy = encoder[MigrateObjCopyPayload]{
		taskID: func(p MigrateObjCopyPayload) string {
			return toTaskID("mgr:co", p.ID.AsString(), p.Bucket, p.Obj.Name, p.Obj.VersionID)
		},
		queue: func(p MigrateObjCopyPayload) string {
			return initMigrationCopyQueue(&p)
		},
		taskType: TypeMigrateObjCopy,
	}
	listObjectVersions = encoder[ListObjectVersionsPayload]{
		taskID: func(p ListObjectVersionsPayload) string {
			return toTaskID("mgr:lov", p.ID.AsString(), p.Bucket, p.Prefix)
		},
		queue: func(p ListObjectVersionsPayload) string {
			return initMigrationListQueue(&p)
		},
		taskType: TypeMigrateObjectListVersions,
	}
	migrateVersionedObject = encoder[MigrateVersionedObjectPayload]{
		taskID: func(p MigrateVersionedObjectPayload) string {
			return toTaskID("mgr:cov", p.ID.AsString(), p.Bucket, p.Prefix)
		},
		queue: func(p MigrateVersionedObjectPayload) string {
			return initMigrationCopyQueue(&p)
		},
		taskType: TypeMigrateVersionedObject,
	}
	zeroDowntimeReplicationSwitch = encoder[ZeroDowntimeReplicationSwitchPayload]{
		taskID: func(p ZeroDowntimeReplicationSwitchPayload) string {
			return toTaskID("api:zdrs", p.ID.AsString())
		},
		queue: func(p ZeroDowntimeReplicationSwitchPayload) string {
			return string(QueueAPI)
		},
		taskType: TypeApiZeroDowntimeSwitch,
	}
	switchWithDowntime = encoder[SwitchWithDowntimePayload]{
		taskID: func(p SwitchWithDowntimePayload) string {
			return toTaskID("api:sd", p.ID.AsString())
		},
		queue: func(p SwitchWithDowntimePayload) string {
			return string(QueueAPI)
		},
		taskType: TypeApiSwitchWithDowntime,
	}
	consistencyCheck = encoder[ConsistencyCheckPayload]{
		taskID: func(p ConsistencyCheckPayload) string {
			locationTokens := make([]string, 0, len(p.Locations))
			for _, location := range p.Locations {
				locationTokens = append(locationTokens, location.Storage, location.Bucket)
			}
			return toTaskID(append([]string{"cc:root"}, locationTokens...)...)
		},
		queue: func(p ConsistencyCheckPayload) string {
			locations := make([]entity.ConsistencyCheckLocation, 0, len(p.Locations))
			for _, payloadLocation := range p.Locations {
				locations = append(locations, entity.NewConsistencyCheckLocation(payloadLocation.Storage, payloadLocation.Bucket))
			}

			checkID := entity.NewConsistencyCheckID(locations...)
			return ConsistencyCheckQueue(checkID)
		},
		taskType: TypeConsistencyCheck,
	}
	consistencyCheckListObjects = encoder[ConsistencyCheckListObjectsPayload]{
		taskID: func(p ConsistencyCheckListObjectsPayload) string {
			locationTokens := make([]string, 0, len(p.Locations))
			for _, location := range p.Locations {
				locationTokens = append(locationTokens, location.Storage, location.Bucket)
			}
			return toTaskID(append(append([]string{"cc:lo"}, locationTokens...), strconv.Itoa(p.Index), p.Prefix)...)
		},
		queue: func(p ConsistencyCheckListObjectsPayload) string {
			locations := make([]entity.ConsistencyCheckLocation, 0, len(p.Locations))
			for _, payloadLocation := range p.Locations {
				locations = append(locations, entity.NewConsistencyCheckLocation(payloadLocation.Storage, payloadLocation.Bucket))
			}

			checkID := entity.NewConsistencyCheckID(locations...)
			return ConsistencyCheckQueue(checkID)
		},
		taskType: TypeConsistencyCheckListObjects,
	}
	consistencyCheckListVersions = encoder[ConsistencyCheckListVersionsPayload]{
		taskID: func(p ConsistencyCheckListVersionsPayload) string {
			locationTokens := make([]string, 0, len(p.Locations))
			for _, location := range p.Locations {
				locationTokens = append(locationTokens, location.Storage, location.Bucket)
			}
			return toTaskID(append(append([]string{"cc:lo"}, locationTokens...), strconv.Itoa(p.Index), p.Prefix)...)
		},
		queue: func(p ConsistencyCheckListVersionsPayload) string {
			locations := make([]entity.ConsistencyCheckLocation, 0, len(p.Locations))
			for _, payloadLocation := range p.Locations {
				locations = append(locations, entity.NewConsistencyCheckLocation(payloadLocation.Storage, payloadLocation.Bucket))
			}

			checkID := entity.NewConsistencyCheckID(locations...)
			return ConsistencyCheckQueue(checkID)
		},
		taskType: TypeConsistencyCheckListVersions,
	}
)

func toTaskID(args ...string) string {
	if len(args) == 0 {
		panic("toTaskID: no args provided")
	}
	return strings.Join(args, ":")
}
