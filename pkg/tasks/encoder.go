package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/buger/jsonparser"
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/hibiken/asynq"
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
	bytes, err := json.Marshal(&payload)
	if err != nil {
		return nil, err
	}
	if replTask, ok := any(payload).(ReplicationTask); ok {
		err = replTask.validate()
		if err != nil {
			return nil, err
		}
	}
	if xctx.GetUser(ctx) != "" {
		// TODO: remove and test. Should be redundant because user is already presented in replicationID in task payload
		bytes, err = jsonparser.Set(bytes, []byte(`"`+xctx.GetUser(ctx)+`"`), "User")
		if err != nil {
			return nil, fmt.Errorf("%w: unable to add User to payload", err)
		}
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
	case *ConsistencyCheckListPayload:
		return consistencyCheckList.Enqueue(ctx, taskClient, *p)
	case ConsistencyCheckListPayload:
		return consistencyCheckList.Enqueue(ctx, taskClient, p)
	case *ConsistencyCheckReadinessPayload:
		return consistencyCheckReadiness.Enqueue(ctx, taskClient, *p)
	case ConsistencyCheckReadinessPayload:
		return consistencyCheckReadiness.Enqueue(ctx, taskClient, p)
	case *ConsistencyCheckDeletePayload:
		return consistencyCheckDelete.Enqueue(ctx, taskClient, *p)
	case ConsistencyCheckDeletePayload:
		return consistencyCheckDelete.Enqueue(ctx, taskClient, p)
	default:
		return fmt.Errorf("%w: unknown payload type %T", dom.ErrInternal, payload)
	}
}

var (
	bucketCreate = encoder[BucketCreatePayload]{
		taskID: func(p BucketCreatePayload) string {
			return toTaskID("cb", p.FromStorage, p.ToStorage, p.Bucket, p.ToBucket)
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
			return toTaskID("mgr:lo", p.FromStorage, p.ToStorage, p.Bucket, p.ToBucket, p.Prefix)
		},
		queue: func(p MigrateBucketListObjectsPayload) string {
			return initMigrationListQueue(&p)
		},
		taskType: TypeMigrateBucketListObjects,
	}
	migrateObjCopy = encoder[MigrateObjCopyPayload]{
		taskID: func(p MigrateObjCopyPayload) string {
			return toTaskID("mgr:co", p.FromStorage, p.ToStorage, p.Bucket, p.ToBucket, p.Obj.Name, p.Obj.VersionID)
		},
		queue: func(p MigrateObjCopyPayload) string {
			return initMigrationCopyQueue(&p)
		},
		taskType: TypeMigrateObjCopy,
	}
	listObjectVersions = encoder[ListObjectVersionsPayload]{
		taskID: func(p ListObjectVersionsPayload) string {
			return toTaskID("mgr:lov", p.FromStorage, p.ToStorage, p.Bucket, p.Prefix)
		},
		queue: func(p ListObjectVersionsPayload) string {
			return initMigrationListQueue(&p)
		},
		taskType: TypeMigrateObjectListVersions,
	}
	migrateVersionedObject = encoder[MigrateVersionedObjectPayload]{
		taskID: func(p MigrateVersionedObjectPayload) string {
			return toTaskID("mgr:cov", p.FromStorage, p.ToStorage, p.Bucket, p.ToBucket, p.Prefix)
		},
		queue: func(p MigrateVersionedObjectPayload) string {
			return initMigrationCopyQueue(&p)
		},
		taskType: TypeMigrateVersionedObject,
	}
	zeroDowntimeReplicationSwitch = encoder[ZeroDowntimeReplicationSwitchPayload]{
		taskID: func(p ZeroDowntimeReplicationSwitchPayload) string {
			return toTaskID("api:zdrs", p.FromStorage, p.ToStorage, p.User, p.FromBucket)
		},
		queue: func(p ZeroDowntimeReplicationSwitchPayload) string {
			return string(QueueAPI)
		},
		taskType: TypeApiZeroDowntimeSwitch,
	}
	switchWithDowntime = encoder[SwitchWithDowntimePayload]{
		taskID: func(p SwitchWithDowntimePayload) string {
			return toTaskID("api:sd", p.FromStorage, p.ToStorage, p.User, p.FromBucket)
		},
		queue: func(p SwitchWithDowntimePayload) string {
			return string(QueueAPI)
		},
		taskType: TypeApiSwitchWithDowntime,
	}
	consistencyCheck = encoder[ConsistencyCheckPayload]{
		taskID: nil,
		queue: func(p ConsistencyCheckPayload) string {
			return string(QueueConsistencyCheck)
		},
		taskType: TypeConsistencyCheck,
	}
	consistencyCheckList = encoder[ConsistencyCheckListPayload]{
		taskID: func(p ConsistencyCheckListPayload) string {
			return toTaskID("cc:l", p.ID, p.Storage, p.Bucket, p.Prefix)
		},
		queue: func(p ConsistencyCheckListPayload) string {
			return string(QueueConsistencyCheck)
		},
		taskType: TypeConsistencyCheckList,
	}
	consistencyCheckReadiness = encoder[ConsistencyCheckReadinessPayload]{
		taskID: func(p ConsistencyCheckReadinessPayload) string {
			return toTaskID("cc:r", p.ID)
		},
		queue: func(p ConsistencyCheckReadinessPayload) string {
			return string(QueueConsistencyCheck)
		},
		taskType: TypeConsistencyCheckReadiness,
	}
	consistencyCheckDelete = encoder[ConsistencyCheckDeletePayload]{
		taskID: func(p ConsistencyCheckDeletePayload) string {
			return toTaskID("cc:d", p.ID)
		},
		queue: func(p ConsistencyCheckDeletePayload) string {
			return string(QueueConsistencyCheck)
		},
		taskType: TypeConsistencyCheckResult,
	}
)

func toTaskID(args ...string) string {
	if len(args) == 0 {
		panic("toTaskID: no args provided")
	}
	return strings.Join(args, ":")
}
