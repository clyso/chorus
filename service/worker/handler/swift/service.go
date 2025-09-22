package swift

import (
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/rclone"
	"github.com/clyso/chorus/pkg/storage"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/swift"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/service/worker/handler"
)

type svc struct {
	swiftClients            swift.Client
	versionSvc              meta.VersionService
	policySvc               policy.Service
	storageSvc              storage.Service
	rc                      rclone.Service
	queueSvc                tasks.QueueService
	limit                   ratelimit.RPM
	objectLocker            *store.ObjectLocker
	bucketLocker            *store.BucketLocker
	userLocker              *store.UserLocker
	replicationstatusLocker *store.ReplicationStatusLocker
	conf                    *handler.Config
	rclone.CopySvc
}

func New(conf *handler.Config, swiftClients swift.Client, versionSvc meta.VersionService,
	policySvc policy.Service, storageSvc storage.Service, rc rclone.Service,
	queueSvc tasks.QueueService, limit ratelimit.RPM, objectLocker *store.ObjectLocker, userLocker *store.UserLocker,
	bucketLocker *store.BucketLocker, replicationstatusLocker *store.ReplicationStatusLocker) *svc {
	return &svc{
		conf:                    conf,
		swiftClients:            swiftClients,
		versionSvc:              versionSvc,
		policySvc:               policySvc,
		storageSvc:              storageSvc,
		rc:                      rc,
		queueSvc:                queueSvc,
		limit:                   limit,
		userLocker:              userLocker,
		objectLocker:            objectLocker,
		bucketLocker:            bucketLocker,
		replicationstatusLocker: replicationstatusLocker,
	}
}
