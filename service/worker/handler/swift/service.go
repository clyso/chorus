package swift

import (
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/storage"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/swift"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/service/worker/handler"
)

type svc struct {
	swiftClients swift.Client
	storageSvc   storage.Service
	queueSvc     tasks.QueueService
	limit        ratelimit.RPM
	objectLocker *store.ObjectLocker
	bucketLocker *store.BucketLocker
	userLocker   *store.UserLocker
	conf         *handler.Config
}

func New(conf *handler.Config, swiftClients swift.Client,
	storageSvc storage.Service,
	queueSvc tasks.QueueService, limit ratelimit.RPM, objectLocker *store.ObjectLocker, userLocker *store.UserLocker,
	bucketLocker *store.BucketLocker) *svc {
	return &svc{
		conf:         conf,
		swiftClients: swiftClients,
		storageSvc:   storageSvc,
		queueSvc:     queueSvc,
		limit:        limit,
		userLocker:   userLocker,
		objectLocker: objectLocker,
		bucketLocker: bucketLocker,
	}
}
