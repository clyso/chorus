package handler

import (
	"context"
	"fmt"
	"github.com/clyso/chorus/pkg/lock"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/rclone"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/storage"
	"github.com/hibiken/asynq"
	"time"
)

const (
	replicationPauseRetryInterval = 30 * time.Second
)

type svc struct {
	clients    s3client.Service
	versionSvc meta.VersionService
	policySvc  policy.Service
	storageSvc storage.Service
	rc         rclone.Service
	taskClient *asynq.Client
	limit      ratelimit.RPM
	locker     lock.Service
}

func New(clients s3client.Service, versionSvc meta.VersionService, policySvc policy.Service, storageSvc storage.Service, rc rclone.Service, taskClient *asynq.Client, limit ratelimit.RPM, locker lock.Service) *svc {
	return &svc{clients: clients, versionSvc: versionSvc, policySvc: policySvc, storageSvc: storageSvc, rc: rc, taskClient: taskClient, limit: limit, locker: locker}
}

func (s *svc) getClients(ctx context.Context, fromStorage, toStorage string) (fromClient s3client.Client, toClient s3client.Client, err error) {
	fromClient, err = s.clients.GetByName(ctx, fromStorage)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get %q s3 client: %v: %w", fromStorage, err, asynq.SkipRetry)
	}

	toClient, err = s.clients.GetByName(ctx, toStorage)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get %q s3 client: %v: %w", toStorage, err, asynq.SkipRetry)
	}
	return
}
