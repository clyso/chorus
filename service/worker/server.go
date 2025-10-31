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

package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"

	"github.com/clyso/chorus/pkg/api"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/notifications"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/rclone"
	"github.com/clyso/chorus/pkg/rpc"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/storage"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/swift"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/trace"
	"github.com/clyso/chorus/pkg/util"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/service/worker/handler"
)

func Start(ctx context.Context, app dom.AppInfo, conf *Config) error {
	if err := conf.Validate(); err != nil {
		return err
	}
	features.Set(conf.Features)
	logger := log.GetLogger(conf.Log, app.App, app.AppID)
	logger.Info().
		Str("version", app.Version).
		Str("commit", app.Commit).
		Msg("app starting...")

	shutdown, tp, err := trace.NewTracerProvider(conf.Trace, app)
	if err != nil {
		return err
	}
	defer func() {
		_ = shutdown(context.Background())
	}()

	appRedis := util.NewRedis(conf.Redis, conf.Redis.MetaDB)
	defer appRedis.Close()
	err = appRedis.Ping(ctx).Err()
	if err != nil {
		return fmt.Errorf("%w: unable to reach app redis", err)
	}
	err = redisotel.InstrumentTracing(appRedis, redisotel.WithTracerProvider(tp))
	if err != nil {
		return fmt.Errorf("%w: unable to instrument tracing app redis", err)
	}
	logger.Info().Msg("app redis connected")

	versionSvc := meta.NewVersionService(appRedis)
	storageSvc := storage.New(appRedis)

	confRedis := util.NewRedis(conf.Redis, conf.Redis.ConfigDB)
	defer confRedis.Close()
	err = redisotel.InstrumentTracing(confRedis, redisotel.WithTracerProvider(tp))
	if err != nil {
		return fmt.Errorf("%w: unable to instrument tracing app redis", err)
	}

	metricsSvc := metrics.NewS3Service(conf.Metrics.Enabled)

	clients := map[dom.StorageType]any{}
	if s3Conf := conf.Storage.S3Storages(); len(s3Conf) != 0 {
		s3Clients, err := s3client.New(ctx, s3Conf, metricsSvc, tp)
		if err != nil {
			return err
		}
		clients[dom.S3] = s3Clients
		logger.Info().Msg("s3 clients connected")
	}
	if swiftConf := conf.Storage.SwiftStorages(); len(swiftConf) != 0 {
		swiftClients, err := swift.New(swiftConf)
		if err != nil {
			return err
		}
		clients[dom.Swift] = swiftClients
		logger.Info().Msg("swift clients connected")
	}
	clientRegistry, err := objstore.New(conf.Storage, clients)
	if err != nil {
		return err
	}

	memLimitBytes, err := util.ParseBytes(conf.RClone.MemoryLimit.Limit)
	if err != nil && conf.RClone.MemoryLimit.Enabled {
		return err
	}
	memLimiter := ratelimit.LocalSemaphore(ratelimit.SemaphoreConfig{
		Enabled:  conf.RClone.MemoryLimit.Enabled,
		Limit:    memLimitBytes,
		RetryMin: conf.RClone.MemoryLimit.RetryMin,
		RetryMax: conf.RClone.MemoryLimit.RetryMax,
	}, "rclone_mem")
	var filesLimiter ratelimit.Semaphore
	if conf.RClone.GlobalFileLimit.Enabled {
		filesLimiter = ratelimit.GlobalSemaphore(appRedis, conf.RClone.GlobalFileLimit, "rclone_files")
	} else {
		filesLimiter = ratelimit.LocalSemaphore(conf.RClone.LocalFileLimit, "rclone_files")
	}
	memCalc := rclone.NewMemoryCalculator(conf.RClone.MemoryCalc)
	rc, err := rclone.New(conf.Storage, conf.Log.Json, metricsSvc, memCalc, memLimiter, filesLimiter)
	if err != nil {
		return err
	}
	logger.Info().Msg("rclone connected")

	queueRedis := util.NewRedisAsynq(conf.Redis, conf.Redis.QueueDB)
	taskClient := asynq.NewClient(queueRedis)
	defer taskClient.Close()
	inspector := asynq.NewInspector(queueRedis)
	defer inspector.Close()
	queueSvc := tasks.NewQueueService(taskClient, inspector)
	err = policy.CheckSchemaCompatibility(ctx, app.Version, confRedis)
	if err != nil {
		return err
	}
	policySvc := policy.NewService(confRedis, queueSvc, conf.Storage.Main)

	limiter := ratelimit.New(appRedis, conf.Storage.RateLimitConf())
	lockRedis := util.NewRedis(conf.Redis, conf.Redis.LockDB)
	defer lockRedis.Close()
	replicationStatusLocker := store.NewReplicationStatusLocker(lockRedis, conf.Lock.Overlap)
	userLocker := store.NewUserLocker(lockRedis, conf.Lock.Overlap)
	objectLocker := store.NewObjectLocker(lockRedis, conf.Lock.Overlap)
	bucketLocker := store.NewBucketLocker(lockRedis, conf.Lock.Overlap)

	memoryLimiterSvc := rclone.NewMemoryLimiterSvc(memCalc, memLimiter, filesLimiter, metricsSvc)
	objectVersionInfoStore := store.NewObjectVersionInfoStore(confRedis)
	copySvc := rclone.NewS3CopySvc(clientRegistry, memoryLimiterSvc, limiter, metricsSvc)
	versionedMigrationSvc := handler.NewVersionedMigrationSvc(policySvc, copySvc, objectVersionInfoStore, objectLocker, conf.Worker.PauseRetryInterval)
	versionedMigrationCtrl := handler.NewVersionedMigrationCtrl(versionedMigrationSvc, queueSvc)

	consistencyCheckIDStore := store.NewConsistencyCheckIDStore(confRedis)
	consistencyCheckSettingsStore := store.NewConsistencyCheckSettingsStore(confRedis)
	consistencyCheckListStateStore := store.NewConsistencyCheckListStateStore(confRedis)
	consistencyCheckSetStore := store.NewConsistencyCheckSetStore(confRedis)
	checkSvc := handler.NewConsistencyCheckSvc(consistencyCheckIDStore, consistencyCheckSettingsStore, consistencyCheckListStateStore, consistencyCheckSetStore, copySvc, queueSvc)
	checkCtrl := handler.NewConsistencyCheckCtrl(checkSvc, queueSvc)

	workerSvc := handler.New(conf.Worker, clientRegistry, versionSvc, policySvc, storageSvc, rc, queueSvc, limiter, objectLocker, bucketLocker, replicationStatusLocker)

	stdLogger := log.NewStdLogger()
	redis.SetLogger(stdLogger)

	srv := asynq.NewServer(
		queueRedis,
		asynq.Config{
			ShutdownTimeout: conf.ShutdownTimeout,
			Concurrency:     conf.Concurrency,
			IsFailure: func(err error) bool {
				var rlErr *dom.ErrRateLimitExceeded
				return !errors.As(err, &rlErr)
			},
			RetryDelayFunc: retryDelay,
			ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {
				retried, _ := asynq.GetRetryCount(ctx)
				maxRetry, _ := asynq.GetMaxRetry(ctx)
				taskID, _ := asynq.GetTaskID(ctx)
				queue, _ := asynq.GetQueueName(ctx)
				taskLogger := zerolog.Ctx(ctx).With().Str("task_type", task.Type()).Str("task_id", taskID).Str("task_queue", queue).Int("task_max_retry", maxRetry).Int("task_retried", retried).Logger()
				if retried >= maxRetry {
					taskLogger.Error().RawJSON("task_payload", task.Payload()).Err(err).Msg("process task failed. task will be dropped")
					return
				}
				var rlErr *dom.ErrRateLimitExceeded
				if errors.As(err, &rlErr) {
					taskLogger.Debug().Err(err).Msg("process task failed due to the rate limit")
					return
				}
				taskLogger.Warn().Err(err).Msg("process task failed. task will be retried")
			}),
			Logger:                     stdLogger,
			LogLevel:                   asynq.LogLevel(zerolog.GlobalLevel() + 1),
			Queues:                     tasks.Priority,
			StrictPriority:             true,
			DynamicQueues:              true,
			DynamicQueueUpdateInterval: conf.Worker.QueueUpdateInterval,
		},
	)

	// mux maps a type to a handler
	mux := asynq.NewServeMux()
	mux.Use(log.WorkerMiddleware(conf.Log, app.App, app.AppID))
	mux.Use(trace.WorkerMiddleware(tp))
	if conf.Metrics.Enabled {
		mux.Use(metrics.WorkerMiddleware())
	}
	mux.HandleFunc(tasks.TypeBucketCreate, workerSvc.HandleBucketCreate)
	mux.HandleFunc(tasks.TypeBucketDelete, workerSvc.HandleBucketDelete)
	mux.HandleFunc(tasks.TypeBucketSyncTags, workerSvc.HandleBucketTags)
	mux.HandleFunc(tasks.TypeBucketSyncACL, workerSvc.HandleBucketACL)
	mux.HandleFunc(tasks.TypeObjectSync, workerSvc.HandleObjectSync)
	mux.HandleFunc(tasks.TypeObjectSyncTags, workerSvc.HandleObjectTags)
	mux.HandleFunc(tasks.TypeObjectSyncACL, workerSvc.HandleObjectACL)
	mux.HandleFunc(tasks.TypeMigrateBucketListObjects, workerSvc.HandleMigrationBucketListObj)
	mux.HandleFunc(tasks.TypeMigrateObjCopy, workerSvc.HandleMigrationObjCopy)
	mux.HandleFunc(tasks.TypeMigrateObjectListVersions, versionedMigrationCtrl.HandleObjectVersionList)
	mux.HandleFunc(tasks.TypeMigrateVersionedObject, versionedMigrationCtrl.HandleVersionedObjectMigration)
	mux.HandleFunc(tasks.TypeConsistencyCheck, checkCtrl.HandleConsistencyCheck)
	mux.HandleFunc(tasks.TypeConsistencyCheckListObjects, checkCtrl.HandleConsistencyCheckList)
	mux.HandleFunc(tasks.TypeConsistencyCheckListVersions, checkCtrl.HandleConsistencyCheckListVersions)
	mux.HandleFunc(tasks.TypeApiZeroDowntimeSwitch, workerSvc.HandleZeroDowntimeReplicationSwitch)
	mux.HandleFunc(tasks.TypeApiSwitchWithDowntime, workerSvc.HandleSwitchWithDowntime)

	server := util.NewServer()
	err = server.Add("queue_workers", func(ctx context.Context) error {
		err := srv.Start(mux)
		if err != nil {
			return err
		}
		<-ctx.Done()
		return nil
	}, func(_ context.Context) error {
		srv.Stop()
		srv.Shutdown()
		return nil
	})
	if err != nil {
		return err
	}

	if conf.Api.Enabled {
		handlers := api.GrpcHandlers(clientRegistry, queueSvc, rc, policySvc, versionSvc, storageSvc, checkSvc, rpc.NewProxyClient(appRedis), rpc.NewAgentClient(appRedis), notifications.NewService(clientRegistry), replicationStatusLocker, userLocker, &app)
		start, stop, err := api.NewGrpcServer(conf.Api.GrpcPort, handlers, tp, conf.Log, app)
		if err != nil {
			return err
		}
		err = server.Add("grpc_api", start, stop)
		if err != nil {
			return err
		}
		start, stop, err = api.GRPCGateway(ctx, conf.Api, func(ctx context.Context, mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) error {
			err = pb.RegisterChorusHandlerFromEndpoint(ctx, mux, endpoint, opts)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		err = server.Add("http_api", start, stop)
		if err != nil {
			return err
		}
		logger.Info().Msg("management api created")
	}

	if conf.Metrics.Enabled {
		start, stop := metrics.Server(ctx, conf.Metrics.Port, app)
		err = server.Add("worker_metrics", start, stop)
		if err != nil {
			return err
		}
		zerolog.Ctx(ctx).Info().Msg("metrics enabled")
	}

	zerolog.Ctx(ctx).Info().Msg("starting workers...")
	return server.Start(ctx)
}

func retryDelay(n int, err error, task *asynq.Task) time.Duration {
	var rlErr *dom.ErrRateLimitExceeded
	if errors.As(err, &rlErr) {
		return rlErr.RetryIn
	}
	return ErrRetryDelayFunc(n, err, task)
}

// override in e2e test for short retry
var ErrRetryDelayFunc = asynq.DefaultRetryDelayFunc
