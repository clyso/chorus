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
	"github.com/clyso/chorus/pkg/rpc"
	"github.com/clyso/chorus/pkg/storage"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/trace"
	"github.com/clyso/chorus/pkg/util"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/service/worker/copy"
	"github.com/clyso/chorus/service/worker/handler"
	swift_worker "github.com/clyso/chorus/service/worker/handler/swift"
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
	uploadSvc := storage.NewUploadSvc(appRedis)
	objectListStateStore := store.NewMigrationObjectListStateStore(appRedis)
	bucketListStateStore := store.NewMigrationBucketListStateStore(appRedis)

	confRedis := util.NewRedis(conf.Redis, conf.Redis.ConfigDB)
	defer confRedis.Close()
	err = redisotel.InstrumentTracing(confRedis, redisotel.WithTracerProvider(tp))
	if err != nil {
		return fmt.Errorf("%w: unable to instrument tracing app redis", err)
	}

	credsSvc, err := objstore.NewCredsSvc(ctx, &conf.Storage, appRedis)
	if err != nil {
		return err
	}
	metricsSvc := metrics.NewS3Service(conf.Metrics.Enabled)
	clientRegistry, err := objstore.NewRegistry(ctx, credsSvc, metricsSvc)
	if err != nil {
		return err
	}

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

	objectVersionInfoStore := store.NewObjectVersionInfoStore(confRedis)
	copySvc := copy.NewS3CopySvc(credsSvc, clientRegistry, metricsSvc)
	versionedMigrationSvc := handler.NewVersionedMigrationSvc(policySvc, copySvc, objectVersionInfoStore, objectLocker, conf.Worker.PauseRetryInterval)

	consistencyCheckIDStore := store.NewConsistencyCheckIDStore(confRedis)
	consistencyCheckSettingsStore := store.NewConsistencyCheckSettingsStore(confRedis)
	consistencyCheckListStateStore := store.NewConsistencyCheckListStateStore(confRedis)
	consistencyCheckSetStore := store.NewConsistencyCheckSetStore(confRedis)
	checkSvc := handler.NewConsistencyCheckSvc(consistencyCheckIDStore, consistencyCheckSettingsStore, consistencyCheckListStateStore, consistencyCheckSetStore, clientRegistry, queueSvc)
	checkCtrl := handler.NewConsistencyCheckCtrl(checkSvc, queueSvc)

	workerSvc := handler.New(conf.Worker, credsSvc, clientRegistry, versionSvc, copySvc, queueSvc, uploadSvc, limiter, objectListStateStore, objectLocker, bucketLocker, replicationStatusLocker, versionedMigrationSvc)

	stdLogger := log.NewStdLogger()
	redis.SetLogger(stdLogger)

	defaultRetry := fallbackRetryDelay(conf.Worker.CustomErrRetryInterval)
	srv := asynq.NewServer(
		queueRedis,
		asynq.Config{
			ShutdownTimeout: conf.ShutdownTimeout,
			Concurrency:     conf.Concurrency,
			IsFailure: func(err error) bool {
				var rlErr *dom.ErrRateLimitExceeded
				return !errors.As(err, &rlErr)
			},
			RetryDelayFunc: retryDelayFunc(defaultRetry),
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
			TaskCheckInterval:          conf.Worker.TaskCheckInterval,
			DelayedTaskCheckInterval:   conf.Worker.DelayedTaskCheckInterval,
		},
	)

	// mux maps a type to a handler
	mux := asynq.NewServeMux()
	mux.Use(log.WorkerMiddleware(conf.Log, app.App, app.AppID))
	mux.Use(trace.WorkerMiddleware(tp))
	if conf.Metrics.Enabled {
		mux.Use(metrics.WorkerMiddleware())
	}
	// common workers
	switchWorker := handler.NewSwitchSvc(conf.Worker, policySvc, uploadSvc, replicationStatusLocker)
	mux.HandleFunc(tasks.TypeApiZeroDowntimeSwitch, switchWorker.HandleZeroDowntimeReplicationSwitch)
	mux.HandleFunc(tasks.TypeApiSwitchWithDowntime, switchWorker.HandleSwitchWithDowntime)
	logger.Info().Msg("registered common workers")

	// S3 workers
	if len(conf.Storage.S3Storages()) != 0 {
		mux.HandleFunc(tasks.TypeBucketCreate, workerSvc.HandleBucketCreate)
		mux.HandleFunc(tasks.TypeBucketDelete, workerSvc.HandleBucketDelete)
		mux.HandleFunc(tasks.TypeBucketSyncTags, workerSvc.HandleBucketTags)
		mux.HandleFunc(tasks.TypeBucketSyncACL, workerSvc.HandleBucketACL)
		mux.HandleFunc(tasks.TypeObjectSync, workerSvc.HandleObjectSync)
		mux.HandleFunc(tasks.TypeObjectSyncTags, workerSvc.HandleObjectTags)
		mux.HandleFunc(tasks.TypeObjectSyncACL, workerSvc.HandleObjectACL)
		mux.HandleFunc(tasks.TypeMigrateS3User, workerSvc.HandleMigrationS3User)
		mux.HandleFunc(tasks.TypeMigrateBucketListObjects, workerSvc.HandleMigrationBucketListObj)
		mux.HandleFunc(tasks.TypeMigrateObjCopy, workerSvc.HandleMigrationObjCopy)
		logger.Info().Msg("registered S3 workers")

		// versioned object migration workers
		mux.HandleFunc(tasks.TypeMigrateObjectListVersions, workerSvc.HandleObjectVersionList)
		mux.HandleFunc(tasks.TypeMigrateVersionedObject, workerSvc.HandleVersionedObjectMigration)
		logger.Info().Msg("registered S3 versioned workers")
	} else {
		logger.Info().Msg("s3 workers not registered: no s3 storage configured")
	}

	// swift workers
	if len(conf.Storage.SwiftStorages()) != 0 {
		swiftWorkerSvc := swift_worker.New(conf.Worker, clientRegistry, bucketListStateStore, objectListStateStore, queueSvc, limiter, objectLocker, userLocker, bucketLocker)
		// swift tasks:
		mux.HandleFunc(tasks.TypeSwiftAccountUpdate, swiftWorkerSvc.HandleAccountUpdate)
		mux.HandleFunc(tasks.TypeSwiftContainerUpdate, swiftWorkerSvc.HandleContainerUpdate)
		mux.HandleFunc(tasks.TypeSwiftObjUpdate, swiftWorkerSvc.HandleObjectUpdate)
		mux.HandleFunc(tasks.TypeSwiftObjMetaUpdate, swiftWorkerSvc.HandleObjectMetaUpdate)
		mux.HandleFunc(tasks.TypeSwiftObjDelete, swiftWorkerSvc.HandleObjectDelete)
		mux.HandleFunc(tasks.TypeSwiftAccountMigration, swiftWorkerSvc.HandleSwiftAccountMigration)
		mux.HandleFunc(tasks.TypeSwiftContainerMigration, swiftWorkerSvc.HandleSwiftContainerMigration)
		mux.HandleFunc(tasks.TypeSwiftObjectMigration, swiftWorkerSvc.HandleSwiftObjectMigration)
		logger.Info().Msg("registered swift workers")
	} else {
		logger.Info().Msg("swift workers not registered: no swift storage configured")
	}

	mux.HandleFunc(tasks.TypeConsistencyCheck, checkCtrl.HandleConsistencyCheck)
	mux.HandleFunc(tasks.TypeConsistencyCheckListObjects, checkCtrl.HandleConsistencyCheckList)
	mux.HandleFunc(tasks.TypeConsistencyCheckListVersions, checkCtrl.HandleConsistencyCheckListVersions)
	logger.Info().Msg("registered consistency check workers")

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
		chorusHandler := api.ChorusHandlers(credsSvc, rpc.NewProxyClient(appRedis), rpc.NewAgentClient(appRedis), &app)
		diffHandler := api.DiffHandlers(credsSvc, queueSvc, checkSvc)
		policyHandler := api.PolicyHandlers(credsSvc, clientRegistry, queueSvc, policySvc, versionSvc, objectListStateStore, bucketListStateStore, rpc.NewAgentClient(appRedis), notifications.NewService(clientRegistry), replicationStatusLocker, userLocker)
		registerServices := func(srv *grpc.Server) {
			pb.RegisterChorusServer(srv, chorusHandler)
			pb.RegisterDiffServer(srv, diffHandler)
			pb.RegisterPolicyServer(srv, policyHandler)
		}
		start, stop, err := api.NewGrpcServer(conf.Api.GrpcPort, registerServices, tp, conf.Log, app)
		if err != nil {
			return err
		}
		err = server.Add("grpc_api", start, stop)
		if err != nil {
			return err
		}
		start, stop, err = api.GRPCGateway(ctx, conf.Api, func(ctx context.Context, mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) error {
			if err := pb.RegisterChorusHandlerFromEndpoint(ctx, mux, endpoint, opts); err != nil {
				return err
			}
			if err := pb.RegisterDiffHandlerFromEndpoint(ctx, mux, endpoint, opts); err != nil {
				return err
			}
			if err := pb.RegisterPolicyHandlerFromEndpoint(ctx, mux, endpoint, opts); err != nil {
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

func retryDelayFunc(fallback asynq.RetryDelayFunc) asynq.RetryDelayFunc {
	return func(n int, err error, task *asynq.Task) time.Duration {
		var rlErr *dom.ErrRateLimitExceeded
		if errors.As(err, &rlErr) {
			return rlErr.RetryIn
		}
		return fallback(n, err, task)
	}
}

func fallbackRetryDelay(customInterval *time.Duration) asynq.RetryDelayFunc {
	if customInterval == nil || *customInterval <= 0 {
		return asynq.DefaultRetryDelayFunc
	}
	return func(_ int, _ error, _ *asynq.Task) time.Duration {
		return *customInterval
	}
}
