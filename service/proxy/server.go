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

package proxy

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/replication"
	"github.com/clyso/chorus/pkg/rpc"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/storage"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/trace"
	"github.com/clyso/chorus/pkg/util"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/service/proxy/auth"
	"github.com/clyso/chorus/service/proxy/cors"
	"github.com/clyso/chorus/service/proxy/router"
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
	redis.SetLogger(log.NewStdLogger())
	logger.Info().Msg("app redis connected")

	verSvc := meta.NewVersionService(appRedis)
	storageSvc := storage.New(appRedis)
	limiter := ratelimit.New(appRedis, conf.Storage.RateLimitConf())

	confRedis := util.NewRedis(conf.Redis, conf.Redis.ConfigDB)
	defer confRedis.Close()
	err = redisotel.InstrumentTracing(confRedis, redisotel.WithTracerProvider(tp))
	if err != nil {
		return fmt.Errorf("%w: unable to instrument tracing app redis", err)
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
	policySvc := policy.NewService(confRedis, queueSvc, conf.MainStorage())

	metricsSvc := metrics.NewS3Service(conf.Metrics.Enabled)

	s3Clients, err := s3client.New(ctx, conf.Storage, metricsSvc, tp)
	if err != nil {
		return err
	}
	logger.Info().Msg("s3 clients connected")

	middlewares := []func(next http.Handler) http.Handler{
		metrics.ProxyMiddleware(conf.Metrics.Enabled),
		trace.HttpMiddleware(tp),
	}
	var handler http.Handler
	switch {
	case conf.Storage != nil && len(conf.Storage.Storages) != 0:
		// init s3 proxy
		s3Router := router.NewS3Router(s3Clients, verSvc, storageSvc, limiter)
		s3Replicator := replication.NewS3(queueSvc, verSvc, policySvc)
		s3Proxy := router.Serve(s3Router, s3Replicator)
		authCheck := auth.Middleware(conf.Auth, conf.Storage.Storages)

		handler = s3Proxy
		middlewares = append(middlewares,
			router.S3Middleware(policySvc),
			authCheck.Wrap,
		)
		logger.Info().Msg("s3 proxy configured")
	case conf.Swift.Enabled:
		// init swift proxy
		swiftRouter := router.NewSwiftRouter(conf.Swift.Storages, limiter)
		swiftReplicator := replication.NewSwift(queueSvc, policySvc)
		swiftProxy := router.Serve(swiftRouter, swiftReplicator)

		handler = swiftProxy
		middlewares = append(middlewares, router.SwiftMiddleware(policySvc))

		logger.Info().Msg("swift proxy configured")
	default:
		return fmt.Errorf("%w: no storages in config", dom.ErrInvalidStorageConfig)
	}

	middlewares = append(middlewares,
		log.HttpMiddleware(conf.Log, app.App, app.AppID),
		cors.HttpMiddleware(conf.Cors),
	)
	for _, m := range middlewares {
		handler = m(handler)
	}

	proxyServer := http.Server{Addr: fmt.Sprintf(":%d", conf.Port), Handler: handler}

	server := util.NewServer()
	err = server.Add("proxy_http", func(_ context.Context) error {
		return proxyServer.ListenAndServe()
	}, proxyServer.Shutdown)
	if err != nil {
		return err
	}
	logger.Info().Msg("proxy created")

	err = server.Add("proxy_request_reply", func(ctx context.Context) error {
		return rpc.ProxyServe(ctx, appRedis, requestReplyServer(conf.Address, conf.Auth.Custom, conf.Storage.Storages[conf.Auth.UseStorage].Credentials))
	}, func(ctx context.Context) error { return nil })
	if err != nil {
		return err
	}

	if conf.Metrics.Enabled {
		start, stop := metrics.Server(ctx, conf.Metrics.Port, app)
		err = server.Add("proxy_metrics", start, stop)
		if err != nil {
			return err
		}
		logger.Info().Msg("metrics enabled")
	}

	return server.Start(ctx)
}

func requestReplyServer(proxyUrl string, credentials ...map[string]s3.CredentialsV4) rpc.ProxyGetCredentials {
	creds := make([]*pb.Credential, 0)
	for _, c := range credentials {
		for alias, cred := range c {
			creds = append(creds, &pb.Credential{
				Alias:     alias,
				AccessKey: cred.AccessKeyID,
				SecretKey: cred.SecretAccessKey,
			})
		}
	}
	slices.SortFunc(creds, func(a, b *pb.Credential) int {
		if n := strings.Compare(a.Alias, b.Alias); n != 0 {
			return n
		}
		return strings.Compare(a.AccessKey, b.AccessKey)
	})
	res := &pb.GetProxyCredentialsResponse{
		Address:     proxyUrl,
		Credentials: creds,
	}
	return func(ctx context.Context) (*pb.GetProxyCredentialsResponse, error) {
		return res, nil
	}
}
