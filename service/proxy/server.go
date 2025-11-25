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

	xctx "github.com/clyso/chorus/pkg/ctx"
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
	uploadSvc := storage.NewUploadSvc(appRedis)
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
	policySvc := policy.NewService(confRedis, queueSvc, conf.Storage.Main)
	metricsSvc := metrics.NewS3Service(conf.Metrics.Enabled)

	// configure object storage proxies:
	proxyConfig := router.Config{
		Storages:          map[dom.StorageType]router.StorageProxy{},
		LogMiddleware:     log.HttpMiddleware(conf.Log, app.App, app.AppID, xctx.Event),
		TraceMiddleware:   nil,
		MetricsMiddleware: nil,
		PolicyMiddleware:  router.PolicyMiddleware(policySvc),
	}
	if conf.Metrics.Enabled {
		proxyConfig.MetricsMiddleware = metrics.ProxyMiddleware()
	}
	if conf.Trace.Enabled {
		proxyConfig.TraceMiddleware = trace.HttpMiddleware(tp)
	}
	if s3Conf := conf.Storage.S3Storages(); len(s3Conf) != 0 {
		s3Clients, err := s3client.New(ctx, s3Conf, metricsSvc, tp)
		if err != nil {
			return err
		}
		routeSvc := router.NewS3Router(s3Clients, verSvc, uploadSvc, limiter)
		replSvc := replication.NewS3(queueSvc, verSvc, policySvc)
		authCheck := auth.Middleware(conf.Auth, conf.Storage.S3Storages())
		proxyConfig.Storages[dom.S3] = router.StorageProxy{
			Router:             routeSvc,
			Replicator:         replSvc,
			AuthMiddleware:     authCheck.Wrap,
			ReqParseMiddleware: router.S3Middleware(),
		}
		logger.Info().Msg("s3 proxy configured")
	}
	if swiftConf := conf.Storage.SwiftStorages(); len(swiftConf) != 0 {
		routeSvc := router.NewSwiftRouter(swiftConf, limiter)
		replSvc := replication.NewSwift(queueSvc, policySvc)
		proxyConfig.Storages[dom.Swift] = router.StorageProxy{
			Router:             routeSvc,
			Replicator:         replSvc,
			AuthMiddleware:     nil, // no auth check for swift
			ReqParseMiddleware: router.SwiftMiddleware(),
		}
		logger.Info().Msg("swift proxy configured")
	}
	proxyMux, err := router.New(proxyConfig)
	if err != nil {
		return err
	}
	proxyServer := http.Server{Addr: fmt.Sprintf(":%d", conf.Port), Handler: proxyMux}

	server := util.NewServer()
	err = server.Add("proxy_http", func(_ context.Context) error {
		return proxyServer.ListenAndServe()
	}, proxyServer.Shutdown)
	if err != nil {
		return err
	}
	logger.Info().Msg("proxy created")

	credentials := []map[string]s3.CredentialsV4{conf.Auth.Custom}
	if s3torageConf, ok := conf.Storage.S3Storages()[conf.Auth.UseStorage]; ok {
		credentials = append(credentials, s3torageConf.Credentials)
	}
	err = server.Add("proxy_request_reply", func(ctx context.Context) error {
		return rpc.ProxyServe(ctx, appRedis, requestReplyServer(conf.Address, credentials...))
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
