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
	"github.com/clyso/chorus/pkg/trace"
	"github.com/clyso/chorus/pkg/util"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/service/proxy/auth"
	"github.com/clyso/chorus/service/proxy/cors"
	"github.com/clyso/chorus/service/proxy/router"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"net/http"
	"slices"
	"strings"
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
	defer shutdown(context.Background())

	appRedis := redis.NewClient(&redis.Options{Addr: conf.Redis.Address, Password: conf.Redis.Password, DB: conf.Redis.MetaDB})
	logger.Info().Interface("redis_pool", appRedis.PoolStats()).Int("redis_pool_size", appRedis.Options().PoolSize).Msg("redis app pool stats")
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

	confRedis := redis.NewClient(&redis.Options{Addr: conf.Redis.Address, Password: conf.Redis.Password, DB: conf.Redis.ConfigDB})
	logger.Info().Interface("redis_pool", appRedis.PoolStats()).Int("redis_pool_size", appRedis.Options().PoolSize).Msg("redis conf pool stats")
	defer confRedis.Close()
	err = redisotel.InstrumentTracing(confRedis, redisotel.WithTracerProvider(tp))
	if err != nil {
		return fmt.Errorf("%w: unable to instrument tracing app redis", err)
	}
	policySvc := policy.NewService(confRedis)

	metricsSvc := metrics.NewS3Service(conf.Metrics.Enabled)

	queueRedis := asynq.RedisClientOpt{Addr: conf.Redis.Address, Password: conf.Redis.Password, DB: conf.Redis.QueueDB}
	logger.Info().Int("redis_pool_size", queueRedis.PoolSize).Msg("redis queue pool stats")
	taskClient := asynq.NewClient(queueRedis)
	defer taskClient.Close()

	s3Clients, err := s3client.New(ctx, conf.Storage, metricsSvc, tp)
	if err != nil {
		return err
	}
	logger.Info().Msg("s3 clients connected")

	routeSvc := router.NewRouter(s3Clients, taskClient, verSvc, policySvc, storageSvc, limiter)
	replSvc := replication.New(taskClient, verSvc, policySvc)
	proxyMux := router.Serve(routeSvc, replSvc)
	authCheck := auth.Middleware(conf.Auth, conf.Storage.Storages)
	var handler http.Handler
	if conf.Metrics.Enabled {
		handler = log.HttpMiddleware(conf.Log, app.App, app.AppID, router.Middleware(trace.HttpMiddleware(tp, metrics.ProxyMiddleware(authCheck.Wrap(proxyMux)))))
	} else {
		handler = log.HttpMiddleware(conf.Log, app.App, app.AppID, router.Middleware(trace.HttpMiddleware(tp, authCheck.Wrap(proxyMux))))
	}
	handler = cors.HttpMiddleware(conf.Cors, handler)

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
