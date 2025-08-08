/*
 * Copyright © 2023 Clyso GmbH
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

package agent

import (
	"context"
	"fmt"
	"net/http"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/notifications"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/replication"
	"github.com/clyso/chorus/pkg/rpc"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/trace"
	"github.com/clyso/chorus/pkg/util"
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
	queueSvc := tasks.NewQueueService(inspector)
	policySvc := policy.NewService(confRedis, queueSvc)

	replSvc := replication.New(taskClient, verSvc, policySvc)

	notificationHandler := notifications.NewHandler(conf.FromStorage, replSvc)
	httpHandler := trace.HttpMiddleware(tp, HTTPHandler(notificationHandler))
	if conf.Metrics.Enabled {
		httpHandler = metrics.AgentMiddleware(httpHandler)
	}
	httpHandler = log.HttpMiddleware(conf.Log, app.App, app.AppID, httpHandler)

	httpServer := http.Server{Addr: fmt.Sprintf(":%d", conf.Port), Handler: httpHandler}

	server := util.NewServer()
	err = server.Add("agent_http", func(_ context.Context) error {
		return httpServer.ListenAndServe()
	}, httpServer.Shutdown)
	if err != nil {
		return err
	}
	logger.Info().Msg("agent created")

	if conf.Metrics.Enabled {
		start, stop := metrics.Server(ctx, conf.Metrics.Port, app)
		err = server.Add("agent_metrics", start, stop)
		if err != nil {
			return err
		}
		logger.Info().Msg("metrics enabled")
	}

	err = server.Add("agent_request_reply", func(ctx context.Context) error {
		return rpc.AgentServe(ctx, appRedis, conf.URL, conf.FromStorage)
	}, func(ctx context.Context) error { return nil })
	if err != nil {
		return err
	}

	return server.Start(ctx)
}
