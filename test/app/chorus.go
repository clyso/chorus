// Copyright 2025 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package app

import (
	"testing"
	"time"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/testutil"
	"github.com/clyso/chorus/pkg/util"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/service/proxy"
	"github.com/clyso/chorus/service/worker"
	"github.com/hibiken/asynq"
	"github.com/rs/xid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type Chorus struct {
	ApiClient pb.ChorusClient

	UrlHttpApi string
	ProxyAddr  string
	ProxyPort  int

	WaitShort  time.Duration
	RetryShort time.Duration
	WaitLong   time.Duration
	RetryLong  time.Duration
}

func SetupChorus(t testing.TB, workerConf *worker.Config, proxyConf *proxy.Config) Chorus {
	t.Helper()
	var err error
	proxyConf, err = deepCopyStruct(proxyConf)
	if err != nil {
		t.Fatal(err)
	}
	workerConf, err = deepCopyStruct(workerConf)
	if err != nil {
		t.Fatal(err)
	}
	e := Chorus{
		WaitShort:  waitShort,
		RetryShort: retryShort,
		WaitLong:   waitLong,
		RetryLong:  retryLong,
	}

	worker.ErrRetryDelayFunc = func(n int, e error, t *asynq.Task) time.Duration {
		return retryLong
	}
	t.Cleanup(func() {
		worker.ErrRetryDelayFunc = asynq.DefaultRetryDelayFunc
	})

	redisAddr := testutil.SetupRedisAddr(t)

	proxyConf.Redis.Address = redisAddr
	workerConf.Redis.Address = redisAddr

	proxyConf.Port, e.ProxyAddr = getRandomPort()
	e.ProxyPort = proxyConf.Port
	workerConf.Api.Enabled = true
	grpcAddr := ""
	workerConf.Api.GrpcPort, grpcAddr = getRandomPort()
	workerConf.Api.HttpPort, e.UrlHttpApi = getRandomPort()
	e.UrlHttpApi = "http://" + e.UrlHttpApi
	ctx := t.Context()

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		app := dom.AppInfo{
			Version: "test",
			App:     "proxy",
			AppID:   xid.New().String(),
		}
		return proxy.Start(ctx, app, proxyConf)
	})
	wg.Go(func() error {
		app := dom.AppInfo{
			Version: "test",
			App:     "worker",
			AppID:   xid.New().String(),
		}
		return worker.Start(ctx, app, workerConf)
	})
	t.Cleanup(func() {
		err := wg.Wait()
		if util.IsServerError(err) {
			t.Error("embedded env services exited with error:", err)
		}
	})

	grpcConn, err := grpc.DialContext(ctx, grpcAddr,
		grpc.WithInsecure(),
		grpc.WithBackoffMaxDelay(time.Second),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatal("failed to dial grpc api:", err)
	}
	e.ApiClient = pb.NewChorusClient(grpcConn)
	return e
}
