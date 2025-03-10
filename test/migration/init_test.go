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

package migration

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http/httptest"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	mclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/redis/go-redis/v9"
	"github.com/rs/xid"
	"google.golang.org/grpc"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/storage"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/service/proxy"
	"github.com/clyso/chorus/service/worker"
)

var (
	mainClient     *mclient.Client
	f1Client       *mclient.Client
	f2Client       *mclient.Client
	proxyClient    *mclient.Client
	proxyAwsClient *aws_s3.S3
	mpMainClient   *mclient.Core
	mpF1Client     *mclient.Core
	mpF2Client     *mclient.Core
	mpProxyClient  *mclient.Core
	tstCtx         context.Context
	apiClient      pb.ChorusClient

	workerConf *worker.Config
	proxyConf  *proxy.Config

	storageSvc storage.Service

	urlHttpApi string
)

const (
	user = "test"
)

func TestMain(m *testing.M) {
	var err error
	workerConf, err = worker.GetConfig()
	if err != nil {
		panic(err)
	}
	workerConf.RClone.MemoryLimit.Enabled = false
	workerConf.RClone.LocalFileLimit.Enabled = false
	workerConf.RClone.GlobalFileLimit.Enabled = false
	workerConf.Features.ACL = false
	workerConf.Features.Tagging = false
	workerConf.Log.Level = "warn"
	workerConf.Worker.SwitchRetryInterval = 500 * time.Millisecond

	proxyConf, err = proxy.GetConfig()
	if err != nil {
		panic(err)
	}
	proxyConf.Features.ACL = false
	proxyConf.Features.Tagging = false
	proxyConf.Log.Level = "warn"

	var redisClient *redis.Client
	if os.Getenv("EXT_REDIS") != "true" {
		fmt.Println("using embedded redis")
		redisSvc, err := miniredis.Run()
		if err != nil {
			panic(err)
		}
		proxyConf.Redis.Address = redisSvc.Addr()
		workerConf.Redis.Address = redisSvc.Addr()
		redisClient = redis.NewClient(&redis.Options{
			Addr: redisSvc.Addr(),
		})
	} else {
		if url := os.Getenv("EXT_REDIS_URL"); url != "" {
			proxyConf.Redis.Address = url
			workerConf.Redis.Address = url
		}
		redisClient = redis.NewClient(&redis.Options{Addr: proxyConf.Redis.Address})
		err = redisClient.FlushAll(context.TODO()).Err()
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("redis url", proxyConf.Redis.Address)
	storageSvc = storage.New(redisClient)

	mainBackend := s3mem.New()
	mainFaker := gofakes3.New(mainBackend)
	mainTs := httptest.NewServer(mainFaker.Server())
	defer mainTs.Close()

	f1Backend := s3mem.New()
	f1Faker := gofakes3.New(f1Backend)
	f1Ts := httptest.NewServer(f1Faker.Server())
	defer f1Ts.Close()

	f2Backend := s3mem.New()
	f2Faker := gofakes3.New(f2Backend)
	f2Ts := httptest.NewServer(f2Faker.Server())
	defer f2Ts.Close()

	//metaSvc = meta.New(redis.NewClient(&redis.Options{Addr: proxyConf.Redis.Address, DB: proxyConf.Redis.MetaDB}))

	proxyConf.Storage.Storages = map[string]s3.Storage{}
	proxyConf.Storage.Storages["main"] = s3.Storage{
		Address:     mainTs.URL,
		Credentials: map[string]s3.CredentialsV4{user: generateCredentials()},
		Provider:    "Other",
		IsMain:      true,
	}

	proxyConf.Storage.Storages["f1"] = s3.Storage{
		Address:     f1Ts.URL,
		Credentials: map[string]s3.CredentialsV4{user: generateCredentials()},
		Provider:    "Other",
		IsMain:      false,
	}

	proxyConf.Storage.Storages["f2"] = s3.Storage{
		Address:     f2Ts.URL,
		Credentials: map[string]s3.CredentialsV4{user: generateCredentials()},
		Provider:    "Other",
		IsMain:      false,
	}
	proxyConf.Storage.CreateRouting = true
	proxyConf.Storage.CreateReplication = false
	workerConf.Storage = proxyConf.Storage

	proxyConf.Auth.UseStorage = "main"

	err = proxyConf.Storage.Init()
	if err != nil {
		panic(err)
	}
	err = workerConf.Storage.Init()
	if err != nil {
		panic(err)
	}

	fmt.Println("main s3", mainTs.URL)
	fmt.Println("f1 s3", f1Ts.URL)
	fmt.Println("f2 s3", f2Ts.URL)
	mainClient, mpMainClient = createClient(proxyConf.Storage.Storages["main"])
	f1Client, mpF1Client = createClient(proxyConf.Storage.Storages["f1"])
	f2Client, mpF2Client = createClient(proxyConf.Storage.Storages["f2"])

	addr := ""
	proxyConf.Port, addr = getRandomPort()
	fmt.Println("proxy", addr)

	workerConf.Api.Enabled = true
	grpcAddr := ""
	workerConf.Api.GrpcPort, grpcAddr = getRandomPort()
	fmt.Println("grpc api", grpcAddr)
	workerConf.Api.HttpPort, urlHttpApi = getRandomPort()
	urlHttpApi = "http://" + urlHttpApi
	fmt.Println("http api", urlHttpApi)

	ctx, cancel := context.WithCancel(context.Background())
	tstCtx = ctx

	go func() {
		app := dom.AppInfo{
			Version: "test",
			App:     "proxy",
			AppID:   xid.New().String(),
		}
		proxyCtx, cancelFn := context.WithCancel(ctx)
		defer cancelFn()

		err = proxy.Start(proxyCtx, app, proxyConf)
		if err != nil {
			panic(err)
		}
	}()
	go func() {
		app := dom.AppInfo{
			Version: "test",
			App:     "worker",
			AppID:   xid.New().String(),
		}
		workerCtx, cancelFn := context.WithCancel(ctx)
		defer cancelFn()

		err = worker.Start(workerCtx, app, workerConf)
		if err != nil {
			panic(err)
		}
	}()

	proxyClient, mpProxyClient = createClient(s3.Storage{
		Address:     addr,
		Credentials: proxyConf.Storage.Storages["main"].Credentials,
		IsSecure:    false,
	})
	fmt.Println("proxy s3", addr)
	proxyAwsClient = newAWSClient(s3.Storage{
		Address:     addr,
		Credentials: proxyConf.Storage.Storages["main"].Credentials,
		IsSecure:    false,
	})

	grpcConn, err := grpc.DialContext(ctx, grpcAddr,
		grpc.WithInsecure(),
		grpc.WithBackoffMaxDelay(time.Second),
		grpc.WithBlock(),
	)
	if err != nil {
		panic(err)
	}
	apiClient = pb.NewChorusClient(grpcConn)

	exitCode := m.Run()
	if exitCode == 0 && os.Getenv("TEST_WAIT") == "true" {

		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGTERM)
		for {
			i := rand.Intn(10)
			client := redis.NewClient(&redis.Options{Addr: proxyConf.Redis.Address})
			_ = client.FlushAll(context.TODO()).Err()
			m.Run()
			time.Sleep(time.Duration(i) * time.Second)
			select {
			case <-signals:
				break
			default:
			}
		}

	}
	cancel()
	// exit
	os.Exit(exitCode)

}

func getRandomPort() (int, string) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}
	addr := l.Addr().String()
	addrs := strings.Split(addr, ":")
	err = l.Close()
	if err != nil {
		panic(err)
	}

	port, err := strconv.Atoi(addrs[len(addrs)-1])
	if err != nil {
		panic(err)
	}
	return port, addr
}

func createClient(c s3.Storage) (*mclient.Client, *mclient.Core) {
	addr := strings.TrimPrefix(c.Address, "http://")
	addr = strings.TrimPrefix(addr, "https://")
	mc, err := mclient.New(addr, &mclient.Options{
		Creds:  credentials.NewStaticV4(c.Credentials[user].AccessKeyID, c.Credentials[user].SecretAccessKey, ""),
		Secure: c.IsSecure,
	})
	if err != nil {
		panic(err)
	}
	cancelHC, err := mc.HealthCheck(time.Second)
	if err != nil {
		panic(err)
	}
	defer cancelHC()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _ = mc.BucketExists(ctx, "probeBucketName")
	ready := !mc.IsOffline()
	for i := 0; !ready && i < 30; i++ {
		time.Sleep(100 * time.Millisecond)
		ready = !mc.IsOffline()
	}
	if !ready {
		panic("client " + addr + " is not ready")
	}

	core, err := mclient.NewCore(addr, &mclient.Options{
		Creds:  credentials.NewStaticV4(c.Credentials[user].AccessKeyID, c.Credentials[user].SecretAccessKey, ""),
		Secure: c.IsSecure,
	})
	if err != nil {
		panic(err)
	}

	return mc, core
}
