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
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	aws_credentials "github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/hibiken/asynq"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	mclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/storage"
	"github.com/clyso/chorus/pkg/swift"
	"github.com/clyso/chorus/pkg/testutil"
	"github.com/clyso/chorus/pkg/util"
	"github.com/clyso/chorus/service/proxy"
	"github.com/clyso/chorus/service/worker"
)

const (
	user = "test"

	retryShort = 100 * time.Millisecond
	retryLong  = 500 * time.Millisecond
)

var (
	// can override with make test TEST_WAIT_SHORT=10s
	waitShort = getDurationEvar("TEST_WAIT_SHORT", 5*time.Second)
	// can override with make test TEST_WAIT_LONG=60s
	waitLong = getDurationEvar("TEST_WAIT_LONG", 20*time.Second)
)

func getDurationEvar(name string, defaultVal time.Duration) time.Duration {
	if v := os.Getenv(name); v != "" {
		dur, err := time.ParseDuration(v)
		if err != nil {
			panic(fmt.Sprintf("cannot parse duration from env var %s=%s: %v", name, v, err))
		}
		return dur
	}
	return defaultVal
}

type EmbeddedEnv struct {
	MainClient     *mclient.Client
	F1Client       *mclient.Client
	F2Client       *mclient.Client
	ProxyClient    *mclient.Client
	ProxyAwsClient *aws_s3.S3
	MpMainClient   *mclient.Core
	MpF1Client     *mclient.Core
	MpF2Client     *mclient.Core
	MpProxyClient  *mclient.Core

	TaskClient *asynq.Client

	ApiClient pb.ChorusClient

	StorageSvc storage.Service

	UrlHttpApi string

	WaitShort  time.Duration
	RetryShort time.Duration
	WaitLong   time.Duration
	RetryLong  time.Duration
}

func (e *EmbeddedEnv) CreateMainFollowerUserReplications(t testing.TB) {
	t.Helper()
	ctx := t.Context()
	r := require.New(t)

	_, err := e.ApiClient.AddReplication(ctx, &pb.AddReplicationRequest{
		User:            user,
		From:            "main",
		To:              "f1",
		Buckets:         []string{},
		IsForAllBuckets: true,
	})
	r.NoError(err)
	_, err = e.ApiClient.AddReplication(ctx, &pb.AddReplicationRequest{
		User:            user,
		From:            "main",
		To:              "f2",
		Buckets:         []string{},
		IsForAllBuckets: true,
	})
	r.NoError(err)
}

func SetupEmbedded(t testing.TB, workerConf *worker.Config, proxyConf *proxy.Config) EmbeddedEnv {
	t.Helper()

	var err error
	e := EmbeddedEnv{
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

	mainBackend := s3mem.New()
	mainFaker := gofakes3.New(mainBackend)
	mainTs := httptest.NewServer(mainFaker.Server())
	t.Cleanup(func() {
		mainTs.Close()
	})

	f1Backend := s3mem.New()
	f1Faker := gofakes3.New(f1Backend)
	f1Ts := httptest.NewServer(f1Faker.Server())
	t.Cleanup(func() {
		f1Ts.Close()
	})

	f2Backend := s3mem.New()
	f2Faker := gofakes3.New(f2Backend)
	f2Ts := httptest.NewServer(f2Faker.Server())
	t.Cleanup(func() {
		f2Ts.Close()
	})

	e.TaskClient = asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddr, DB: proxyConf.Redis.QueueDB})
	t.Cleanup(func() {
		e.TaskClient.Close()
	})

	storages := map[string]s3.Storage{
		"main": {

			Address:     mainTs.URL,
			Credentials: map[string]s3.CredentialsV4{user: generateCredentials()},
			Provider:    "Other",
		},
		"f1": {

			Address:     f1Ts.URL,
			Credentials: map[string]s3.CredentialsV4{user: generateCredentials()},
			Provider:    "Other",
		},
		"f2": {
			Address:     f2Ts.URL,
			Credentials: map[string]s3.CredentialsV4{user: generateCredentials()},
			Provider:    "Other",
		},
	}
	proxyConf.Storage = ProxyS3Config("main", storages)

	workerConf.Storage = WorkerS3Config("main", storages)
	// deep copy proxy config
	pcBytes, err := yaml.Marshal(&workerConf.Storage)
	if err != nil {
		t.Fatal(err)
	}
	err = yaml.Unmarshal(pcBytes, &workerConf.Storage)
	if err != nil {
		t.Fatal(err)
	}

	proxyConf.Auth.UseStorage = "main"
	e.MainClient, e.MpMainClient = createClient(storages["main"])
	e.F1Client, e.MpF1Client = createClient(storages["f1"])
	e.F2Client, e.MpF2Client = createClient(storages["f2"])

	addr := ""
	proxyConf.Port, addr = getRandomPort()
	workerConf.Api.Enabled = true
	grpcAddr := ""
	workerConf.Api.GrpcPort, grpcAddr = getRandomPort()
	workerConf.Api.HttpPort, e.UrlHttpApi = getRandomPort()
	e.UrlHttpApi = "http://" + e.UrlHttpApi
	ctx := t.Context()

	// do deep copy of configs before passing to goroutines to avoid panic on concurrent hashmap usage:
	proxyConfCopy, err := deepCopyStruct(proxyConf)
	if err != nil {
		t.Fatal(err)
	}
	workerConfCopy, err := deepCopyStruct(workerConf)
	if err != nil {
		t.Fatal(err)
	}

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		app := dom.AppInfo{
			Version: "test",
			App:     "proxy",
			AppID:   xid.New().String(),
		}
		return proxy.Start(ctx, app, proxyConfCopy)
	})
	wg.Go(func() error {
		app := dom.AppInfo{
			Version: "test",
			App:     "worker",
			AppID:   xid.New().String(),
		}
		return worker.Start(ctx, app, workerConfCopy)
	})
	t.Cleanup(func() {
		err := wg.Wait()
		if util.IsServerError(err) {
			t.Error("embedded env services exited with error:", err)
		}
	})
	e.ProxyClient, e.MpProxyClient = createClient(s3.Storage{
		Address:     addr,
		Credentials: storages["main"].Credentials,
		IsSecure:    false,
	})
	e.ProxyAwsClient = newAWSClient(s3.Storage{
		Address:     addr,
		Credentials: storages["main"].Credentials,
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
	e.ApiClient = pb.NewChorusClient(grpcConn)
	return e
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

func generateCredentials() s3.CredentialsV4 {
	res := s3.CredentialsV4{}
	const (

		// Maximum length for MinIO access key.
		// There is no max length enforcement for access keys
		accessKeyMaxLen = 20

		// Maximum secret key length for MinIO, this
		// is used when autogenerating new credentials.
		// There is no max length enforcement for secret keys
		secretKeyMaxLen = 40

		// Alpha numeric table used for generating access keys.
		alphaNumericTable = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"

		// Total length of the alpha numeric table.
		alphaNumericTableLen = byte(len(alphaNumericTable))
	)
	readBytes := func(size int) (data []byte, err error) {
		data = make([]byte, size)
		var n int
		if n, err = rand.Read(data); err != nil {
			return nil, err
		} else if n != size {
			panic(fmt.Sprintf("Not enough data. Expected to read: %v bytes, got: %v bytes", size, n))
		}
		return data, nil
	}

	// Generate access key.
	keyBytes, err := readBytes(accessKeyMaxLen)
	if err != nil {
		panic(err)
	}
	for i := 0; i < accessKeyMaxLen; i++ {
		keyBytes[i] = alphaNumericTable[keyBytes[i]%alphaNumericTableLen]
	}
	res.AccessKeyID = string(keyBytes)

	// Generate secret key.
	keyBytes, err = readBytes(secretKeyMaxLen)
	if err != nil {
		panic(err)
	}

	res.SecretAccessKey = strings.ReplaceAll(string([]byte(base64.StdEncoding.EncodeToString(keyBytes))[:secretKeyMaxLen]),
		"/", "+")

	return res
}

func newAWSClient(conf s3.Storage) *aws_s3.S3 {
	cred := aws_credentials.NewCredentials(&aws_credentials.StaticProvider{Value: aws_credentials.Value{
		AccessKeyID:     conf.Credentials[user].AccessKeyID,
		SecretAccessKey: conf.Credentials[user].SecretAccessKey,
	}})
	endpoint := conf.Address
	if !strings.HasPrefix(endpoint, "http") {
		if conf.IsSecure {
			endpoint = "https://" + endpoint
		} else {
			endpoint = "http://" + endpoint
		}
	}
	awsConfig := aws.NewConfig().
		WithMaxRetries(3).
		WithCredentials(cred).
		WithHTTPClient(&http.Client{Timeout: conf.HttpTimeout}).
		WithS3ForcePathStyle(true).
		WithDisableSSL(!conf.IsSecure).
		WithEndpoint(endpoint).
		WithRegion("us-east-1").
		WithS3UsEast1RegionalEndpoint(endpoints.RegionalS3UsEast1Endpoint)

	ses, err := session.NewSessionWithOptions(session.Options{
		Config: *awsConfig,
	})
	if err != nil {
		panic(err)
	}
	return aws_s3.New(ses)
}

// generic parameter function to deepcopy struct using yaml marshal/unmarshal
func deepCopyStruct[T any](in T) (out T, err error) {
	b, err := yaml.Marshal(in)
	if err != nil {
		return out, err
	}
	err = yaml.Unmarshal(b, &out)
	return out, err
}

func WorkerS3Config(main string, storages map[string]s3.Storage) objstore.Config {
	res := objstore.Config{
		Main:     main,
		Storages: map[string]objstore.GenericStorage[*s3.Storage, *swift.Storage]{},
	}
	for name, stor := range storages {
		s := stor
		res.Storages[name] = objstore.GenericStorage[*s3.Storage, *swift.Storage]{
			S3: &s,
			CommonConfig: objstore.CommonConfig{
				Type: dom.S3,
			},
		}
	}
	return res
}

func ProxyS3Config(main string, storages map[string]s3.Storage) proxy.Storages {
	res := proxy.Storages{
		Main:     main,
		Storages: map[string]objstore.GenericStorage[*s3.Storage, *proxy.SwiftStorage]{},
	}
	for name, stor := range storages {
		s := stor
		res.Storages[name] = objstore.GenericStorage[*s3.Storage, *proxy.SwiftStorage]{
			S3: &s,
			CommonConfig: objstore.CommonConfig{
				Type: dom.S3,
			},
		}
	}
	return res
}
