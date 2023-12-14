//go:build agent

package agent

import (
	"context"
	"fmt"
	"github.com/alicebob/miniredis/v2"
	"github.com/clyso/chorus/pkg/config"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/s3"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/service/agent"
	"github.com/clyso/chorus/service/worker"
	mclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rs/xid"
	"google.golang.org/grpc"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	mainClient   *mclient.Client
	f1Client     *mclient.Client
	mpMainClient *mclient.Core
	mpF1Client   *mclient.Core
	tstCtx       context.Context
	apiClient    pb.ChorusClient

	workerConf *worker.Config
	agentConf  *agent.Config

	urlHttpApi string
)

const (
	user = "test"
)

func TestMain(m *testing.M) {
	var err error
	workerConf, err = worker.GetConfig(config.Path("override.yaml"))
	if err != nil {
		panic(err)
	}
	workerConf.RClone.MemoryLimit.Enabled = false
	workerConf.RClone.LocalFileLimit.Enabled = false
	workerConf.RClone.GlobalFileLimit.Enabled = false
	workerConf.Features.ACL = false

	agentConf, err = agent.GetConfig(config.Path("agent_config.yaml"))
	if err != nil {
		panic(err)
	}

	redisSvc, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	workerConf.Redis.Address = redisSvc.Addr()
	agentConf.Redis.Address = redisSvc.Addr()
	agentConf.Features.ACL = false

	fmt.Println("redis url", agentConf.Redis.Address)

	//f1Backend := s3mem.New()
	//f1Faker := gofakes3.New(f1Backend)
	//f1Ts := httptest.NewServer(f1Faker.Server())
	//defer f1Ts.Close()

	//workerConf.Storage.Storages["f1"] = s3.Storage{
	//	Address:     f1Ts.URL,
	//	Credentials: map[string]s3.CredentialsV4{user: generateCredentials()},
	//	Provider:    "Other",
	//	IsMain:      false,
	//}
	//
	//fmt.Println("f1 s3", f1Ts.URL)

	mainClient, mpMainClient = createClient(workerConf.Storage.Storages["main"])
	f1Client, mpF1Client = createClient(workerConf.Storage.Storages["f1"])

	workerConf.Api.Enabled = true
	grpcAddr := fmt.Sprintf("127.0.0.1:%d", workerConf.Api.GrpcPort)
	fmt.Println("grpc api", grpcAddr)

	ctx, cancel := context.WithCancel(context.Background())
	tstCtx = ctx

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

	go func() {
		app := dom.AppInfo{
			Version: "test",
			App:     "agent",
			AppID:   xid.New().String(),
		}
		agentCtx, cancelFn := context.WithCancel(ctx)
		defer cancelFn()

		err = agent.Start(agentCtx, app, agentConf)
		if err != nil {
			panic(err)
		}
	}()

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
	cancel()
	// exit
	os.Exit(exitCode)

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
