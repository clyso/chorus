package standalone

import (
	"context"
	"fmt"
	"github.com/alicebob/miniredis/v2"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/service/proxy"
	"github.com/clyso/chorus/service/worker"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
	"net"
	"strconv"
	"strings"
)

const (
	connectInfo = `[92m_________ .__                               
\_   ___ \|  |__   ___________ __ __  ______
/    \  \/|  |  \ /  _ \_  __ \  |  \/  ___/
\     \___|   Y  (  <_> )  | \/  |  /\___ \ 
 \______  /___|  /\____/|__|  |____//____  >
        \/     \/                        \/[0m


Mgmt UI URL:	%s

S3 Proxy URL: 	%s
S3 Proxy Credentials (AccessKey|SecretKey): 		
%s

GRPC mgmt API:	%s
HTTP mgmt API:	%s
Redis URL:	%s

Storage list:
%s
`
)

func Start(ctx context.Context, app dom.AppInfo, conf *Config) error {
	if err := conf.Validate(); err != nil {
		return err
	}
	features.Set(conf.Features)
	logger := log.GetLogger(conf.Log, "", "")
	logger.Info().
		Str("version", app.Version).
		Str("commit", app.Commit).
		Msg("app starting...")

	// start embedded redis:
	redisSvc, err := miniredis.Run()
	if err != nil {
		return fmt.Errorf("%w: unable to start redis", err)
	}

	fake := map[string]bool{}
	g, ctx := errgroup.WithContext(ctx)
	for name, storage := range conf.Storage.Storages {
		isFake := false
		fakePort := 0
		if storage.Address == "" {
			_, fakePort, err = getRandomPort()
			if err != nil {
				return fmt.Errorf("%w: unable to get random port", err)
			}
			isFake = true
		} else if strings.HasPrefix(storage.Address, ":") {
			fakePort, err = strconv.Atoi(strings.TrimPrefix(storage.Address, ":"))
			if err != nil {
				return fmt.Errorf("%w: unable to parse storage address %s", err, storage.Address)
			}
			isFake = true
		}
		if isFake {
			fake[name] = true
			g.Go(func() error {
				return serveFakeS3(ctx, fakePort)
			})
			//conf.Storage.Storages[name].Address=httpLocalhost(fakePort)
			//conf.Storage.Storages[name].IsSecure=false
			storage.Address = httpLocalhost(fakePort)
			storage.IsSecure = false
			conf.Storage.Storages[name] = storage
		}
	}

	proxyURL := "\u001B[91m<disabled in config>\u001B[0m"
	if conf.Proxy.Enabled {
		proxyURL = httpLocalhost(conf.Proxy.Port)
	}
	fmt.Printf(connectInfo,
		httpLocalhost(conf.UIPort),
		proxyURL,
		printCreds(conf),
		localhost(conf.Api.GrpcPort),
		httpLocalhost(conf.Api.HttpPort),
		redisSvc.Addr(),
		printStorages(fake, conf.Storage),
	)

	workerConf := conf.Config
	workerConf.Redis.Address = redisSvc.Addr()

	//deep copy worker config
	wcBytes, err := yaml.Marshal(&workerConf)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(wcBytes, &workerConf)
	if err != nil {
		return err
	}
	//start worker
	g.Go(func() error {
		return worker.Start(ctx, app, &workerConf)
	})

	if conf.Proxy.Enabled {
		proxyConf := proxy.Config{
			Common:  conf.Common,
			Auth:    conf.Proxy.Auth,
			Port:    conf.Proxy.Port,
			Address: conf.Proxy.Address,
			Storage: conf.Storage,
			Cors:    conf.Proxy.Cors,
		}
		proxyConf.Redis.Address = redisSvc.Addr()

		//deep copy proxy config
		pcBytes, err := yaml.Marshal(&proxyConf)
		if err != nil {
			return err
		}
		err = yaml.Unmarshal(pcBytes, &proxyConf)
		if err != nil {
			return err
		}
		//start proxy
		g.Go(func() error {
			return proxy.Start(ctx, app, &proxyConf)
		})
	}

	// start UI
	g.Go(func() error {
		return serveUI(ctx, conf.UIPort)
	})

	return g.Wait()
}

func httpLocalhost(port int) string {
	return fmt.Sprintf("http://127.0.0.1:%d", port)
}

func localhost(port int) string {
	return fmt.Sprintf("127.0.0.1:%d", port)
}

func printCreds(conf *Config) string {
	if !conf.Proxy.Enabled {
		return ""
	}
	var creds map[string]s3.CredentialsV4
	if conf.Proxy.Auth.UseStorage != "" {
		creds = conf.Storage.Storages[conf.Proxy.Auth.UseStorage].Credentials
	} else {
		creds = conf.Proxy.Auth.Custom
	}
	if len(creds) == 0 {
		return "<no credentials provided in config>"
	}
	res := make([]string, 0, len(creds))
	for s, v4 := range creds {
		res = append(res, fmt.Sprintf(" - %s: [%s|%s]", s, v4.AccessKeyID, v4.SecretAccessKey))
	}
	return strings.Join(res, "\n")
}

func printStorages(fake map[string]bool, conf *s3.StorageConfig) string {
	if len(conf.Storages) == 0 {
		return "<no storages provided in config>"
	}
	res := make([]string, 0, len(conf.Storages))
	for name, stor := range conf.Storages {
		f := ""
		if fake[name] {
			f = "[\u001B[33mFAKE\u001B[0m] "
		}
		m := ""
		if stor.IsMain {
			m = " < \u001B[94mMAIN\u001B[0m"
		}
		res = append(res, fmt.Sprintf(" - %s%s: %s%s", f, name, stor.Address, m))
	}
	return strings.Join(res, "\n")
}

func getRandomPort() (string, int, error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return "", 0, err
	}
	addr := l.Addr().String()
	addrs := strings.Split(addr, ":")
	err = l.Close()
	if err != nil {
		return "", 0, err
	}

	port, err := strconv.Atoi(addrs[len(addrs)-1])
	if err != nil {
		return "", 0, err
	}
	return addr, port, nil
}
