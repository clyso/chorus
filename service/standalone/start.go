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

package standalone

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/alicebob/miniredis/v2"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/service/proxy"
	"github.com/clyso/chorus/service/worker"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
)

const (
	connectInfo = `[92m_________ .__                               
\_   ___ \|  |__   ___________ __ __  ______
/    \  \/|  |  \ /  _ \_  __ \  |  \/  ___/
\     \___|   Y  (  <_> )  | \/  |  /\___ \ 
 \______  /___|  /\____/|__|  |____//____  >
        \/     \/                        \/[0m


%s

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
	// detect fake s3 storages in config
	fake := map[string]int{}
	for name, storage := range conf.Storage.Storages {
		var err error
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
			fake[name] = fakePort
			storage.Address = httpLocalhost(fakePort)
			storage.IsSecure = false
			conf.Storage.Storages[name] = storage
		}
	}

	// validate config
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
	go func() {
		<-ctx.Done()
		redisSvc.Close()
	}()

	// start fake s3 storages
	g, ctx := errgroup.WithContext(ctx)
	for _, fakePort := range fake {
		port := fakePort
		g.Go(func() error {
			return serveFakeS3(ctx, port)
		})
	}

	proxyURL := "\u001B[91m<disabled in config>\u001B[0m"
	if conf.Proxy.Enabled {
		proxyURL = httpLocalhost(conf.Proxy.Port)
	}

	workerConf := conf.Config
	workerConf.Redis.Address = redisSvc.Addr()

	// deep copy worker config
	wcBytes, err := yaml.Marshal(&workerConf)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(wcBytes, &workerConf)
	if err != nil {
		return err
	}
	// start worker
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

		// deep copy proxy config
		pcBytes, err := yaml.Marshal(&proxyConf)
		if err != nil {
			return err
		}
		err = yaml.Unmarshal(pcBytes, &proxyConf)
		if err != nil {
			return err
		}
		// start proxy
		g.Go(func() error {
			return proxy.Start(ctx, app, &proxyConf)
		})
	}

	uiURL := ""
	uiServer, err := serveUI(ctx, conf.UIPort)
	if err == nil {
		// start UI
		g.Go(func() error {
			return uiServer()
		})
		uiURL = fmt.Sprintf("Mgmt UI URL:	%s", httpLocalhost(conf.UIPort))
	} else if !errors.Is(err, dom.ErrNotFound) {
		return err
	}

	fmt.Printf(connectInfo,
		uiURL,
		proxyURL,
		printCreds(conf),
		localhost(conf.Api.GrpcPort),
		httpLocalhost(conf.Api.HttpPort),
		redisSvc.Addr(),
		printStorages(fake, conf.Storage),
	)

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

func printStorages(fake map[string]int, conf *s3.StorageConfig) string {
	if len(conf.Storages) == 0 {
		return "<no storages provided in config>"
	}
	res := make([]string, 0, len(conf.Storages))
	for name, stor := range conf.Storages {
		f := ""
		if _, ok := fake[name]; ok {
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
