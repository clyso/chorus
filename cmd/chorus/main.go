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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/rs/zerolog"
	stdlog "github.com/rs/zerolog/log"

	"github.com/clyso/chorus/pkg/config"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/service/standalone"
)

// this information will be collected when built, by -ldflags="-X 'main.version=$(tag)' -X 'main.commit=$(commit)'".
var (
	version    = "development"
	date       = "not set"
	commit     = "not set"
	configPath = flag.String("config", "", "Set path to config yaml file. Default config location is $HOME/.config/chorus/config.yaml")
)

const (
	defaultConfigPath = ".config/chorus/config.yaml"
	helpText          = `Runs Chorus S3 replication proxy in standalone mode.

Configuration:
1) No config provided. Chorus will start with 2 fake in-memory s3 storages.
2) Config placed in $HOME/.config/chorus/config.yaml
3) Config provided explicitly with -config flag

Usage:
  chorus [flags] <command>(optional)'.

Example 1: start chorus in standalone mode with fake s3
  chorus

Example 2: start chorus in standalone mode with custom config
  chorus -config ./my-config.yaml

Example 3: print chorus config
  chorus print-config
  chorus -config ./my-config.yaml print-config 

Commands:
  print-config - prints config, can be used with -config flag
  version - prints Chorus version

Flags:
`
)

func main() {
	var h, help, printVer, v bool
	flag.BoolVar(&h, "h", false, "Print help. Example: chorus -h")
	flag.BoolVar(&v, "v", false, "Verbose output. Example: chorus -v")
	flag.BoolVar(&help, "help", false, "Print help. Example: chorus -help")
	flag.BoolVar(&printVer, "version", false, "Print version. Example: chorus -version")
	flag.Parse()
	if h || help {
		fmt.Print(helpText)
		flag.PrintDefaults()
		os.Exit(0)
	}
	if printVer || (len(flag.Args()) > 0 && flag.Args()[0] == "version") {
		printVersion()
		os.Exit(0)
	}

	zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	if v {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	var configSrc config.Src
	if configPath != nil && *configPath != "" {
		configSrc = config.Path(*configPath)
	} else {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			stdlog.Err(err).Msgf("unable to access homedir to read default config %s", defaultConfigPath)
			os.Exit(1)
		}
		confPath := filepath.Join(homeDir, defaultConfigPath)
		_, err = os.Stat(confPath)
		if err != nil {
			stdlog.Info().Msgf("default config file %s not found", confPath)
		} else {
			configSrc = config.Path(confPath)
		}
	}
	if len(flag.Args()) > 0 && flag.Args()[0] == "print-config" {
		var err error
		if configSrc != nil {
			err = standalone.PrintConfig(configSrc)
		} else {
			err = standalone.PrintConfig()
		}
		if err != nil {
			stdlog.Err(err).Msg("unable to print config")
			os.Exit(1)
		}
		os.Exit(0)
	}

	var conf *standalone.Config
	var err error
	if configSrc != nil {
		conf, err = standalone.GetConfig(configSrc)
	} else {
		stdlog.Warn().Msg("no config location provided")
		conf, err = standalone.GetConfig()
	}
	if err != nil {
		stdlog.Err(err).Msg("unable to read config")
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGTERM)
	go func() {
		<-signals
		zerolog.Ctx(ctx).Info().Msg("received shutdown signal.")
		cancel()
	}()

	err = standalone.Start(ctx, dom.AppInfo{
		Version: version,
		Commit:  commit,
	}, conf)
	if err != nil {
		stdlog.Err(err).Msg("critical error. Shutdown application")
		os.Exit(1)
	}
}

func printVersion() {
	fmt.Printf("Version:    %s\n", version)
	fmt.Printf("Build Time: %s\n", date)
	fmt.Printf("Git Commit: %s\n", commit)
}
