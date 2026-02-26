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

package log

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

const (
	Storage    = "stor_name"
	Object     = "obj_name"
	ObjectVer  = "obj_ver"
	Bucket     = "bucket"
	Method     = "method"
	storType   = "stor_type"
	user       = "user"
	TraceID    = "trace_id"
	httpPath   = "http_path"
	grpcMethod = "grpc_method"
	httpMethod = "http_method"
	httpQuery  = "http_query"
	flow       = "flow"
)

type Config struct {
	Level string `yaml:"level"`
	Json  bool   `yaml:"json"`
}

func GetLogger(cfg *Config, app, appID string) zerolog.Logger {
	logger := CreateLogger(cfg, app, appID)
	zerolog.DefaultContextLogger = &logger
	return logger
}

func CreateLogger(cfg *Config, app, appID string) zerolog.Logger {
	var logger zerolog.Logger
	if cfg.Json {
		logger = zerolog.New(os.Stdout)
	} else {
		logger = zerolog.New(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
		})
	}
	lvl, err := zerolog.ParseLevel(cfg.Level)
	if err != nil {
		lvl = zerolog.InfoLevel
	}

	zerolog.SetGlobalLevel(lvl)
	l := logger.With().Caller().Timestamp()
	if appID != "" {
		l = l.Str("app_id", appID)
	}
	if app != "" {
		l = l.Str("app", app)
	}

	logger = l.Logger()
	return logger
}
