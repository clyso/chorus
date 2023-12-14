package log

import (
	"github.com/rs/zerolog"
	"os"
	"time"
)

const (
	Storage    = "stor_name"
	Object     = "obj_name"
	Bucket     = "bucket"
	Method     = "method"
	user       = "user"
	TraceID    = "trace_id"
	httpPath   = "http_path"
	grpcMethod = "grpc_method"
	httpMethod = "http_method"
	httpQuery  = "http_query"
	flow       = "flow"
)

type Config struct {
	Json  bool   `yaml:"json"`
	Level string `yaml:"level"`
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
