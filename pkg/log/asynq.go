package log

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
)

type Logger struct{}

func NewStdLogger() *Logger {
	return &Logger{}
}

func (logger *Logger) Printf(ctx context.Context, format string, v ...interface{}) {
	zerolog.Ctx(ctx).Debug().Msgf(format, v...)
}

func (logger *Logger) Debug(args ...interface{}) {
	zerolog.DefaultContextLogger.Debug().Msg(fmt.Sprint(args...))
}

func (logger *Logger) Info(args ...interface{}) {
	zerolog.DefaultContextLogger.Info().Msg(fmt.Sprint(args...))
}

func (logger *Logger) Warn(args ...interface{}) {
	zerolog.DefaultContextLogger.Warn().Msg(fmt.Sprint(args...))
}

func (logger *Logger) Error(args ...interface{}) {
	zerolog.DefaultContextLogger.Error().Msg(fmt.Sprint(args...))
}

func (logger *Logger) Fatal(args ...interface{}) {
	zerolog.DefaultContextLogger.Fatal().Msg(fmt.Sprint(args...))
}
