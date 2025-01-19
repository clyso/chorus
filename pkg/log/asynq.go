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
