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

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"google.golang.org/grpc"

	xctx "github.com/clyso/chorus/pkg/ctx"
)

func UnaryInterceptor(cfg *Config, app, appID string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (_ interface{}, err error) {
		l := CreateLogger(cfg, app, appID)
		builder := l.With().Str(flow, string(xctx.Api)).Str(grpcMethod, info.FullMethod)
		newLogger := builder.Logger()
		ctx = newLogger.WithContext(ctx)

		return handler(ctx, req)
	}
}

func StreamInterceptor(cfg *Config, app, appID string) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		l := CreateLogger(cfg, app, appID)
		builder := l.With().Str(flow, string(xctx.Api)).Str(grpcMethod, info.FullMethod)
		newLogger := builder.Logger()
		ctx := newLogger.WithContext(stream.Context())

		return handler(srv, &grpc_middleware.WrappedServerStream{
			ServerStream:   stream,
			WrappedContext: ctx,
		})
	}
}
