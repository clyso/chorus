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

package trace

import (
	"context"
	"github.com/clyso/chorus/pkg/log"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

func UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (_ interface{}, err error) {
		traceID := oteltrace.SpanFromContext(ctx).
			SpanContext().
			TraceID()
		ctx = log.WithTraceID(ctx, traceID.String())

		return handler(ctx, req)
	}
}

func StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		traceID := oteltrace.SpanFromContext(stream.Context()).
			SpanContext().
			TraceID()
		ctx := log.WithTraceID(stream.Context(), traceID.String())

		return handler(srv, &grpc_middleware.WrappedServerStream{
			ServerStream:   stream,
			WrappedContext: ctx,
		})
	}
}
