/*
 * Copyright © 2023 Clyso GmbH
 * Copyright © 2025 STRATO GmbH
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

package api

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime/debug"
	"testing"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	otel_trace "go.opentelemetry.io/otel/trace"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/runtime/protoiface"
	"google.golang.org/protobuf/types/known/durationpb"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/trace"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

func NewGrpcServer(port int, handlers pb.ChorusServer, tracer otel_trace.TracerProvider, logConf *log.Config, version dom.AppInfo) (start func(context.Context) error, stop func(context.Context) error, err error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, nil, err
	}

	srv := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    50 * time.Second,
			Timeout: 10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             30 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			prometheus.UnaryServerInterceptor,
			otelgrpc.UnaryServerInterceptor(otelgrpc.WithTracerProvider(tracer)),
			grpc_ctxtags.UnaryServerInterceptor(),
			log.UnaryInterceptor(logConf, version.App, version.AppID),
			trace.UnaryInterceptor(),
			ErrorInterceptor(),
			unaryServerAccessLog,
			unaryServerRecover,
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			prometheus.StreamServerInterceptor,
			otelgrpc.StreamServerInterceptor(otelgrpc.WithTracerProvider(tracer)),
			grpc_ctxtags.StreamServerInterceptor(),
			log.StreamInterceptor(logConf, version.App, version.AppID),
			trace.StreamInterceptor(),
			ErrorStreamInterceptor(),
			streamServerAccessLog,
			streamServerRecover,
		)))

	pb.RegisterChorusServer(srv, handlers)
	return func(ctx context.Context) error {
			return srv.Serve(lis)
		}, func(ctx context.Context) error {
			srv.Stop()
			lis.Close()
			return nil
		}, nil
}

func unaryServerRecover(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (_ interface{}, err error) {
	defer func() {
		if p := recover(); p != nil {
			l := zerolog.Ctx(ctx)
			l.Error().
				Stack().
				Uint32("grpc_code", uint32(codes.Internal)).
				Interface("panic", p).Stack().Msg(string(debug.Stack()))
			err = status.Errorf(codes.Internal, "%v", p)
		}
	}()

	return handler(ctx, req)
}

func streamServerRecover(srv interface{}, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	defer func() {
		if p := recover(); p != nil {
			l := zerolog.Ctx(stream.Context())
			l.Error().
				Uint32("grpc_code", uint32(codes.Internal)).
				Interface("panic", p).Stack().Msg("panic")

			err = status.Errorf(codes.Internal, "%v", p)
		}
	}()

	return handler(srv, stream)
}

func unaryServerAccessLog(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (_ interface{}, err error) {
	resp, err := handler(ctx, req)
	logger := *zerolog.Ctx(ctx)
	rpcLogHandler(logger, err, info.FullMethod)

	return resp, err
}

func rpcLogHandler(l zerolog.Logger, err error, _ string) {
	s := status.Convert(err)
	code, msg := s.Code(), s.Message()
	switch code {
	case codes.OK, codes.Canceled, codes.NotFound:
		l.Debug().Str("grpc_code", code.String()).Str("response_status", "success").Msg(msg)
	case codes.Unauthenticated:
		l.Debug().Str("grpc_code", code.String()).Str("response_status", "failed").Msg(msg)
	case codes.PermissionDenied, codes.FailedPrecondition, codes.AlreadyExists, codes.InvalidArgument:
		// log expected errors with WARN level
		l.Warn().Str("grpc_code", code.String()).Str("response_status", "failed").Msg(msg)
	default:
		l.Error().Str("grpc_code", code.String()).Str("response_status", "failed").Msg(msg)
	}
}

func streamServerAccessLog(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	err = handler(srv, stream)
	logger := *zerolog.Ctx(stream.Context())
	rpcLogHandler(logger, err, info.FullMethod)

	return err
}

func ErrorInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		return resp, convertApiError(ctx, err)
	}
}

func ErrorStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return convertApiError(ss.Context(), handler(srv, ss))
	}
}

func convertApiError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	details := []protoiface.MessageV1{&errdetails.RequestInfo{RequestId: xctx.GetTraceID(ctx)}}
	var code codes.Code
	var mappedErr error
	var retryErr *dom.ErrRateLimitExceeded
	switch {
	case errors.Is(err, dom.ErrInternal):
		code = codes.Internal
		mappedErr = dom.ErrInternal
	case errors.Is(err, dom.ErrNotImplemented):
		code = codes.Unimplemented
		mappedErr = dom.ErrNotImplemented
	case errors.Is(err, dom.ErrInvalidArg):
		code = codes.InvalidArgument
		mappedErr = dom.ErrInvalidArg
		details = append(details, &errdetails.ErrorInfo{
			Reason: err.Error(),
		})
	case errors.Is(err, dom.ErrAuth):
		code = codes.Unauthenticated
		mappedErr = dom.ErrAuth
	case errors.Is(err, dom.ErrInvalidStorageConfig):
		code = codes.InvalidArgument
		mappedErr = dom.ErrInvalidStorageConfig
		details = append(details, &errdetails.ErrorInfo{
			Reason: err.Error(),
		})
	case errors.Is(err, dom.ErrAlreadyExists):
		code = codes.AlreadyExists
		mappedErr = dom.ErrAlreadyExists
		details = append(details, &errdetails.ErrorInfo{
			Reason: err.Error(),
		})
	case errors.Is(err, dom.ErrDestinationConflict) ||
		errors.Is(err, dom.ErrAmbiguousDestination) ||
		errors.Is(err, dom.ErrUnknownDestination):
		code = codes.FailedPrecondition
		mappedErr = err
		details = append(details, &errdetails.ErrorInfo{
			Reason: err.Error(),
		})
	case errors.Is(err, dom.ErrNotFound):
		code = codes.NotFound
		mappedErr = dom.ErrNotFound
		details = append(details, &errdetails.ErrorInfo{
			Reason: err.Error(),
		})
	case errors.As(err, &retryErr):
		code = codes.ResourceExhausted
		mappedErr = &dom.ErrRateLimitExceeded{}
		details = append(details, &errdetails.RetryInfo{
			RetryDelay: durationpb.New(retryErr.RetryIn),
		})
	default:
		code = codes.Internal
		mappedErr = dom.ErrInternal
	}
	zerolog.Ctx(ctx).Err(err).Msg("api error returned")

	st := status.New(code, mappedErr.Error())
	// If we are in testing mode, we return the error without details to allow easy error comparison via r.ErrorIs().
	if testing.Testing() {
		return st.Err()
	}
	stInfo, wdErr := st.WithDetails(details...)
	if wdErr != nil {
		zerolog.Ctx(ctx).Err(wdErr).Msg("unable to build details for grpc error message")
		return st.Err()
	}
	return stInfo.Err()
}
