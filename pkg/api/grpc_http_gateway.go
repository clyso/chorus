package api

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/cors"
	"github.com/tmc/grpc-websocket-proxy/wsproxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"net/http"
	"strconv"
)

type RegisterHandlersFunc func(ctx context.Context, mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) error

func GRPCGateway(ctx context.Context, conf *Config, register RegisterHandlersFunc) (start func(context.Context) error, stop func(context.Context) error, err error) {
	mux := runtime.NewServeMux()
	var opts []grpc.DialOption

	if conf.Secure {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: false})))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	serverAddress := ":" + strconv.Itoa(conf.GrpcPort)

	// Register all handlers
	err = register(ctx, mux, serverAddress, opts)
	if err != nil {
		return nil, nil, err
	}

	withCors := cors.New(cors.Options{
		AllowOriginFunc:  func(origin string) bool { return true },
		AllowedMethods:   []string{"GET", "POST", "PATCH", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"ACCEPT", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300,
	}).Handler(mux)

	handler := wsproxy.WebsocketProxy(withCors)
	srv := &http.Server{Addr: fmt.Sprintf("0.0.0.0:%d", conf.HttpPort)}
	srv.Handler = handler

	start = func(ctx context.Context) error {
		return srv.ListenAndServe()
	}
	stop = func(ctx context.Context) error {
		return srv.Shutdown(ctx)
	}
	return
}
