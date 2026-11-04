/*
 * Copyright © 2026 Clyso GmbH
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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

var apiServices = []string{"chorus.Chorus", "chorus.Policy", "chorus.Diff"}

func newTestGateway(t *testing.T, chorusSrv pb.ChorusServer) *Conn {
	t.Helper()
	r := require.New(t)
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
			MarshalOptions:   protojson.MarshalOptions{EmitUnpopulated: true},
			UnmarshalOptions: protojson.UnmarshalOptions{DiscardUnknown: true},
		}),
	)
	ctx := t.Context()
	r.NoError(pb.RegisterChorusHandlerServer(ctx, mux, chorusSrv))
	r.NoError(pb.RegisterPolicyHandlerServer(ctx, mux, pb.UnimplementedPolicyServer{}))
	r.NoError(pb.RegisterDiffHandlerServer(ctx, mux, pb.UnimplementedDiffServer{}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	conn, err := Connect(ctx, srv.URL, false)
	r.NoError(err)
	return conn
}

func methodDescriptor(t *testing.T, fullMethod string) protoreflect.MethodDescriptor {
	t.Helper()
	r := require.New(t)
	svcName, methodName, found := strings.Cut(strings.TrimPrefix(fullMethod, "/"), "/")
	r.True(found, "invalid full method name %q", fullMethod)
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(svcName))
	r.NoError(err, "service %s not found in proto registry", svcName)
	svc, ok := d.(protoreflect.ServiceDescriptor)
	r.True(ok, "%s is not a service", svcName)
	m := svc.Methods().ByName(protoreflect.Name(methodName))
	r.NotNil(m, "method %s not found in service %s", methodName, svcName)
	return m
}

func newMessage(t *testing.T, md protoreflect.MessageDescriptor) proto.Message {
	t.Helper()
	mt, err := protoregistry.GlobalTypes.FindMessageByName(md.FullName())
	require.NoError(t, err, "message %s not found in proto registry", md.FullName())
	return mt.New().Interface()
}

func TestRestRoutes_MatchGeneratedGateway(t *testing.T) {
	conn := newTestGateway(t, pb.UnimplementedChorusServer{})
	for fullMethod := range restRoutes {
		t.Run(fullMethod, func(t *testing.T) {
			r := require.New(t)
			md := methodDescriptor(t, fullMethod)
			req := newMessage(t, md.Input())
			reply := newMessage(t, md.Output())
			err := conn.Invoke(t.Context(), fullMethod, req, reply)
			st, ok := status.FromError(err)
			r.True(ok, "expected gRPC status error, got %v", err)
			r.Equal(codes.Unimplemented, st.Code(), "route was not dispatched to the gRPC method: %s", st.Message())
			r.Equal(fmt.Sprintf("method %s not implemented", md.Name()), st.Message())
		})
	}
}

func TestRestRoutes_CoverAllAPIMethods(t *testing.T) {
	r := require.New(t)
	for _, svcName := range apiServices {
		d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(svcName))
		r.NoError(err)
		svc, ok := d.(protoreflect.ServiceDescriptor)
		r.True(ok)
		for i := 0; i < svc.Methods().Len(); i++ {
			m := svc.Methods().Get(i)
			if m.IsStreamingServer() || m.IsStreamingClient() {
				continue
			}
			fullMethod := fmt.Sprintf("/%s/%s", svcName, m.Name())
			_, found := restRoutes[fullMethod]
			r.True(found, "method %s has no entry in restRoutes; keep it in sync with proto/http.yaml", fullMethod)
		}
	}
}

type browserBucketsStub struct {
	pb.UnimplementedChorusServer
	request *pb.ListBrowserBucketsRequest
}

func (s *browserBucketsStub) ListBrowserBuckets(_ context.Context, req *pb.ListBrowserBucketsRequest) (*pb.ListBrowserBucketsResponse, error) {
	s.request = req
	return &pb.ListBrowserBucketsResponse{Buckets: []string{"bucket-a"}}, nil
}

func TestConn_InvokeGETEncodesRequestAsQuery(t *testing.T) {
	r := require.New(t)
	stub := &browserBucketsStub{}
	conn := newTestGateway(t, stub)
	var reply pb.ListBrowserBucketsResponse
	err := conn.Invoke(t.Context(), pb.Chorus_ListBrowserBuckets_FullMethodName, &pb.ListBrowserBucketsRequest{
		Storage: "main storage",
		User:    "browser-user",
	}, &reply)
	r.NoError(err)
	r.Equal("main storage", stub.request.GetStorage())
	r.Equal("browser-user", stub.request.GetUser())
	r.Equal([]string{"bucket-a"}, reply.GetBuckets())
}

func TestRestRoutes_GetRoutesEncodeRequestFields(t *testing.T) {
	r := require.New(t)
	for fullMethod, rt := range restRoutes {
		if rt.method != http.MethodGet {
			continue
		}
		md := methodDescriptor(t, fullMethod)
		req := newMessage(t, md.Input())
		_, err := encodeQuery(req)
		r.NoError(err, "GET route %s request cannot be encoded as query params", fullMethod)
	}
}

func TestRestRoutes_GetRequestsUseProtoJSONNames(t *testing.T) {
	r := require.New(t)
	query, err := encodeQuery(&pb.ListBrowserObjectsRequest{
		Storage: "main",
		User:    "user",
		Bucket:  "bucket",
		Prefix:  "a/b",
		After:   "next",
		Limit:   50,
	})
	r.NoError(err)
	r.Equal("main", query.Get("storage"))
	r.Equal("user", query.Get("user"))
	r.Equal("bucket", query.Get("bucket"))
	r.Equal("a/b", query.Get("prefix"))
	r.Equal("next", query.Get("after"))
	r.Equal("50", query.Get("limit"))
}

type stubChorusServer struct {
	pb.UnimplementedChorusServer
}

func (stubChorusServer) GetAppVersion(context.Context, *emptypb.Empty) (*pb.GetAppVersionResponse, error) {
	return &pb.GetAppVersionResponse{Version: "v1.2.3", Commit: "abc", Date: "2026-01-01"}, nil
}

func TestConn_InvokeRoundTrip(t *testing.T) {
	r := require.New(t)
	conn := newTestGateway(t, stubChorusServer{})
	var reply pb.GetAppVersionResponse
	r.NoError(conn.Invoke(t.Context(), pb.Chorus_GetAppVersion_FullMethodName, &emptypb.Empty{}, &reply))
	r.Equal("v1.2.3", reply.Version)
	r.Equal("abc", reply.Commit)
	r.Equal("2026-01-01", reply.Date)
}
