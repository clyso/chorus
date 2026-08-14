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
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

type route struct {
	method string
	path   string
}

// restRoutes maps gRPC full method names to the REST endpoints exposed by the
// chorus management API grpc-gateway. Must be kept in sync with proto/http.yaml.
// Omitted on purpose:
//   - Webhook service endpoints: use path parameters and are not called by chorctl.
//   - Policy.StreamReplication: streaming, has no REST mapping in http.yaml and
//     no chorctl caller (see NewStream).
var restRoutes = map[string]route{
	// Diff service:
	pb.Diff_Start_FullMethodName:            {http.MethodPost, "/diff/start"},
	pb.Diff_List_FullMethodName:             {http.MethodGet, "/diff/list"},
	pb.Diff_GetReport_FullMethodName:        {http.MethodPost, "/diff/report"},
	pb.Diff_GetReportEntries_FullMethodName: {http.MethodPost, "/diff/report-entries"},
	pb.Diff_DeleteReport_FullMethodName:     {http.MethodPut, "/diff/delete"},
	pb.Diff_Fix_FullMethodName:              {http.MethodPost, "/diff/fix"},
	pb.Diff_Restart_FullMethodName:          {http.MethodPut, "/diff/restart"},
	// Chorus service:
	pb.Chorus_GetAppVersion_FullMethodName:       {http.MethodGet, "/version"},
	pb.Chorus_GetStorages_FullMethodName:         {http.MethodGet, "/storage"},
	pb.Chorus_GetProxyCredentials_FullMethodName: {http.MethodGet, "/proxy"},
	pb.Chorus_SetUserCredentials_FullMethodName:  {http.MethodPost, "/credentials"},
	// Policy service - replication:
	pb.Policy_AvailableBuckets_FullMethodName:       {http.MethodPost, "/replication/list-buckets"},
	pb.Policy_AddReplication_FullMethodName:         {http.MethodPost, "/replication/add"},
	pb.Policy_GetReplication_FullMethodName:         {http.MethodPost, "/replication/get"},
	pb.Policy_ListReplications_FullMethodName:       {http.MethodPost, "/replication"},
	pb.Policy_PauseReplication_FullMethodName:       {http.MethodPut, "/replication/pause"},
	pb.Policy_ResumeReplication_FullMethodName:      {http.MethodPut, "/replication/resume"},
	pb.Policy_DeleteReplication_FullMethodName:      {http.MethodPut, "/replication/delete"},
	pb.Policy_SwitchWithZeroDowntime_FullMethodName: {http.MethodPost, "/replication/switch/zero-downtime"},
	pb.Policy_SwitchWithDowntime_FullMethodName:     {http.MethodPost, "/replication/switch"},
	pb.Policy_DeleteSwitch_FullMethodName:           {http.MethodPost, "/replication/switch/delete"},
	pb.Policy_GetSwitchStatus_FullMethodName:        {http.MethodPost, "/replication/switch/get"},
	// Policy service - routing:
	pb.Policy_ListRoutings_FullMethodName:   {http.MethodPost, "/routing"},
	pb.Policy_AddRouting_FullMethodName:     {http.MethodPost, "/routing/add"},
	pb.Policy_DeleteRouting_FullMethodName:  {http.MethodPut, "/routing/delete"},
	pb.Policy_BlockRouting_FullMethodName:   {http.MethodPut, "/routing/block"},
	pb.Policy_UnblockRouting_FullMethodName: {http.MethodPut, "/routing/unblock"},
	pb.Policy_TestProxy_FullMethodName:      {http.MethodPost, "/test-proxy"},
}

// Conn calls the chorus management API over HTTP/JSON via the grpc-gateway
// REST endpoints. It implements grpc.ClientConnInterface so that the
// generated gRPC client stubs (pb.NewChorusClient, pb.NewPolicyClient, ...)
// can be used as-is.
type Conn struct {
	httpClient *http.Client
	baseURL    *url.URL
}

var _ grpc.ClientConnInterface = (*Conn)(nil)

func (c *Conn) Invoke(ctx context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
	rt, ok := restRoutes[method]
	if !ok {
		return status.Errorf(codes.Unimplemented, "no REST mapping for method %s", method)
	}
	reqMsg, ok := args.(proto.Message)
	if !ok {
		return status.Errorf(codes.Internal, "request for %s is not a proto message", method)
	}

	var body io.Reader
	if rt.method == http.MethodGet {
		// All GET routes take google.protobuf.Empty. Fail loudly if a future
		// GET route carries request fields: they would require query
		// parameter encoding, which is not implemented.
		if !isEmptyMessage(reqMsg) {
			return status.Errorf(codes.Internal, "GET route for %s cannot carry request fields", method)
		}
	} else {
		data, err := protojson.Marshal(reqMsg)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to encode request for %s: %v", method, err)
		}
		body = bytes.NewReader(data)
	}

	httpReq, err := http.NewRequestWithContext(ctx, rt.method, c.baseURL.JoinPath(rt.path).String(), body)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to build request for %s: %v", method, err)
	}
	httpReq.Header.Set("Accept", "application/json")
	if body != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return status.Error(transportErrCode(err), err.Error())
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return status.Errorf(transportErrCode(err), "failed to read response: %v", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return errorFromResponse(resp.StatusCode, data)
	}

	replyMsg, ok := reply.(proto.Message)
	if !ok {
		return status.Errorf(codes.Internal, "reply for %s is not a proto message", method)
	}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(data, replyMsg); err != nil {
		return status.Errorf(codes.Internal, "failed to decode response for %s: %v", method, err)
	}
	return nil
}

func (c *Conn) NewStream(_ context.Context, _ *grpc.StreamDesc, method string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, status.Errorf(codes.Unimplemented, "streaming method %s is not supported by the REST client", method)
}

func (c *Conn) Close() error {
	c.httpClient.CloseIdleConnections()
	return nil
}

// isEmptyMessage reports whether the message has no populated fields.
func isEmptyMessage(m proto.Message) bool {
	empty := true
	m.ProtoReflect().Range(func(protoreflect.FieldDescriptor, protoreflect.Value) bool {
		empty = false
		return false
	})
	return empty
}

// transportErrCode maps HTTP client errors to gRPC codes preserving
// context cancellation semantics.
func transportErrCode(err error) codes.Code {
	switch {
	case errors.Is(err, context.Canceled):
		return codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return codes.DeadlineExceeded
	default:
		return codes.Unavailable
	}
}

// errorFromResponse converts a grpc-gateway error response into a gRPC status
// error. The gateway serializes errors as google.rpc.Status JSON
// ({"code":..,"message":..,"details":[..]}), which allows PrintGrpcError to
// present them the same way as before.
func errorFromResponse(httpStatus int, body []byte) error {
	var st spb.Status
	err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(body, &st)
	// Trust the body only if the code is a valid gRPC error code. This filters
	// out non grpc-gateway JSON responses, e.g. {"code":401,...} produced by an
	// auth proxy in front of the API.
	if err == nil && st.GetCode() > int32(codes.OK) && st.GetCode() <= int32(codes.Unauthenticated) {
		return status.ErrorProto(&st)
	}
	const maxErrBody = 512
	if len(body) > maxErrBody {
		body = body[:maxErrBody]
	}
	return status.Errorf(codeFromHTTPStatus(httpStatus), "HTTP %d: %s", httpStatus, bytes.TrimSpace(body))
}

// codeFromHTTPStatus is a fallback for non grpc-gateway error responses,
// e.g. produced by a load balancer in front of the API.
func codeFromHTTPStatus(httpStatus int) codes.Code {
	switch httpStatus {
	case http.StatusBadRequest:
		return codes.InvalidArgument
	case http.StatusInternalServerError:
		return codes.Internal
	case http.StatusBadGateway:
		return codes.Unavailable
	case http.StatusUnauthorized:
		return codes.Unauthenticated
	case http.StatusForbidden:
		return codes.PermissionDenied
	case http.StatusNotFound:
		return codes.NotFound
	case http.StatusConflict:
		return codes.AlreadyExists
	case http.StatusTooManyRequests:
		return codes.ResourceExhausted
	case http.StatusNotImplemented:
		return codes.Unimplemented
	case http.StatusServiceUnavailable:
		return codes.Unavailable
	case http.StatusGatewayTimeout:
		return codes.DeadlineExceeded
	default:
		return codes.Unknown
	}
}
