// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             (unknown)
// source: chorus/chorus.proto

package pb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	Chorus_GetAppVersion_FullMethodName             = "/chorus.Chorus/GetAppVersion"
	Chorus_GetStorages_FullMethodName               = "/chorus.Chorus/GetStorages"
	Chorus_GetProxyCredentials_FullMethodName       = "/chorus.Chorus/GetProxyCredentials"
	Chorus_ListBucketsForReplication_FullMethodName = "/chorus.Chorus/ListBucketsForReplication"
	Chorus_AddReplication_FullMethodName            = "/chorus.Chorus/AddReplication"
	Chorus_ListReplications_FullMethodName          = "/chorus.Chorus/ListReplications"
	Chorus_ListUserReplications_FullMethodName      = "/chorus.Chorus/ListUserReplications"
	Chorus_StreamBucketReplication_FullMethodName   = "/chorus.Chorus/StreamBucketReplication"
	Chorus_PauseReplication_FullMethodName          = "/chorus.Chorus/PauseReplication"
	Chorus_ResumeReplication_FullMethodName         = "/chorus.Chorus/ResumeReplication"
	Chorus_DeleteReplication_FullMethodName         = "/chorus.Chorus/DeleteReplication"
	Chorus_DeleteUserReplication_FullMethodName     = "/chorus.Chorus/DeleteUserReplication"
	Chorus_SwitchMainBucket_FullMethodName          = "/chorus.Chorus/SwitchMainBucket"
	Chorus_CompareBucket_FullMethodName             = "/chorus.Chorus/CompareBucket"
	Chorus_GetAgents_FullMethodName                 = "/chorus.Chorus/GetAgents"
	Chorus_AddBucketReplication_FullMethodName      = "/chorus.Chorus/AddBucketReplication"
)

// ChorusClient is the client API for Chorus service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ChorusClient interface {
	// Get app version
	GetAppVersion(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*GetAppVersionResponse, error)
	// Lists configured storages with users
	GetStorages(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*GetStoragesResponse, error)
	// Returns connection details for proxy s3 endpoint
	GetProxyCredentials(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*GetProxyCredentialsResponse, error)
	// Returns list of buckets to configure new replication.
	ListBucketsForReplication(ctx context.Context, in *ListBucketsForReplicationRequest, opts ...grpc.CallOption) (*ListBucketsForReplicationResponse, error)
	// Configures new replication for user or bucket(-s)
	AddReplication(ctx context.Context, in *AddReplicationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	// Lists configured replications with statuses
	ListReplications(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*ListReplicationsResponse, error)
	ListUserReplications(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*ListUserReplicationsResponse, error)
	StreamBucketReplication(ctx context.Context, in *ReplicationRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[Replication], error)
	// Pauses given replication
	PauseReplication(ctx context.Context, in *ReplicationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	// Resumes given replication
	ResumeReplication(ctx context.Context, in *ReplicationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	// Deletes given replication
	DeleteReplication(ctx context.Context, in *ReplicationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	DeleteUserReplication(ctx context.Context, in *DeleteUserReplicationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	SwitchMainBucket(ctx context.Context, in *SwitchMainBucketRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	// Compares contents of given bucket in given storages
	CompareBucket(ctx context.Context, in *CompareBucketRequest, opts ...grpc.CallOption) (*CompareBucketResponse, error)
	GetAgents(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*GetAgentsResponse, error)
	AddBucketReplication(ctx context.Context, in *AddBucketReplicationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

type chorusClient struct {
	cc grpc.ClientConnInterface
}

func NewChorusClient(cc grpc.ClientConnInterface) ChorusClient {
	return &chorusClient{cc}
}

func (c *chorusClient) GetAppVersion(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*GetAppVersionResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetAppVersionResponse)
	err := c.cc.Invoke(ctx, Chorus_GetAppVersion_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *chorusClient) GetStorages(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*GetStoragesResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetStoragesResponse)
	err := c.cc.Invoke(ctx, Chorus_GetStorages_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *chorusClient) GetProxyCredentials(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*GetProxyCredentialsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetProxyCredentialsResponse)
	err := c.cc.Invoke(ctx, Chorus_GetProxyCredentials_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *chorusClient) ListBucketsForReplication(ctx context.Context, in *ListBucketsForReplicationRequest, opts ...grpc.CallOption) (*ListBucketsForReplicationResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ListBucketsForReplicationResponse)
	err := c.cc.Invoke(ctx, Chorus_ListBucketsForReplication_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *chorusClient) AddReplication(ctx context.Context, in *AddReplicationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, Chorus_AddReplication_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *chorusClient) ListReplications(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*ListReplicationsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ListReplicationsResponse)
	err := c.cc.Invoke(ctx, Chorus_ListReplications_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *chorusClient) ListUserReplications(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*ListUserReplicationsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ListUserReplicationsResponse)
	err := c.cc.Invoke(ctx, Chorus_ListUserReplications_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *chorusClient) StreamBucketReplication(ctx context.Context, in *ReplicationRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[Replication], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &Chorus_ServiceDesc.Streams[0], Chorus_StreamBucketReplication_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[ReplicationRequest, Replication]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type Chorus_StreamBucketReplicationClient = grpc.ServerStreamingClient[Replication]

func (c *chorusClient) PauseReplication(ctx context.Context, in *ReplicationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, Chorus_PauseReplication_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *chorusClient) ResumeReplication(ctx context.Context, in *ReplicationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, Chorus_ResumeReplication_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *chorusClient) DeleteReplication(ctx context.Context, in *ReplicationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, Chorus_DeleteReplication_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *chorusClient) DeleteUserReplication(ctx context.Context, in *DeleteUserReplicationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, Chorus_DeleteUserReplication_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *chorusClient) SwitchMainBucket(ctx context.Context, in *SwitchMainBucketRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, Chorus_SwitchMainBucket_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *chorusClient) CompareBucket(ctx context.Context, in *CompareBucketRequest, opts ...grpc.CallOption) (*CompareBucketResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(CompareBucketResponse)
	err := c.cc.Invoke(ctx, Chorus_CompareBucket_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *chorusClient) GetAgents(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*GetAgentsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetAgentsResponse)
	err := c.cc.Invoke(ctx, Chorus_GetAgents_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *chorusClient) AddBucketReplication(ctx context.Context, in *AddBucketReplicationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, Chorus_AddBucketReplication_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ChorusServer is the server API for Chorus service.
// All implementations should embed UnimplementedChorusServer
// for forward compatibility.
type ChorusServer interface {
	// Get app version
	GetAppVersion(context.Context, *emptypb.Empty) (*GetAppVersionResponse, error)
	// Lists configured storages with users
	GetStorages(context.Context, *emptypb.Empty) (*GetStoragesResponse, error)
	// Returns connection details for proxy s3 endpoint
	GetProxyCredentials(context.Context, *emptypb.Empty) (*GetProxyCredentialsResponse, error)
	// Returns list of buckets to configure new replication.
	ListBucketsForReplication(context.Context, *ListBucketsForReplicationRequest) (*ListBucketsForReplicationResponse, error)
	// Configures new replication for user or bucket(-s)
	AddReplication(context.Context, *AddReplicationRequest) (*emptypb.Empty, error)
	// Lists configured replications with statuses
	ListReplications(context.Context, *emptypb.Empty) (*ListReplicationsResponse, error)
	ListUserReplications(context.Context, *emptypb.Empty) (*ListUserReplicationsResponse, error)
	StreamBucketReplication(*ReplicationRequest, grpc.ServerStreamingServer[Replication]) error
	// Pauses given replication
	PauseReplication(context.Context, *ReplicationRequest) (*emptypb.Empty, error)
	// Resumes given replication
	ResumeReplication(context.Context, *ReplicationRequest) (*emptypb.Empty, error)
	// Deletes given replication
	DeleteReplication(context.Context, *ReplicationRequest) (*emptypb.Empty, error)
	DeleteUserReplication(context.Context, *DeleteUserReplicationRequest) (*emptypb.Empty, error)
	SwitchMainBucket(context.Context, *SwitchMainBucketRequest) (*emptypb.Empty, error)
	// Compares contents of given bucket in given storages
	CompareBucket(context.Context, *CompareBucketRequest) (*CompareBucketResponse, error)
	GetAgents(context.Context, *emptypb.Empty) (*GetAgentsResponse, error)
	AddBucketReplication(context.Context, *AddBucketReplicationRequest) (*emptypb.Empty, error)
}

// UnimplementedChorusServer should be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedChorusServer struct{}

func (UnimplementedChorusServer) GetAppVersion(context.Context, *emptypb.Empty) (*GetAppVersionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAppVersion not implemented")
}
func (UnimplementedChorusServer) GetStorages(context.Context, *emptypb.Empty) (*GetStoragesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetStorages not implemented")
}
func (UnimplementedChorusServer) GetProxyCredentials(context.Context, *emptypb.Empty) (*GetProxyCredentialsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetProxyCredentials not implemented")
}
func (UnimplementedChorusServer) ListBucketsForReplication(context.Context, *ListBucketsForReplicationRequest) (*ListBucketsForReplicationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListBucketsForReplication not implemented")
}
func (UnimplementedChorusServer) AddReplication(context.Context, *AddReplicationRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddReplication not implemented")
}
func (UnimplementedChorusServer) ListReplications(context.Context, *emptypb.Empty) (*ListReplicationsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListReplications not implemented")
}
func (UnimplementedChorusServer) ListUserReplications(context.Context, *emptypb.Empty) (*ListUserReplicationsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListUserReplications not implemented")
}
func (UnimplementedChorusServer) StreamBucketReplication(*ReplicationRequest, grpc.ServerStreamingServer[Replication]) error {
	return status.Errorf(codes.Unimplemented, "method StreamBucketReplication not implemented")
}
func (UnimplementedChorusServer) PauseReplication(context.Context, *ReplicationRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PauseReplication not implemented")
}
func (UnimplementedChorusServer) ResumeReplication(context.Context, *ReplicationRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ResumeReplication not implemented")
}
func (UnimplementedChorusServer) DeleteReplication(context.Context, *ReplicationRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteReplication not implemented")
}
func (UnimplementedChorusServer) DeleteUserReplication(context.Context, *DeleteUserReplicationRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteUserReplication not implemented")
}
func (UnimplementedChorusServer) SwitchMainBucket(context.Context, *SwitchMainBucketRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SwitchMainBucket not implemented")
}
func (UnimplementedChorusServer) CompareBucket(context.Context, *CompareBucketRequest) (*CompareBucketResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CompareBucket not implemented")
}
func (UnimplementedChorusServer) GetAgents(context.Context, *emptypb.Empty) (*GetAgentsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAgents not implemented")
}
func (UnimplementedChorusServer) AddBucketReplication(context.Context, *AddBucketReplicationRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddBucketReplication not implemented")
}
func (UnimplementedChorusServer) testEmbeddedByValue() {}

// UnsafeChorusServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ChorusServer will
// result in compilation errors.
type UnsafeChorusServer interface {
	mustEmbedUnimplementedChorusServer()
}

func RegisterChorusServer(s grpc.ServiceRegistrar, srv ChorusServer) {
	// If the following call pancis, it indicates UnimplementedChorusServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&Chorus_ServiceDesc, srv)
}

func _Chorus_GetAppVersion_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ChorusServer).GetAppVersion(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Chorus_GetAppVersion_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ChorusServer).GetAppVersion(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _Chorus_GetStorages_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ChorusServer).GetStorages(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Chorus_GetStorages_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ChorusServer).GetStorages(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _Chorus_GetProxyCredentials_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ChorusServer).GetProxyCredentials(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Chorus_GetProxyCredentials_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ChorusServer).GetProxyCredentials(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _Chorus_ListBucketsForReplication_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListBucketsForReplicationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ChorusServer).ListBucketsForReplication(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Chorus_ListBucketsForReplication_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ChorusServer).ListBucketsForReplication(ctx, req.(*ListBucketsForReplicationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Chorus_AddReplication_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddReplicationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ChorusServer).AddReplication(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Chorus_AddReplication_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ChorusServer).AddReplication(ctx, req.(*AddReplicationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Chorus_ListReplications_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ChorusServer).ListReplications(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Chorus_ListReplications_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ChorusServer).ListReplications(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _Chorus_ListUserReplications_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ChorusServer).ListUserReplications(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Chorus_ListUserReplications_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ChorusServer).ListUserReplications(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _Chorus_StreamBucketReplication_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ReplicationRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(ChorusServer).StreamBucketReplication(m, &grpc.GenericServerStream[ReplicationRequest, Replication]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type Chorus_StreamBucketReplicationServer = grpc.ServerStreamingServer[Replication]

func _Chorus_PauseReplication_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReplicationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ChorusServer).PauseReplication(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Chorus_PauseReplication_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ChorusServer).PauseReplication(ctx, req.(*ReplicationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Chorus_ResumeReplication_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReplicationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ChorusServer).ResumeReplication(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Chorus_ResumeReplication_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ChorusServer).ResumeReplication(ctx, req.(*ReplicationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Chorus_DeleteReplication_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReplicationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ChorusServer).DeleteReplication(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Chorus_DeleteReplication_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ChorusServer).DeleteReplication(ctx, req.(*ReplicationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Chorus_DeleteUserReplication_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteUserReplicationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ChorusServer).DeleteUserReplication(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Chorus_DeleteUserReplication_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ChorusServer).DeleteUserReplication(ctx, req.(*DeleteUserReplicationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Chorus_SwitchMainBucket_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SwitchMainBucketRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ChorusServer).SwitchMainBucket(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Chorus_SwitchMainBucket_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ChorusServer).SwitchMainBucket(ctx, req.(*SwitchMainBucketRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Chorus_CompareBucket_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CompareBucketRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ChorusServer).CompareBucket(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Chorus_CompareBucket_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ChorusServer).CompareBucket(ctx, req.(*CompareBucketRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Chorus_GetAgents_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ChorusServer).GetAgents(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Chorus_GetAgents_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ChorusServer).GetAgents(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _Chorus_AddBucketReplication_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddBucketReplicationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ChorusServer).AddBucketReplication(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Chorus_AddBucketReplication_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ChorusServer).AddBucketReplication(ctx, req.(*AddBucketReplicationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Chorus_ServiceDesc is the grpc.ServiceDesc for Chorus service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Chorus_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "chorus.Chorus",
	HandlerType: (*ChorusServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetAppVersion",
			Handler:    _Chorus_GetAppVersion_Handler,
		},
		{
			MethodName: "GetStorages",
			Handler:    _Chorus_GetStorages_Handler,
		},
		{
			MethodName: "GetProxyCredentials",
			Handler:    _Chorus_GetProxyCredentials_Handler,
		},
		{
			MethodName: "ListBucketsForReplication",
			Handler:    _Chorus_ListBucketsForReplication_Handler,
		},
		{
			MethodName: "AddReplication",
			Handler:    _Chorus_AddReplication_Handler,
		},
		{
			MethodName: "ListReplications",
			Handler:    _Chorus_ListReplications_Handler,
		},
		{
			MethodName: "ListUserReplications",
			Handler:    _Chorus_ListUserReplications_Handler,
		},
		{
			MethodName: "PauseReplication",
			Handler:    _Chorus_PauseReplication_Handler,
		},
		{
			MethodName: "ResumeReplication",
			Handler:    _Chorus_ResumeReplication_Handler,
		},
		{
			MethodName: "DeleteReplication",
			Handler:    _Chorus_DeleteReplication_Handler,
		},
		{
			MethodName: "DeleteUserReplication",
			Handler:    _Chorus_DeleteUserReplication_Handler,
		},
		{
			MethodName: "SwitchMainBucket",
			Handler:    _Chorus_SwitchMainBucket_Handler,
		},
		{
			MethodName: "CompareBucket",
			Handler:    _Chorus_CompareBucket_Handler,
		},
		{
			MethodName: "GetAgents",
			Handler:    _Chorus_GetAgents_Handler,
		},
		{
			MethodName: "AddBucketReplication",
			Handler:    _Chorus_AddBucketReplication_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StreamBucketReplication",
			Handler:       _Chorus_StreamBucketReplication_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "chorus/chorus.proto",
}
