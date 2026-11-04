/*
 * Copyright © 2024 Clyso GmbH
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

	"github.com/minio/minio-go/v7"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/rpc"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/swift"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

func ChorusHandlers(
	credsSvc objstore.CredsService,
	clientRegistry objstore.Clients,
	proxyClient rpc.Proxy,
	appInfo *dom.AppInfo,
) pb.ChorusServer {
	return &chorusHandlers{
		credsSvc:    credsSvc,
		clients:     clientRegistry,
		proxyClient: proxyClient,
		appInfo:     appInfo,
	}
}

var _ pb.ChorusServer = &chorusHandlers{}

type chorusHandlers struct {
	proxyClient rpc.Proxy
	appInfo     *dom.AppInfo
	credsSvc    objstore.CredsService
	clients     objstore.Clients
}

func (h *chorusHandlers) GetAppVersion(_ context.Context, _ *emptypb.Empty) (*pb.GetAppVersionResponse, error) {
	return &pb.GetAppVersionResponse{
		Version: h.appInfo.Version,
		Commit:  h.appInfo.Commit,
		Date:    h.appInfo.Date,
	}, nil
}

func (h *chorusHandlers) GetStorages(_ context.Context, _ *emptypb.Empty) (*pb.GetStoragesResponse, error) {
	res := toPbStorages(h.credsSvc)
	return &pb.GetStoragesResponse{Storages: res}, nil
}

func (h *chorusHandlers) ListBrowserBuckets(ctx context.Context, req *pb.ListBrowserBucketsRequest) (*pb.ListBrowserBucketsResponse, error) {
	client, err := h.browserClient(ctx, req.Storage, req.User)
	if err != nil {
		return nil, err
	}
	buckets, err := client.S3().ListBuckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to list buckets: %w", err)
	}

	result := make([]string, 0, len(buckets))
	for _, bucket := range buckets {
		result = append(result, bucket.Name)
	}
	return &pb.ListBrowserBucketsResponse{Buckets: result}, nil
}

func (h *chorusHandlers) ListBrowserObjects(ctx context.Context, req *pb.ListBrowserObjectsRequest) (*pb.ListBrowserObjectsResponse, error) {
	if req.Bucket == "" {
		return nil, fmt.Errorf("%w: bucket is required", dom.ErrInvalidArg)
	}
	client, err := h.browserClient(ctx, req.Storage, req.User)
	if err != nil {
		return nil, err
	}

	limit := req.Limit
	if limit == 0 {
		limit = 50
	} else if limit > 100 {
		limit = 100
	}

	listed := make([]minio.ObjectInfo, 0, limit+1)
	objects := client.S3().ListObjects(ctx, req.Bucket, minio.ListObjectsOptions{
		Prefix:     req.Prefix,
		StartAfter: req.After,
		Recursive:  true,
		MaxKeys:    int(limit + 1),
	})
	for object := range objects {
		if object.Err != nil {
			return nil, fmt.Errorf("unable to list objects: %w", object.Err)
		}
		listed = append(listed, object)
	}

	hasMore := len(listed) > int(limit)
	if hasMore {
		listed = listed[:limit]
	}

	result := &pb.ListBrowserObjectsResponse{
		Objects: make([]*pb.BrowserObject, 0, len(listed)),
		HasMore: hasMore,
	}
	for _, object := range listed {
		info, err := client.S3().StatObject(ctx, req.Bucket, object.Key, minio.StatObjectOptions{})
		if err != nil {
			return nil, fmt.Errorf("unable to read metadata for object %q: %w", object.Key, err)
		}
		result.Objects = append(result.Objects, &pb.BrowserObject{
			Key:                object.Key,
			Size:               uint64(object.Size),
			LastModified:       timestamppb.New(object.LastModified),
			SourceLastModified: info.Metadata.Get("x-amz-meta-chorus-source-last-modified"),
		})
	}
	if hasMore && len(listed) > 0 {
		result.NextAfter = listed[len(listed)-1].Key
	}
	return result, nil
}

func (h *chorusHandlers) browserClient(ctx context.Context, storage, user string) (s3client.Client, error) {
	if storage == "" || user == "" {
		return nil, fmt.Errorf("%w: storage and user are required", dom.ErrInvalidArg)
	}
	storageType, ok := h.credsSvc.Storages()[storage]
	if !ok {
		return nil, fmt.Errorf("%w: storage %q not found", dom.ErrNotFound, storage)
	}
	if storageType != dom.S3 {
		return nil, fmt.Errorf("%w: object browser only supports S3 storage", dom.ErrInvalidArg)
	}
	return h.clients.AsS3(ctx, storage, user)
}

func (h *chorusHandlers) GetProxyCredentials(ctx context.Context, _ *emptypb.Empty) (*pb.GetProxyCredentialsResponse, error) {
	return h.proxyClient.GetCredentials(ctx)
}

func (h *chorusHandlers) SetUserCredentials(ctx context.Context, req *pb.SetUserCredentialsRequest) (*emptypb.Empty, error) {
	if req.Storage == "" {
		return nil, fmt.Errorf("%w: storage is required", dom.ErrInvalidArg)
	}
	if req.User == "" {
		return nil, fmt.Errorf("%w: user is required", dom.ErrInvalidArg)
	}
	storType, ok := h.credsSvc.Storages()[req.Storage]
	if !ok {
		return nil, fmt.Errorf("%w: storage %s not found", dom.ErrNotFound, req.Storage)
	}
	switch storType {
	case dom.S3:
		if req.S3Cred == nil {
			return nil, fmt.Errorf("%w: s3 credentials are required for storage type S3", dom.ErrInvalidArg)
		}
		err := h.credsSvc.SetS3Credentials(ctx, req.Storage, req.User, s3.CredentialsV4{
			AccessKeyID:     req.S3Cred.AccessKey,
			SecretAccessKey: req.S3Cred.SecretKey,
		})
		if err != nil {
			return nil, err
		}
		return &emptypb.Empty{}, nil
	case dom.Swift:
		if req.SwiftCred == nil {
			return nil, fmt.Errorf("%w: swift credentials are required for storage type Swift", dom.ErrInvalidArg)
		}
		err := h.credsSvc.SetSwiftCredentials(ctx, req.Storage, req.User, swift.Credentials{
			Username:   req.SwiftCred.Username,
			Password:   req.SwiftCred.Password,
			DomainName: req.SwiftCred.DomainName,
			TenantName: req.SwiftCred.TenantName,
		})
		if err != nil {
			return nil, err
		}
		return &emptypb.Empty{}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported storage type %s", dom.ErrInvalidArg, storType)
	}
}
