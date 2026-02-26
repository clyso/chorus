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

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/rpc"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/swift"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

func ChorusHandlers(
	credsSvc objstore.CredsService,
	proxyClient rpc.Proxy,
	appInfo *dom.AppInfo,
) pb.ChorusServer {
	return &chorusHandlers{
		credsSvc:    credsSvc,
		proxyClient: proxyClient,
		appInfo:     appInfo,
	}
}

var _ pb.ChorusServer = &chorusHandlers{}

type chorusHandlers struct {
	proxyClient rpc.Proxy
	appInfo     *dom.AppInfo
	credsSvc    objstore.CredsService
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
