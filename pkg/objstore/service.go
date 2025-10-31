// Copyright 2025 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objstore

import (
	"context"
	"fmt"

	"github.com/gophercloud/gophercloud/v2"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/swift"
)

type Config = StoragesConfig[*s3.Storage, *swift.Storage]
type Storage = GenericStorage[*s3.Storage, *swift.Storage]

type Clients interface {
	Config() Config
	AsS3(ctx context.Context, storage, user string) (s3client.Client, error)
	AsSwift(ctx context.Context, storage, user string) (*gophercloud.ServiceClient, error)
	commonGetter
}

type commonGetter interface {
	AsCommon(ctx context.Context, storage string, user string) (Common, error)
}

// Common - common object storage client interface
type Common interface {
	BucketExists(ctx context.Context, bucket string) (bool, error)
	ListBuckets(ctx context.Context) ([]string, error)
}

func New(conf Config, providers map[dom.StorageType]any) (*clients, error) {
	common := make(map[dom.StorageType]commonGetter, len(providers))
	if len(providers) == 0 {
		return nil, fmt.Errorf("%w: storage registry providers are empty", dom.ErrInvalidArg)
	}
	// check that all storage types from config are presented in providers
	for _, storConf := range conf.Storages {
		if _, ok := providers[storConf.Type]; !ok {
			return nil, fmt.Errorf("%w: storage provider is not presented for storage type %q", dom.ErrInvalidArg, storConf.Type)
		}
	}
	// create common client wrappers for each provider
	for storType, provider := range providers {
		switch storType {
		case dom.S3:
			s3, ok := provider.(s3client.Service)
			if !ok {
				return nil, fmt.Errorf("%w: S3 provider does not implement s3client.Service interface", dom.ErrInvalidArg)
			}
			common[storType] = wrapS3Common(s3)
		case dom.Swift:
			swiftClient, ok := provider.(swift.Client)
			if !ok {
				return nil, fmt.Errorf("%w: Swift provider does not implement swift.Client interface", dom.ErrInvalidArg)
			}
			common[storType] = wrapSwiftCommon(swiftClient)
		default:
			return nil, fmt.Errorf("%w: unknown storage provider type %q", dom.ErrInvalidArg, storType)
		}
	}
	return &clients{
		conf:   conf,
		common: common,
		raw:    providers,
	}, nil
}

var _ Clients = (*clients)(nil)

type clients struct {
	raw    map[dom.StorageType]any
	common map[dom.StorageType]commonGetter
	conf   Config
}

func (r *clients) AsS3(ctx context.Context, storage, user string) (s3client.Client, error) {
	provider, ok := r.raw[dom.S3]
	if !ok {
		return nil, fmt.Errorf("%w: S3 provider is not registered", dom.ErrInvalidArg)
	}
	s3, ok := provider.(s3client.Service)
	if !ok {
		return nil, fmt.Errorf("%w: S3 provider does not implement s3client.Service interface", dom.ErrInvalidArg)
	}
	return s3.GetByName(ctx, user, storage)
}

func (r *clients) AsSwift(ctx context.Context, storage string, user string) (*gophercloud.ServiceClient, error) {
	provider, ok := r.raw[dom.Swift]
	if !ok {
		return nil, fmt.Errorf("%w: Swift provider is not registered", dom.ErrInvalidArg)
	}
	swiftProvider, ok := provider.(swift.Client)
	if !ok {
		return nil, fmt.Errorf("%w: Swift provider does not implement swift.Client interface", dom.ErrInvalidArg)
	}
	return swiftProvider.For(ctx, storage, user)
}

func (r *clients) AsCommon(ctx context.Context, storage string, user string) (Common, error) {
	conf, ok := r.conf.Storages[storage]
	if !ok {
		return nil, fmt.Errorf("%w: storage %q not found in registry", dom.ErrInvalidArg, storage)
	}
	provider, ok := r.common[conf.Type]
	if !ok {
		return nil, fmt.Errorf("%w: storage provider for type %q not found in registry", dom.ErrInvalidArg, conf.Type)
	}
	return provider.AsCommon(ctx, storage, user)
}

func (r *clients) Config() Config {
	return r.conf
}
