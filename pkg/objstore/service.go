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
	"errors"
	"fmt"
	"io"
	"iter"
	"sync"
	"time"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
	"github.com/minio/minio-go/v7"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/swift"
)

var (
	ErrBucketDoesntExist = errors.New("bucket doesn't exists")
)

type Config = StoragesConfig[*s3.Storage, *swift.Storage]
type Storage = GenericStorage[*s3.Storage, *swift.Storage]

type CommonObjectInfo struct {
	LastModified time.Time
	Key          string
	VersionID    string
	Etag         string
	Size         uint64
}

type commonListOptions struct {
	prefix    string
	after     string
	versioned bool
}

func (r *commonListOptions) toSwiftListOptions() objects.ListOpts {
	return objects.ListOpts{
		Prefix:   r.prefix,
		Marker:   r.after,
		Versions: r.versioned,
	}
}

func (r *commonListOptions) toMinioListOptions() minio.ListObjectsOptions {
	return minio.ListObjectsOptions{
		Prefix:       r.prefix,
		StartAfter:   r.after,
		WithVersions: r.versioned,
	}
}

func WithVersions() func(o *commonListOptions) {
	return func(o *commonListOptions) {
		o.versioned = true
	}
}

func WithPrefix(prefix string) func(o *commonListOptions) {
	return func(o *commonListOptions) {
		o.prefix = prefix
	}
}

func WithAfter(after string) func(o *commonListOptions) {
	return func(o *commonListOptions) {
		o.after = after
	}
}

type commonObjectOptions struct {
	versionID string
}

func WithVersionID(versionID string) func(o *commonObjectOptions) {
	return func(o *commonObjectOptions) {
		o.versionID = versionID
	}
}

type Clients interface {
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
	IsBucketVersioned(ctx context.Context, bucket string) (bool, error)
	ListBuckets(ctx context.Context) ([]string, error)
	CreateBucket(ctx context.Context, bucket string) error
	RemoveBucket(ctx context.Context, bucket string) error
	EnableBucketVersioning(ctx context.Context, bucket string) error
	PutObject(ctx context.Context, bucket string, name string, reader io.Reader, len uint64) error
	ObjectInfo(ctx context.Context, bucket string, name string, opts ...func(o *commonObjectOptions)) (*CommonObjectInfo, error)
	ObjectExists(ctx context.Context, bucket string, name string, opts ...func(o *commonObjectOptions)) (bool, error)
	RemoveObject(ctx context.Context, bucket string, name string, opts ...func(o *commonObjectOptions)) error
	RemoveObjects(ctx context.Context, bucket string, names []string) error
	ListObjects(ctx context.Context, bucket string, opts ...func(o *commonListOptions)) iter.Seq2[CommonObjectInfo, error]
}

// TODO:
// TODO-DC: todos for dynamic credentials support:
// - use DynamicCredeintilas Service everywhere instead of using Storage Config directly
// - use this Clients registry everywhere instead of using providers directly
// After done, make sure that:
// - Storage config is used only in this package. Other packages use only DynamicCredentials Service
// - Storage clients (S3|Swift) not instanitated outside of this package
// Then:
// - implement management API for dynamic credentials
// - add e2e tests
func NewRegistry(ctx context.Context, creds CredsService, metricsSvc metrics.Service) (*clients, error) {
	res := &clients{
		metricsSvc:   metricsSvc,
		credsSvc:     creds,
		s3Clients:    make(map[string]s3CacheVal),
		swiftClients: make(map[string]swiftCacheVal),
	}
	// pre-populate clients cache with existing storages and users
	storsages := creds.Storages()
	for storageName, storageType := range storsages {
		users := creds.ListUsers(storageName)
		for _, user := range users {
			switch storageType {
			case dom.S3:
				_, err := res.AsS3(ctx, storageName, user)
				if err != nil {
					return nil, fmt.Errorf("failed to init S3 client for storage %q user %q: %w", storageName, user, err)
				}
			case dom.Swift:
				_, err := res.AsSwift(ctx, storageName, user)
				if err != nil {
					return nil, fmt.Errorf("failed to init Swift client for storage %q user %q: %w", storageName, user, err)
				}
			default:
				return nil, fmt.Errorf("%w: unsupported storage type %q for storage %q", dom.ErrInvalidStorageConfig, storageType, storageName)
			}
		}
	}
	return res, nil
}

var _ Clients = (*clients)(nil)

type clients struct {
	metricsSvc metrics.Service

	credsSvc CredsService

	s3Clients    map[string]s3CacheVal
	swiftClients map[string]swiftCacheVal
	sync.RWMutex
}

type s3CacheVal struct {
	client s3client.Client
	cred   s3.CredentialsV4
}

type swiftCacheVal struct {
	client *gophercloud.ServiceClient
	cred   swift.Credentials
}

func (r *clients) AsS3(ctx context.Context, storage, user string) (s3client.Client, error) {
	creds, err := r.credsSvc.GetS3Credentials(storage, user)
	if err != nil {
		return nil, err
	}
	key := credsCacheKey(storage, user)
	// obtain read lock to check cache
	r.RLock()
	cacheVal, ok := r.s3Clients[key]
	r.RUnlock()
	if ok && cacheVal.cred == creds {
		// return cached client
		return cacheVal.client, nil
	}
	// not found in cache or creds changed - obtain write lock to create new client

	s3Addr, err := r.credsSvc.GetS3Address(storage)
	if err != nil {
		return nil, err
	}

	r.Lock()
	defer r.Unlock()
	// re-check cache after acquiring write lock
	cred, err := r.credsSvc.GetS3Credentials(storage, user)
	if err != nil {
		return nil, err
	}
	cacheVal, ok = r.s3Clients[key]
	if ok && cacheVal.cred == cred {
		return cacheVal.client, nil
	}
	// create new client
	client, err := s3client.NewClient(ctx, r.metricsSvc, s3Addr, cred, storage, user)
	if err != nil {
		return nil, err
	}
	r.s3Clients[key] = s3CacheVal{
		cred:   cred,
		client: client,
	}
	return client, nil
}

func (r *clients) AsSwift(ctx context.Context, storage string, user string) (*gophercloud.ServiceClient, error) {
	creds, err := r.credsSvc.GetSwiftCredentials(storage, user)
	if err != nil {
		return nil, err
	}
	key := credsCacheKey(storage, user)
	r.RLock()
	cacheVal, ok := r.swiftClients[key]
	r.RUnlock()
	if ok && cacheVal.cred == creds {
		return cacheVal.client, nil
	}

	swiftAddr, err := r.credsSvc.GetSwiftAddress(storage)
	if err != nil {
		return nil, err
	}

	r.Lock()
	defer r.Unlock()
	// re-check cache after acquiring write lock
	cred, err := r.credsSvc.GetSwiftCredentials(storage, user)
	if err != nil {
		return nil, err
	}
	cacheVal, ok = r.swiftClients[key]
	if ok && cacheVal.cred == cred {
		return cacheVal.client, nil
	}
	// create new client
	client, err := swift.NewClient(ctx, swiftAddr, cred)
	if err != nil {
		return nil, err
	}
	r.swiftClients[key] = swiftCacheVal{
		cred:   cred,
		client: client,
	}
	return client, nil
}

func (r *clients) AsCommon(ctx context.Context, storage string, user string) (Common, error) {
	storageType, ok := r.credsSvc.Storages()[storage]
	if !ok {
		return nil, fmt.Errorf("%w: unknown storage %q", dom.ErrInvalidArg, storage)
	}
	switch storageType {
	case dom.S3:
		client, err := r.AsS3(ctx, storage, user)
		if err != nil {
			return nil, err
		}
		storageAddress, err := r.credsSvc.GetS3Address(storage)
		if err != nil {
			return nil, err
		}
		return WrapS3common(client, storageAddress.Provider), nil
	case dom.Swift:
		client, err := r.AsSwift(ctx, storage, user)
		if err != nil {
			return nil, err
		}
		return WrapSwiftCommon(client), nil
	default:
		return nil, fmt.Errorf("%w: unsupported storage type %q for storage %q", dom.ErrInvalidStorageConfig, storageType, storage)
	}
}
