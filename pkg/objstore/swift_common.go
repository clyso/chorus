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
	"io"
	"iter"
	"net/http"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/containers"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
	"github.com/gophercloud/gophercloud/v2/pagination"
)

func WrapSwiftCommon(client *gophercloud.ServiceClient) Common {
	return &swiftCommonClient{
		client: client,
	}
}

type swiftCommonClient struct {
	client *gophercloud.ServiceClient
}

func (s *swiftCommonClient) BucketExists(ctx context.Context, bucket string) (bool, error) {
	err := containers.Get(ctx, s.client, bucket, containers.GetOpts{}).Err
	if gophercloud.ResponseCodeIs(err, http.StatusNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("unable to check if bucket exists: %w", err)
	}
	return true, nil
}

func (s *swiftCommonClient) ListBuckets(ctx context.Context) ([]string, error) {
	res := []string{}
	const pageSize = 1000
	pager := containers.List(s.client, containers.ListOpts{
		Limit: pageSize,
	})
	err := pager.EachPage(ctx, func(ctx context.Context, page pagination.Page) (bool, error) {
		containerList, err := containers.ExtractNames(page)
		if err != nil {
			return false, err
		}
		res = append(res, containerList...)
		// Continue to the next page
		return len(containerList) == pageSize, nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *swiftCommonClient) IsBucketVersioned(ctx context.Context, bucket string) (bool, error) {
	getResult, err := containers.Get(ctx, s.client, bucket, containers.GetOpts{}).Extract()
	if err != nil {
		return false, fmt.Errorf("unable to get container: %w", err)
	}

	return getResult.VersionsEnabled, nil
}

func (s *swiftCommonClient) CreateBucket(ctx context.Context, bucket string) error {
	if _, err := containers.Create(ctx, s.client, bucket, containers.CreateOpts{}).Extract(); err != nil {
		return fmt.Errorf("unable to create bucket: %w", err)
	}
	return nil
}

func (s *swiftCommonClient) EnableBucketVersioning(ctx context.Context, bucket string) error {
	enabled := true
	if _, err := containers.Update(ctx, s.client, bucket, containers.UpdateOpts{VersionsEnabled: &enabled}).Extract(); err != nil {
		return fmt.Errorf("unable to enable versioning: %w", err)
	}
	return nil
}

func (s *swiftCommonClient) RemoveBucket(ctx context.Context, bucket string) error {
	if _, err := containers.Delete(ctx, s.client, bucket).Extract(); err != nil {
		return fmt.Errorf("unable to delete bucket: %w", err)
	}
	return nil
}

func (s *swiftCommonClient) ListObjects(ctx context.Context, bucket string, opts ...func(o *commonListOptions)) iter.Seq2[CommonObjectInfo, error] {
	commonOpts := &commonListOptions{}

	for _, opt := range opts {
		opt(commonOpts)
	}

	swiftOpts := commonOpts.toSwiftListOptions()

	pager := objects.List(s.client, bucket, swiftOpts)

	return func(yield func(CommonObjectInfo, error) bool) {
		err := pager.EachPage(ctx, func(ctx context.Context, page pagination.Page) (bool, error) {
			objectList, err := objects.ExtractInfo(page)
			if gophercloud.ResponseCodeIs(err, http.StatusNotFound) {
				return false, ErrBucketDoesntExist
			}
			if err != nil {
				return false, fmt.Errorf("unable to extract info: %w", err)
			}
			// https://docs.openstack.org/swift/latest/middleware.html#module-swift.common.middleware.versioned_writes.object_versioning
			// For versioned buckets (with X-Versions-Enabled), when objects are listed with versioned=true,
			// for each listed object, a symlinks is listed alongside as version as well.
			// This does not apply to "folders".
			// Meaning the structure
			// /a/
			//  |- b - having 2 versions
			//	|- c - having 2 versions
			// will produce 9 records:
			// 4 for object b (2 objects, 2 symlinks)
			// 4 for object c (2 objects, 2 symlinks)
			// 1 for "folder" a
			// Object version list will look as follows
			// - symlinks record marked as latest
			// - actual object
			// - symlinks record
			// - actual object
			// - ...
			// Symlinks will have size 0 and md5 checksum d41d8cd98f00b204e9800998ecf8427e

			for _, object := range objectList {
				objectInfo := CommonObjectInfo{
					LastModified: object.LastModified,
					Key:          object.Name,
					VersionID:    object.VersionID,
					Etag:         object.Hash,
					Size:         uint64(object.Bytes),
				}
				if !yield(objectInfo, nil) {
					return false, nil
				}
			}
			return true, nil
		})

		if err != nil {
			yield(CommonObjectInfo{}, fmt.Errorf("unable iterate over pages: %w", err))
		}
	}
}

func (s *swiftCommonClient) PutObject(ctx context.Context, bucket string, name string, reader io.Reader, len uint64) error {
	if _, err := objects.Create(ctx, s.client, bucket, name, objects.CreateOpts{
		Content:       reader,
		ContentLength: int64(len),
	}).Extract(); err != nil {
		return fmt.Errorf("unable to put object: %w", err)
	}
	return nil
}

func (s *swiftCommonClient) ObjectExists(ctx context.Context, bucket string, name string, opts ...func(o *commonObjectOptions)) (bool, error) {
	commonOpts := &commonObjectOptions{}

	for _, opt := range opts {
		opt(commonOpts)
	}

	_, err := objects.Get(ctx, s.client, bucket, name, objects.GetOpts{
		ObjectVersionID: commonOpts.versionID,
	}).Extract()
	if err == nil {
		return true, nil
	}
	if gophercloud.ResponseCodeIs(err, http.StatusNotFound) {
		return false, nil
	}
	return false, fmt.Errorf("unable to get object: %w", err)
}

func (s *swiftCommonClient) ObjectInfo(ctx context.Context, bucket string, name string, opts ...func(o *commonObjectOptions)) (*CommonObjectInfo, error) {
	commonOpts := &commonObjectOptions{}

	for _, opt := range opts {
		opt(commonOpts)
	}

	info, err := objects.Get(ctx, s.client, bucket, name, objects.GetOpts{
		ObjectVersionID: commonOpts.versionID,
	}).Extract()
	if err != nil {
		return nil, fmt.Errorf("unable to get object: %w", err)
	}
	return &CommonObjectInfo{
		LastModified: info.LastModified,
		Key:          name,
		VersionID:    info.ObjectVersionID,
		Etag:         info.ETag,
		Size:         uint64(info.ContentLength),
	}, nil
}

func (s *swiftCommonClient) RemoveObject(ctx context.Context, bucket string, name string, opts ...func(o *commonObjectOptions)) error {
	commonOpts := &commonObjectOptions{}

	for _, opt := range opts {
		opt(commonOpts)
	}

	if _, err := objects.Delete(ctx, s.client, bucket, name, objects.DeleteOpts{
		ObjectVersionID: commonOpts.versionID,
	}).Extract(); err != nil {
		return fmt.Errorf("unable to remove object: %w", err)
	}
	return nil
}

func (s *swiftCommonClient) RemoveObjects(ctx context.Context, bucket string, names []string) error {
	resp, err := objects.BulkDelete(ctx, s.client, bucket, names).Extract()
	if err != nil {
		return fmt.Errorf("unable remove objects: %w", err)
	}
	if len(resp.Errors) > 0 {
		return fmt.Errorf("unable to delete objects: %+v", resp.Errors)
	}
	return nil
}
