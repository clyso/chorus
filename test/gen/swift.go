// Copyright 2026 Clyso GmbH
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

package gen

import (
	"context"
	"fmt"
	"io"
	"slices"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
	"github.com/gophercloud/gophercloud/v2/pagination"
)

type SwiftFiller struct {
	tree   *Tree[*GeneratedObject]
	client *gophercloud.ServiceClient
}

func NewSwiftFiller(tree *Tree[*GeneratedObject], client *gophercloud.ServiceClient) *SwiftFiller {
	return &SwiftFiller{
		tree:   tree,
		client: client,
	}
}
func (r *SwiftFiller) putObject(ctx context.Context, bucket string, name string, reader io.Reader, len uint64) error {
	if _, err := objects.Create(ctx, r.client, bucket, name, objects.CreateOpts{
		Content:       reader,
		ContentLength: int64(len),
	}).Extract(); err != nil {
		return fmt.Errorf("unable to upload object: %w", err)
	}
	return nil
}

func (r *SwiftFiller) FillBucket(ctx context.Context, bucket string) error {
	for item := range r.tree.DepthFirstValueIterator().Must() {
		for _, reader := range item.ContentReaderIterator() {
			if err := r.putObject(ctx, bucket, item.fullPath, reader, reader.Len()); err != nil {
				return fmt.Errorf("unable to upload object: %w", err)
			}
			_, err := objects.Create(ctx, r.client, bucket, item.fullPath, objects.CreateOpts{
				Content:       reader,
				ContentLength: int64(reader.Len()),
			}).Extract()
			if err != nil {
				return fmt.Errorf("unable to upload object: %w", err)
			}
		}
	}

	return nil
}

func (r *SwiftFiller) FillBucketWithLastVersions(ctx context.Context, bucket string) error {
	for item := range r.tree.DepthFirstValueIterator().Must() {
		if item.GetVersionCount() == 0 {
			continue
		}
		reader := item.GetLastVersionContentReader()
		if err := r.putObject(ctx, bucket, item.fullPath, reader, reader.Len()); err != nil {
			return fmt.Errorf("unable to upload object: %w", err)
		}
	}

	return nil
}

type SwiftValidator struct {
	tree   *Tree[*GeneratedObject]
	client *gophercloud.ServiceClient
}

func NewSwiftValidator(tree *Tree[*GeneratedObject], client *gophercloud.ServiceClient) *SwiftValidator {
	return &SwiftValidator{
		tree:   tree,
		client: client,
	}
}

func (r *SwiftValidator) Validator(ctx context.Context, bucket string) error {
	for item := range r.tree.DepthFirstValueIterator().Must() {
		versions := []string{}
		pager := objects.List(r.client, bucket, objects.ListOpts{
			Versions: true,
			Prefix:   item.fullPath,
		})

		err := pager.EachPage(ctx, func(ctx context.Context, p pagination.Page) (bool, error) {
			objects, err := objects.ExtractInfo(p)
			if err != nil {
				return false, fmt.Errorf("unable to extract info: %w", err)
			}
			for _, object := range objects {
				versions = append(versions, object.VersionID)
			}
			return true, nil
		})
		if err != nil {
			return fmt.Errorf("unable to get object page: %w", err)
		}

		slices.Reverse(versions)

		for idx, reader := range item.ContentReaderIterator() {
			downloader := objects.Download(ctx, r.client, bucket, item.fullPath, objects.DownloadOpts{
				ObjectVersionID: versions[idx],
			})
			if downloader.Err != nil {
				return fmt.Errorf("unable to get object: %w", err)
			}
			defer downloader.Body.Close()

			if !readersHaveSameContent(reader, downloader.Body) {
				return fmt.Errorf("object %s version %s has different content", item.fullPath, versions[idx])
			}
		}
	}

	return nil
}
