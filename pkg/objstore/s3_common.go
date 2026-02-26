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
	"slices"
	"strings"

	"github.com/minio/minio-go/v7"

	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
)

func WrapS3common(client s3client.Client, provider s3.Provider) Common {
	return &s3CommonClient{
		client:   client,
		provider: provider,
	}
}

type s3CommonClient struct {
	client   s3client.Client
	provider s3.Provider
}

func (s *s3CommonClient) BucketExists(ctx context.Context, bucket string) (bool, error) {
	exists, err := s.client.S3().BucketExists(ctx, bucket)
	if err != nil {
		return false, fmt.Errorf("unable to check if bucket exists: %w", err)
	}
	return exists, nil
}

func (s *s3CommonClient) ListBuckets(ctx context.Context) ([]string, error) {
	res, err := s.client.S3().ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	buckets := make([]string, 0, len(res))
	for _, b := range res {
		buckets = append(buckets, b.Name)
	}
	return buckets, nil
}

func (s *s3CommonClient) IsBucketVersioned(ctx context.Context, bucket string) (bool, error) {
	versioningConfig, err := s.client.S3().GetBucketVersioning(ctx, bucket)
	if err != nil {
		return false, fmt.Errorf("unable to get storage versioning config: %w", err)
	}

	return versioningConfig.Enabled(), nil
}

func (s *s3CommonClient) CreateBucket(ctx context.Context, bucket string) error {
	if err := s.client.S3().MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		return fmt.Errorf("unable to create bucket: %w", err)
	}
	return nil
}

func (s *s3CommonClient) RemoveBucket(ctx context.Context, bucket string) error {
	if err := s.client.S3().RemoveBucket(ctx, bucket); err != nil {
		return fmt.Errorf("unable to remove bucket: %w", err)
	}
	return nil
}

func (s *s3CommonClient) EnableBucketVersioning(ctx context.Context, bucket string) error {
	if err := s.client.S3().EnableVersioning(ctx, bucket); err != nil {
		return fmt.Errorf("unable to enable versioning: %w", err)
	}
	return nil
}

func (s *s3CommonClient) ListObjects(ctx context.Context, bucket string, opts ...func(o *commonListOptions)) iter.Seq2[CommonObjectInfo, error] {
	commonOpts := &commonListOptions{}

	for _, opt := range opts {
		opt(commonOpts)
	}

	minioOpts := commonOpts.toMinioListOptions()
	minioOpts.Recursive = true
	minioOpts.Prefix = s.normalizeInputName(minioOpts.Prefix)

	objects := s.client.S3().ListObjects(ctx, bucket, minioOpts)
	return func(yield func(CommonObjectInfo, error) bool) {
		for object := range objects {
			err := object.Err
			if err != nil {
				minioErr := &minio.ErrorResponse{}
				if !errors.As(err, minioErr) {
					yield(CommonObjectInfo{}, fmt.Errorf("unable to list objects for bucket %s: %w", bucket, err))
					return
				}

				switch minioErr.Code {
				case minio.NoSuchBucket:
					yield(CommonObjectInfo{}, ErrBucketDoesntExist)
				default:
					yield(CommonObjectInfo{}, fmt.Errorf("unable to list objects for bucket %s: %w", bucket, err))
				}
				return
			}
			info := CommonObjectInfo{
				Key:          s.normalizeOutputName(object.Key),
				VersionID:    object.VersionID,
				LastModified: object.LastModified,
				Etag:         object.ETag,
				Size:         uint64(object.Size),
			}
			if !yield(info, nil) {
				return
			}
		}
	}
}

func (s *s3CommonClient) PutObject(ctx context.Context, bucket string, name string, reader io.Reader, len uint64) error {
	if _, err := s.client.S3().PutObject(ctx, bucket, name, reader, int64(len), minio.PutObjectOptions{}); err != nil {
		return fmt.Errorf("unable to upload object: %w", err)
	}
	return nil
}

func (s *s3CommonClient) ObjectExists(ctx context.Context, bucket string, name string, opts ...func(o *commonObjectOptions)) (bool, error) {
	commonOpts := &commonObjectOptions{}

	for _, opt := range opts {
		opt(commonOpts)
	}

	_, err := s.client.S3().StatObject(ctx, bucket, name, minio.StatObjectOptions{
		VersionID: commonOpts.versionID,
	})
	if err == nil {
		return true, nil
	}

	minioErr := minio.ErrorResponse{}
	if errors.As(err, &minioErr) && (minioErr.Code == minio.NoSuchKey || minioErr.Code == minio.NoSuchVersion) {
		return false, nil
	}

	return false, fmt.Errorf("unable to stat object: %w", err)
}

func (s *s3CommonClient) RemoveObject(ctx context.Context, bucket string, name string, opts ...func(o *commonObjectOptions)) error {
	commonOpts := &commonObjectOptions{}

	for _, opt := range opts {
		opt(commonOpts)
	}

	if err := s.client.S3().RemoveObject(ctx, bucket, name, minio.RemoveObjectOptions{
		VersionID: commonOpts.versionID,
	}); err != nil {
		return fmt.Errorf("unable delete object: %w", err)
	}
	return nil
}

func (s *s3CommonClient) RemoveObjects(ctx context.Context, bucket string, names []string) error {
	objectInfos := make([]minio.ObjectInfo, 0, len(names))
	for _, name := range names {
		objectInfos = append(objectInfos, minio.ObjectInfo{
			Key: name,
		})
	}
	infoIter := slices.Values(objectInfos)
	resultIter, err := s.client.S3().RemoveObjectsWithIter(ctx, bucket, infoIter, minio.RemoveObjectsOptions{})
	if err != nil {
		return fmt.Errorf("unable delete objects: %w", err)
	}
	for result := range resultIter {
		if result.Err != nil {
			return fmt.Errorf("unable delete object %s: %w", result.ObjectName, err)
		}
	}
	return nil
}

func (s *s3CommonClient) ObjectInfo(ctx context.Context, bucket string, name string, opts ...func(o *commonObjectOptions)) (*CommonObjectInfo, error) {
	commonOpts := &commonObjectOptions{}

	for _, opt := range opts {
		opt(commonOpts)
	}

	stat, err := s.client.S3().StatObject(ctx, bucket, name, minio.StatObjectOptions{
		VersionID: commonOpts.versionID,
	})
	if err != nil {
		return nil, fmt.Errorf("unabe to stat object: %w", err)
	}
	return &CommonObjectInfo{
		LastModified: stat.LastModified,
		Key:          stat.Key,
		VersionID:    stat.VersionID,
		Etag:         stat.ETag,
		Size:         uint64(stat.Size),
	}, nil
}

// https://github.com/minio/minio/issues/17356
// While working with lists, minio will output object keys as a/b/c, i.e, without leading slash.
// It will also consider a/b as a valid value for prefix search parameter.
// Prefix value /a/b will be considered invalid, search will return no results.
// At the same time, other implementations, e.g. Ceph, will act the other way.
// Meaning object keys will be listed as /a/b/c, with leading slash.
// List operation with prefix /a/b will give results, with prefix a/b won't.
// Therefore, in order to allow interoperability between providers, we should brind prefix parameter
// and object keys in list to the expected format.
func (s *s3CommonClient) normalizeInputName(name string) string {
	if name == "" {
		return name
	}
	isMinio := s.provider == s3.ProviderMinIO
	hasLeadingSlash := strings.HasPrefix(name, "/")
	if isMinio && hasLeadingSlash {
		return strings.TrimPrefix(name, "/")
	}
	if !isMinio && !hasLeadingSlash {
		return "/" + name
	}
	return name
}

func (s *s3CommonClient) normalizeOutputName(name string) string {
	if name == "" {
		return name
	}
	if !strings.HasPrefix(name, "/") {
		return "/" + name
	}
	return name
}
