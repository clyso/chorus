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

	"github.com/clyso/chorus/pkg/s3client"
)

func wrapS3common(client s3client.Client) Common {
	return &s3CommonClient{
		client: client,
	}
}

type s3CommonClient struct {
	client s3client.Client
}

func (s *s3CommonClient) BucketExists(ctx context.Context, bucket string) (bool, error) {
	return s.client.S3().BucketExists(ctx, bucket)
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
