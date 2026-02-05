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

package gen

import (
	"context"
	"fmt"
	"io"
	"slices"

	"github.com/minio/minio-go/v7"
)

func GenerateS3SafeCharacters() []rune {
	return GenerateCommonSafeCharacters()
}

func GenerateS3SpecialHandlingCharacters() []rune {
	runes := []rune{}
	for i := 0; i < 31; i++ {
		runes = append(runes, rune(i))
	}
	runes = append(runes, rune(127))
	for _, ch := range "&$@=;/:+,?" {
		runes = append(runes, ch)
	}
	return runes
}

func GenerateS3AvoidCharacters() []rune {
	runes := []rune{}
	for i := 128; i < 255; i++ {
		runes = append(runes, rune(i))
	}
	for _, ch := range "\\{}^%[]`\"<>~#|" {
		runes = append(runes, ch)
	}
	return runes
}

type S3Filler struct {
	tree   *Tree[*GeneratedObject]
	client *minio.Client
}

func NewS3Filler(tree *Tree[*GeneratedObject], client *minio.Client) *S3Filler {
	return &S3Filler{
		tree:   tree,
		client: client,
	}
}

func (r *S3Filler) putObject(ctx context.Context, bucket string, name string, reader io.Reader, len uint64) error {
	if _, err := r.client.PutObject(ctx, bucket, name, reader, int64(len), minio.PutObjectOptions{}); err != nil {
		return fmt.Errorf("unable to upload object: %w", err)
	}
	return nil
}

func (r *S3Filler) FillBucket(ctx context.Context, bucket string) error {
	for item := range r.tree.DepthFirstValueIterator().Must() {
		for _, reader := range item.ContentReaderIterator() {
			if err := r.putObject(ctx, bucket, item.fullPath, reader, reader.Len()); err != nil {
				return fmt.Errorf("unable to upload object: %w", err)
			}
		}
	}

	return nil
}

func (r *S3Filler) FillBucketWithLastVersions(ctx context.Context, bucket string) error {
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

type S3Validator struct {
	tree   *Tree[*GeneratedObject]
	client *minio.Client
}

func NewS3Validator(tree *Tree[*GeneratedObject], client *minio.Client) *S3Validator {
	return &S3Validator{
		tree:   tree,
		client: client,
	}
}

func (r *S3Validator) Validator(ctx context.Context, bucket string) error {
	for item := range r.tree.DepthFirstValueIterator().Must() {
		versions := []string{}
		objectList := r.client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
			WithVersions: true,
			Prefix:       item.fullPath,
		})
		for object := range objectList {
			versions = append(versions, object.VersionID)
		}

		slices.Reverse(versions)

		for idx, reader := range item.ContentReaderIterator() {
			object, err := r.client.GetObject(ctx, bucket, item.fullPath, minio.GetObjectOptions{
				VersionID: versions[idx],
			})
			if err != nil {
				return fmt.Errorf("unable to get object: %w", err)
			}
			defer object.Close()

			if !readersHaveSameContent(reader, object) {
				return fmt.Errorf("object %s version %s has different content", item.fullPath, versions[idx])
			}
		}
	}

	return nil
}
