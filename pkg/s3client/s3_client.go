/*
 * Copyright © 2023 Clyso GmbH
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

package s3client

import (
	"context"

	mclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
	"github.com/minio/minio-go/v7/pkg/tags"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/s3"
)

func newMinioClient(name, user string, c *mclient.Client, metricsSvc metrics.S3Service) *S3 {
	return &S3{c, metricsSvc, name, user}
}

type S3 struct {
	*mclient.Client
	metricsSvc metrics.S3Service
	name       string
	user       string
}

func (s *S3) MakeBucket(ctx context.Context, bucketName string, opts mclient.MakeBucketOptions) (err error) {
	ctx, span := otel.Tracer("").Start(ctx, s3.CreateBucket.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.CreateBucket)
	return s.Client.MakeBucket(ctx, bucketName, opts)
}

func (s *S3) RemoveBucket(ctx context.Context, bucketName string) error {
	ctx, span := otel.Tracer("").Start(ctx, s3.DeleteBucket.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.DeleteBucket)
	return s.Client.RemoveBucket(ctx, bucketName)
}

func (s *S3) RemoveObjects(ctx context.Context, bucketName string, objectsCh <-chan mclient.ObjectInfo, opts mclient.RemoveObjectsOptions) <-chan mclient.RemoveObjectError {
	ctx, span := otel.Tracer("").Start(ctx, s3.DeleteObjects.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.DeleteObjects)
	return s.Client.RemoveObjects(ctx, bucketName, objectsCh, opts)
}

func (s *S3) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	ctx, span := otel.Tracer("").Start(ctx, s3.HeadBucket.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.HeadBucket)
	return s.Client.BucketExists(ctx, bucketName)
}

func (s *S3) ListObjects(ctx context.Context, bucketName string, opts mclient.ListObjectsOptions) <-chan mclient.ObjectInfo {
	ctx, span := otel.Tracer("").Start(ctx, s3.ListObjects.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.ListObjects)
	return s.Client.ListObjects(ctx, bucketName, opts)
}

func (s *S3) ListBuckets(ctx context.Context) ([]mclient.BucketInfo, error) {
	ctx, span := otel.Tracer("").Start(ctx, s3.ListBuckets.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.ListBuckets)
	return s.Client.ListBuckets(ctx)
}

func (s *S3) GetBucketLifecycle(ctx context.Context, bucketName string) (*lifecycle.Configuration, error) {
	ctx, span := otel.Tracer("").Start(ctx, s3.GetBucketLifecycle.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.GetBucketLifecycle)
	return s.Client.GetBucketLifecycle(ctx, bucketName)
}

func (s *S3) SetBucketLifecycle(ctx context.Context, bucketName string, config *lifecycle.Configuration) error {
	ctx, span := otel.Tracer("").Start(ctx, s3.PutBucketLifecycle.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.PutBucketLifecycle)
	return s.Client.SetBucketLifecycle(ctx, bucketName, config)
}
func (s *S3) GetBucketPolicy(ctx context.Context, bucketName string) (string, error) {
	ctx, span := otel.Tracer("").Start(ctx, s3.GetBucketPolicy.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.GetBucketPolicy)
	return s.Client.GetBucketPolicy(ctx, bucketName)
}
func (s *S3) SetBucketPolicy(ctx context.Context, bucketName, policy string) error {
	ctx, span := otel.Tracer("").Start(ctx, s3.PutBucketPolicy.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.PutBucketPolicy)
	return s.Client.SetBucketPolicy(ctx, bucketName, policy)
}

func (s *S3) GetBucketTagging(ctx context.Context, bucketName string) (*tags.Tags, error) {
	ctx, span := otel.Tracer("").Start(ctx, s3.GetBucketTagging.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.GetBucketTagging)
	return s.Client.GetBucketTagging(ctx, bucketName)
}

func (s *S3) SetBucketTagging(ctx context.Context, bucketName string, tags *tags.Tags) error {
	ctx, span := otel.Tracer("").Start(ctx, s3.PutBucketTagging.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.PutBucketTagging)
	return s.Client.SetBucketTagging(ctx, bucketName, tags)
}

func (s *S3) GetBucketVersioning(ctx context.Context, bucketName string) (mclient.BucketVersioningConfiguration, error) {
	ctx, span := otel.Tracer("").Start(ctx, s3.GetBucketVersioning.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.GetBucketVersioning)
	return s.Client.GetBucketVersioning(ctx, bucketName)
}

func (s *S3) SetBucketVersioning(ctx context.Context, bucketName string, config mclient.BucketVersioningConfiguration) error {
	ctx, span := otel.Tracer("").Start(ctx, s3.PutBucketVersioning.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.PutBucketVersioning)
	return s.Client.SetBucketVersioning(ctx, bucketName, config)
}

func (s *S3) GetObjectTagging(ctx context.Context, bucketName, objectName string, opts mclient.GetObjectTaggingOptions) (*tags.Tags, error) {
	ctx, span := otel.Tracer("").Start(ctx, s3.GetObjectTagging.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.GetObjectTagging)
	return s.Client.GetObjectTagging(ctx, bucketName, objectName, opts)
}

func (s *S3) PutObjectTagging(ctx context.Context, bucketName, objectName string, otags *tags.Tags, opts mclient.PutObjectTaggingOptions) error {
	ctx, span := otel.Tracer("").Start(ctx, s3.PutObjectTagging.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.PutObjectTagging)
	return s.Client.PutObjectTagging(ctx, bucketName, objectName, otags, opts)
}

func (s *S3) RemoveObject(ctx context.Context, bucketName, objectName string, opts mclient.RemoveObjectOptions) error {
	ctx, span := otel.Tracer("").Start(ctx, s3.DeleteObject.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.DeleteObject)
	return s.Client.RemoveObject(ctx, bucketName, objectName, opts)
}

func (s *S3) RemoveBucketTagging(ctx context.Context, bucketName string) error {
	ctx, span := otel.Tracer("").Start(ctx, s3.DeleteBucketTagging.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.DeleteBucketTagging)
	return s.Client.RemoveBucketTagging(ctx, bucketName)
}

func (s *S3) RemoveObjectTagging(ctx context.Context, bucketName, objectName string, opts mclient.RemoveObjectTaggingOptions) error {
	ctx, span := otel.Tracer("").Start(ctx, s3.DeleteObjectTagging.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.DeleteObjectTagging)
	return s.Client.RemoveObjectTagging(ctx, bucketName, objectName, opts)
}
