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
	"errors"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/s3"
)

func newAWSClient(conf s3.Storage, name, user string, metricsSvc metrics.Service) (*AWS, error) {

	cred := credentials.NewCredentials(&credentials.StaticProvider{Value: credentials.Value{
		AccessKeyID:     conf.Credentials[user].AccessKeyID,
		SecretAccessKey: conf.Credentials[user].SecretAccessKey,
	}})

	endpoint := conf.Address
	if !strings.HasPrefix(endpoint, "http") {
		if conf.IsSecure {
			endpoint = "https://" + endpoint
		} else {
			endpoint = "http://" + endpoint
		}
	}

	awsConfig := aws.NewConfig().
		WithMaxRetries(3).
		WithCredentials(cred).
		WithHTTPClient(&http.Client{Timeout: conf.HttpTimeout}).
		WithS3ForcePathStyle(true).
		WithDisableSSL(!conf.IsSecure).
		WithEndpoint(endpoint).
		WithRegion("us-east-1").
		WithS3UsEast1RegionalEndpoint(endpoints.RegionalS3UsEast1Endpoint)

	ses, err := session.NewSessionWithOptions(session.Options{
		Config: *awsConfig,
	})
	if err != nil {
		return nil, err
	}

	return &AWS{
		S3:         aws_s3.New(ses),
		ses:        ses,
		metricsSvc: metricsSvc,
		name:       name,
		user:       user,
	}, nil
}

type AWS struct {
	*aws_s3.S3
	ses        *session.Session
	metricsSvc metrics.Service
	name       string
	user       string
}

func AwsErrRetry(err error) bool {
	if err == nil {
		return false
	}
	var aerr awserr.Error
	if ok := errors.As(err, &aerr); ok && aerr.Code() == request.CanceledErrorCode {
		return true
	}
	return request.IsErrorRetryable(err)
}

func (s *AWS) PutObjectAclWithContext(ctx aws.Context, input *aws_s3.PutObjectAclInput, opts ...request.Option) (*aws_s3.PutObjectAclOutput, error) {
	ctx, span := otel.Tracer("").Start(ctx, s3.PutObjectAcl.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.PutObjectAcl.String())
	return s.S3.PutObjectAclWithContext(ctx, input, opts...)
}

func (s *AWS) GetObjectAclWithContext(ctx aws.Context, input *aws_s3.GetObjectAclInput, opts ...request.Option) (*aws_s3.GetObjectAclOutput, error) {
	ctx, span := otel.Tracer("").Start(ctx, s3.GetObjectAcl.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.GetObjectAcl.String())
	return s.S3.GetObjectAclWithContext(ctx, input, opts...)
}

func (s *AWS) GetBucketAclWithContext(ctx aws.Context, input *aws_s3.GetBucketAclInput, opts ...request.Option) (*aws_s3.GetBucketAclOutput, error) {
	ctx, span := otel.Tracer("").Start(ctx, s3.GetBucketAcl.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.GetBucketAcl.String())
	return s.S3.GetBucketAclWithContext(ctx, input, opts...)
}

func (s *AWS) PutBucketAclWithContext(ctx aws.Context, input *aws_s3.PutBucketAclInput, opts ...request.Option) (*aws_s3.PutBucketAclOutput, error) {
	ctx, span := otel.Tracer("").Start(ctx, s3.PutBucketAcl.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.PutBucketAcl.String())
	return s.S3.PutBucketAclWithContext(ctx, input, opts...)
}

func (s *AWS) HeadBucketWithContext(ctx aws.Context, input *aws_s3.HeadBucketInput, opts ...request.Option) (*aws_s3.HeadBucketOutput, error) {
	ctx, span := otel.Tracer("").Start(ctx, s3.HeadBucket.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.HeadBucket.String())
	return s.S3.HeadBucketWithContext(ctx, input, opts...)
}

func (s *AWS) CreateBucketWithContext(ctx aws.Context, input *aws_s3.CreateBucketInput, opts ...request.Option) (*aws_s3.CreateBucketOutput, error) {
	ctx, span := otel.Tracer("").Start(ctx, s3.CreateBucket.String())
	span.SetAttributes(attribute.String("storage", s.name), attribute.String("user", s.user))
	defer span.End()
	defer s.metricsSvc.Count(xctx.GetFlow(ctx), s.name, s3.CreateBucket.String())
	return s.S3.CreateBucketWithContext(ctx, input, opts...)
}
