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

	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/s3"
)

func newAWSClient(conf s3.Storage, name, user string, metricsSvc metrics.S3Service) (*AWS, error) {

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
	metricsSvc metrics.S3Service
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
