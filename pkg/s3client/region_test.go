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
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"

	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/s3"
)

func TestSigningRegion(t *testing.T) {
	if got := signingRegion(s3.StorageAddress{}); got != "us-east-1" {
		t.Errorf("empty default region: got %q, want us-east-1", got)
	}
	if got := signingRegion(s3.StorageAddress{DefaultRegion: "ru-central-1"}); got != "ru-central-1" {
		t.Errorf("configured default region: got %q, want ru-central-1", got)
	}
}

func TestAWSClientUsesDefaultRegion(t *testing.T) {
	c, err := newAWSClient(s3.StorageAddress{Address: "s3.example.com", IsSecure: true, DefaultRegion: "ru-central-1"},
		s3.CredentialsV4{AccessKeyID: "tenant:key", SecretAccessKey: "secret"}, "storage", "user", metrics.NewS3Service(false))
	if err != nil {
		t.Fatal(err)
	}
	if got := *c.S3.Config.Region; got != "ru-central-1" {
		t.Errorf("aws client region: got %q, want ru-central-1", got)
	}
	req, _ := c.HeadBucketRequest(&aws_s3.HeadBucketInput{Bucket: aws.String("bucket")})
	if err := req.Sign(); err != nil {
		t.Fatal(err)
	}
	if auth := req.HTTPRequest.Header.Get("Authorization"); !strings.Contains(auth, "/ru-central-1/s3/aws4_request") {
		t.Errorf("signature scope does not use the default region: %s", auth)
	}
}
