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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"

	mclient "github.com/minio/minio-go/v7"

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

func TestMinioClientUsesDefaultRegion(t *testing.T) {
	var mu sync.Mutex
	var reqs []*http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		reqs = append(reqs, r)
		mu.Unlock()
		if r.URL.Path == "/probe-health-test" || r.URL.Path == "/probe-health-test/" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", `"d41d8cd98f00b204e9800998ecf8427e"`)
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		w.Header().Set("Content-Length", "0")
	}))
	defer srv.Close()

	c, err := NewClient(context.Background(), metrics.NewS3Service(false),
		s3.StorageAddress{Address: strings.TrimPrefix(srv.URL, "http://"), DefaultRegion: "ru-central-1"},
		s3.CredentialsV4{AccessKeyID: "tenant:key", SecretAccessKey: "secret"}, "storage", "user")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.S3().StatObject(context.Background(), "bucket", "key", mclient.StatObjectOptions{}); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	probed := false
	for _, r := range reqs {
		if _, ok := r.URL.Query()["location"]; ok {
			t.Errorf("unexpected GetBucketLocation request: %s", r.URL)
		}
		if auth := r.Header.Get("Authorization"); !strings.Contains(auth, "/ru-central-1/s3/aws4_request") {
			t.Errorf("%s %s signed without the default region: %s", r.Method, r.URL.Path, auth)
		}
		if strings.HasPrefix(r.URL.Path, "/probe-health-test") {
			probed = true
		}
	}
	if !probed {
		t.Error("online check did not reach the storage")
	}
}
