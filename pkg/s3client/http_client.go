/*
 * Copyright © 2024 Clyso GmbH
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
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	aws_credentials "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/s3"
	mclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

func newClient(ctx context.Context, conf s3.Storage, name, user string, metricsSvc metrics.S3Service, tp trace.TracerProvider) (Client, error) {
	c := &client{
		c: &http.Client{
			Timeout: conf.HttpTimeout,
		},
		online:     &atomic.Bool{},
		conf:       conf,
		name:       name,
		user:       user,
		cred:       conf.Credentials[user],
		metricsSvc: metricsSvc,
	}
	host := strings.TrimPrefix(conf.Address, "http://")
	host = strings.TrimPrefix(host, "https://")

	mc, err := mclient.New(host, &mclient.Options{
		Creds:  credentials.NewStaticV4(c.cred.AccessKeyID, c.cred.SecretAccessKey, ""),
		Secure: conf.IsSecure,
	})
	if err != nil {
		return nil, err
	}
	c.s3 = newMinioClient(name, user, mc, metricsSvc)
	if err = isOnline(ctx, c); err != nil {
		return nil, fmt.Errorf("s3 is offline: %w", err)
	}
	c.online.Store(true)
	go func(duration time.Duration) {
		timer := time.NewTimer(duration)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				// Do health check the first time and ONLY if the connection is marked offline
				if !c.online.Load() {
					if err = isOnline(ctx, c); err != nil {
						c.online.Store(true)
					}
				}
				timer.Reset(duration)
			}
		}
	}(conf.HealthCheckInterval)

	awsClient, err := newAWSClient(conf, name, user, metricsSvc)
	if err != nil {
		return nil, err
	}
	c.aws = awsClient
	snsEndpoint := conf.Address
	if !strings.HasPrefix(snsEndpoint, "http") {
		if conf.IsSecure {
			snsEndpoint = "https://" + snsEndpoint
		} else {
			snsEndpoint = "http://" + snsEndpoint
		}
	}

	c.sns = sns.NewFromConfig(aws.Config{
		Region:      "default",
		Credentials: aws_credentials.NewStaticCredentialsProvider(conf.Credentials[user].AccessKeyID, conf.Credentials[user].SecretAccessKey, ""),
		EndpointResolver: aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			return aws.Endpoint{URL: snsEndpoint}, nil
		}),
	})

	return c, nil
}

func isOnline(ctx context.Context, c *client) error {
	_, err := c.s3.GetBucketLocation(ctx, "probe-health-test")
	if err == nil {
		return nil
	} else if !mclient.IsNetworkOrHostDown(err, false) {
		switch mclient.ToErrorResponse(err).Code {
		case "NoSuchBucket", "AccessDenied", "":
			return nil
		}
	}
	return err
}

type client struct {
	c          *http.Client
	s3         *S3
	aws        *AWS
	sns        *sns.Client
	online     *atomic.Bool
	conf       s3.Storage
	name       string
	user       string
	cred       s3.CredentialsV4
	metricsSvc metrics.S3Service
}

func (c *client) SNS() *sns.Client {
	return c.sns
}

func (c *client) AWS() *AWS {
	return c.aws
}

func (c *client) Name() string {
	return c.name
}

func (c *client) Config() s3.Storage {
	return c.conf
}

func (c *client) S3() *S3 {
	return c.s3
}

func (c *client) Do(req *http.Request) (resp *http.Response, isApiErr bool, err error) {
	ctx, span := otel.Tracer("").Start(req.Context(), fmt.Sprintf("clientDo.%s", xctx.GetMethod(req.Context()).String()))
	span.SetAttributes(attribute.String("storage", c.name), attribute.String("user", c.user))
	if xctx.GetBucket(ctx) != "" {
		span.SetAttributes(attribute.String("bucket", xctx.GetBucket(ctx)))
	}
	if xctx.GetObject(ctx) != "" {
		span.SetAttributes(attribute.String("object", xctx.GetObject(ctx)))
	}
	defer span.End()
	req = req.WithContext(ctx)
	defer func() {
		if mclient.IsNetworkOrHostDown(err, false) {
			c.online.Store(false)
		}
	}()
	defer func() {
		if err != nil {
			return
		}
		method := xctx.GetMethod(req.Context())
		flow := xctx.GetFlow(req.Context())
		c.metricsSvc.Count(flow, c.name, method)
		switch method {
		case s3.GetObject:
			if resp.ContentLength != 0 {
				c.metricsSvc.Download(flow, c.name, xctx.GetBucket(req.Context()), int(resp.ContentLength))
			}
		case s3.PutObject, s3.UploadPart:
			if req.ContentLength != 0 {
				c.metricsSvc.Upload(flow, c.name, xctx.GetBucket(req.Context()), int(req.ContentLength))
			}
		}
	}()
	var (
		path   = strings.Trim(req.URL.Path, "/")
		parts  = strings.SplitN(path, "/", 2)
		bucket = parts[0]
		object = ""
		newReq *http.Request
	)

	if len(parts) == 2 {
		object = parts[1]
	}
	url := *req.URL
	//todo: support virtual host
	// see: github.com/minio/minio-go/v7@v7.0.52/api.go:890
	host := strings.TrimPrefix(c.conf.Address, "http://")
	host = strings.TrimPrefix(host, "https://")
	url.Host = host
	url.Scheme = "http"
	if c.conf.IsSecure {
		url.Scheme = "https"
	}
	url.OmitHost = false
	url.ForceQuery = false

	_, copyReqSpan := otel.Tracer("").Start(ctx, fmt.Sprintf("clientDo.%s.CopyReq", xctx.GetMethod(req.Context()).String()))
	var body io.Reader = http.NoBody
	if req.ContentLength != 0 {
		body = io.NopCloser(req.Body)
	}
	newReq, err = http.NewRequest(req.Method, url.String(), body)
	if err != nil {
		copyReqSpan.End()
		return nil, false, err
	}
	newReq.ContentLength = req.ContentLength

	for name, vals := range req.Header {
		if name == "Authorization" || name == "X-Amz-Date" {
			continue
		}
		for _, val := range vals {
			newReq.Header.Add(name, val)
		}
	}
	copyReqSpan.End()
	_, signReqSpan := otel.Tracer("").Start(ctx, fmt.Sprintf("clientDo.%s.SignReq", xctx.GetMethod(req.Context()).String()))
	newReq, err = signV4(*newReq, c.cred.AccessKeyID, c.cred.SecretAccessKey, "", "us-east-1") //todo: get location if needed ("us-east-1")
	signReqSpan.End()
	if err != nil {
		return nil, false, err
	}
	_, doReqSpan := otel.Tracer("").Start(ctx, fmt.Sprintf("clientDo.%s.DoReq", xctx.GetMethod(req.Context()).String()))
	resp, err = c.c.Do(newReq)
	doReqSpan.End()
	if resp != nil && !successStatus[resp.StatusCode] {
		isApiErr = true
		// Read the body to be saved later.
		var errBodyBytes []byte
		errBodyBytes, err = io.ReadAll(resp.Body)
		// res.Body should be closed
		closeResponse(resp)

		// Save the body.
		errBodySeeker := bytes.NewReader(errBodyBytes)
		resp.Body = io.NopCloser(errBodySeeker)

		// For errors verify if its retryable otherwise fail quickly.
		err = mclient.ToErrorResponse(httpRespToErrorResponse(resp, bucket, object))

		// Save the body back again.
		errBodySeeker.Seek(0, 0) // Seek back to starting point.
		resp.Body = io.NopCloser(errBodySeeker)
		return
	}
	if err != nil {
		return nil, false, err
	}
	return
}

func (c *client) IsOnline() bool {
	return c.online.Load()
}
