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
	"fmt"
	"io"
	"net/http"
	"strings"

	mclient "github.com/minio/minio-go/v7"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/s3"
)

func (c *client) Do(req *http.Request) (resp *http.Response, isApiErr bool, err error) {
	ctx, span := otel.Tracer("").Start(req.Context(), fmt.Sprintf("clientDo.%s", xctx.GetMethod(req.Context()).String()))
	span.SetAttributes(attribute.String("storage", c.storageName), attribute.String("user", c.userName))
	if xctx.GetBucket(ctx) != "" {
		span.SetAttributes(attribute.String("bucket", xctx.GetBucket(ctx)))
	}
	if xctx.GetObject(ctx) != "" {
		span.SetAttributes(attribute.String("object", xctx.GetObject(ctx)))
	}
	defer span.End()
	req = req.WithContext(ctx)
	defer func() {
		if err != nil {
			return
		}
		method := xctx.GetMethod(req.Context())
		flow := xctx.GetFlow(req.Context())
		c.metricsSvc.Count(flow, c.storageName, method.String())
		switch method {
		case s3.GetObject:
			if resp.ContentLength != 0 {
				c.metricsSvc.Download(flow, c.storageName, xctx.GetBucket(req.Context()), int(resp.ContentLength))
			}
		case s3.PutObject, s3.UploadPart:
			if req.ContentLength != 0 {
				c.metricsSvc.Upload(flow, c.storageName, xctx.GetBucket(req.Context()), int(req.ContentLength))
			}
		}
	}()

	// Parse bucket and object using the s3 package helper
	bucket, object := s3.ParseBucketAndObject(req)

	var newReq *http.Request
	url := *req.URL
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
	toSign, notToSign := processHeaders(req.Header)
	newReq.Header = toSign

	copyReqSpan.End()
	_, signReqSpan := otel.Tracer("").Start(ctx, fmt.Sprintf("clientDo.%s.SignReq", xctx.GetMethod(req.Context()).String()))
	newReq, err = signV4(*newReq, c.cred.AccessKeyID, c.cred.SecretAccessKey, "", "us-east-1") // todo: get location if needed ("us-east-1")
	signReqSpan.End()
	if err != nil {
		return nil, false, err
	}
	for name, vals := range notToSign {
		newReq.Header[name] = vals
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
		_, _ = errBodySeeker.Seek(0, 0) // Seek back to starting point.
		resp.Body = io.NopCloser(errBodySeeker)
		return
	}
	if err != nil {
		return nil, false, err
	}
	return
}
