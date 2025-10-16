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

package router

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/swift"
	"github.com/clyso/chorus/pkg/tasks"
)

type SwiftConfig struct {
	// Storages is a map of swift storages to redirect requests to.
	// Storage aliases in this map should be the same across all chorus services and configs and should not be changed.
	Storages map[string]Storage `yaml:"storages"`
	// MainStorage [REQUIRED] is a name of the main storage from storages map.
	// By default, proxy will forward all requests to main storage unless configured routing policy says different.
	MainStorage string `yaml:"mainStorage"`
	// HttpTimeout - timeout to target storage
	HttpTimeout time.Duration `yaml:"httpTimeout"`
}

type Storage struct {
	// StorageURL - [REQUIRED] swift storage URL.
	StorageURL string `yaml:"storageURL"`
}

func (c *SwiftConfig) Validate() error {
	if c.MainStorage == "" {
		return fmt.Errorf("swift proxy config: main storage not set")
	}
	if _, ok := c.Storages[c.MainStorage]; !ok {
		return fmt.Errorf("swift proxy config: main storage %q not found in storages", c.MainStorage)
	}
	for name, storage := range c.Storages {
		if storage.StorageURL == "" {
			return fmt.Errorf("swift proxy config: storage %q URL not set", name)
		}
	}
	return nil
}

func NewSwiftRouter(
	conf SwiftConfig,
	limit ratelimit.RPM) Router {
	return &swiftRouter{
		conf:  conf,
		limit: limit,
		client: &http.Client{
			Timeout: conf.HttpTimeout,
		},
	}
}

type swiftRouter struct {
	conf   SwiftConfig
	limit  ratelimit.RPM
	client *http.Client
}

func (r *swiftRouter) Route(req *http.Request) (resp *http.Response, taskList []tasks.ReplicationTask, storage string, isApiErr bool, err error) {
	var (
		ctx    = req.Context()
		logger = zerolog.Ctx(ctx)
		method = xctx.GetSwiftMethod(req.Context())
		bucket = xctx.GetBucket(ctx)
		object = xctx.GetObject(ctx)
		task   tasks.ReplicationTask
	)
	storage = xctx.GetRoutingPolicy(ctx)

	// forward request:
	resp, isApiErr, err = r.forwardToStorage(ctx, req, storage)
	if err != nil || isApiErr {
		// unsuccessful request, return to client.
		// no data replication needed
		return
	}
	switch method {
	case swift.GetInfo, swift.GetEndpoints, swift.GetAccount, swift.GetContainer, swift.GetObject, swift.HeadAccount, swift.HeadContainer, swift.HeadObject:
	// read requests, no replication task needed
	case swift.PostAccount, swift.DeleteAccount:
		// handle account changes:
		task = &tasks.SwiftAccountUpdatePayload{
			Date: getDate(resp), // Use date because Last-modified not returned by swift
		}
	case swift.PutContainer, swift.PostContainer, swift.DeleteContainer:
		// handle container changes:
		task = &tasks.SwiftContainerUpdatePayload{
			Bucket: bucket,
			Date:   getDate(resp), // Use date because Last-modified not returned by swift
		}
	case swift.PostObject:
		// updates only object meta
		// meta update does not change obj version
		task = &tasks.SwiftObjectMetaUpdatePayload{
			Bucket: bucket,
			Object: object,
			Date:   getDate(resp), // Use date because Last-modified not returned by swift
		}
	case swift.PutObject, swift.CopyObject:
		// same as POST but also updates object payload
		// returns version id
		task = &tasks.SwiftObjectUpdatePayload{
			Bucket:       bucket,
			Object:       object,
			VersionID:    getObjVersion(resp),
			Etag:         getObjEtag(resp),
			LastModified: getLastModified(resp),
		}
	case swift.DeleteObject:
		// can contain version id
		task = &tasks.SwiftObjectDeletePayload{
			Bucket:          bucket,
			Object:          object,
			VersionID:       getVersionFromRequest(req),
			Date:            getDate(resp), // Use date because Last-modified not returned by swift
			DeleteMultipart: req.URL.Query().Get("multipart-manifest") == "delete",
		}

	case swift.UndefinedMethod:
	// not swift method - no action needed
	default:
		// should never happen
		// switch should be exhaustive
		// log error if enum value not covered
		logger.Error().Str("method", method.String()).Msg("unknown swift method")
		return nil, nil, "", false, fmt.Errorf("%w: unknown swift method %q", dom.ErrNotImplemented, method.String())
	}

	taskList = []tasks.ReplicationTask{task}
	return
}

func (r *swiftRouter) forwardToStorage(ctx context.Context, req *http.Request, toStorage string) (resp *http.Response, isApiErr bool, err error) {
	ctx, span := otel.Tracer("").Start(ctx, fmt.Sprintf("swiftForward.%s", xctx.GetSwiftMethod(ctx).String()))
	span.SetAttributes(attribute.String("storage", toStorage))
	if xctx.GetUser(ctx) != "" {
		span.SetAttributes(attribute.String("account", xctx.GetUser(ctx)))
	}
	if xctx.GetBucket(ctx) != "" {
		span.SetAttributes(attribute.String("bucket", xctx.GetBucket(ctx)))
	}
	if xctx.GetObject(ctx) != "" {
		span.SetAttributes(attribute.String("object", xctx.GetObject(ctx)))
	}
	defer span.End()

	// replace request base url with target storage
	baseURL, err := url.Parse(r.conf.Storages[toStorage].StorageURL)
	if err != nil {
		return nil, false, err
	}
	newURL := replaceSwiftReqBaseURL(req.URL, *baseURL)

	newReq, err := http.NewRequest(req.Method, newURL.String(), req.Body)
	if err != nil {
		return nil, false, err
	}
	newReq.ContentLength = req.ContentLength
	for header, vals := range req.Header {
		for _, val := range vals {
			newReq.Header.Add(header, val)
		}
	}

	// forward request:
	resp, err = r.client.Do(newReq)
	defer func() {
		if err != nil {
			return
		}
		// update rate limit & metrics:
		_ = r.limit.StorReq(ctx, toStorage)
		// TODO: implement metrics for swift
	}()
	if err != nil {
		return nil, false, err
	}
	isApiErr = resp.StatusCode < 200 || resp.StatusCode >= 400
	return
}

func replaceSwiftReqBaseURL(old *url.URL, newBase url.URL) *url.URL {
	if newBase.Scheme == "" {
		newBase.Scheme = "http" // Default to http if no scheme provided
	}
	newBase.RawQuery = old.RawQuery

	path := old.Path
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	swiftPathStartIdx := strings.Index(path, "/v1/")
	if swiftPathStartIdx == -1 {
		swiftPathStartIdx = strings.Index(path, "/info")
	}
	if swiftPathStartIdx != -1 {
		path = path[swiftPathStartIdx:]
	}
	newBase.Path = strings.Trim(newBase.Path, "/") + path
	return &newBase
}

func getObjEtag(resp *http.Response) string {
	return resp.Header.Get("Etag")
}

func getLastModified(resp *http.Response) string {
	return resp.Header.Get("Last-Modified")
}

func getDate(resp *http.Response) string {
	return resp.Header.Get("Date")
}

func getObjVersion(resp *http.Response) string {
	return resp.Header.Get("X-Object-Version-Id")
}

func getVersionFromRequest(req *http.Request) string {
	return req.URL.Query().Get("version-id")
}
