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
	"errors"
	"fmt"
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/rs/zerolog"
	"io"
	"net/http"
)

func (r *router) createBucket(req *http.Request) (resp *http.Response, task *tasks.BucketCreatePayload, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user, bucket := xctx.GetUser(ctx), xctx.GetBucket(ctx)

	storage, err = r.policySvc.GetRoutingPolicy(ctx, user, bucket)
	if err != nil {
		return
	}

	client, err := r.clients.GetByName(ctx, storage)
	if err != nil {
		return nil, nil, "", false, err
	}
	// get bucket location from request:
	reqBody := createBucketConfiguration{}
	err = s3client.ExtractReqBody(req, &reqBody)
	if err != nil && !errors.Is(err, io.EOF) {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("unable to unmarshal createBucketConfiguration request body")
	}

	resp, isApiErr, err = client.Do(req)
	if err != nil || isApiErr {
		return
	}

	// create task:
	task = &tasks.BucketCreatePayload{Bucket: bucket, Location: reqBody.Location, Sync: tasks.Sync{FromStorage: storage}}

	return
}

func (r *router) deleteBucket(req *http.Request) (resp *http.Response, task *tasks.BucketDeletePayload, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user, bucket := xctx.GetUser(ctx), xctx.GetBucket(ctx)
	storage, err = r.policySvc.GetRoutingPolicy(ctx, user, bucket)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			return nil, nil, "", false, fmt.Errorf("%w: routing policy not configured: %v", dom.ErrPolicy, err)
		}
		return nil, nil, "", false, err
	}

	client, err := r.clients.GetByName(ctx, storage)
	if err != nil {
		return nil, nil, "", false, err
	}
	// delete bucket
	resp, isApiErr, err = client.Do(req)
	if err != nil || isApiErr {
		return
	}
	task = &tasks.BucketDeletePayload{Bucket: bucket}

	// don't delete bucket routing policy to route future HeadBucket requests.
	return
}

func (r *router) listBuckets(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user := xctx.GetUser(ctx)

	storage, err = r.policySvc.GetUserRoutingPolicy(ctx, user)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			// todo: call all storages and merge buckets????
			return nil, "", false, fmt.Errorf("%w: routing policy not configured: %v", dom.ErrPolicy, err)
		}
		return nil, "", false, err
	}
	client, err := r.clients.GetByName(ctx, storage)
	if err != nil {
		return nil, "", false, err
	}
	resp, isApiErr, err = client.Do(req)
	return
}
