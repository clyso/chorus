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
	"net/http"

	mclient "github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/tasks"
)

func (r *router) createMultipartUpload(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user, bucket, object := xctx.GetUser(ctx), xctx.GetBucket(ctx), xctx.GetObject(ctx)

	resp, storage, isApiErr, err = r.commonWrite(req)
	if err != nil || isApiErr {
		return
	}

	inProgressSwitch := xctx.GetInProgressZeroDowntime(ctx)
	if inProgressSwitch == nil {
		// no switch in progress
		return
	}
	// switch in progress

	respBody := initiateMultipartUploadResult{}
	err = s3client.ExtractRespBody(resp, &respBody)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("unable to unmarshal initiateMultipartUploadResult response body")
		return
	}
	err = r.storageSvc.StoreUploadID(ctx, user, bucket, object, respBody.UploadID, inProgressSwitch.MultipartTTL)

	return
}

func (r *router) completeMultipartUpload(req *http.Request) (resp *http.Response, taskList []tasks.ReplicationTask, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user, bucket, object := xctx.GetUser(ctx), xctx.GetBucket(ctx), xctx.GetObject(ctx)
	var switchInProgress bool
	storage, switchInProgress, err = r.routeMultipart(req)
	if err != nil {
		return
	}

	client, err := r.clients.GetByName(ctx, user, storage)
	if err != nil {
		return nil, nil, "", false, err
	}
	resp, isApiErr, err = client.Do(req)
	if err != nil || isApiErr {
		return
	}
	if switchInProgress {
		_ = r.storageSvc.DeleteUploadID(ctx, user, bucket, object, req.URL.Query().Get("uploadId"))
	}

	var res completeMultipartUploadResult
	xmlErr := s3client.ExtractRespBody(resp, &res)
	if xmlErr != nil {
		zerolog.Ctx(ctx).Err(xmlErr).Msg("unable to unmarshal response body")
	} else {
		zerolog.Ctx(ctx).Debug().Str("etag", res.ETag).Msg("multipart uploaded object with etag")
	}

	var objSize int64
	objInfo, err := client.S3().StatObject(ctx, bucket, object, mclient.StatObjectOptions{})
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("unable to get uploaded object size")
		err = nil
	} else {
		objSize = objInfo.Size
	}
	obj := dom.Object{Bucket: bucket, Name: object}
	taskList = []tasks.ReplicationTask{
		&tasks.ObjectSyncPayload{
			Object:  obj,
			ObjSize: objSize,
		},
		&tasks.ObjSyncACLPayload{
			Object: obj,
		},
		&tasks.ObjSyncTagsPayload{
			Object: obj,
		},
	}
	return

}

func (r *router) abortMultipartUpload(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user, bucket, object := xctx.GetUser(ctx), xctx.GetBucket(ctx), xctx.GetObject(ctx)
	var switchInProgress bool
	storage, switchInProgress, err = r.routeMultipart(req)
	if err != nil {
		return
	}

	client, err := r.clients.GetByName(ctx, user, storage)
	if err != nil {
		return nil, "", false, err
	}
	resp, isApiErr, err = client.Do(req)
	if err != nil || isApiErr {
		return
	}
	if switchInProgress {
		_ = r.storageSvc.DeleteUploadID(ctx, user, bucket, object, req.URL.Query().Get("uploadId"))
	}
	return
}

func (r *router) listMultipartUploads(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user := xctx.GetUser(ctx)
	storage, err = r.routeListMultipart(req)
	if err != nil {
		return
	}
	client, err := r.clients.GetByName(ctx, user, storage)
	if err != nil {
		return nil, "", false, err
	}
	resp, isApiErr, err = client.Do(req)
	return
}

func (r *router) uploadPart(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user := xctx.GetUser(ctx)
	storage, _, err = r.routeMultipart(req)
	if err != nil {
		return
	}

	client, err := r.clients.GetByName(ctx, user, storage)
	if err != nil {
		return nil, "", false, err
	}
	resp, isApiErr, err = client.Do(req)
	return
}

func (r *router) routeMultipart(req *http.Request) (storage string, switchInProgress bool, err error) {
	ctx := req.Context()
	user, bucket, object := xctx.GetUser(ctx), xctx.GetBucket(ctx), xctx.GetObject(ctx)
	storage = xctx.GetRoutingPolicy(ctx)

	inProgressSwitch := xctx.GetInProgressZeroDowntime(ctx)
	if inProgressSwitch == nil {
		// no switch in progress
		return storage, false, nil
	}
	uploadID := req.URL.Query().Get("uploadId")
	var exists bool
	exists, err = r.storageSvc.ExistsUploadID(ctx, user, bucket, object, uploadID)
	if err != nil {
		return storage, true, err
	}
	if exists {
		// multipart upload id exists in redis.
		// route to new storage
		return storage, true, nil
	}
	// multipart upload was started before switch.
	// route to old storage
	oldReplicationID := inProgressSwitch.ReplicationID()
	return oldReplicationID.FromStorage, true, nil
}

func (r *router) routeListMultipart(req *http.Request) (storage string, err error) {
	ctx := req.Context()
	user, bucket := xctx.GetUser(ctx), xctx.GetBucket(ctx)
	storage = xctx.GetRoutingPolicy(ctx)

	inProgressSwitch := xctx.GetInProgressZeroDowntime(ctx)
	if inProgressSwitch == nil {
		// no switch in progress
		return storage, nil
	}
	// todo: maybe better always return old?
	exists, err := r.storageSvc.ExistsUploads(ctx, user, bucket)
	if err != nil {
		return "", err
	}
	if exists {
		// multipart upload id exists in redis.
		// route to new storage
		return storage, nil
	}
	// multipart upload was started before switch.
	// route to old storage
	oldReplicationID := inProgressSwitch.ReplicationID()
	return oldReplicationID.FromStorage, nil
}
