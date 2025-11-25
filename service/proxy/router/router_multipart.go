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
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/tasks"
)

func (r *s3Router) createMultipartUpload(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
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
	id := entity.NewUserUploadObjectID(user, bucket)
	val := entity.NewUserUploadObject(object, respBody.UploadID)
	err = r.uploadSvc.StoreUpload(ctx, id, val, inProgressSwitch.MultipartTTL)

	return
}

func (r *s3Router) completeMultipartUpload(req *http.Request) (resp *http.Response, taskList []tasks.ReplicationTask, storage string, isApiErr bool, err error) {
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
		id := entity.NewUserUploadObjectID(user, bucket)
		val := entity.NewUserUploadObject(object, req.URL.Query().Get("uploadId"))
		_ = r.uploadSvc.DeleteUpload(ctx, id, val)
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

func (r *s3Router) abortMultipartUpload(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
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
		id := entity.NewUserUploadObjectID(user, bucket)
		val := entity.NewUserUploadObject(object, req.URL.Query().Get("uploadId"))

		_ = r.uploadSvc.DeleteUpload(ctx, id, val)
	}
	return
}

func (r *s3Router) listMultipartUploads(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
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

func (r *s3Router) uploadPart(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
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

func (r *s3Router) routeMultipart(req *http.Request) (storage string, switchInProgress bool, err error) {
	ctx := req.Context()
	storage = xctx.GetRoutingPolicy(ctx)

	inProgressSwitch := xctx.GetInProgressZeroDowntime(ctx)
	if inProgressSwitch == nil {
		// no switch in progress
		return storage, false, nil
	}
	var exists bool

	id := entity.NewUserUploadObjectID(xctx.GetUser(ctx), xctx.GetBucket(ctx))
	val := entity.NewUserUploadObject(xctx.GetObject(ctx), req.URL.Query().Get("uploadId"))
	exists, err = r.uploadSvc.UploadExists(ctx, id, val)

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
	return oldReplicationID.FromStorage(), true, nil
}

func (r *s3Router) routeListMultipart(req *http.Request) (storage string, err error) {
	ctx := req.Context()
	storage = xctx.GetRoutingPolicy(ctx)

	inProgressSwitch := xctx.GetInProgressZeroDowntime(ctx)
	if inProgressSwitch == nil {
		// no switch in progress
		return storage, nil
	}
	// todo: maybe better always return old?
	id := entity.NewUserUploadObjectID(xctx.GetUser(ctx), xctx.GetBucket(ctx))
	exists, err := r.uploadSvc.UploadsExistForUserBucket(ctx, id)

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
	return oldReplicationID.FromStorage(), nil
}
