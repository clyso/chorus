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

func (r *s3Router) putObject(req *http.Request) (resp *http.Response, taskList []tasks.ReplicationTask, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user, bucket, object := xctx.GetUser(ctx), xctx.GetBucket(ctx), xctx.GetObject(ctx)

	storage = xctx.GetRoutingPolicy(ctx)
	ctx = xctx.SetStorage(ctx, storage)
	req = req.WithContext(ctx)

	client, err := r.clients.AsS3(ctx, storage, user)
	if err != nil {
		return nil, nil, "", false, err
	}
	resp, isApiErr, err = client.Do(req)
	if err != nil || isApiErr {
		return
	}
	zerolog.Ctx(ctx).Debug().Str("etag", resp.Header.Get("ETag")).Msg("uploaded object with etag")

	obj := dom.Object{Bucket: bucket,
		Name:    object,
		Version: "", // versionID not supported for obj PUT
	}
	var objSize int64
	objInfo, err := client.S3().StatObject(ctx, bucket, object, mclient.StatObjectOptions{})
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("unable to get uploaded object size")
		err = nil
	} else {
		objSize = objInfo.Size
	}

	taskList = append(taskList, &tasks.ObjectSyncPayload{
		Object:  obj,
		ObjSize: objSize,
	})
	if hasACLChanged(req) {
		taskList = append(taskList, &tasks.ObjSyncACLPayload{
			Object: obj,
		})
	}
	if hasTagsChanged(req) {
		taskList = append(taskList, &tasks.ObjSyncTagsPayload{
			Object: obj,
		})
	}

	return
}

func (r *s3Router) deleteObjects(req *http.Request) (resp *http.Response, taskList []tasks.ReplicationTask, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user := xctx.GetUser(ctx)
	bucket := xctx.GetBucket(ctx)

	storage = xctx.GetRoutingPolicy(ctx)
	ctx = xctx.SetStorage(ctx, storage)
	req = req.WithContext(ctx)

	client, err := r.clients.AsS3(ctx, storage, user)
	if err != nil {
		return nil, nil, "", false, err
	}
	reqBody := deleteObjectsRequest{}
	err = s3client.ExtractReqBody(req, &reqBody)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("unable to unmarshal deleteObjectsRequest request body")
		return
	}

	resp, isApiErr, err = client.Do(req)
	if err != nil || isApiErr {
		return
	}

	respBody := multiDeleteResult{}
	err = s3client.ExtractRespBody(resp, &respBody)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("unable to unmarshal multiDeleteResult response body")
		return
	}

	if reqBody.Quiet {
		// quiet mode: filter request objects by response errors
		errSet := make(map[string]struct{}, len(respBody.Error))
		for _, delErr := range respBody.Error {
			errSet[delErr.Key] = struct{}{}
		}
		for _, object := range reqBody.Objects {
			if _, ok := errSet[object.Key]; ok {
				continue
			}
			taskList = append(taskList, &tasks.ObjectSyncPayload{
				Object:  object.toDom(bucket),
				Deleted: true,
			})
		}
	} else {
		// normal mode: use deleted obj list from response
		taskList = make([]tasks.ReplicationTask, len(respBody.Deleted))
		for i, object := range respBody.Deleted {
			taskList[i] = &tasks.ObjectSyncPayload{
				Object:  object.toDom(bucket),
				Deleted: true,
			}
		}
	}
	return
}
