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
	"fmt"
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/storage"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/hibiken/asynq"
	"net/http"
)

type Router interface {
	Route(r *http.Request) (resp *http.Response, task []tasks.SyncTask, storage string, isApiErr bool, err error)
}

func NewRouter(
	clients s3client.Service,
	taskClient *asynq.Client,
	versionSvc meta.VersionService,
	policySvc policy.Service,
	storageSvc storage.Service,
	limit ratelimit.RPM) Router {
	return &router{
		clients:    clients,
		taskClient: taskClient,
		versionSvc: versionSvc,
		policySvc:  policySvc,
		storageSvc: storageSvc,
		limit:      limit,
	}
}

type router struct {
	clients    s3client.Service
	taskClient *asynq.Client
	versionSvc meta.VersionService
	policySvc  policy.Service
	storageSvc storage.Service
	limit      ratelimit.RPM
}

func (r *router) Route(req *http.Request) (resp *http.Response, taskList []tasks.SyncTask, storage string, isApiErr bool, err error) {
	var (
		ctx    = req.Context()
		method = xctx.GetMethod(req.Context())
		bucket = xctx.GetBucket(ctx)
		object = xctx.GetObject(ctx)
		task   tasks.SyncTask
	)

	switch method {
	case s3.GetObjectAcl, s3.PutObjectAcl, s3.GetBucketAcl, s3.PutBucketAcl:
		if !features.ACL(ctx) {
			return nil, nil, "", false, fmt.Errorf("ACL api is disabled: %w", dom.ErrNotImplemented)
		}
	case s3.GetObjectTagging, s3.PutObjectTagging, s3.DeleteObjectTagging,
		s3.GetBucketTagging, s3.PutBucketTagging, s3.DeleteBucketTagging:
		if !features.Tagging(ctx) {
			return nil, nil, "", false, fmt.Errorf("tagging api is disabled: %w", dom.ErrNotImplemented)
		}
	case s3.PutBucketVersioning, s3.GetBucketVersioning, s3.ListObjectVersions:
		if !features.Versioning(ctx) {
			return nil, nil, "", false, fmt.Errorf("versioning api is disabled: %w", dom.ErrNotImplemented)
		}
	}

	switch method {
	case s3.CreateBucket:
		resp, task, storage, isApiErr, err = r.createBucket(req)
	case s3.DeleteBucket:
		resp, task, storage, isApiErr, err = r.deleteBucket(req)
	case s3.ListBuckets:
		resp, storage, isApiErr, err = r.listBuckets(req)
	case s3.HeadBucket,
		s3.GetBucketLocation,
		s3.GetBucketTagging,
		s3.GetBucketAcl,
		s3.GetBucketVersioning,
		s3.GetObject,
		s3.GetObjectAttributes,
		s3.ListObjectVersions,
		s3.GetObjectTagging,
		s3.GetObjectAcl,
		s3.ListObjects, s3.ListObjectsV2,
		s3.HeadObject:
		resp, storage, isApiErr, err = r.commonRead(req)
	case s3.PutBucketTagging, s3.DeleteBucketTagging:
		resp, storage, isApiErr, err = r.commonWrite(req)
		task = &tasks.BucketSyncTagsPayload{Bucket: bucket}
	case s3.PutBucketAcl, s3.PutObjectAcl:
		resp, storage, isApiErr, err = r.commonWrite(req)
		task = &tasks.ObjSyncACLPayload{Object: dom.Object{Bucket: bucket, Name: object}}
	case s3.PutObjectTagging, s3.DeleteObjectTagging:
		resp, storage, isApiErr, err = r.commonWrite(req)
		task = &tasks.ObjSyncTagsPayload{Object: dom.Object{Bucket: bucket, Name: object}}
	case s3.PutObject, s3.CopyObject:
		resp, taskList, storage, isApiErr, err = r.putObject(req)
	case s3.DeleteObject:
		resp, storage, isApiErr, err = r.commonWrite(req)
		task = &tasks.ObjectDeletePayload{Object: dom.Object{Bucket: bucket, Name: object}}
	case s3.DeleteObjects:
		resp, taskList, storage, isApiErr, err = r.deleteObjects(req)
	case s3.CreateMultipartUpload:
		resp, storage, isApiErr, err = r.createMultipartUpload(req)
	case s3.CompleteMultipartUpload:
		resp, taskList, storage, isApiErr, err = r.completeMultipartUpload(req)
	case s3.AbortMultipartUpload:
		resp, storage, isApiErr, err = r.abortMultipartUpload(req)
	case s3.ListMultipartUploads:
		resp, storage, isApiErr, err = r.listMultipartUploads(req)
	case s3.UploadPart, s3.UploadPartCopy, s3.ListParts:
		resp, storage, isApiErr, err = r.uploadPart(req)
	default:
		//todo: proxy unknown requests anyway???
		err = dom.ErrNotImplemented
	}
	if err == nil && task != nil {
		task.SetFrom(storage)
		if taskList == nil {
			taskList = []tasks.SyncTask{task}
		}
	}

	return
}
