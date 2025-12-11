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

package router

import (
	"context"
	"fmt"
	"net/http"

	"github.com/rs/zerolog"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/s3"
)

func (r *s3Router) commonRead(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user := xctx.GetUser(ctx)
	storage = xctx.GetRoutingPolicy(ctx)
	storage, err = r.adjustObjReadRoute(ctx, storage)
	if err != nil {
		return nil, "", false, err
	}
	ctx = xctx.SetStorage(ctx, storage)
	req = req.WithContext(ctx)

	client, err := r.clients.GetByName(ctx, user, storage)
	if err != nil {
		return nil, "", false, err
	}
	resp, isApiErr, err = client.Do(req)
	return
}

func (r *s3Router) commonWrite(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user := xctx.GetUser(ctx)
	storage = xctx.GetRoutingPolicy(ctx)
	ctx = xctx.SetStorage(ctx, storage)
	req = req.WithContext(ctx)

	client, err := r.clients.GetByName(ctx, user, storage)
	if err != nil {
		return nil, "", false, err
	}
	resp, isApiErr, err = client.Do(req)
	return
}

// adjustObjReadRoute adjust routing policy for read requests during switch process if old storage still has most recent obj version
func (r *s3Router) adjustObjReadRoute(ctx context.Context, prevStorage string) (string, error) {
	inProgressZeroDowntime := xctx.GetInProgressZeroDowntime(ctx)
	if inProgressZeroDowntime == nil {
		// no zero-downtime switch in progress
		return prevStorage, nil
	}
	// since switch is in progress, we need to check if the object version is higher in other storage

	// zero-downtime switch allowed only for a single replication:
	replications := xctx.GetReplications(ctx)
	if len(replications) != 1 {
		return "", fmt.Errorf("%w: in-progress zero-downtime switch can have only one replication", dom.ErrInternal)
	}
	replID := replications[0]
	version, err := r.getVersion(ctx, replID)
	if err != nil {
		return "", err
	}
	if version.From == version.To {
		// versions are equal, no need to change route
		return prevStorage, nil
	}
	currentDest := meta.Destination{Storage: prevStorage, Bucket: xctx.GetBucket(ctx)}
	currVer, otherVer := version.From, version.To
	if !currentDest.IsFrom(replID) {
		//swap
		currVer, otherVer = version.To, version.From
	}
	if currVer > otherVer {
		return prevStorage, nil
	}
	// return other storage as destination
	otherStorage := replID.FromStorage()
	if otherStorage == prevStorage {
		otherStorage = replID.ToStorage()
	}
	zerolog.Ctx(ctx).Info().Msgf("change read route during switch process: storage %s has higher obj ver %d than main storage %s ver %d", otherStorage, otherVer, prevStorage, currVer)
	return otherStorage, nil
}

func (r *s3Router) getVersion(ctx context.Context, replID entity.UniversalReplicationID) (meta.Version, error) {
	method := xctx.GetMethod(ctx)
	switch {
	case method == s3.GetObjectAcl || method == s3.PutObjectAcl:
		return r.versionSvc.GetACL(ctx, replID, dom.Object{Bucket: xctx.GetBucket(ctx), Name: xctx.GetObject(ctx), Version: xctx.GetObjectVer(ctx)})
	case method == s3.GetObjectTagging || method == s3.PutObjectTagging || method == s3.DeleteObjectTagging:
		return r.versionSvc.GetTags(ctx, replID, dom.Object{Bucket: xctx.GetBucket(ctx), Name: xctx.GetObject(ctx), Version: xctx.GetObjectVer(ctx)})
	case method == s3.GetBucketAcl || method == s3.PutBucketAcl:
		return r.versionSvc.GetBucketACL(ctx, replID, xctx.GetBucket(ctx))
	case method == s3.GetBucketTagging || method == s3.PutBucketTagging || method == s3.DeleteBucketTagging:
		return r.versionSvc.GetBucketTags(ctx, replID, xctx.GetBucket(ctx))
	case xctx.GetObject(ctx) != "":
		return r.versionSvc.GetObj(ctx, replID, dom.Object{Bucket: xctx.GetBucket(ctx), Name: xctx.GetObject(ctx)})
	case xctx.GetBucket(ctx) != "":
		return r.versionSvc.GetBucket(ctx, replID, xctx.GetBucket(ctx))

	}
	zerolog.Ctx(ctx).Warn().Msg("trying to obtain version metadata for unsupported method")
	return meta.Version{}, nil
}

func hasACLChanged(r *http.Request) bool {
	if r.Header.Get("x-amz-acl") != "" {
		return true
	}
	if r.Header.Get("x-amz-grant-full-control") != "" {
		return true
	}
	if r.Header.Get("x-amz-grant-read") != "" {
		return true
	}
	if r.Header.Get("x-amz-grant-read-acp") != "" {
		return true
	}
	if r.Header.Get("x-amz-grant-write") != "" {
		return true
	}
	if r.Header.Get("x-amz-grant-write-acp") != "" {
		return true
	}
	if r.Header.Get("x-amz-object-ownership") != "" {
		return true
	}
	return false
}

func hasTagsChanged(r *http.Request) bool {
	return r.Header.Get("x-amz-tagging") != ""
}
