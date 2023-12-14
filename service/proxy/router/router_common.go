package router

import (
	"context"
	"errors"
	"fmt"
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/rs/zerolog"
	"net/http"
)

func (r *router) commonRead(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user, bucket := xctx.GetUser(ctx), xctx.GetBucket(ctx)
	storage, err = r.policySvc.GetRoutingPolicy(ctx, user, bucket)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			return nil, "", false, fmt.Errorf("%w: routing policy not configured: %v", dom.ErrPolicy, err)
		}
		return nil, "", false, err
	}
	storage, err = r.adjustObjReadRoute(ctx, storage, user, bucket)
	if err != nil {
		return nil, "", false, err
	}
	ctx = xctx.SetStorage(ctx, storage)
	req = req.WithContext(ctx)

	client, err := r.clients.GetByName(ctx, storage)
	if err != nil {
		return nil, "", false, err
	}

	if xctx.GetMethod(ctx) == s3.GetObject {
		_ = r.limit.StorReq(ctx, client.Name()) //todo: refactor rate-limiting
	}
	resp, isApiErr, err = client.Do(req)
	return
}

func (r *router) commonWrite(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user, bucket := xctx.GetUser(ctx), xctx.GetBucket(ctx)
	storage, err = r.policySvc.GetRoutingPolicy(ctx, user, bucket)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			return nil, "", false, fmt.Errorf("%w: routing policy not configured: %v", dom.ErrPolicy, err)
		}
		return nil, "", false, err
	}
	ctx = xctx.SetStorage(ctx, storage)
	req = req.WithContext(ctx)

	client, err := r.clients.GetByName(ctx, storage)
	if err != nil {
		return nil, "", false, err
	}
	resp, isApiErr, err = client.Do(req)
	return
}

// adjustObjReadRoute adjust routing policy for read requests during switch process if old storage still has most recent obj version
func (r *router) adjustObjReadRoute(ctx context.Context, prevStorage, user, bucket string) (string, error) {
	switchInProgress, err := r.policySvc.IsReplicationSwitchInProgress(ctx, user, bucket)
	if err != nil {
		return "", err
	}
	if !switchInProgress {
		return prevStorage, nil
	}
	objMeta, err := r.getVersion(ctx)
	if err != nil {
		return "", err
	}
	prevStorageVer := objMeta[prevStorage]
	maxVerStorage, maxVer := prevStorage, prevStorageVer
	for storage, version := range objMeta {
		if version > maxVer {
			maxVerStorage = storage
			maxVer = version
		}
	}
	if maxVerStorage != prevStorage {
		zerolog.Ctx(ctx).Info().Msgf("change read route during switch process: storage %s obj ver %d is higher than main storage %s %d", maxVerStorage, maxVer, prevStorage, prevStorageVer)
	}
	return maxVerStorage, nil
}

func (r *router) getVersion(ctx context.Context) (map[string]int64, error) {
	method := xctx.GetMethod(ctx)
	switch {
	case method == s3.GetObjectAcl || method == s3.PutObjectAcl:
		return r.versionSvc.GetACL(ctx, dom.Object{Bucket: xctx.GetBucket(ctx), Name: xctx.GetObject(ctx)})
	case method == s3.GetObjectTagging || method == s3.PutObjectTagging || method == s3.DeleteObjectTagging:
		return r.versionSvc.GetTags(ctx, dom.Object{Bucket: xctx.GetBucket(ctx), Name: xctx.GetObject(ctx)})
	case method == s3.GetBucketAcl || method == s3.PutBucketAcl:
		return r.versionSvc.GetBucketACL(ctx, xctx.GetBucket(ctx))
	case method == s3.GetBucketTagging || method == s3.PutBucketTagging || method == s3.DeleteBucketTagging:
		return r.versionSvc.GetBucketTags(ctx, xctx.GetBucket(ctx))
	case xctx.GetObject(ctx) != "":
		return r.versionSvc.GetObj(ctx, dom.Object{Bucket: xctx.GetBucket(ctx), Name: xctx.GetObject(ctx)})
	case xctx.GetBucket(ctx) != "":
		return r.versionSvc.GetBucket(ctx, xctx.GetBucket(ctx))

	}
	zerolog.Ctx(ctx).Warn().Msg("trying to obtain version metadata for unsupported method")
	return map[string]int64{}, nil
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
