// Copyright 2025 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rclone

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/util"
)

const (
	CChorusSourceVersionIDMetaHeader = "x-amz-meta-chorus-source-version-id"
	CRGWVersionMigrationMetaHeader   = "x-amz-meta-rgwx-version-id"
)

type ObjectVersionInfo struct {
	Version string
	Size    uint64
}

type CopySvc interface {
	GetVersionInfo(ctx context.Context, to File) ([]ObjectVersionInfo, error)
	DeleteDestinationObject(ctx context.Context, to File) error
	GetLastMigratedVersionInfo(ctx context.Context, to File) (ObjectVersionInfo, error)
	CopyObject(ctx context.Context, from File, to File) error
}

type S3CopySvc struct {
	// logger         *zerolog.Logger
	clientRegistry    s3client.Service
	memoryLimiterSvc  LimiterSvc
	requestLimiterSvc ratelimit.RPM
	metricsSvc        metrics.S3Service
}

func NewS3CopySvc(clientRegistry s3client.Service, memoryLimiterSvc LimiterSvc, requestLimiterSvc ratelimit.RPM, metricsSvc metrics.S3Service) *S3CopySvc {
	return &S3CopySvc{
		clientRegistry:    clientRegistry,
		memoryLimiterSvc:  memoryLimiterSvc,
		requestLimiterSvc: requestLimiterSvc,
		metricsSvc:        metricsSvc,
	}
}

func (r *S3CopySvc) GetVersionInfo(ctx context.Context, to File) ([]ObjectVersionInfo, error) {
	storageClient, err := r.clientRegistry.GetByName(ctx, to.Storage)
	if err != nil {
		return nil, fmt.Errorf("unable to get %s storage client: %w", to.Storage, err)
	}

	if err := r.requestLimiterSvc.StorReq(ctx, to.Storage); err != nil {
		return nil, fmt.Errorf("unable to get storage rate limit: %w", err)
	}

	objects := storageClient.S3().ListObjects(ctx, to.Bucket, minio.ListObjectsOptions{
		WithVersions: true,
		Prefix:       to.Name,
	})

	result := []ObjectVersionInfo{}
	for object := range objects {
		if object.Err != nil {
			return nil, fmt.Errorf("unable to list versions: %w", object.Err)
		}

		info := ObjectVersionInfo{
			Version: object.VersionID,
			Size:    uint64(object.Size),
		}

		result = append(result, info)
	}

	return result, nil
}

func (r *S3CopySvc) GetLastMigratedVersionInfo(ctx context.Context, to File) (ObjectVersionInfo, error) {
	toClient, err := r.clientRegistry.GetByName(ctx, to.Storage)
	if err != nil {
		return ObjectVersionInfo{}, fmt.Errorf("unable to get to client: %w", err)
	}

	if err := r.requestLimiterSvc.StorReq(ctx, to.Storage); err != nil {
		return ObjectVersionInfo{}, fmt.Errorf("unable to get storage rate limit: %w", err)
	}

	objectStat, err := toClient.S3().StatObject(ctx, to.Bucket, to.Name, minio.StatObjectOptions{})
	if err != nil {
		var errorResp minio.ErrorResponse
		ok := errors.As(err, &errorResp)
		if !ok {
			return ObjectVersionInfo{}, fmt.Errorf("unable to cast error to s3 error %w", err)
		}
		if errorResp.StatusCode == http.StatusNotFound {
			return ObjectVersionInfo{}, dom.ErrNotFound
		}
	}

	sourceVersionID := objectStat.Metadata.Get(CChorusSourceVersionIDMetaHeader)

	if sourceVersionID == "" {
		return ObjectVersionInfo{}, dom.ErrInvalidArg
	}

	return ObjectVersionInfo{
		Version: sourceVersionID,
		Size:    uint64(objectStat.Size),
	}, nil
}

func (r *S3CopySvc) DeleteDestinationObject(ctx context.Context, to File) error {
	toClient, err := r.clientRegistry.GetByName(ctx, to.Storage)
	if err != nil {
		return fmt.Errorf("unable to get to client: %w", err)
	}

	if err := r.requestLimiterSvc.StorReq(ctx, to.Storage); err != nil {
		return fmt.Errorf("unable to get storage rate limit: %w", err)
	}

	if err := toClient.S3().RemoveObject(ctx, to.Bucket, to.Name, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("unable to remove object: %w", err)
	}
	return nil
}

func (r *S3CopySvc) CopyObject(ctx context.Context, from File, to File) error {
	fromClient, err := r.clientRegistry.GetByName(ctx, from.Storage)
	if err != nil {
		return fmt.Errorf("unable to get from client: %w", err)
	}
	toClient, err := r.clientRegistry.GetByName(ctx, to.Storage)
	if err != nil {
		return fmt.Errorf("unable to get to client: %w", err)
	}

	if err := r.requestLimiterSvc.StorReqN(ctx, from.Storage, 2); err != nil {
		return fmt.Errorf("unable to get storage rate limit: %w", err)
	}

	if err := r.requestLimiterSvc.StorReqN(ctx, to.Storage, 3); err != nil {
		return fmt.Errorf("unable to get storage rate limit: %w", err)
	}

	fromObjectStat, err := fromClient.S3().StatObject(ctx, from.Bucket, from.Name, minio.GetObjectOptions{
		VersionID: from.Version,
	})
	if err != nil {
		var errorResp minio.ErrorResponse
		ok := errors.As(err, &errorResp)
		if !ok {
			return fmt.Errorf("unable to get from object stat: %w", err)
		}
		if errorResp.StatusCode == http.StatusNotFound {
			return dom.ErrNotFound
		}
	}

	toObjectPresent := true
	toObjectStat, err := toClient.S3().StatObject(ctx, to.Bucket, to.Name, minio.GetObjectOptions{
		VersionID: to.Version,
	})
	if err != nil {
		var errorResp minio.ErrorResponse
		ok := errors.As(err, &errorResp)
		if !ok {
			return fmt.Errorf("unable to get from object stat: %w", err)
		}
		if errorResp.StatusCode == http.StatusNotFound {
			toObjectPresent = false
		}
	}

	if toObjectPresent && fromObjectStat.ETag == toObjectStat.ETag && fromObjectStat.Size == toObjectStat.Size {
		return nil
	}

	fromObjectSize := fromObjectStat.Size

	release, err := r.memoryLimiterSvc.Reserve(ctx, fromObjectSize)
	if err != nil {
		return fmt.Errorf("unable to check limit: %w", err)
	}
	defer release()

	ctx, span := otel.Tracer("").Start(ctx, "rclone.CopyToWithVersion")
	span.SetAttributes(attribute.String("bucket", from.Bucket), attribute.String("object", from.Name),
		attribute.String("version", from.Version),
		attribute.String("from", from.Storage), attribute.String("to", to.Storage), attribute.Int64("size", fromObjectSize))
	defer span.End()

	provider := strings.ToLower(toClient.Config().Provider)

	putObjectOpts := minio.PutObjectOptions{
		UserMetadata: fromObjectStat.UserMetadata,
	}

	if features.Tagging(ctx) {
		putObjectOpts.UserTags = fromObjectStat.UserTags
	}

	if to.Version != "" {
		putObjectOpts.UserMetadata[CChorusSourceVersionIDMetaHeader] = to.Version

		switch provider {
		case "ceph":
			// putObjectOpts.UserMetadata[CRGWVersionMigrationMetaHeader] = to.Version
		case "minio":
			// minio will accept only uuid as a version id
			if _, err := uuid.Parse(to.Version); err == nil {
				putObjectOpts.Internal.SourceVersionID = to.Version
			}
		default:
		}
	}

	zerolog.Ctx(ctx).
		Debug().
		Str("file_size", util.ByteCountSI(fromObjectSize)).
		Msg("starting obj copy")

	fromObject, err := fromClient.S3().GetObject(ctx, from.Bucket, from.Name, minio.GetObjectOptions{
		VersionID: from.Version,
	})
	if err != nil {
		return fmt.Errorf("unable to get from object: %w", err)
	}
	defer fromObject.Close()

	_, err = toClient.S3().PutObject(ctx, to.Bucket, to.Name, fromObject, fromObjectStat.Size, putObjectOpts)
	if err != nil {
		return fmt.Errorf("unable to upload object: %w", err)
	}

	if features.ACL(ctx) {
		if err := r.CopyACLs(ctx, from, to); err != nil {
			return fmt.Errorf("unable to copy acl %w", err)
		}
	}

	if fromObjectStat.IsDeleteMarker {
		if err := toClient.S3().RemoveObject(ctx, to.Bucket, to.Name, minio.RemoveObjectOptions{
			VersionID: to.Version,
		}); err != nil {
			return fmt.Errorf("unable to replicate delete marker: %w", err)
		}
	}

	r.AddMetrics(ctx, from, to, fromObjectSize)

	return nil
}

func (r *S3CopySvc) CopyACLs(ctx context.Context, from File, to File) error {
	fromClient, err := r.clientRegistry.GetByName(ctx, from.Storage)
	if err != nil {
		return fmt.Errorf("unable to get from client: %w", err)
	}
	toClient, err := r.clientRegistry.GetByName(ctx, to.Storage)
	if err != nil {
		return fmt.Errorf("unable to get to client: %w", err)
	}

	fromACL, err := fromClient.AWS().GetObjectAclWithContext(ctx, &awss3.GetObjectAclInput{
		Bucket:    &from.Bucket,
		Key:       &from.Name,
		VersionId: &from.Version,
	})
	if err != nil {
		if s3client.AwsErrRetry(err) {
			return fmt.Errorf("unable to get from acl: %w", err)
		}
		zerolog.Ctx(ctx).Err(err).Msg("skip object ACL sync due to get ACL err")
		return nil
	}

	toACL, err := toClient.AWS().GetObjectAclWithContext(ctx, &awss3.GetObjectAclInput{
		Bucket:    &to.Bucket,
		Key:       &to.Name,
		VersionId: &to.Version,
	})
	if err != nil {
		if s3client.AwsErrRetry(err) {
			return fmt.Errorf("unable to get to acl: %w", err)
		}
		zerolog.Ctx(ctx).Err(err).Msg("skip object ACL sync due to get ACL err")
		return nil
	}

	var toOwnerID *string
	if toACL != nil && toACL.Owner != nil {
		toOwnerID = toACL.Owner.ID
	}

	_, err = toClient.AWS().PutObjectAclWithContext(ctx, &awss3.PutObjectAclInput{
		AccessControlPolicy: r.mapOwnersACL(fromACL.Owner, fromACL.Grants, toOwnerID, features.PreserveACLGrants(ctx)),
		Bucket:              &to.Bucket,
		Key:                 &to.Name,
		VersionId:           &to.Version,
	})
	if err != nil {
		if s3client.AwsErrRetry(err) {
			return fmt.Errorf("unable to put to acl: %w", err)
		}
		zerolog.Ctx(ctx).Err(err).Msg("skip object ACL sync due to put ACL err")
		return nil
	}

	return nil
}

func (r *S3CopySvc) srcOwnerToDestOwner(owner, srcBucketOwner, dstBucketOwner *string) *string {
	if owner == nil || *owner != *srcBucketOwner {
		return owner
	}
	return dstBucketOwner
}

func (r *S3CopySvc) mapOwnersACL(srcOwner *awss3.Owner, srcGrants []*awss3.Grant, dstOwner *string, preserveACLGrants bool) *awss3.AccessControlPolicy {
	destGrants := make([]*awss3.Grant, 0, len(srcGrants))
	for _, grant := range srcGrants {
		var destID *string
		if preserveACLGrants {
			destID = grant.Grantee.ID
		} else {
			destID = r.srcOwnerToDestOwner(grant.Grantee.ID, srcOwner.ID, dstOwner)
		}

		destGrant := &awss3.Grant{
			Grantee: &awss3.Grantee{
				ID:           destID,
				EmailAddress: grant.Grantee.EmailAddress,
				Type:         grant.Grantee.Type,
				URI:          grant.Grantee.URI,
				DisplayName:  grant.Grantee.DisplayName,
			},
			Permission: grant.Permission,
		}
		destGrants = append(destGrants, destGrant)
	}

	res := &awss3.AccessControlPolicy{
		Grants: destGrants,
	}
	if srcOwner != nil {
		res.Owner = &awss3.Owner{
			ID:          r.srcOwnerToDestOwner(srcOwner.ID, srcOwner.ID, dstOwner),
			DisplayName: srcOwner.DisplayName,
		}
	}

	return res
}

func (r *S3CopySvc) AddMetrics(ctx context.Context, from File, to File, size int64) {
	flow := xctx.GetFlow(ctx)
	r.metricsSvc.Count(flow, from.Storage, s3.HeadObject)
	r.metricsSvc.Count(flow, from.Storage, s3.GetObject)
	r.metricsSvc.Count(flow, from.Storage, s3.GetObjectAcl)
	r.metricsSvc.Count(flow, to.Storage, s3.HeadObject)
	r.metricsSvc.Count(flow, to.Storage, s3.PutObject)
	r.metricsSvc.Count(flow, to.Storage, s3.PutObjectAcl)
	if size != 0 {
		r.metricsSvc.Download(flow, from.Storage, from.Bucket, int(size))
		r.metricsSvc.Upload(flow, to.Storage, to.Bucket, int(size))
	}
}

type LimiterSvc interface {
	Reserve(ctx context.Context, size int64) (func(), error)
}

type MemoryLimiterSvc struct {
	memCalc     *MemCalculator
	memLimiter  ratelimit.Semaphore
	fileLimiter ratelimit.Semaphore

	metricsSvc metrics.S3Service
}

func NewMemoryLimiterSvc(memCalc *MemCalculator, memLimiter ratelimit.Semaphore, fileLimiter ratelimit.Semaphore, metricsSvc metrics.S3Service) *MemoryLimiterSvc {
	return &MemoryLimiterSvc{
		memCalc:     memCalc,
		memLimiter:  memLimiter,
		fileLimiter: fileLimiter,
		metricsSvc:  metricsSvc,
	}
}

func (r *MemoryLimiterSvc) Reserve(ctx context.Context, fileSize int64) (func(), error) {
	fileLimitRelease, err := r.fileLimiter.TryAcquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to allocate memory with file limiter: %w", err)
	}

	reserve := r.memCalc.calcMemFromFileSize(fileSize)
	var memLimitRelease func()
	memLimitRelease, err = r.memLimiter.TryAcquireN(ctx, reserve)
	if err != nil {
		fileLimitRelease()
		return nil, fmt.Errorf("unable to allocate memory with memory limiter: %w", err)
	}

	r.metricsSvc.RcloneCalcMemUsageInc(reserve)
	r.metricsSvc.RcloneCalcFileNumInc()
	r.metricsSvc.RcloneCalcFileSizeInc(fileSize)
	release := func() {
		fileLimitRelease()
		memLimitRelease()
		r.metricsSvc.RcloneCalcMemUsageDec(reserve)
		r.metricsSvc.RcloneCalcFileNumDec()
		r.metricsSvc.RcloneCalcFileSizeDec(fileSize)
	}
	return release, nil
}
