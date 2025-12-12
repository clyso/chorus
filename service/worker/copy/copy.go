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

package copy

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net/http"
	"net/url"
	"time"

	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/util"
)

// TODO:
// swift client metrics
const (
	CChorusSourceVersionIDMetaHeader = "x-amz-meta-chorus-source-version-id"

	CRGWVersionMigrationQueryParam = "rgwx-version-id"
)

type Bucket struct {
	Storage string
	Bucket  string
}

func NewBucket(storage string, bucket string) Bucket {
	return Bucket{
		Storage: storage,
		Bucket:  bucket,
	}
}

type File struct {
	Storage string
	Bucket  string
	Name    string
	Version string
}

func NewVersionedFile(storage string, bucket string, name string, version string) File {
	return File{
		Storage: storage,
		Bucket:  bucket,
		Name:    name,
		Version: version,
	}
}

func NewFile(storage string, bucket string, name string) File {
	return File{
		Storage: storage,
		Bucket:  bucket,
		Name:    name,
	}
}

type ObjectInfo struct {
	LastModified time.Time
	Key          string
	VersionID    string
	Etag         string
	StorageClass string
	Size         uint64
}

type CopySvc interface {
	BucketObjects(ctx context.Context, user string, bucket Bucket, opts ...func(o *minio.ListObjectsOptions)) iter.Seq2[ObjectInfo, error]
	IsBucketVersioned(ctx context.Context, user string, bucket Bucket) (bool, error)
	GetVersionInfo(ctx context.Context, user string, to File) ([]entity.ObjectVersionInfo, error)
	DeleteDestinationObject(ctx context.Context, user string, to File) error
	GetLastMigratedVersionInfo(ctx context.Context, user string, to File) (entity.ObjectVersionInfo, error)
	CopyObject(ctx context.Context, user string, from File, to File) error
	CopyACLs(ctx context.Context, user string, from File, to File) error
}

var _ CopySvc = (*S3CopySvc)(nil)

type S3CopySvc struct {
	clientRegistry objstore.Clients
	metricsSvc     metrics.WorkerService
}

func NewS3CopySvc(clientRegistry objstore.Clients, metricsSvc metrics.WorkerService) *S3CopySvc {
	return &S3CopySvc{
		clientRegistry: clientRegistry,
		metricsSvc:     metricsSvc,
	}
}

func WithVersions() func(o *minio.ListObjectsOptions) {
	return func(o *minio.ListObjectsOptions) {
		o.WithVersions = true
	}
}

func WithPrefix(prefix string) func(o *minio.ListObjectsOptions) {
	return func(o *minio.ListObjectsOptions) {
		o.Prefix = prefix
	}
}

func WithAfter(after string) func(o *minio.ListObjectsOptions) {
	return func(o *minio.ListObjectsOptions) {
		o.StartAfter = after
	}
}

func (r *S3CopySvc) BucketObjects(ctx context.Context, user string, bucket Bucket, opts ...func(o *minio.ListObjectsOptions)) iter.Seq2[ObjectInfo, error] {
	storageClient, err := r.clientRegistry.AsS3(ctx, bucket.Storage, user)
	if err != nil {
		return func(yield func(ObjectInfo, error) bool) {
			yield(ObjectInfo{}, fmt.Errorf("unable to get %s storage client: %w", bucket.Storage, err))
		}
	}

	listOpts := minio.ListObjectsOptions{}
	for _, opt := range opts {
		opt(&listOpts)
	}

	objects := storageClient.S3().ListObjects(ctx, bucket.Bucket, listOpts)
	return func(yield func(ObjectInfo, error) bool) {
		for object := range objects {
			err := object.Err
			// skip if bucket not found
			if err != nil && minio.ToErrorResponse(err).Code == minio.NoSuchBucket {
				return
			}
			info := ObjectInfo{
				Key:          object.Key,
				VersionID:    object.VersionID,
				LastModified: object.LastModified,
				Etag:         object.ETag,
				Size:         uint64(object.Size),
				StorageClass: object.StorageClass,
			}
			if !yield(info, err) {
				return
			}
			if err != nil {
				return
			}
		}
	}
}

func (r *S3CopySvc) IsBucketVersioned(ctx context.Context, user string, bucket Bucket) (bool, error) {
	storageClient, err := r.clientRegistry.AsS3(ctx, bucket.Storage, user)
	if err != nil {
		return false, fmt.Errorf("unable to get %s storage client: %w", bucket.Storage, err)
	}

	versioningConfig, err := storageClient.S3().GetBucketVersioning(ctx, bucket.Bucket)
	if err != nil {
		return false, fmt.Errorf("unable to get storage rversioning config: %w", err)
	}

	return versioningConfig.Enabled(), nil
}

func (r *S3CopySvc) GetVersionInfo(ctx context.Context, user string, to File) ([]entity.ObjectVersionInfo, error) {
	storageClient, err := r.clientRegistry.AsS3(ctx, to.Storage, user)
	if err != nil {
		return nil, fmt.Errorf("unable to get %s storage client: %w", to.Storage, err)
	}

	objects := storageClient.S3().ListObjects(ctx, to.Bucket, minio.ListObjectsOptions{
		WithVersions: true,
		Prefix:       to.Name,
	})

	result := []entity.ObjectVersionInfo{}
	for object := range objects {
		if object.Err != nil {
			return nil, fmt.Errorf("unable to list versions: %w", object.Err)
		}

		info := entity.ObjectVersionInfo{
			Version: object.VersionID,
			Size:    uint64(object.Size),
		}

		result = append(result, info)
	}

	return result, nil
}

func (r *S3CopySvc) GetLastMigratedVersionInfo(ctx context.Context, user string, to File) (entity.ObjectVersionInfo, error) {
	toClient, err := r.clientRegistry.AsS3(ctx, to.Storage, user)
	if err != nil {
		return entity.ObjectVersionInfo{}, fmt.Errorf("unable to get to client: %w", err)
	}

	objectStat, err := toClient.S3().StatObject(ctx, to.Bucket, to.Name, minio.StatObjectOptions{})
	if err != nil {
		var errorResp minio.ErrorResponse
		ok := errors.As(err, &errorResp)
		if !ok {
			return entity.ObjectVersionInfo{}, fmt.Errorf("unable to cast error to s3 error %w", err)
		}
		if errorResp.StatusCode == http.StatusNotFound {
			return entity.ObjectVersionInfo{}, dom.ErrNotFound
		}
	}

	sourceVersionID := objectStat.Metadata.Get(CChorusSourceVersionIDMetaHeader)

	if sourceVersionID == "" {
		return entity.ObjectVersionInfo{}, dom.ErrInvalidArg
	}

	return entity.ObjectVersionInfo{
		Version: sourceVersionID,
		Size:    uint64(objectStat.Size),
	}, nil
}

func (r *S3CopySvc) DeleteDestinationObject(ctx context.Context, user string, to File) error {
	toClient, err := r.clientRegistry.AsS3(ctx, to.Storage, user)
	if err != nil {
		return fmt.Errorf("unable to get to client: %w", err)
	}

	if err := toClient.S3().RemoveObject(ctx, to.Bucket, to.Name, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("unable to remove object: %w", err)
	}
	return nil
}

func (r *S3CopySvc) CopyObject(ctx context.Context, user string, from File, to File) error {
	fromClient, err := r.clientRegistry.AsS3(ctx, from.Storage, user)
	if err != nil {
		return fmt.Errorf("unable to get from client: %w", err)
	}
	toClient, err := r.clientRegistry.AsS3(ctx, to.Storage, user)
	if err != nil {
		return fmt.Errorf("unable to get to client: %w", err)
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
		if errorResp.Code == minio.NoSuchKey || errorResp.StatusCode == http.StatusNotFound {
			return dom.ErrNotFound
		}
		return fmt.Errorf("unable to get from object stat: %w", err)
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
		if errorResp.Code == minio.NoSuchKey || errorResp.StatusCode == http.StatusNotFound {
			toObjectPresent = false
		} else {
			return fmt.Errorf("unable to get to object stat: %w", err)
		}
	}

	if toObjectPresent && fromObjectStat.ETag == toObjectStat.ETag && fromObjectStat.Size == toObjectStat.Size {
		return nil
	}

	fromObjectSize := fromObjectStat.Size

	ctx, span := otel.Tracer("").Start(ctx, "copy.CopyToWithVersion")
	span.SetAttributes(attribute.String("bucket", from.Bucket), attribute.String("object", from.Name),
		attribute.String("version", from.Version),
		attribute.String("from", from.Storage), attribute.String("to", to.Storage), attribute.Int64("size", fromObjectSize))
	defer span.End()

	provider := toClient.Config().Provider

	putObjectOpts := minio.PutObjectOptions{
		UserMetadata: fromObjectStat.UserMetadata,
		ContentType:  fromObjectStat.ContentType,
	}

	if features.Tagging(ctx) {
		putObjectOpts.UserTags = fromObjectStat.UserTags
	}

	if to.Version != "" {
		putObjectOpts.UserMetadata[CChorusSourceVersionIDMetaHeader] = to.Version

		switch provider {
		case s3.ProviderCeph:
			putObjectOpts.Internal.CustomQueryParams = url.Values{
				CRGWVersionMigrationQueryParam: []string{to.Version},
			}
		case s3.ProviderMinIO:
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

	r.metricsSvc.WorkerInProgressBytesInc(ctx, fromObjectSize)
	defer r.metricsSvc.WorkerInProgressBytesDec(ctx, fromObjectSize)
	fromObject, err := fromClient.S3().GetObject(ctx, from.Bucket, from.Name, minio.GetObjectOptions{
		VersionID: from.Version,
	})
	if err != nil {
		return fmt.Errorf("unable to get from object: %w", err)
	}
	defer fromObject.Close()
	stat, err := fromObject.Stat()
	if err != nil {
		return fmt.Errorf("unable to stat from object: %w", err)
	}
	if stat.Size == 0 {
		// workaround for e2e test test/object_test.go TestApi_Object_Folder
		putObjectOpts.DisableContentSha256 = true
	}
	info, err := toClient.S3().PutObject(ctx, to.Bucket, to.Name, fromObject, stat.Size, putObjectOpts)
	if err != nil {
		return fmt.Errorf("unable to upload object: %w", err)
	}
	if info.VersionID != "" {
		to.Version = info.VersionID
	}

	if features.ACL(ctx) {
		if err := r.CopyACLs(ctx, user, from, to); err != nil {
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

	return nil
}

func (r *S3CopySvc) CopyACLs(ctx context.Context, user string, from File, to File) error {
	fromClient, err := r.clientRegistry.AsS3(ctx, from.Storage, user)
	if err != nil {
		return fmt.Errorf("unable to get from client: %w", err)
	}
	toClient, err := r.clientRegistry.AsS3(ctx, to.Storage, user)
	if err != nil {
		return fmt.Errorf("unable to get to client: %w", err)
	}
	// check if copy ACL supported for versioned objects with the same version ID
	toProvider := toClient.Config().Provider
	isVersioned := from.Version != ""
	sameVersion := from.Version == to.Version
	if isVersioned && sameVersion && !toProvider.SupportsRetainVersionID() {
		// TODO: iterate over from/to versions and find "to" version with the same index(or eTag?) as "from" version???
		zerolog.Ctx(ctx).Warn().Msgf("skip object ACL sync: same version IDs on different storages not supported for provider %q", toProvider)
		return nil
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
		AccessControlPolicy: MapOwnersACL(fromACL.Owner, fromACL.Grants, toOwnerID, features.PreserveACLGrants(ctx)),
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

func MapOwnersACL(srcOwner *awss3.Owner, srcGrants []*awss3.Grant, dstOwner *string, preserveACLGrants bool) *awss3.AccessControlPolicy {
	destGrants := make([]*awss3.Grant, 0, len(srcGrants))
	for _, grant := range srcGrants {
		var destID *string
		if preserveACLGrants {
			destID = grant.Grantee.ID
		} else {
			destID = srcOwnerToDestOwner(grant.Grantee.ID, srcOwner.ID, dstOwner)
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
			ID:          srcOwnerToDestOwner(srcOwner.ID, srcOwner.ID, dstOwner),
			DisplayName: srcOwner.DisplayName,
		}
	}

	return res
}

func srcOwnerToDestOwner(owner, srcBucketOwner, dstBucketOwner *string) *string {
	if owner == nil || *owner != *srcBucketOwner {
		return owner
	}
	return dstBucketOwner
}
