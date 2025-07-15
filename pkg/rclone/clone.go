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

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/util"
)

type CopySvc interface {
	CopyObject(ctx context.Context, from File, to File) error
}

type S3CopySvc struct {
	// logger         *zerolog.Logger
	clientRegistry s3client.Service
	limiterSvc     LimiterSvc
	metricsSvc     metrics.S3Service
}

func NewS3CopySvc(clientRegistry s3client.Service, limiterSvc LimiterSvc, metricsSvc metrics.S3Service) *S3CopySvc {
	return &S3CopySvc{
		clientRegistry: clientRegistry,
		limiterSvc:     limiterSvc,
		metricsSvc:     metricsSvc,
	}
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
	toObjectStat, err := toClient.S3().StatObject(ctx, from.Bucket, from.Name, minio.GetObjectOptions{
		VersionID: from.Version,
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

	release, err := r.limiterSvc.Reserve(ctx, fromObjectSize)
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
		UserTags:     fromObjectStat.UserTags,
	}
	if to.Version != "" {
		putObjectOpts.UserMetadata["x-amz-meta-chorus-source-version-id"] = to.Version

		switch provider {
		case "ceph":
			putObjectOpts.UserMetadata["x-amz-meta-rgwx-version-id"] = to.Version
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
	// fromClient, err := r.clientRegistry.GetByName(ctx, from.Storage)
	// if err != nil {
	// 	return fmt.Errorf("unable to get from client: %w", err)
	// }
	// toClient, err := r.clientRegistry.GetByName(ctx, to.Storage)
	// if err != nil {
	// 	return fmt.Errorf("unable to get to client: %w", err)
	// }

	return nil
}

func (r *S3CopySvc) CopyTags(ctx context.Context) error {
	return nil
}

func (r *S3CopySvc) CopyMetadata(ctx context.Context) error {
	return nil
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
