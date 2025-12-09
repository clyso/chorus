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

package metrics

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	xctx "github.com/clyso/chorus/pkg/ctx"
)

var countRequests = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "storage_requests_total",
		Help: "Number of api calls to storage.",
	},
	[]string{"flow", "storage", "method"},
)

var bytesUpload = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "storage_bucket_bytes_upload",
		Help: "Number of bytes uploaded to storage.",
	},
	[]string{"flow", "storage", "bucket"},
)

var bytesDownload = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "storage_bucket_bytes_download",
		Help: "Number of bytes downloaded from storage.",
	},
	[]string{"flow", "storage", "bucket"},
)

var copyInProgressBytes = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "copy_in_progress_obj_bytes",
		Help: "Size of files currently copied by worker.",
	},
	[]string{"flow", "user", "bucket"},
)

type Service interface {
	Count(flow xctx.Flow, storage string, method string)
	Upload(flow xctx.Flow, storage, bucket string, bytes int)
	Download(flow xctx.Flow, storage, bucket string, bytes int)
}

type WorkerService interface {
	WorkerInProgressBytesInc(ctx context.Context, bytes int64)
	WorkerInProgressBytesDec(ctx context.Context, bytes int64)
}

func NewS3Service(enabled bool) *svc {
	return &svc{enabled: enabled}
}

var _ Service = &svc{}
var _ WorkerService = &svc{}

type svc struct {
	enabled bool
}

func (s svc) WorkerInProgressBytesInc(ctx context.Context, bytes int64) {
	if !s.enabled {
		return
	}
	labels := prometheus.Labels{}
	if flow := xctx.GetFlow(ctx); flow != "" {
		labels["flow"] = string(flow)
	}
	if user := xctx.GetUser(ctx); user != "" {
		labels["user"] = user
	}
	if bucket := xctx.GetBucket(ctx); bucket != "" {
		labels["bucket"] = bucket
	}
	copyInProgressBytes.With(labels).Add(float64(bytes))
}

func (s svc) WorkerInProgressBytesDec(ctx context.Context, bytes int64) {
	if !s.enabled {
		return
	}
	labels := prometheus.Labels{}
	if flow := xctx.GetFlow(ctx); flow != "" {
		labels["flow"] = string(flow)
	}
	if user := xctx.GetUser(ctx); user != "" {
		labels["user"] = user
	}
	if bucket := xctx.GetBucket(ctx); bucket != "" {
		labels["bucket"] = bucket
	}
	copyInProgressBytes.With(labels).Sub(float64(bytes))
}

func (s svc) Count(flow xctx.Flow, storage string, method string) {
	if !s.enabled {
		return
	}
	countRequests.With(prometheus.Labels{
		"flow":    string(flow),
		"storage": storage,
		"method":  method}).Inc()
}

func (s svc) Upload(flow xctx.Flow, storage, bucket string, bytes int) {
	if !s.enabled {
		return
	}
	bytesUpload.With(prometheus.Labels{
		"flow":    string(flow),
		"storage": storage,
		"bucket":  bucket}).Add(float64(bytes))
}

func (s svc) Download(flow xctx.Flow, storage, bucket string, bytes int) {
	if !s.enabled {
		return
	}
	bytesDownload.With(prometheus.Labels{
		"flow":    string(flow),
		"storage": storage,
		"bucket":  bucket}).Add(float64(bytes))
}
