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
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var countRequests = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "storage_requests_total",
		Help: "Number of api calls to s3 storage.",
	},
	[]string{"flow", "storage", "method"},
)

var bytesUpload = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "storage_bucket_bytes_upload",
		Help: "Number of bytes uploaded to s3 storage.",
	},
	[]string{"flow", "storage", "bucket"},
)

var bytesDownload = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "storage_bucket_bytes_download",
		Help: "Number of bytes downloaded from s3 storage.",
	},
	[]string{"flow", "storage", "bucket"},
)

var rcloneCalcUsage = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "rclone_calc_mem_usage",
		Help: "Calculated rclone_memory_usage.",
	},
)

var rcloneFilesSize = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "rclone_file_size_processing",
		Help: "Size of files currently processed with rclone.",
	},
)

var rcloneFilesNum = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "rclone_file_num_processing",
		Help: "Amount of files currently processed with rclone.",
	},
)

type S3Service interface {
	Count(flow xctx.Flow, storage string, method s3.Method)
	Upload(flow xctx.Flow, storage, bucket string, bytes int)
	Download(flow xctx.Flow, storage, bucket string, bytes int)

	RcloneCalcMemUsageInc(bytes int64)
	RcloneCalcMemUsageDec(bytes int64)
	RcloneCalcFileSizeInc(bytes int64)
	RcloneCalcFileSizeDec(bytes int64)
	RcloneCalcFileNumInc()
	RcloneCalcFileNumDec()
}

func NewS3Service(enabled bool) S3Service {
	return &svcS3{enabled: enabled}
}

type svcS3 struct {
	enabled bool
}

func (s svcS3) RcloneCalcMemUsageInc(bytes int64) {
	if !s.enabled {
		return
	}
	rcloneCalcUsage.Add(float64(bytes))
}

func (s svcS3) RcloneCalcMemUsageDec(bytes int64) {
	if !s.enabled {
		return
	}
	rcloneCalcUsage.Sub(float64(bytes))
}

func (s svcS3) RcloneCalcFileSizeInc(bytes int64) {
	if !s.enabled {
		return
	}
	rcloneFilesSize.Add(float64(bytes))
}

func (s svcS3) RcloneCalcFileSizeDec(bytes int64) {
	if !s.enabled {
		return
	}
	rcloneFilesSize.Sub(float64(bytes))
}

func (s svcS3) RcloneCalcFileNumInc() {
	if !s.enabled {
		return
	}
	rcloneFilesNum.Add(float64(1))
}

func (s svcS3) RcloneCalcFileNumDec() {
	if !s.enabled {
		return
	}
	rcloneFilesNum.Sub(float64(1))
}

func (s svcS3) Count(flow xctx.Flow, storage string, method s3.Method) {
	if !s.enabled {
		return
	}
	countRequests.With(prometheus.Labels{
		"flow":    string(flow),
		"storage": storage,
		"method":  method.String()}).Inc()
}

func (s svcS3) Upload(flow xctx.Flow, storage, bucket string, bytes int) {
	if !s.enabled {
		return
	}
	bytesUpload.With(prometheus.Labels{
		"flow":    string(flow),
		"storage": storage,
		"bucket":  bucket}).Add(float64(bytes))
}

func (s svcS3) Download(flow xctx.Flow, storage, bucket string, bytes int) {
	if !s.enabled {
		return
	}
	bytesDownload.With(prometheus.Labels{
		"flow":    string(flow),
		"storage": storage,
		"bucket":  bucket}).Add(float64(bytes))
}
