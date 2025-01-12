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

package rclone

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/util"
	_ "github.com/rclone/rclone/backend/s3"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type File struct {
	Storage string
	Bucket  string
	Name    string
}

type CompareRes struct {
	SrcStor  string
	DestStor string
	Bucket   string
	IsMatch  bool

	MissFrom []string
	MissTo   []string
	Differ   []string
	Error    []string
	Match    []string
}

func (f File) path() string {
	return f.Name
}

type Service interface {
	CopyTo(ctx context.Context, from, to File, size int64) error

	Compare(ctx context.Context, listMatch bool, from, to, fromBucket string, toBucket *string) (*CompareRes, error)
}

func New(conf *s3.StorageConfig, jsonLog bool, metricsSvc metrics.S3Service, mamCalc *MemCalculator, memLimiter, fileLimiter ratelimit.Semaphore) (Service, error) {
	if len(conf.Storages) == 0 {
		return nil, dom.ErrInvalidStorageConfig
	}
	s3, err := fs.Find("s3")
	if err != nil {
		return nil, err
	}

	s := svc{s3: s3, _configs: make(map[string]*configmap.Map, len(conf.Storages)), metricsSvc: metricsSvc, mamCalc: mamCalc, memLimiter: memLimiter, fileLimiter: fileLimiter}

	for storName, stor := range conf.Storages {
		for user, cred := range stor.Credentials {
			name := storName + ":" + user
			scm := configmap.Simple{}
			keyValues := rc.Params{
				"env_auth":          false,
				"access_key_id":     cred.AccessKeyID,
				"secret_access_key": cred.SecretAccessKey,
				"endpoint":          stor.Address,
				"provider":          stor.Provider,
			}
			for k, v := range keyValues {
				vStr := fmt.Sprint(v)
				scm.Set(k, vStr)
			}
			cm := fs.ConfigMap(s3, name, scm)
			s._configs[name] = cm
		}
	}

	ci := fs.GetConfig(context.TODO())
	ci.UseJSONLog = jsonLog
	ci.LogLevel = mapLogLvl()
	ci.Metadata = true

	return &s, nil
}

func mapLogLvl() fs.LogLevel {
	switch zerolog.GlobalLevel() {
	case zerolog.ErrorLevel, zerolog.PanicLevel:
		return fs.LogLevelError
	case zerolog.WarnLevel, zerolog.InfoLevel:
		return fs.LogLevelWarning
	default:
		return fs.LogLevelInfo
	}
}

type svc struct {
	s3         *fs.RegInfo
	_configs   map[string]*configmap.Map
	metricsSvc metrics.S3Service

	mamCalc     *MemCalculator
	memLimiter  ratelimit.Semaphore
	fileLimiter ratelimit.Semaphore
}

func (s *svc) getConf(storage, user string) (*configmap.Map, error) {
	name := storage + ":" + user
	res, ok := s._configs[name]
	if !ok {
		return nil, fmt.Errorf("%w: config for storage %q, user %q not found", dom.ErrInvalidStorageConfig, storage, user)
	}
	return res, nil
}

func (s *svc) Compare(ctx context.Context, listMatch bool, from, to, fromBucket string, toBucket *string) (*CompareRes, error) {
	ctx, span := otel.Tracer("").Start(ctx, "rclone.Compare")
	span.SetAttributes(attribute.String("bucket", fromBucket), attribute.String("from", from), attribute.String("to", to))
	defer span.End()

	src, err := s.getFS(ctx, from, fromBucket)
	if err != nil {
		return nil, err
	}
	toBucketName := fromBucket
	if toBucket != nil {
		toBucketName = *toBucket
	}
	dest, err := s.getFS(ctx, to, toBucketName)
	if err != nil {
		return nil, err
	}
	var missingSrcBuf, missingDstBuf, matchBuf, differBuf, errorBuf bytes.Buffer
	opt := &operations.CheckOpt{
		Fdst: dest,
		Fsrc: src,
		Check: func(ctx context.Context, dst, src fs.Object) (differ bool, noHash bool, err error) {
			same, ht, err := operations.CheckHashes(ctx, src, dst)
			if err != nil {
				return true, false, err
			}
			if ht == hash.None {
				return false, true, nil
			}
			if !same {
				return true, false, nil
			}
			return false, false, nil
		},
		OneWay:       false,
		MissingOnSrc: &missingSrcBuf,
		MissingOnDst: &missingDstBuf,
		Differ:       &differBuf,
		Error:        &errorBuf,
	}
	if listMatch {
		opt.Match = &matchBuf
	}

	err = operations.Check(ctx, opt)
	if err != nil && !fserrors.IsCounted(err) {
		return nil, err
	}

	return &CompareRes{
		SrcStor:  from,
		DestStor: to,
		Bucket:   fromBucket,
		IsMatch:  err == nil,
		MissFrom: readFileNames(missingSrcBuf.Bytes()),
		MissTo:   readFileNames(missingDstBuf.Bytes()),
		Differ:   readFileNames(differBuf.Bytes()),
		Error:    readFileNames(errorBuf.Bytes()),
		Match:    readFileNames(matchBuf.Bytes()),
	}, nil
}

func readFileNames(in []byte) []string {
	var res []string
	reader := bufio.NewScanner(bytes.NewReader(in))
	for reader.Scan() {
		res = append(res, reader.Text())
	}
	return res
}

func (s *svc) CopyTo(ctx context.Context, from, to File, size int64) (err error) {
	ctx, span := otel.Tracer("").Start(ctx, "rclone.CopyTo")
	span.SetAttributes(attribute.String("bucket", from.Bucket), attribute.String("object", from.Name),
		attribute.String("from", from.Storage), attribute.String("to", to.Storage), attribute.Int64("size", size))
	defer span.End()
	release, err := s.checkLimit(ctx, size)
	if err != nil {
		return err
	}
	defer release()
	ctx, ci := fs.AddConfig(ctx)
	ci.CheckSum = true
	ci.UseJSONLog = true
	//ci.UseServerModTime = true // todo: test if needed
	//ci.UpdateOlder = true      // todo: test if needed
	ci.Metadata = true
	ci.ErrorOnNoTransfer = true
	//ci.IgnoreErrors = false
	//ci.UseListR = true         // Use recursive list if available; uses more memory but fewer transactions

	defer func() {
		if err != nil {
			return
		}
		s.metricsSvc.Count(xctx.GetFlow(ctx), from.Storage, s3.HeadObject)
		s.metricsSvc.Count(xctx.GetFlow(ctx), from.Storage, s3.GetObject)
		s.metricsSvc.Count(xctx.GetFlow(ctx), from.Storage, s3.GetObjectAcl)
		s.metricsSvc.Count(xctx.GetFlow(ctx), to.Storage, s3.HeadObject)
		s.metricsSvc.Count(xctx.GetFlow(ctx), to.Storage, s3.PutObject)
		s.metricsSvc.Count(xctx.GetFlow(ctx), to.Storage, s3.PutObjectAcl)
		if size != 0 {
			s.metricsSvc.Download(xctx.GetFlow(ctx), from.Storage, from.Bucket, int(size))
			s.metricsSvc.Upload(xctx.GetFlow(ctx), to.Storage, to.Bucket, int(size))
		}
	}()

	src, err := s.getFS(ctx, from.Storage, from.Bucket)
	if err != nil {
		return err
	}
	dest, err := s.getFS(ctx, to.Storage, to.Bucket)
	if err != nil {
		return err
	}
	zerolog.Ctx(ctx).Debug().
		Str("file_size", util.ByteCountSI(size)).
		Msg("starting obj copy")
	err = operations.CopyFile(ctx, dest, src, to.path(), from.path())
	if err != nil && err.Error() == "object not found" {
		// todo: handle dom.ErrNotFound in worker
		return dom.ErrNotFound
	}
	return
}

func (s *svc) getFS(ctx context.Context, storage, bucket string) (fs.Fs, error) {
	storageConf, err := s.getConf(storage, xctx.GetUser(ctx))
	if err != nil {
		return nil, err
	}

	configName, fsPath := storage, bucket
	return s.s3.NewFs(ctx, configName, fsPath, storageConf)
}

func (s *svc) checkLimit(ctx context.Context, fileSize int64) (release func(), err error) {
	fileLimitRelease, err := s.fileLimiter.TryAcquire(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			fileLimitRelease()
		}
	}()

	reserve := s.mamCalc.calcMemFromFileSize(fileSize)
	var memLimitRelease func()
	memLimitRelease, err = s.memLimiter.TryAcquireN(ctx, reserve)
	if err != nil {
		return
	}

	s.metricsSvc.RcloneCalcMemUsageInc(reserve)
	s.metricsSvc.RcloneCalcFileNumInc()
	s.metricsSvc.RcloneCalcFileSizeInc(fileSize)
	release = func() {
		fileLimitRelease()
		memLimitRelease()
		s.metricsSvc.RcloneCalcMemUsageDec(reserve)
		s.metricsSvc.RcloneCalcFileNumDec()
		s.metricsSvc.RcloneCalcFileSizeDec(fileSize)
	}
	return
}
