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
	"errors"
	"fmt"

	_ "github.com/rclone/rclone/backend/s3"
	_ "github.com/rclone/rclone/backend/swift"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/swift"
	"github.com/clyso/chorus/pkg/util"
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

type CompareRes struct {
	SrcStor  string
	DestStor string
	Bucket   string

	MissFrom []string
	MissTo   []string
	Differ   []string
	Error    []string
	Match    []string
	IsMatch  bool
}

func (f File) path() string {
	return f.Name
}

type Service interface {
	CopyTo(ctx context.Context, user string, from, to File, size int64) error

	Compare(ctx context.Context, listMatch bool, user, from, to, fromBucket string, toBucket string) (*CompareRes, error)
}

var supported = map[dom.StorageType]string{
	dom.S3:    "s3",
	dom.Swift: "swift",
}

func New(storagesConf objstore.Config, jsonLog bool, metricsSvc metrics.S3Service, mamCalc *MemCalculator, memLimiter, fileLimiter ratelimit.Semaphore) (Service, error) {
	var err error
	fsMap := make(map[dom.StorageType]*fs.RegInfo, len(supported))
	res := svc{_configs: make(map[string]config), metricsSvc: metricsSvc, memCalc: mamCalc, memLimiter: memLimiter, fileLimiter: fileLimiter}
	for storName, stor := range storagesConf.Storages {
		fsInfo, ok := fsMap[stor.Type]
		if !ok {
			// init fs type and add to cache
			fsName, ok := supported[stor.Type]
			if !ok {
				return nil, fmt.Errorf("%w: storage type %q not supported by RClone", dom.ErrInvalidStorageConfig, stor.Type)
			}
			fsInfo, err = fs.Find(fsName)
			if err != nil {
				return nil, fmt.Errorf("%w: storage type %q not supported by RClone", dom.ErrInvalidStorageConfig, fsName)
			}
			fsMap[stor.Type] = fsInfo
		}
		switch stor.Type {
		case dom.S3:
			for user := range stor.S3.Credentials {
				name := configKey(storName, user)
				scm := s3ConfigMap(stor.S3, user)
				cm := fs.ConfigMap(fsInfo.Prefix, fsInfo.Options, name, scm)
				res._configs[name] = config{fs: fsInfo, cm: cm}
			}
		case dom.Swift:
			for user := range stor.Swift.Credentials {
				name := configKey(storName, user)
				scm := swiftConfigMap(stor.Swift, user)
				cm := fs.ConfigMap(fsInfo.Prefix, fsInfo.Options, name, scm)
				res._configs[name] = config{fs: fsInfo, cm: cm}
			}
		default:
			return nil, fmt.Errorf("%w: storage type %q not supported by RClone", dom.ErrInvalidStorageConfig, stor.Type)
		}
	}

	ci := fs.GetConfig(context.TODO())
	ci.UseJSONLog = jsonLog
	ci.LogLevel = mapLogLvl()
	ci.Metadata = true

	return &res, nil
}

func s3ConfigMap(stor *s3.Storage, user string) configmap.Simple {
	cred := stor.Credentials[user]
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
	return scm
}

func swiftConfigMap(stor *swift.Storage, user string) configmap.Simple {
	cred := stor.Credentials[user]
	scm := configmap.Simple{}
	keyValues := rc.Params{
		"env_auth": false,
		"user":     cred.Username,   //OS_USERNAME
		"key":      cred.Password,   //OS_PASSWORD
		"auth_url": stor.AuthURL,    //OS_AUTH_URL
		"domain":   cred.DomainName, //OS_USER_DOMAIN_NAME
		"tenant":   cred.TenantName, //OS_TENANT_NAME
	}
	if stor.Region != "" {
		keyValues["region"] = stor.Region
	}
	for k, v := range keyValues {
		vStr := fmt.Sprint(v)
		scm.Set(k, vStr)
	}
	return scm
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
	_configs   map[string]config
	metricsSvc metrics.S3Service

	memCalc     *MemCalculator
	memLimiter  ratelimit.Semaphore
	fileLimiter ratelimit.Semaphore
}

type config struct {
	fs *fs.RegInfo
	cm *configmap.Map
}

func configKey(storage, user string) string {
	return storage + ":" + user
}

func (s *svc) getConf(storage, user string) (config, error) {
	name := configKey(storage, user)
	res, ok := s._configs[name]
	if !ok {
		return config{}, fmt.Errorf("%w: config for storage %q, user %q not found", dom.ErrInvalidStorageConfig, storage, user)
	}
	return res, nil
}

func (s *svc) Compare(ctx context.Context, listMatch bool, user, from, to, fromBucket string, toBucket string) (*CompareRes, error) {
	ctx, span := otel.Tracer("").Start(ctx, "rclone.Compare")
	span.SetAttributes(attribute.String("bucket", fromBucket), attribute.String("from", from), attribute.String("to", to))
	defer span.End()

	src, err := s.getFS(ctx, user, from, fromBucket)
	if err != nil {
		return nil, err
	}
	dest, err := s.getFS(ctx, user, to, toBucket)
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
	// don't return an error on ErrorDirNotFound to maintain api backwards compatibility
	// with older versions of rclone
	if err != nil && !fserrors.IsCounted(err) && !errors.Is(err, fs.ErrorDirNotFound) {
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

func (s *svc) CopyTo(ctx context.Context, user string, from, to File, size int64) (err error) {
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

	src, err := s.getFS(ctx, user, from.Storage, from.Bucket)
	if err != nil {
		return err
	}
	dest, err := s.getFS(ctx, user, to.Storage, to.Bucket)
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

func (s *svc) getFS(ctx context.Context, user, storage, bucket string) (fs.Fs, error) {
	storageConf, err := s.getConf(storage, user)
	if err != nil {
		return nil, err
	}

	configName, fsPath := storage, bucket
	return storageConf.fs.NewFs(ctx, configName, fsPath, storageConf.cm)
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

	reserve := s.memCalc.calcMemFromFileSize(fileSize)
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
