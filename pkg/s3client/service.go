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

package s3client

import (
	"context"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/s3"
)

type Service interface {
	GetByName(ctx context.Context, user, storageName string) (Client, error)
	DefaultRegion() string
}

type Client interface {
	Config() s3.Storage
	Name() string
	S3() *S3
	AWS() *AWS
	SNS() *sns.Client
	Do(req *http.Request) (*http.Response, bool, error)
	IsOnline() bool
}

func New(ctx context.Context, conf *s3.StorageConfig, metricsSvc metrics.S3Service, tp trace.TracerProvider) (Service, error) {
	s := &svc{
		conf:     conf,
		_clients: make(map[string]Client, len(conf.Storages)),
	}

	for storage, val := range conf.Storages {
		clientConf := val
		for user := range clientConf.Credentials {
			c, err := newClient(ctx, clientConf, storage, user, metricsSvc, tp)
			if err != nil {
				return nil, fmt.Errorf("unable to create client for storage %q: %w", storage, err)
			}
			s._clients[clientName(storage, user)] = c
		}
	}

	return s, nil
}

type svc struct {
	_clients map[string]Client
	conf     *s3.StorageConfig
}

func clientName(storage, user string) string {
	return storage + ":" + user
}

func (s *svc) getClient(ctx context.Context, user, storage string) (Client, error) {
	c, ok := s._clients[clientName(storage, user)]
	if !ok {
		return nil, fmt.Errorf("%w: storage %q, user %q not exists", dom.ErrInvalidStorageConfig, storage, user)
	}

	if !c.IsOnline() {
		zerolog.Ctx(ctx).Warn().Str(log.Storage, storage).Msg("storage is offline")
	}
	return c, nil
}

func (s *svc) DefaultRegion() string {
	return s.conf.DefaultRegion
}

func (s *svc) GetByName(ctx context.Context, user, storageName string) (Client, error) {
	logger := zerolog.Ctx(ctx).With().Str(log.Storage, storageName).Logger()
	c, err := s.getClient(ctx, user, storageName)
	if err != nil {
		return nil, err
	}
	if !c.IsOnline() {
		logger.Warn().Msg("storage is offline")
	}
	return c, nil
}
