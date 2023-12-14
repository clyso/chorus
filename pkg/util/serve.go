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

package util

import (
	"context"
	"fmt"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
	"sync"
)

func NewServer() *server {
	return &server{}
}

type server struct {
	workers []worker
}

type worker struct {
	name    string
	work    func(ctx context.Context) error
	cleanUp func(ctx context.Context) error
}

func (s *server) Add(name string, work func(ctx context.Context) error, cleanUp func(ctx context.Context) error) error {
	if work == nil {
		return fmt.Errorf("%w: work func is nil", dom.ErrInvalidArg)
	}
	if name == "" {
		return fmt.Errorf("%w: worker name is empty", dom.ErrInvalidArg)
	}
	s.workers = append(s.workers, worker{
		name:    name,
		work:    work,
		cleanUp: cleanUp,
	})
	return nil
}
func (s *server) Start(ctx context.Context) error {
	if len(s.workers) == 0 {
		return fmt.Errorf("%w: no workers registered", dom.ErrInvalidArg)
	}
	zerolog.Ctx(ctx).Info().Msgf("server: start serving %d workers", len(s.workers))

	g, groupCtx := errgroup.WithContext(ctx)

	for _, wrk := range s.workers {
		w := wrk
		g.Go(func() error {
			zerolog.Ctx(groupCtx).Info().Msgf("server: starting worker %q", w.name)
			err := w.work(groupCtx)
			if err != nil {
				zerolog.Ctx(groupCtx).Err(err).Msgf("server: worker %q returned error", w.name)
			} else {
				zerolog.Ctx(groupCtx).Info().Msgf("server: worker %q done", w.name)
			}
			return err
		})
	}
	cleanWG := sync.WaitGroup{}
	for _, wrk := range s.workers {
		if wrk.cleanUp == nil {
			zerolog.Ctx(ctx).Info().Msgf("server: no cleanup func for worker %q", wrk.name)
			continue
		}
		cleanWG.Add(1)
		go func(name string, fn func(ctx context.Context) error) {
			defer cleanWG.Done()
			<-groupCtx.Done()
			cleanUpCtx := context.Background()
			zerolog.Ctx(cleanUpCtx).Info().Msgf("server: start cleanup for worker %q", name)
			err := fn(cleanUpCtx)
			if err != nil {
				zerolog.Ctx(cleanUpCtx).Err(err).Msgf("server: for worker %q error", name)
			} else {
				zerolog.Ctx(cleanUpCtx).Info().Msgf("server: done cleanup for worker %q", name)
			}
		}(wrk.name, wrk.cleanUp)
	}
	zerolog.Ctx(ctx).Info().Msg("server: start serving")
	err := g.Wait()
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("unable to serve start")
	}
	zerolog.Ctx(ctx).Info().Msg("server: done serving, waiting for cleanup done")
	cleanWG.Wait()
	zerolog.Ctx(ctx).Info().Msg("server: done serving, cleanup done")
	return err
}
