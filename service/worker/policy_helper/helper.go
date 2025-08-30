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

package policy_helper

import (
	"context"
	"errors"

	"golang.org/x/sync/errgroup"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/s3"
)

func CreateMainFollowerRouting(
	ctx context.Context,
	conf s3.StorageConfig,
	policySvc policy.Service) error {

	g, ctx := errgroup.WithContext(ctx)

	for u := range conf.Storages[conf.Main()].Credentials {
		user := u
		if conf.CreateRouting {
			g.Go(func() error {
				return createRouting(ctx, policySvc, user, conf.Main())
			})
		}
	}
	return g.Wait()
}

func createRouting(
	ctx context.Context,
	policySvc policy.Service,
	user, main string) error {

	err := policySvc.AddUserRoutingPolicy(ctx, user, main)
	if err != nil {
		if errors.Is(err, dom.ErrAlreadyExists) {
			return nil
		}
		return err
	}

	return nil
}
