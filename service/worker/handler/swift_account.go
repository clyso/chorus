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

package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/lock"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/accounts"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"
)

func (s *svc) HandleAccountUpdate(ctx context.Context, t *asynq.Task) (err error) {
	var p tasks.AccountUpdatePayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("AccountUpdatePayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	logger := zerolog.Ctx(ctx)

	//TODO:swift support account-level repl policy
	paused, err := s.policySvc.IsReplicationPolicyPaused(ctx, p.FromAccount, "", p.FromStorage, p.ToStorage, nil)
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			zerolog.Ctx(ctx).Err(err).Msg("drop replication task: replication policy not found")
			return nil
		}
		return err
	}
	if paused {
		return &dom.ErrRateLimitExceeded{RetryIn: s.conf.PauseRetryInterval}
	}

	if err = s.limit.StorReq(ctx, p.FromStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.FromStorage).Msg("rate limit error")
		return err
	}
	if err = s.limit.StorReq(ctx, p.ToStorage); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ToStorage).Msg("rate limit error")
		return err
	}
	defer func() {
		if err != nil {
			return
		}
		//TODO:swift support account-level repl policy
		verErr := s.policySvc.IncReplEventsDone(ctx, p.FromAccount, "", p.FromStorage, p.ToStorage, nil, p.CreatedAt)
		if verErr != nil {
			zerolog.Ctx(ctx).Err(verErr).Msg("unable to inc processed events")
		}
	}()

	release, refresh, err := s.locker.Lock(ctx, lock.UserKey(p.FromAccount))
	if err != nil {
		return err
	}
	defer release()
	err = lock.WithRefresh(ctx, func() error {
		return s.handleAccountUpdate(ctx, p)
	}, refresh, time.Second*2)

	return
}

// handleAccountUpdate handles the account update task, copying metadata from the source account to the destination account.
func (s *svc) handleAccountUpdate(ctx context.Context, p tasks.AccountUpdatePayload) (err error) {
	fromClient, err := s.swiftClients.For(ctx, p.FromStorage, p.FromAccount)
	if err != nil {
		return err
	}
	fromHeaders, fromMeta, err := getSwiftAccMeta(ctx, fromClient)
	if err != nil {
		return fmt.Errorf("failed to get source account %q headers: %w", p.FromAccount, err)
	}
	toClient, err := s.swiftClients.For(ctx, p.ToStorage, p.ToAccount)
	if err != nil {
		return err
	}
	toHeaders, toMeta, err := getSwiftAccMeta(ctx, toClient)
	if err != nil {
		return fmt.Errorf("failed to get destination account %q headers: %w", p.ToAccount, err)
	}
	// update destination account metadata
	updateOpts := accountCopyMetaRequest(fromHeaders, fromMeta, toHeaders, toMeta)
	err = accounts.Update(ctx, toClient, updateOpts).Err
	if err != nil {
		return fmt.Errorf("failed to update destination account %q headers: %w", p.ToAccount, err)
	}
	return nil
}

func getSwiftAccMeta(ctx context.Context, client *gophercloud.ServiceClient) (headers *accounts.GetHeader, meta map[string]string, err error) {
	res := accounts.Get(ctx, client, accounts.GetOpts{
		Newest: true,
	})
	if res.Err != nil {
		return nil, nil, res.Err
	}
	meta, err = res.ExtractMetadata()
	if err != nil {
		return nil, nil, err
	}
	if meta == nil {
		meta = make(map[string]string)
	}
	headers, err = res.Extract()
	if err != nil {
		return nil, nil, err
	}
	return
}

func accountCopyMetaRequest(fromHeaders *accounts.GetHeader, fromMeta map[string]string, toHeaders *accounts.GetHeader, toMeta map[string]string) *accounts.UpdateOpts {
	updateOpts := accounts.UpdateOpts{
		Metadata:       make(map[string]string),
		RemoveMetadata: []string{},
		TempURLKey:     fromHeaders.TempURLKey,
		TempURLKey2:    fromHeaders.TempURLKey2,
	}

	// copy metadata from source to destination
	for k, v := range fromMeta {
		updateOpts.Metadata[k] = v
	}

	// collect remove metadata keys
	for k := range toMeta {
		if _, ok := fromMeta[k]; !ok {
			// remove only if not exists in source
			updateOpts.RemoveMetadata = append(updateOpts.RemoveMetadata, k)
		}
	}
	if fromHeaders.ContentType != "" {
		updateOpts.ContentType = &fromHeaders.ContentType
	}
	return &updateOpts
}
