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

package swift

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/accounts"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/tasks"
)

func (s *svc) HandleAccountUpdate(ctx context.Context, t *asynq.Task) (err error) {
	var p tasks.SwiftAccountUpdatePayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("AccountUpdatePayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	logger := zerolog.Ctx(ctx)

	if err = s.limit.StorReq(ctx, p.ID.FromStorage()); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.FromStorage()).Msg("rate limit error")
		return err
	}
	if err = s.limit.StorReq(ctx, p.ID.ToStorage()); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.ToStorage()).Msg("rate limit error")
		return err
	}

	lock, err := s.userLocker.Lock(ctx, "upd"+p.ID.User())
	if err != nil {
		return err
	}
	defer lock.Release(context.Background())
	return lock.Do(ctx, time.Second*2, func() error {
		return s.AccountUpdate(ctx, p)
	})
}

// AccountUpdate handles the account update task, copying metadata from the source account to the destination account.
func (s *svc) AccountUpdate(ctx context.Context, p tasks.SwiftAccountUpdatePayload) (err error) {
	fromClient, err := s.clients.AsSwift(ctx, p.ID.FromStorage(), p.ID.User())
	if err != nil {
		return err
	}
	fromHeaders, fromMeta, err := getSwiftAccMeta(ctx, fromClient)
	if err != nil {
		return fmt.Errorf("failed to get source account %q headers: %w", p.ID.User(), err)
	}
	toClient, err := s.clients.AsSwift(ctx, p.ID.ToStorage(), p.ID.User())
	if err != nil {
		return err
	}
	toHeaders, toMeta, err := getSwiftAccMeta(ctx, toClient)
	if err != nil {
		return fmt.Errorf("failed to get destination account %q headers: %w", p.ID.User(), err)
	}
	// update destination account metadata
	updateOpts := accountCopyMetaRequest(fromHeaders, fromMeta, toHeaders, toMeta)
	err = accounts.Update(ctx, toClient, updateOpts).Err
	if err != nil {
		return fmt.Errorf("failed to update destination account %q headers: %w", p.ID.User(), err)
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
	}

	// copy metadata from source to destination
	maps.Copy(updateOpts.Metadata, fromMeta)

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
