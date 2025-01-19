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

package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hibiken/asynq"

	"github.com/clyso/chorus/pkg/tasks"
)

func (s *svc) CostsEstimation(ctx context.Context, t *asynq.Task) error {
	var p tasks.CostEstimationPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("CostEstimationPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}

	//from, to := p.FromStorage, p.ToStorage
	//release, refresh, err := s.locker.Lock(ctx, lock.MigrationCostsKey(from, to))
	//if err != nil {
	//	return err
	//}
	//defer release()
	//existing, err := s.metaSvc.GetMigrationCosts(ctx, from, to)
	//if err != nil && !errors.Is(err, dom.ErrNotFound) {
	//	return err
	//}
	//if existing != nil {
	//	if !existing.Done() {
	//		zerolog.Ctx(ctx).Info().Msgf("migration costs %q -> %q already in progress", from, to)
	//		return nil
	//	}
	//	err = s.metaSvc.MigrationCostsDelete(ctx, from, to)
	//	if err != nil {
	//		return err
	//	}
	//}
	//
	//err = lock.WithRefresh(ctx, func() error {
	//	err = s.metaSvc.MigrationCostsStart(ctx, from, to)
	//	if err != nil {
	//		return err
	//	}
	//
	//	client, err := s.clients.GetByName(ctx, from)
	//	if err != nil {
	//		return err
	//	}
	//	buckets, err := client.S3().ListBuckets(ctx)
	//	if err != nil {
	//		return err
	//	}
	//	for _, b := range buckets {
	//		err = s.metaSvc.MigrationCostsIncBucket(ctx, from, to)
	//		if err != nil {
	//			return err
	//		}
	//		name := b.Name
	//		task, err := tasks.NewTask(ctx, tasks.CostEstimationListPayload{
	//			FromStorage: from,
	//			ToStorage:   to,
	//			Bucket:      name,
	//			Prefix:      "",
	//		})
	//		if err != nil {
	//			return err
	//		}
	//		_, err = s.taskClient.EnqueueContext(ctx, task)
	//		if err != nil {
	//			return err
	//		}
	//		err = s.metaSvc.MigrationCostsIncJob(ctx, from, to)
	//		if err != nil {
	//			return err
	//		}
	//	}
	//	return nil
	//}, refresh, time.Second)
	//if err != nil {
	//	return s.metaSvc.MigrationCostsDelete(ctx, from, to)
	//}
	return nil
}

func (s *svc) CostsEstimationList(ctx context.Context, t *asynq.Task) error {
	var p tasks.CostEstimationListPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("CostEstimationListPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	//fromClient, err := s.clients.GetByName(ctx, p.FromStorage)
	//if err != nil {
	//	return fmt.Errorf("migration costs list obj: unable to get %q s3 client: %v: %w", p.FromStorage, err, asynq.SkipRetry)
	//}
	//_, err = s.metaSvc.GetMigrationCosts(ctx, p.FromStorage, p.ToStorage)
	//if errors.Is(err, dom.ErrNotFound) {
	//	zerolog.Ctx(ctx).Info().Msg("migration costs list obj: skip - costs deleted")
	//	return nil
	//}
	//
	//lastObjName, err := s.metaSvc.MigrationCostsLastObjGet(ctx, p.FromStorage, p.ToStorage, p.Bucket, p.Prefix)
	//if err != nil {
	//	return err
	//}
	//
	//objects := fromClient.S3().ListObjects(ctx, p.Bucket, mclient.ListObjectsOptions{StartAfter: lastObjName, Prefix: p.Prefix})
	//for object := range objects {
	//	if object.Err != nil {
	//		return fmt.Errorf("migration costs list obj: list objects error %w", object.Err)
	//	}
	//
	//	isDir := object.Size == 0 && object.ContentType == ""
	//	if isDir {
	//		subP := p
	//		subP.Prefix = object.Key
	//		subTask, err := tasks.NewTask(ctx, subP)
	//		if err != nil {
	//			return fmt.Errorf("migration costs list obj: unable to create list obj sub task: %w", err)
	//		}
	//		_, err = s.taskClient.EnqueueContext(ctx, subTask)
	//		if err != nil {
	//			return err
	//		}
	//		err = s.metaSvc.MigrationCostsIncJob(ctx, p.FromStorage, p.ToStorage)
	//		if err != nil {
	//			return err
	//		}
	//		err = s.metaSvc.MigrationCostsLastObjSet(ctx, p.FromStorage, p.ToStorage, p.Bucket, p.Prefix, object.Key)
	//		if err != nil {
	//			return fmt.Errorf("migration costs list obj: unable to update last obj: %w", err)
	//		}
	//		continue
	//	}
	//
	//	err = s.metaSvc.MigrationCostsIncObj(ctx, p.FromStorage, p.ToStorage)
	//	if err != nil {
	//		return fmt.Errorf("migration costs list obj: unable to inc obj listed: %w", err)
	//	}
	//	err = s.metaSvc.MigrationCostsIncSize(ctx, p.FromStorage, p.ToStorage, object.Size)
	//	if err != nil {
	//		return fmt.Errorf("migration costs list obj: unable to inc obj size: %w", err)
	//	}
	//	err = s.metaSvc.MigrationCostsLastObjSet(ctx, p.FromStorage, p.ToStorage, p.Bucket, p.Prefix, object.Key)
	//	if err != nil {
	//		return fmt.Errorf("migration bucket list obj: unable to update last obj meta: %w", err)
	//	}
	//}
	//
	//return s.metaSvc.MigrationCostsIncJobDone(ctx, p.FromStorage, p.ToStorage)
	return nil
}
