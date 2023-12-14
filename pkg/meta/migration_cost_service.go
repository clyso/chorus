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

package meta

import (
	"context"
	"fmt"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/redis/go-redis/v9"
	"time"
)

type MigrationCostService interface {
	GetMigrationCosts(ctx context.Context, from, to string) (MigrationCosts, error)
	MigrationCostsDelete(ctx context.Context, from, to string) error
	MigrationCostsStart(ctx context.Context, from, to string) error
	MigrationCostsIncBucket(ctx context.Context, from, to string) error
	MigrationCostsIncObj(ctx context.Context, from, to string) error
	MigrationCostsIncSize(ctx context.Context, from, to string, size int64) error
	MigrationCostsLastObjSet(ctx context.Context, from, to, bucket, prefix, object string) error
	MigrationCostsLastObjGet(ctx context.Context, from, to, bucket, prefix string) (string, error)
	MigrationCostsIncJob(ctx context.Context, from, to string) error
	MigrationCostsIncJobDone(ctx context.Context, from, to string) error
}

func NewMigrationCostService(client *redis.Client) MigrationCostService {
	return &migrationCostSvc{client: client}
}

type migrationCostSvc struct {
	client *redis.Client
}

func (s *migrationCostSvc) GetMigrationCosts(ctx context.Context, from, to string) (MigrationCosts, error) {
	objMap, err := s.client.HGetAll(ctx, toMigrationCostsKey(from, to)).Result()
	if err != nil {
		return nil, err
	}
	if len(objMap) == 0 {
		return nil, dom.ErrNotFound
	}
	return &migrationCosts{src: objMap}, nil
}

func (s *migrationCostSvc) MigrationCostsDelete(ctx context.Context, from, to string) error {
	keys, err := s.client.HKeys(ctx, toMigrationCostsKey(from, to)).Result()
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	return s.client.HDel(ctx, toMigrationCostsKey(from, to), keys...).Err()
}

func (s *migrationCostSvc) MigrationCostsStart(ctx context.Context, from, to string) error {
	return s.client.HSetNX(ctx, toMigrationCostsKey(from, to), "startedAt", time.Now().UTC().UnixMilli()).Err()
}

func (s *migrationCostSvc) MigrationCostsIncBucket(ctx context.Context, from, to string) error {
	return s.client.HIncrBy(ctx, toMigrationCostsKey(from, to), "bucketNum", 1).Err()
}

func (s *migrationCostSvc) MigrationCostsIncObj(ctx context.Context, from, to string) error {
	return s.client.HIncrBy(ctx, toMigrationCostsKey(from, to), "objNum", 1).Err()
}

func (s *migrationCostSvc) MigrationCostsIncSize(ctx context.Context, from, to string, size int64) error {
	return s.client.HIncrBy(ctx, toMigrationCostsKey(from, to), "objSize", size).Err()
}

func (s *migrationCostSvc) MigrationCostsLastObjSet(ctx context.Context, from, to, bucket, prefix, object string) error {
	field := fmt.Sprintf("bkt:%s:lastObjName", bucket)
	if prefix != "" {
		field = fmt.Sprintf("bkt:%s:%s:lastObjName", bucket, prefix)
	}
	return s.client.HSet(ctx, toMigrationCostsKey(from, to), field, object).Err()
}

func (s *migrationCostSvc) MigrationCostsLastObjGet(ctx context.Context, from, to, bucket, prefix string) (string, error) {
	field := fmt.Sprintf("bkt:%s:lastObjName", bucket)
	if prefix != "" {
		field = fmt.Sprintf("bkt:%s:%s:lastObjName", bucket, prefix)
	}
	res, err := s.client.HGet(ctx, toMigrationCostsKey(from, to), field).Result()
	if err == redis.Nil {
		return "", nil
	}
	return res, err
}

func (s *migrationCostSvc) MigrationCostsIncJob(ctx context.Context, from, to string) error {
	return s.client.HIncrBy(ctx, toMigrationCostsKey(from, to), "jobStarted", 1).Err()
}

func (s *migrationCostSvc) MigrationCostsIncJobDone(ctx context.Context, from, to string) error {
	return s.client.HIncrBy(ctx, toMigrationCostsKey(from, to), "jobDone", 1).Err()
}

func toMigrationCostsKey(from, to string) string {
	return fmt.Sprintf("chorus:migration-costs:%s:%s", from, to)
}
