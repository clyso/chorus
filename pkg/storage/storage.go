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

package storage

import (
	"context"
	"fmt"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/redis/go-redis/v9"
	"time"
)

const (
	lastListedObjTTL = time.Hour * 8
)

type Service interface {
	GetLastListedObj(ctx context.Context, task tasks.MigrateBucketListObjectsPayload) (string, error)
	SetLastListedObj(ctx context.Context, task tasks.MigrateBucketListObjectsPayload, val string) error
	DelLastListedObj(ctx context.Context, task tasks.MigrateBucketListObjectsPayload) error

	StoreUploadID(ctx context.Context, user, bucket, object, uploadID string, ttl time.Duration) error
	ExistsUploadID(ctx context.Context, user, bucket, object, uploadID string) (bool, error)
	ExistsUploads(ctx context.Context, user, bucket string) (bool, error)
}

func New(client *redis.Client) Service {
	return &svc{client: client}
}

type svc struct {
	client *redis.Client
}

func (s *svc) DelLastListedObj(ctx context.Context, task tasks.MigrateBucketListObjectsPayload) error {
	key := fmt.Sprintf("s:%s:%s:%s", task.FromStorage, task.ToStorage, task.Bucket)
	if task.Prefix != "" {
		key += ":" + task.Prefix
	}
	return s.client.Del(ctx, key).Err()
}

func (s *svc) GetLastListedObj(ctx context.Context, task tasks.MigrateBucketListObjectsPayload) (string, error) {
	key := fmt.Sprintf("s:%s:%s:%s", task.FromStorage, task.ToStorage, task.Bucket)
	if task.Prefix != "" {
		key += ":" + task.Prefix
	}
	val, err := s.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	}
	return val, err
}

func (s *svc) SetLastListedObj(ctx context.Context, task tasks.MigrateBucketListObjectsPayload, val string) error {
	key := fmt.Sprintf("s:%s:%s:%s", task.FromStorage, task.ToStorage, task.Bucket)
	if task.Prefix != "" {
		key += ":" + task.Prefix
	}
	return s.client.Set(ctx, key, val, lastListedObjTTL).Err()
}

func (s *svc) StoreUploadID(ctx context.Context, user, bucket, object, uploadID string, ttl time.Duration) error {
	if user == "" {
		return fmt.Errorf("%w: user is requred to set uploadID", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return fmt.Errorf("%w: bucket is requred to set uploadID", dom.ErrInvalidArg)
	}
	if uploadID == "" {
		return fmt.Errorf("%w: uploadID is requred", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("s:up:%s:%s", user, bucket)
	val := fmt.Sprintf("%s:%s", object, uploadID)
	err := s.client.SAdd(ctx, key, val).Err()
	if err != nil {
		return err
	}
	_ = s.client.Expire(ctx, key, ttl)
	return nil
}

func (s *svc) ExistsUploadID(ctx context.Context, user, bucket, object, uploadID string) (bool, error) {
	if user == "" {
		return false, fmt.Errorf("%w: user is requred to set uploadID", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return false, fmt.Errorf("%w: bucket is requred to set uploadID", dom.ErrInvalidArg)
	}
	if uploadID == "" {
		return false, fmt.Errorf("%w: uploadID is requred", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("s:up:%s:%s", user, bucket)
	val := fmt.Sprintf("%s:%s", object, uploadID)
	return s.client.SIsMember(ctx, key, val).Result()
}

func (s *svc) ExistsUploads(ctx context.Context, user, bucket string) (bool, error) {
	if user == "" {
		return false, fmt.Errorf("%w: user is requred to set uploadID", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return false, fmt.Errorf("%w: bucket is requred to set uploadID", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("s:up:%s:%s", user, bucket)
	num, err := s.client.SCard(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return num > 0, nil
}
