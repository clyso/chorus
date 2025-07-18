package policy

import (
	"context"
	"errors"
	"fmt"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/redis/go-redis/v9"
)

func (s *policySvc) getRoutingPolicy(ctx context.Context, user, bucket string) (string, error) {
	storage, err := s.getBucketRoutingPolicy(ctx, user, bucket)
	if err == nil {
		return storage, nil
	}
	if !errors.Is(err, dom.ErrNotFound) {
		return "", err
	}
	// bucket policy not found, try user policy:
	return s.GetUserRoutingPolicy(ctx, user)
}

func (s *policySvc) GetRoutingPolicy(ctx context.Context, user, bucket string) (string, error) {
	storage, err := s.getRoutingPolicy(ctx, user, bucket)
	if err != nil {
		return "", err
	}
	blocked, err := s.isRoutingBlocked(ctx, storage, bucket)
	if err != nil {
		return "", err
	}
	if blocked {
		return "", dom.ErrRoutingBlock
	}
	return storage, nil
}

func routingBlockSetKey(storage string) string {
	return fmt.Sprintf("p:rout_block:%s", storage)
}

func (s *policySvc) AddRoutingBlock(ctx context.Context, storage, bucket string) error {
	return addRoutingBlockWithClient(ctx, s.client, storage, bucket)
}

func addRoutingBlockWithClient(ctx context.Context, client redis.Cmdable, storage, bucket string) error {
	return client.SAdd(ctx, routingBlockSetKey(storage), bucket).Err()
}

func (s *policySvc) DeleteRoutingBlock(ctx context.Context, storage, bucket string) error {
	return deleteRoutingBlockWithClient(ctx, s.client, storage, bucket)
}

func deleteRoutingBlockWithClient(ctx context.Context, client redis.Cmdable, storage, bucket string) error {
	err := client.SRem(ctx, routingBlockSetKey(storage), bucket).Err()
	if errors.Is(err, redis.Nil) {
		return nil
	}
	return err
}

func (s *policySvc) isRoutingBlocked(ctx context.Context, storage, bucket string) (bool, error) {
	return s.client.SIsMember(ctx, routingBlockSetKey(storage), bucket).Result()
}

func (s *policySvc) GetUserRoutingPolicy(ctx context.Context, user string) (string, error) {
	if user == "" {
		return "", fmt.Errorf("%w: user is required to get routing policy", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:route:%s", user)
	toStor, err := s.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", fmt.Errorf("%w: no routing policy for user %q", dom.ErrNotFound, user)
		}
		return "", err
	}
	return toStor, nil
}

func (s *policySvc) getBucketRoutingPolicy(ctx context.Context, user, bucket string) (string, error) {
	if user == "" {
		return "", fmt.Errorf("%w: user is required to get routing policy", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return "", fmt.Errorf("%w: bucket is required to get routing policy", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:route:%s:%s", user, bucket)
	toStor, err := s.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", fmt.Errorf("%w: no routing policy for user %q, bucket %q", dom.ErrNotFound, user, bucket)
		}
		return "", err
	}
	return toStor, nil
}

func (s *policySvc) AddUserRoutingPolicy(ctx context.Context, user, toStorage string) error {
	if user == "" {
		return fmt.Errorf("%w: user is required to add user routing policy", dom.ErrInvalidArg)
	}
	if toStorage == "" {
		return fmt.Errorf("%w: toStorage is required to add user routing policy", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:route:%s", user)
	set, err := s.client.SetNX(ctx, key, toStorage, 0).Result()
	if err != nil {
		return err
	}
	if !set {
		return fmt.Errorf("%w: user %q routing policy already exists", dom.ErrAlreadyExists, user)
	}
	return nil
}

func (s *policySvc) addBucketRoutingPolicy(ctx context.Context, user, bucket, toStorage string, replace bool) error {
	return addBucketRoutingPolicyWithClient(ctx, s.client, user, bucket, toStorage, replace)
}

func addBucketRoutingPolicyWithClient(ctx context.Context, client redis.Cmdable, user, bucket, toStorage string, replace bool) error {
	if user == "" {
		return fmt.Errorf("%w: user is required to add bucket routing policy", dom.ErrInvalidArg)
	}
	if bucket == "" {
		return fmt.Errorf("%w: bucket is required to add bucket routing policy", dom.ErrInvalidArg)
	}
	if toStorage == "" {
		return fmt.Errorf("%w: toStorage is required to add bucket routing policy", dom.ErrInvalidArg)
	}
	key := fmt.Sprintf("p:route:%s:%s", user, bucket)
	if replace {
		return client.Set(ctx, key, toStorage, 0).Err()
	}
	// set only if not exists
	set, err := client.SetNX(ctx, key, toStorage, 0).Result()
	if err != nil {
		return err
	}
	if !set {
		return fmt.Errorf("%w: bucket routing policy %s:%s already exists", dom.ErrAlreadyExists, user, bucket)
	}
	return nil
}
