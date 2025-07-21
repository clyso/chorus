package policy

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/tasks"
)

type PolicyService struct {
	userRoutingPolicyStore       *UserRoutingPolicyStore
	bucketRoutingPolicyStore     *BucketRoutingPolicyStore
	routingBlockStore            *RoutingBlockStore
	bucketReplicationPolicyStore *BucketReplicationPolicyStore
	replicationStatusStore       *ReplicationStatusStore
}

func (r *PolicyService) GetRoutingPolicyStorage(ctx context.Context, id BucketRoutingPolicyID) (string, error) {
	storage, err := r.bucketRoutingPolicyStore.Get(ctx, id)
	if err == nil {
		return storage, nil
	}
	if !errors.Is(err, dom.ErrNotFound) {
		return "", err
	}
	storage, err = r.userRoutingPolicyStore.Get(ctx, id.User)
	if err != nil {
		return "", err
	}
	return storage, nil
}

func (r *PolicyService) GetRoutingPolicyStorageIfNotBlocked(ctx context.Context, id BucketRoutingPolicyID) (string, error) {
	storage, err := r.GetRoutingPolicyStorage(ctx, id)
	if err != nil {
		return "", err
	}
	blocked, err := r.routingBlockStore.IsMember(ctx, storage, id.Bucket)
	if err != nil {
		return "", err
	}
	if blocked {
		return "", dom.ErrRoutingBlock
	}
	return storage, err
}

func (r *PolicyService) GetBucketReplicationPolicyPriorities(ctx context.Context, id BucketReplicationPolicyID) (map[BucketReplicationPolicy]tasks.Priority, error) {
	policies, err := r.bucketReplicationPolicyStore.GetAll(ctx, id)
	if err != nil {
		return nil, err
	}

	var fromStorage string
	priorityMap := map[BucketReplicationPolicy]tasks.Priority{}
	for _, policy := range policies {
		if fromStorage == "" {
			fromStorage = policy.Value.FromStorage
		} else if fromStorage != policy.Value.FromStorage {
			return nil, fmt.Errorf("%w: invalid replication policy key: all keys should have same from: %+v", dom.ErrInternal, policies)
		}

		if fromStorage == policy.Value.ToStorage {
			return nil, fmt.Errorf("%w: invalid replication policy key: from and to should be different: %+v", dom.ErrInternal, policies)
		}
		if policy.Score > uint8(tasks.PriorityHighest5) {
			return nil, fmt.Errorf("%w: invalid replication policy key %q score: %d", dom.ErrInternal, policy, policy.Score)
		}
		priorityMap[policy.Value] = tasks.Priority(policy.Score)
	}
	return priorityMap, nil
}

func (r *PolicyService) AccountReplicationEvent(ctx context.Context, id ReplicationStatusID, eventTime time.Time) error {
	if _, err := r.replicationStatusStore.IncrementEvents(ctx, id); err != nil {
		return err
	}
	if err := r.replicationStatusStore.SetLastEmittedAt(ctx, id, eventTime); err != nil {
		return err
	}
	return nil
}
