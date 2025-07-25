package policy

import (
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/tasks"
)

type StorageBucketReplicationPolicies struct {
	Storage string
	Priorities map[entity.BucketReplicationPolicy]tasks.Priority
}