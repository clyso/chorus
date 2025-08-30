package policy

import (
	"context"
	"time"

	"github.com/clyso/chorus/pkg/entity"
)

type Service2 interface {
	// -------------- Routing policy related methods: --------------
	GetRoutingPolicy(ctx context.Context, user string, bucket *string) (string, error)
	GetZerodowntimeSwitch(ctx context.Context, user string, bucket *string) (string, error)

	GetReplicationPolicies(ctx context.Context, user string, bucket *string) (*entity.StorageReplicationPolicies, error)

	// -------------- Replication switch related methods: --------------

	// Upsert downtime replication switch. If switch already exists and not in progress, it will be updated.
	SetDowntimeReplicationSwitch(ctx context.Context, replID entity.ReplicationStatusID, opts *entity.ReplicationSwitchDowntimeOpts) error
	// Change downtime replication switch status. Makes required adjustments to routing and replication policies.
	// According to switch status and configured options.
	UpdateDowntimeSwitchStatus(ctx context.Context, replID entity.ReplicationStatusID, newStatus entity.ReplicationSwitchStatus, description string, startedAt, doneAt *time.Time) error
	// Creates new zero downtime replication switch.
	AddZeroDowntimeReplicationSwitch(ctx context.Context, replID entity.ReplicationStatusID, opts *entity.ReplicationSwitchZeroDowntimeOpts) error
	// Completes zero downtime replication switch.
	CompleteZeroDowntimeReplicationSwitch(ctx context.Context, replID entity.ReplicationStatusID) error
	// Deletes any replication switch if exists and reverts routing policy if switch was not done.
	DeleteReplicationSwitch(ctx context.Context, replID entity.ReplicationStatusID) error
	// Returns replication switch config and status information.
	GetReplicationSwitchInfo(ctx context.Context, replID entity.ReplicationStatusID) (entity.ReplicationSwitchInfo, error)
	ListReplicationSwitchInfo(ctx context.Context) ([]entity.ReplicationSwitchInfo, error)
	// GetInProgressZeroDowntimeSwitchInfo shortcut method for chorus proxy to get required information
	// to adjust route only when zero downtime switch is in progress.
	GetInProgressZeroDowntimeSwitchInfo(ctx context.Context, id entity.ReplicationSwitchInfoID) (entity.ZeroDowntimeSwitchInProgressInfo, error)

	// -------------- Replication policy related methods: --------------

	GetBucketReplicationPolicies(ctx context.Context, id entity.BucketReplicationPolicyID) (*entity.StorageReplicationPolicies, error)
	GetUserReplicationPolicies(ctx context.Context, user string) (*entity.StorageReplicationPolicies, error)
	AddUserReplicationPolicy(ctx context.Context, user string, policy entity.UserReplicationPolicy) error
	DeleteUserReplication(ctx context.Context, user string, policy entity.UserReplicationPolicy) error

	AddBucketReplicationPolicy(ctx context.Context, id entity.ReplicationStatusID, agentURL *string) error
	GetReplicationPolicyInfo(ctx context.Context, id entity.ReplicationStatusID) (entity.ReplicationStatus, error)
	GetReplicationPolicyInfoExtended(ctx context.Context, id entity.ReplicationStatusID) (entity.ReplicationStatusExtended, error)
	ListReplicationPolicyInfo(ctx context.Context) (map[entity.ReplicationStatusID]entity.ReplicationStatusExtended, error)
	IsReplicationPolicyExists(ctx context.Context, id entity.ReplicationStatusID) (bool, error)
	ObjListStarted(ctx context.Context, id entity.ReplicationStatusID) error

	PauseReplication(ctx context.Context, id entity.ReplicationStatusID) error
	ResumeReplication(ctx context.Context, id entity.ReplicationStatusID) error
	DeleteReplication(ctx context.Context, id entity.ReplicationStatusID) error
	// Archive replication. Will stop generating new events for this replication.
	// Existing events will be processed and replication status metadata will be kept.
	DeleteBucketReplicationsByUser(ctx context.Context, user, from string, to string) ([]string, error)
}
