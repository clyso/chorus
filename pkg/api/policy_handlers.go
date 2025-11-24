/*
 * Copyright © 2024 Clyso GmbH
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

package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/notifications"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/rclone"
	"github.com/clyso/chorus/pkg/rpc"
	"github.com/clyso/chorus/pkg/storage"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

const (
	// TODO: move to config
	defaultZeroDowntimeMultipartTTL = time.Hour
)

func PolicyHandlers(
	clients objstore.Clients,
	queueSvc tasks.QueueService,
	rclone rclone.Service,
	policySvc policy.Service,
	versionSvc meta.VersionService,
	storageSvc storage.Service,
	agentClient *rpc.AgentClient,
	notificationSvc *notifications.Service,
	replicationStatusLocker *store.ReplicationStatusLocker,
	userLocker *store.UserLocker,
) pb.PolicyServer {
	return &policyHandlers{
		rclone:                  rclone,
		clients:                 clients,
		queueSvc:                queueSvc,
		policySvc:               policySvc,
		versionSvc:              versionSvc,
		storageSvc:              storageSvc,
		agentClient:             agentClient,
		notificationSvc:         notificationSvc,
		replicationStatusLocker: replicationStatusLocker,
		userLocker:              userLocker,
	}
}

var _ pb.PolicyServer = &policyHandlers{}

type policyHandlers struct {
	clients                 objstore.Clients
	queueSvc                tasks.QueueService
	rclone                  rclone.Service
	policySvc               policy.Service
	versionSvc              meta.VersionService
	storageSvc              storage.Service
	agentClient             *rpc.AgentClient
	notificationSvc         *notifications.Service
	replicationStatusLocker *store.ReplicationStatusLocker
	userLocker              *store.UserLocker
}

func (h *policyHandlers) AddReplication(ctx context.Context, req *pb.AddReplicationRequest) (*emptypb.Empty, error) {
	// validate request:
	uid, err := pbToReplicationID(req.Id)
	if err != nil {
		return nil, err
	}
	if err := h.clients.Config().ValidateReplicationID(uid); err != nil {
		return nil, err
	}
	// acquire user lock
	err = h.inUserLock(ctx, req.Id.User, func() error {
		if userRepl, ok := uid.AsUserID(); ok {
			// create user replication
			return h.addUserReplication(ctx, userRepl, req)
		} else if bucketRepl, ok := uid.AsBucketID(); ok {
			// create bucket replication
			return h.addBucketReplication(ctx, bucketRepl, req)
		} else {
			return fmt.Errorf("%w: invalid replication ID", dom.ErrInvalidArg)
		}
	})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (h *policyHandlers) addUserReplication(ctx context.Context, replicationID entity.UserReplicationPolicy, req *pb.AddReplicationRequest) (err error) {
	opts := pbToReplicationOpts(req.Opts)
	if opts.AgentURL != "" {
		return fmt.Errorf("%w: agentUrl is not supported for user replication", dom.ErrInvalidArg)
	}
	err = h.policySvc.AddUserReplicationPolicy(ctx, replicationID, opts)
	if err != nil {
		return err
	}
	defer func() {
		// rollback policy on error
		if err == nil {
			return
		}
		cleanupErr := h.policySvc.DeleteUserReplication(ctx, replicationID)
		if cleanupErr != nil {
			zerolog.Ctx(ctx).Err(cleanupErr).Msgf("unable to cleanup user replication policy for user %s", replicationID.User)
		}
	}()

	// create s3/swift task:
	var task tasks.ReplicationTask
	storageType := h.clients.Config().Storages[replicationID.FromStorage].Type
	switch storageType {
	case dom.S3:
		task = &tasks.MigrateS3UserPayload{}
	case dom.Swift:
		task = &tasks.SwiftAccountMigrationPayload{}
	default:
		return fmt.Errorf("%w: unsupported storage type %s for user replication", dom.ErrInvalidArg, storageType)
	}
	// enqueue
	uid := entity.UniversalFromUserReplication(replicationID)
	task.SetReplicationID(uid)
	return h.queueSvc.EnqueueTask(ctx, task)
}

func (h *policyHandlers) addBucketReplication(ctx context.Context, replicationID entity.BucketReplicationPolicy, req *pb.AddReplicationRequest) error {
	opts := pbToReplicationOpts(req.Opts)
	isAgent := opts.AgentURL != ""
	storageType := h.clients.Config().Storages[replicationID.FromStorage].Type
	// disallow agent replication for swift:
	if storageType == dom.Swift && isAgent {
		return fmt.Errorf("%w: agent is not supported for swift bucket replication", dom.ErrInvalidArg)
	}
	if err := h.validateAgentURL(ctx, replicationID.FromStorage, opts.AgentURL); err != nil {
		return fmt.Errorf("invalid agent URL: %w", err)
	}

	fromClient, err := h.clients.AsCommon(ctx, replicationID.FromStorage, replicationID.User)
	if err != nil {
		return err
	}
	// check if bucket exists in source
	ok, err := fromClient.BucketExists(ctx, replicationID.FromBucket)
	if err != nil {
		return fmt.Errorf("%w: unable to validate source bucket", err)
	}
	if !ok {
		return fmt.Errorf("%w: bucket %s not exists in source storage %s", dom.ErrInvalidArg, replicationID.FromBucket, replicationID.FromStorage)
	}

	// create policy:
	err = h.policySvc.AddBucketReplicationPolicy(ctx, replicationID, opts)
	if err != nil {
		return fmt.Errorf("%w: unable to add bucket replication policy", err)
	}
	// create bucket notification for agent:
	err = h.createAgentBucketNotification(ctx, replicationID, opts.AgentURL)
	if err != nil {
		return fmt.Errorf("%w: unable to create agent bucket notification", err)
	}
	// create task
	var task tasks.ReplicationTask
	switch storageType {
	case dom.S3:
		task = &tasks.BucketCreatePayload{
			Bucket: replicationID.FromBucket,
		}
	case dom.Swift:
		task = &tasks.SwiftContainerMigrationPayload{
			Bucket: replicationID.FromBucket,
		}
	default:
		return fmt.Errorf("%w: unsupported storage type %s for bucket replication", dom.ErrInvalidArg, storageType)
	}
	task.SetReplicationID(entity.UniversalFromBucketReplication(replicationID))
	return h.queueSvc.EnqueueTask(ctx, task)
}

func (h *policyHandlers) AvailableBuckets(ctx context.Context, req *pb.AvailableBucketsRequest) (*pb.AvailableBucketsResponse, error) {
	// validate request:
	if err := h.clients.Config().Exists(req.FromStorage, req.User); err != nil {
		return nil, err
	}
	if err := h.clients.Config().Exists(req.ToStorage, req.User); err != nil {
		return nil, err
	}

	userReplExists, err := h.policySvc.IsUserReplicationExists(ctx, req.User)
	if err != nil {
		return nil, err
	}
	if userReplExists {
		// user replication exists - no need to create bucket replications
		return &pb.AvailableBucketsResponse{}, nil
	}

	bucketRepl, err := h.policySvc.ListBucketReplicationsInfo(ctx, req.User)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return nil, err
	}

	usedBuckets := make(map[string]struct{})
	for k := range bucketRepl {
		// create set of buckets already used either as source or destination
		if k.FromStorage == req.FromStorage && k.ToStorage == req.ToStorage && k.User == req.User {
			usedBuckets[k.FromBucket] = struct{}{}
			usedBuckets[k.ToBucket] = struct{}{}
		}
	}

	client, err := h.clients.AsCommon(ctx, req.FromStorage, req.User)
	if err != nil {
		return nil, err
	}
	buckets, err := client.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	res := &pb.AvailableBucketsResponse{}
	for _, bucket := range buckets {
		_, used := usedBuckets[bucket]
		if !used {
			res.Buckets = append(res.Buckets, bucket)
			continue
		}
		// already used
		if req.ShowReplicated {
			res.ReplicatedBuckets = append(res.ReplicatedBuckets, bucket)
		}
	}
	return res, nil
}

func (h *policyHandlers) CompareBucket(ctx context.Context, req *pb.CompareBucketRequest) (*pb.CompareBucketResponse, error) {
	uid, err := pbToReplicationID(req.Target)
	if err != nil {
		return nil, err
	}
	if err := h.clients.Config().ValidateReplicationID(uid); err != nil {
		return nil, err
	}
	bucketRepl, ok := uid.AsBucketID()
	if !ok {
		return nil, fmt.Errorf("%w: only bucket replication supported", dom.ErrInvalidArg)
	}

	res, err := h.rclone.Compare(ctx, req.ShowMatch, bucketRepl.User, bucketRepl.FromStorage, bucketRepl.ToStorage, bucketRepl.FromBucket, bucketRepl.ToBucket)
	if err != nil {
		return nil, err
	}
	return &pb.CompareBucketResponse{
		IsMatch:  res.IsMatch,
		MissFrom: res.MissFrom,
		MissTo:   res.MissTo,
		Differ:   res.Differ,
		Error:    res.Error,
		Match:    res.Match,
	}, nil
}

func (h *policyHandlers) DeleteReplication(ctx context.Context, req *pb.ReplicationID) (*emptypb.Empty, error) {
	uid, err := pbToReplicationID(req)
	if err != nil {
		return nil, err
	}
	err = h.inReplicationLock(ctx, uid, func() error {
		if userRepl, ok := uid.AsUserID(); ok {
			// create user replication
			// TODO: refactor version meta service to cleanup data by universal replicationID
			return h.policySvc.DeleteUserReplication(ctx, userRepl)
		} else if bucketRepl, ok := uid.AsBucketID(); ok {
			// create bucket replication
			err = h.policySvc.DeleteBucketReplication(ctx, bucketRepl)
			if err != nil {
				return fmt.Errorf("%w: unable to delete replication policy", err)
			}
			err = h.versionSvc.DeleteBucketMeta(ctx, meta.ToDest(bucketRepl.ToStorage, bucketRepl.ToBucket), bucketRepl.FromBucket)
			if err != nil {
				return fmt.Errorf("%w: unable to delete version metadata", err)
			}
			err = h.storageSvc.CleanLastListedObj(ctx, bucketRepl.FromStorage, bucketRepl.ToStorage, bucketRepl.FromBucket, bucketRepl.ToBucket)
			if err != nil {
				return fmt.Errorf("%w: unable to delete list obj metadata", err)
			}
			err = h.notificationSvc.DeleteBucketNotification(ctx, bucketRepl.FromStorage, bucketRepl.User, bucketRepl.FromBucket)
			if err != nil {
				zerolog.Ctx(ctx).Err(err).Msg("unable to delete agent bucket notification")
			}
			return nil
		} else {
			return fmt.Errorf("%w: invalid replication ID", dom.ErrInvalidArg)
		}
	})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (h *policyHandlers) DeleteSwitch(ctx context.Context, req *pb.ReplicationID) (*emptypb.Empty, error) {
	uid, err := pbToReplicationID(req)
	if err != nil {
		return nil, err
	}
	err = h.inReplicationLock(ctx, uid, func() error {
		return h.policySvc.DeleteReplicationSwitch(ctx, uid)
	})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (h *policyHandlers) GetReplication(ctx context.Context, req *pb.ReplicationID) (*pb.Replication, error) {
	uid, err := pbToReplicationID(req)
	if err != nil {
		return nil, err
	}
	status, err := h.policySvc.GetReplicationPolicyInfoExtended(ctx, uid)
	if err != nil {
		return nil, err
	}
	return replicationToPb(uid, status), nil
}

func (h *policyHandlers) GetSwitchStatus(ctx context.Context, req *pb.ReplicationID) (*pb.ReplicationSwitch, error) {
	uid, err := pbToReplicationID(req)
	if err != nil {
		return nil, err
	}
	res, err := h.policySvc.GetReplicationSwitchInfo(ctx, uid)
	if err != nil {
		return nil, err
	}
	return toPbSwitchStatus(&res), nil
}

func (h *policyHandlers) ListReplications(ctx context.Context, req *pb.ListReplicationsRequest) (*pb.ListReplicationsResponse, error) {
	var res []*pb.Replication
	if !req.HideBucketReplications {
		bucketRepls, err := h.listBucketReplications(ctx, req.Filter)
		if err != nil {
			return nil, err
		}
		res = bucketRepls
	}
	if !req.HideUserReplications {
		userRepls, err := h.listUserReplications(ctx, req.Filter)
		if err != nil {
			return nil, err
		}
		res = append(res, userRepls...)
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].CreatedAt.AsTime().After(res[j].CreatedAt.AsTime())
	})
	if req.HideSwitchInfo {
		for _, r := range res {
			r.SwitchInfo = nil
		}
	}
	return &pb.ListReplicationsResponse{Replications: res}, nil
}

func (h *policyHandlers) listBucketReplications(ctx context.Context, filter *pb.ListReplicationsRequest_Filter) ([]*pb.Replication, error) {
	users := h.clients.Config().GetMain().UserList()
	if filter != nil && filter.User != nil && *filter.User != "" {
		// filter by user if set
		users = []string{*filter.User}
	}
	usersRes := make([]map[entity.BucketReplicationPolicy]entity.ReplicationStatusExtended, len(users))
	// list bucket replications per user in parallel
	g, gCtx := errgroup.WithContext(ctx)
	for i, user := range users {
		g.Go(func() error {
			replications, err := h.policySvc.ListBucketReplicationsInfo(gCtx, user)
			if err != nil && !errors.Is(err, dom.ErrNotFound) {
				return fmt.Errorf("unable to list replications for user %s: %w", user, err)
			}
			usersRes[i] = replications
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	totalNum := 0
	for _, userReplications := range usersRes {
		for k, v := range userReplications {
			if h.skipBucketReplication(k, v, filter) {
				delete(userReplications, k)
			}
		}
		totalNum += len(userReplications)
	}
	res := make([]*pb.Replication, 0, totalNum)
	for _, replications := range usersRes {
		if replications == nil {
			continue
		}
		for k, v := range replications {
			res = append(res, replicationToPb(entity.UniversalFromBucketReplication(k), v))
		}
	}
	return res, nil
}

func (h *policyHandlers) listUserReplications(ctx context.Context, filter *pb.ListReplicationsRequest_Filter) ([]*pb.Replication, error) {
	replications, err := h.policySvc.ListUserReplicationsInfo(ctx)
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return nil, err
	}
	for k, v := range replications {
		if h.skipUserReplication(k, v, filter) {
			delete(replications, k)
		}
	}
	res := make([]*pb.Replication, 0, len(replications))
	for k, v := range replications {
		res = append(res, replicationToPb(entity.UniversalFromUserReplication(k), v))
	}
	return res, nil
}

func (h *policyHandlers) skipBucketReplication(k entity.BucketReplicationPolicy, v entity.ReplicationStatusExtended, filter *pb.ListReplicationsRequest_Filter) bool {
	if filter == nil {
		return false
	}
	if filter.User != nil && *filter.User != "" && k.User != *filter.User {
		return true
	}
	if filter.FromStorage != nil && *filter.FromStorage != "" && k.FromStorage != *filter.FromStorage {
		return true
	}
	if filter.ToStorage != nil && *filter.ToStorage != "" && k.ToStorage != *filter.ToStorage {
		return true
	}
	if filter.FromBucket != nil && *filter.FromBucket != "" && k.FromBucket != *filter.FromBucket {
		return true
	}
	if filter.ToBucket != nil && *filter.ToBucket != "" && k.ToBucket != *filter.ToBucket {
		return true
	}
	if filter.IsPaused != nil && v.IsPaused != *filter.IsPaused {
		return true
	}
	if filter.IsAgent != nil && v.AgentURL != "" && *filter.IsAgent {
		return true
	}
	if filter.IsArchived != nil && v.IsArchived != *filter.IsArchived {
		return true
	}
	if filter.HasSwitch != nil && v.Switch != nil && *filter.HasSwitch {
		return true
	}
	return false
}

func (h *policyHandlers) skipUserReplication(k entity.UserReplicationPolicy, v entity.ReplicationStatusExtended, filter *pb.ListReplicationsRequest_Filter) bool {
	if filter == nil {
		return false
	}
	if filter.User != nil && *filter.User != "" && k.User != *filter.User {
		return true
	}
	if filter.FromStorage != nil && *filter.FromStorage != "" && k.FromStorage != *filter.FromStorage {
		return true
	}
	if filter.ToStorage != nil && *filter.ToStorage != "" && k.ToStorage != *filter.ToStorage {
		return true
	}
	if filter.IsPaused != nil && v.IsPaused != *filter.IsPaused {
		return true
	}
	if filter.IsAgent != nil && v.AgentURL != "" && *filter.IsAgent {
		return true
	}
	if filter.IsArchived != nil && v.IsArchived != *filter.IsArchived {
		return true
	}
	if filter.HasSwitch != nil && v.Switch != nil && *filter.HasSwitch {
		return true
	}
	return false
}

func (h *policyHandlers) PauseReplication(ctx context.Context, req *pb.ReplicationID) (*emptypb.Empty, error) {
	uid, err := pbToReplicationID(req)
	if err != nil {
		return nil, err
	}
	err = h.policySvc.PauseReplication(ctx, uid)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (h *policyHandlers) ResumeReplication(ctx context.Context, req *pb.ReplicationID) (*emptypb.Empty, error) {
	uid, err := pbToReplicationID(req)
	if err != nil {
		return nil, err
	}
	err = h.policySvc.ResumeReplication(ctx, uid)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (h *policyHandlers) StreamReplication(req *pb.ReplicationID, server grpc.ServerStreamingServer[pb.Replication]) error {
	uid, err := pbToReplicationID(req)
	if err != nil {
		return err
	}
	const pollInterval = time.Millisecond * 500
	timer := time.NewTimer(pollInterval)
	defer timer.Stop()

	ctx := server.Context()
	var prev *pb.Replication
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			res, err := h.policySvc.GetReplicationPolicyInfoExtended(ctx, uid)
			if err != nil {
				return err
			}
			curr := replicationToPb(uid, res)
			if prev == nil || !proto.Equal(prev, curr) {
				prev = curr
				err = server.Send(curr)
				if err != nil {
					return err
				}
			}
			timer.Reset(pollInterval)
		}
	}
}

func (h *policyHandlers) SwitchWithDowntime(ctx context.Context, req *pb.SwitchDowntimeRequest) (*emptypb.Empty, error) {
	if req.DowntimeOpts != nil {
		if err := validateSwitchDonwtimeOpts(req.DowntimeOpts); err != nil {
			return nil, err
		}
	}
	uid, err := pbToReplicationID(req.ReplicationId)
	if err != nil {
		return nil, err
	}
	err = h.inReplicationLock(ctx, uid, func() error {
		// persist switch metadata
		err = h.policySvc.SetDowntimeReplicationSwitch(ctx, uid, pbToDowntimeOpts(req.DowntimeOpts))
		if err != nil {
			return fmt.Errorf("unable to store switch metadata: %w", err)
		}
		// create switch task
		err = h.queueSvc.EnqueueTask(ctx, tasks.SwitchWithDowntimePayload{
			ID: uid,
		})
		if err != nil {
			return fmt.Errorf("unable to enqueue switch task: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (h *policyHandlers) SwitchWithZeroDowntime(ctx context.Context, req *pb.SwitchZeroDowntimeRequest) (*emptypb.Empty, error) {
	uid, err := pbToReplicationID(req.ReplicationId)
	if err != nil {
		return nil, err
	}
	storageType := h.clients.Config().Storages[uid.FromStorage()].Type
	if storageType != dom.S3 {
		return nil, fmt.Errorf("%w: zero-downtime switch is only supported for S3 storage type", dom.ErrInvalidArg)
	}
	err = h.inReplicationLock(ctx, uid, func() error {
		// persist switch metadata
		opts := &entity.ReplicationSwitchZeroDowntimeOpts{
			MultipartTTL: defaultZeroDowntimeMultipartTTL,
		}
		if req.MultipartTtl != nil && req.MultipartTtl.AsDuration() > 0 {
			opts.MultipartTTL = req.MultipartTtl.AsDuration()
		}

		err = h.policySvc.AddZeroDowntimeReplicationSwitch(ctx, uid, opts)
		if err != nil {
			return fmt.Errorf("unable to store switch metadata: %w", err)
		}
		// create switch task
		err = h.queueSvc.EnqueueTask(ctx, tasks.ZeroDowntimeReplicationSwitchPayload{
			ID: uid,
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (h *policyHandlers) validateAgentURL(ctx context.Context, fromStorage string, agentURL string) error {
	if agentURL == "" {
		// agent is not set
		return nil
	}

	agents, err := h.agentClient.Ping(ctx)
	if err != nil {
		return err
	}
	for _, agent := range agents {
		if agent.URL != agentURL {
			continue
		}
		if agent.FromStorage != fromStorage {
			return fmt.Errorf("%w: from storage %s is different from agent storage %s", dom.ErrInvalidArg, fromStorage, agent.FromStorage)
		}
		// valid
		return nil
	}
	return fmt.Errorf("%w: agent not found", dom.ErrInvalidArg)
}

func (h *policyHandlers) createAgentBucketNotification(ctx context.Context, replicationID entity.BucketReplicationPolicy, agentURL string) error {
	if agentURL == "" {
		// agent is not set
		return nil
	}
	// create a topic and bucket notification for agent event source.
	err := h.notificationSvc.SubscribeToBucketNotifications(ctx, replicationID.FromStorage, replicationID.User, replicationID.FromBucket, agentURL)
	if err != nil {
		cleanupErr := h.policySvc.DeleteBucketReplication(context.Background(), replicationID)
		if cleanupErr != nil {
			zerolog.Ctx(ctx).Err(cleanupErr).Msgf("unable to cleanup replication policy for bucket %s", replicationID.FromBucket)
		}
		return err
	}
	return nil
}

func (h *policyHandlers) inUserLock(ctx context.Context, user string, fn func() error) error {
	lock, err := h.userLocker.Lock(ctx, user, store.WithDuration(time.Second), store.WithRetry(true))
	if err != nil {
		return err
	}
	defer lock.Release(ctx)
	return lock.Do(ctx, time.Second, fn)
}

func (h *policyHandlers) inReplicationLock(ctx context.Context, id entity.UniversalReplicationID, fn func() error) error {
	lock, err := h.replicationStatusLocker.Lock(ctx, id, store.WithDuration(time.Second), store.WithRetry(true))
	if err != nil {
		return err
	}
	defer lock.Release(ctx)
	return lock.Do(ctx, time.Second, fn)
}
