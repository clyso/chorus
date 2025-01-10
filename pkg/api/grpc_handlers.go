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
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/lock"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/notifications"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/rclone"
	"github.com/clyso/chorus/pkg/rpc"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/storage"
	"github.com/clyso/chorus/pkg/tasks"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func GrpcHandlers(storages *s3.StorageConfig, s3clients s3client.Service, taskClient *asynq.Client, rclone rclone.Service, policySvc policy.Service, versionSvc meta.VersionService, storageSvc storage.Service, locker lock.Service, proxyClient rpc.Proxy, agentClient *rpc.AgentClient, notificationSvc *notifications.Service) pb.ChorusServer {
	return &handlers{storages: storages, rclone: rclone, s3clients: s3clients, taskClient: taskClient, policySvc: policySvc, versionSvc: versionSvc, storageSvc: storageSvc, locker: locker, proxyClient: proxyClient, agentClient: agentClient, notificationSvc: notificationSvc}
}

type handlers struct {
	storages        *s3.StorageConfig
	s3clients       s3client.Service
	taskClient      *asynq.Client
	rclone          rclone.Service
	policySvc       policy.Service
	versionSvc      meta.VersionService
	storageSvc      storage.Service
	locker          lock.Service
	proxyClient     rpc.Proxy
	agentClient     *rpc.AgentClient
	notificationSvc *notifications.Service
}

func (h *handlers) GetStorages(_ context.Context, _ *emptypb.Empty) (*pb.GetStoragesResponse, error) {
	res := make([]*pb.Storage, 0, len(h.storages.Storages))
	for name, stor := range h.storages.Storages {
		creds := make([]*pb.Credential, 0, len(stor.Credentials))
		for alias, cred := range stor.Credentials {
			creds = append(creds, &pb.Credential{
				Alias:     alias,
				AccessKey: cred.AccessKeyID,
				SecretKey: "",
			})
		}
		slices.SortFunc(creds, func(a, b *pb.Credential) int {
			if n := strings.Compare(a.Alias, b.Alias); n != 0 {
				return n
			}
			return strings.Compare(a.AccessKey, b.AccessKey)
		})
		res = append(res, &pb.Storage{
			Name:        name,
			Address:     stor.Address,
			Provider:    pb.Storage_Provider(pb.Storage_Provider_value[stor.Provider]),
			Credentials: creds,
			IsMain:      stor.IsMain,
		})
	}
	return &pb.GetStoragesResponse{Storages: res}, nil
}

func (h *handlers) GetProxyCredentials(ctx context.Context, _ *emptypb.Empty) (*pb.GetProxyCredentialsResponse, error) {
	return h.proxyClient.GetCredentials(ctx)
}

func (h *handlers) ListBucketsForReplication(ctx context.Context, req *pb.ListBucketsForReplicationRequest) (*pb.ListBucketsForReplicationResponse, error) {
	ctx = log.WithUser(ctx, req.User)
	client, err := h.s3clients.GetByName(ctx, req.From)
	if err != nil {
		return nil, err
	}
	buckets, err := client.S3().ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	res := &pb.ListBucketsForReplicationResponse{}

	for _, bucket := range buckets {
		exists, err := h.policySvc.IsReplicationPolicyExists(ctx, req.User, bucket.Name, req.From, req.To)
		if err != nil {
			return nil, err
		}
		if !exists {
			res.Buckets = append(res.Buckets, bucket.Name)
			continue
		}
		// replication exists
		if req.ShowReplicated {
			res.ReplicatedBuckets = append(res.ReplicatedBuckets, bucket.Name)
		}
	}
	return res, nil
}

func (h *handlers) AddReplication(ctx context.Context, req *pb.AddReplicationRequest) (*emptypb.Empty, error) {
	if _, ok := h.storages.Storages[req.From]; !ok {
		return nil, fmt.Errorf("%w: unknown from storage %s", dom.ErrInvalidArg, req.From)
	}
	if _, ok := h.storages.Storages[req.To]; !ok {
		return nil, fmt.Errorf("%w: unknown to storage %s", dom.ErrInvalidArg, req.To)
	}
	if _, ok := h.storages.Storages[req.From].Credentials[req.User]; !ok {
		return nil, fmt.Errorf("%w: unknown user %s", dom.ErrInvalidArg, req.User)
	}
	if req.From == req.To {
		return nil, fmt.Errorf("%w: from and to should be different", dom.ErrInvalidArg)
	}
	ctx = log.WithUser(ctx, req.User)
	release, refresh, err := h.locker.Lock(ctx, lock.UserKey(req.User), lock.WithDuration(time.Second*5), lock.WithRetry(true))
	if err != nil {
		return nil, err
	}
	defer release()
	err = lock.WithRefresh(ctx, func() error {
		if req.IsForAllBuckets {
			// create user replication policy:
			err := h.addUserReplication(ctx, req)
			if err != nil {
				return err
			}
			return nil
		}
		// create bucket replication policies:
		if len(req.Buckets) == 0 {
			return fmt.Errorf("%w: buckets not set", dom.ErrInvalidArg)
		}
		client, err := h.s3clients.GetByName(ctx, req.From)
		if err != nil {
			return err
		}
		if req.AgentUrl != nil && *req.AgentUrl != "" {
			agents, err := h.agentClient.Ping(ctx)
			if err != nil {
				return err
			}
			found := false
			for _, agent := range agents {
				if agent.URL == *req.AgentUrl {
					if agent.FromStorage != req.From {
						return fmt.Errorf("%w: from storage %s is different from agent storage %s", dom.ErrInvalidArg, req.From, agent.FromStorage)
					}
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("%w: agent not found", dom.ErrInvalidArg)
			}
		}

		// validate buckets
		for _, bucket := range req.Buckets {
			ok, err := client.S3().BucketExists(ctx, bucket)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("%w: unknown bucket %s", dom.ErrInvalidArg, bucket)
			}
			route, err := h.policySvc.GetRoutingPolicy(ctx, req.User, bucket)
			if err == nil && route != req.From {
				return fmt.Errorf("%w: from storage %s is different from bucket %s routing policy storage %s", dom.ErrInvalidArg, req.From, bucket, route)
			}
			if err != nil && !errors.Is(err, dom.ErrNotFound) {
				// ignore not found
				return err
			}
		}

		// create policies:
		for _, bucket := range req.Buckets {
			err = h.policySvc.AddBucketReplicationPolicy(ctx, req.User, bucket, req.From, req.To, tasks.PriorityDefault1, req.AgentUrl)
			if err != nil {
				if errors.Is(err, dom.ErrAlreadyExists) {
					continue
				}
				return err
			}
			if req.AgentUrl != nil && *req.AgentUrl != "" {
				// create a topic and bucket notification for agent event source.
				err = h.notificationSvc.SubscribeToBucketNotifications(ctx, req.From, req.User, bucket, *req.AgentUrl)
				if err != nil {
					cleanupErr := h.policySvc.DeleteReplication(context.Background(), req.User, bucket, req.From, req.To)
					if cleanupErr != nil {
						zerolog.Ctx(ctx).Err(cleanupErr).Msgf("unable to cleanup replication policy for bucket %s", bucket)
					}
					return err
				}
			}
			task, err := tasks.NewTask(ctx, tasks.BucketCreatePayload{
				Sync: tasks.Sync{
					FromStorage: req.From,
					ToStorage:   req.To,
				},
				Bucket: bucket,
			})
			if err != nil {
				return err
			}
			_, err = h.taskClient.EnqueueContext(ctx, task)
			if err != nil && !errors.Is(err, asynq.ErrDuplicateTask) && !errors.Is(err, asynq.ErrTaskIDConflict) {
				return err
			}
		}
		return nil
	}, refresh, time.Second*5)

	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (h *handlers) addUserReplication(ctx context.Context, req *pb.AddReplicationRequest) error {
	if req.AgentUrl != nil && *req.AgentUrl != "" {
		return fmt.Errorf("%w: agentUrl is not supported for user replication", dom.ErrInvalidArg)
	}
	route, err := h.policySvc.GetUserRoutingPolicy(ctx, req.User)
	if err == nil && route != req.From {
		return fmt.Errorf("%w: from storage %s is different from routing policy storage %s", dom.ErrInvalidArg, req.From, route)
	}
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		// ignore not found
		return err
	}
	client, err := h.s3clients.GetByName(ctx, req.From)
	if err != nil {
		return err
	}
	buckets, err := client.S3().ListBuckets(ctx)
	if err != nil {
		return err
	}
	err = h.policySvc.AddUserReplicationPolicy(ctx, req.User, req.From, req.To, tasks.PriorityDefault1)
	if err != nil && !errors.Is(err, dom.ErrAlreadyExists) {
		return err
	}
	for _, bucket := range buckets {
		err = h.policySvc.AddBucketReplicationPolicy(ctx, req.User, bucket.Name, req.From, req.To, tasks.PriorityDefault1, nil)
		if err != nil {
			if errors.Is(err, dom.ErrAlreadyExists) {
				continue
			}
			return err
		}
		task, err := tasks.NewTask(ctx, tasks.BucketCreatePayload{
			Sync: tasks.Sync{
				FromStorage: req.From,
				ToStorage:   req.To,
			},
			Bucket: bucket.Name,
		})
		if err != nil {
			return err
		}
		_, err = h.taskClient.EnqueueContext(ctx, task)
		if err != nil && !errors.Is(err, asynq.ErrDuplicateTask) && !errors.Is(err, asynq.ErrTaskIDConflict) {
			return err
		}
	}
	return nil
}

func (h *handlers) ListReplications(ctx context.Context, _ *emptypb.Empty) (*pb.ListReplicationsResponse, error) {
	replications, err := h.policySvc.ListReplicationPolicyInfo(ctx)
	if err != nil {
		return nil, err
	}
	res := make([]*pb.Replication, len(replications))
	for i, replication := range replications {
		res[i] = replicationToPb(replication)
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].CreatedAt.AsTime().After(res[j].CreatedAt.AsTime())
	})

	return &pb.ListReplicationsResponse{Replications: res}, nil
}

func (h *handlers) ListUserReplications(ctx context.Context, _ *emptypb.Empty) (*pb.ListUserReplicationsResponse, error) {
	var res []*pb.UserReplication
	for user := range h.storages.Storages[h.storages.Main()].Credentials {
		policies, err := h.policySvc.GetUserReplicationPolicies(ctx, user)
		if err != nil {
			if errors.Is(err, dom.ErrNotFound) {
				continue
			}
			return nil, err
		}
		for to := range policies.To {
			res = append(res, &pb.UserReplication{
				User: user,
				From: policies.From,
				To:   to,
			})
		}
	}
	return &pb.ListUserReplicationsResponse{Replications: res}, nil
}

func (h *handlers) DeleteUserReplication(ctx context.Context, req *pb.DeleteUserReplicationRequest) (*emptypb.Empty, error) {
	ctx = log.WithUser(ctx, req.User)
	release, refresh, err := h.locker.Lock(ctx, lock.UserKey(req.User), lock.WithDuration(time.Second), lock.WithRetry(true))
	if err != nil {
		return nil, err
	}
	defer release()
	err = lock.WithRefresh(ctx, func() error {
		err = h.policySvc.DeleteUserReplication(ctx, req.User, req.From, req.To)
		if err != nil {
			return err
		}
		if !req.DeleteBucketReplications {
			return nil
		}
		deleted, err := h.policySvc.DeleteBucketReplicationsByUser(ctx, req.User, req.From, req.To)
		if err != nil {
			return err
		}
		for _, bucket := range deleted {
			err = h.versionSvc.DeleteBucketMeta(ctx, req.To, bucket)
			if err != nil {
				zerolog.Ctx(ctx).Err(err).Msg("unable to delete bucket version metadata")
			}
			err = h.storageSvc.CleanLastListedObj(ctx, req.From, req.To, bucket)
			if err != nil {
				zerolog.Ctx(ctx).Err(err).Msg("unable to delete bucket obj list metadata")
			}
			err = h.notificationSvc.DeleteBucketNotification(ctx, req.From, req.User, bucket)
			if err != nil {
				zerolog.Ctx(ctx).Err(err).Msg("unable to delete agent bucket notification")
			}
		}
		return nil
	}, refresh, time.Second)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (h *handlers) PauseReplication(ctx context.Context, req *pb.ReplicationRequest) (*emptypb.Empty, error) {
	err := h.policySvc.PauseReplication(ctx, req.User, req.Bucket, req.From, req.To)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (h *handlers) ResumeReplication(ctx context.Context, req *pb.ReplicationRequest) (*emptypb.Empty, error) {
	err := h.policySvc.ResumeReplication(ctx, req.User, req.Bucket, req.From, req.To)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (h *handlers) DeleteReplication(ctx context.Context, req *pb.ReplicationRequest) (*emptypb.Empty, error) {
	ctx = log.WithUser(ctx, req.User)
	release, refresh, err := h.locker.Lock(ctx, lock.UserKey(req.User), lock.WithDuration(time.Second), lock.WithRetry(true))
	if err != nil {
		return nil, err
	}
	defer release()
	err = lock.WithRefresh(ctx, func() error {
		err = h.policySvc.DeleteReplication(ctx, req.User, req.Bucket, req.From, req.To)
		if err != nil {
			return fmt.Errorf("%w: unable to delete replication policy", err)
		}
		err = h.versionSvc.DeleteBucketMeta(ctx, req.To, req.Bucket)
		if err != nil {
			return fmt.Errorf("%w: unable to delete version metadata", err)
		}
		err = h.storageSvc.CleanLastListedObj(ctx, req.From, req.To, req.Bucket)
		if err != nil {
			return fmt.Errorf("%w: unable to delete list obj metadata", err)
		}
		err = h.notificationSvc.DeleteBucketNotification(ctx, req.From, req.User, req.Bucket)
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Msg("unable to delete agent bucket notification")
		}
		return nil
	}, refresh, time.Second)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (h *handlers) CompareBucket(ctx context.Context, req *pb.CompareBucketRequest) (*pb.CompareBucketResponse, error) {
	if _, ok := h.storages.Storages[req.From]; !ok {
		return nil, fmt.Errorf("%w: invalid FromStorage", dom.ErrInvalidArg)
	}
	if _, ok := h.storages.Storages[req.To]; !ok {
		return nil, fmt.Errorf("%w: invalid ToStorage", dom.ErrInvalidArg)
	}
	if _, ok := h.storages.Storages[req.To].Credentials[req.User]; !ok {
		return nil, fmt.Errorf("%w: invalid User", dom.ErrInvalidArg)
	}
	ctx = log.WithUser(ctx, req.User)

	res, err := h.rclone.Compare(ctx, req.ShowMatch, req.From, req.To, req.Bucket)
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

func (h *handlers) StreamBucketReplication(req *pb.ReplicationRequest, server pb.Chorus_StreamBucketReplicationServer) error {
	const pollInterval = time.Millisecond * 500
	ctx := server.Context()
	timer := time.NewTimer(pollInterval)

	defer timer.Stop()
	var prev *pb.Replication
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			m, err := h.policySvc.GetReplicationPolicyInfo(ctx, req.User, req.Bucket, req.From, req.To)
			if err != nil {
				return err
			}
			pol := replicationToPb(policy.ReplicationPolicyStatusExtended{
				ReplicationPolicyStatus: m,
				User:                    req.User,
				Bucket:                  req.Bucket,
				From:                    req.From,
				To:                      req.To,
			})
			if prev == nil || !proto.Equal(prev, pol) {
				prev = pol
				err = server.Send(pol)
				if err != nil {
					return err
				}
			}
			timer.Reset(pollInterval)
		}
	}
}

func (h *handlers) GetAgents(ctx context.Context, _ *emptypb.Empty) (*pb.GetAgentsResponse, error) {
	agents, err := h.agentClient.Ping(ctx)
	if err != nil {
		return nil, err
	}
	res := make([]*pb.Agent, len(agents))
	for i, agent := range agents {
		res[i] = &pb.Agent{
			Storage: agent.FromStorage,
			Url:     agent.URL,
		}
	}
	return &pb.GetAgentsResponse{Agents: res}, nil
}

func (h *handlers) SwitchMainBucket(ctx context.Context, req *pb.SwitchMainBucketRequest) (*emptypb.Empty, error) {
	// validate req
	if _, ok := h.storages.Storages[req.NewMain]; !ok {
		return nil, fmt.Errorf("%w: invalid NewMain", dom.ErrInvalidArg)
	}
	ctx = log.WithUser(ctx, req.User)
	release, refresh, err := h.locker.Lock(ctx, lock.UserKey(req.User), lock.WithDuration(time.Second), lock.WithRetry(true))
	if err != nil {
		return nil, err
	}
	defer release()
	err = lock.WithRefresh(ctx, func() error {
		err := h.policySvc.DoReplicationSwitch(ctx, req.User, req.Bucket, req.NewMain)
		if err != nil {
			return err
		}
		task, err := tasks.NewTask(ctx, tasks.FinishReplicationSwitchPayload{
			User:   req.User,
			Bucket: req.Bucket,
		})
		if err != nil {
			return err
		}
		_, err = h.taskClient.EnqueueContext(ctx, task)
		if err != nil && !errors.Is(err, asynq.ErrDuplicateTask) && !errors.Is(err, asynq.ErrTaskIDConflict) {
			return err
		}
		return nil
	}, refresh, time.Second)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}
