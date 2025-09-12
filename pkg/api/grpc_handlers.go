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

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/notifications"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/rclone"
	"github.com/clyso/chorus/pkg/rpc"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/storage"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/validate"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/service/worker/handler"
)

const (
	// TODO: move to config
	defaultZeroDowntimeMultipartTTL = time.Hour
)

func GrpcHandlers(storages *s3.StorageConfig, s3clients s3client.Service, queueSvc tasks.QueueService,
	rclone rclone.Service, policySvc policy.Service, versionSvc meta.VersionService,
	storageSvc storage.Service, checkSvc *handler.ConsistencyCheckSvc, proxyClient rpc.Proxy,
	agentClient *rpc.AgentClient, notificationSvc *notifications.Service,
	replicationStatusLocker *store.ReplicationStatusLocker, userLocker *store.UserLocker,
	appInfo *dom.AppInfo) pb.ChorusServer {
	return &handlers{
		storages:                storages,
		rclone:                  rclone,
		s3clients:               s3clients,
		queueSvc:                queueSvc,
		policySvc:               policySvc,
		versionSvc:              versionSvc,
		storageSvc:              storageSvc,
		checkSvc:                checkSvc,
		proxyClient:             proxyClient,
		agentClient:             agentClient,
		notificationSvc:         notificationSvc,
		replicationStatusLocker: replicationStatusLocker,
		userLocker:              userLocker,
		appInfo:                 appInfo,
	}
}

var _ pb.ChorusServer = &handlers{}

type handlers struct {
	storages                *s3.StorageConfig
	s3clients               s3client.Service
	queueSvc                tasks.QueueService
	rclone                  rclone.Service
	policySvc               policy.Service
	versionSvc              meta.VersionService
	storageSvc              storage.Service
	checkSvc                *handler.ConsistencyCheckSvc
	proxyClient             rpc.Proxy
	agentClient             *rpc.AgentClient
	notificationSvc         *notifications.Service
	appInfo                 *dom.AppInfo
	replicationStatusLocker *store.ReplicationStatusLocker
	userLocker              *store.UserLocker
}

func (h *handlers) StartConsistencyCheck(ctx context.Context, req *pb.StartConsistencyCheckRequest) (*emptypb.Empty, error) {
	if err := validate.StorageLocationsWithUser(h.storages, req.Locations, req.User); err != nil {
		return nil, fmt.Errorf("unable to validate storage locations: %w", err)
	}

	locationCount := len(req.Locations)
	checkLocations := make([]entity.ConsistencyCheckLocation, 0, locationCount)
	taskLocations := make([]tasks.MigrateLocation, 0, locationCount)
	for _, reqLocation := range req.Locations {
		checkLocations = append(checkLocations, entity.NewConsistencyCheckLocation(reqLocation.Storage, reqLocation.Bucket))
		taskLocations = append(taskLocations, tasks.MigrateLocation{
			Storage: reqLocation.Storage,
			Bucket:  reqLocation.Bucket,
		})
	}

	checkID := entity.NewConsistencyCheckID(checkLocations...)
	if err := h.checkSvc.RegisterConsistencyCheck(ctx, checkID, taskLocations); err != nil {
		return nil, fmt.Errorf("unable to start consistency check: %w", err)
	}

	consistencyCheckTask := tasks.ConsistencyCheckPayload{
		Locations: taskLocations,
		User:      req.User,
	}
	if err := h.queueSvc.EnqueueTask(ctx, consistencyCheckTask); err != nil {
		return nil, fmt.Errorf("unable to enqueue consistency check task: %w", err)
	}

	return &emptypb.Empty{}, nil
}

func (h *handlers) ListConsistencyChecks(ctx context.Context, _ *emptypb.Empty) (*pb.ListConsistencyChecksResponse, error) {
	checkList, err := h.checkSvc.GetConsistencyCheckList(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to list consistency checks: %w", err)
	}

	consistencyChecks := make([]*pb.ConsistencyCheck, 0, len(checkList))
	for _, check := range checkList {
		locations := make([]*pb.MigrateLocation, 0, len(check.Locations))
		for _, checkLocation := range check.Locations {
			locations = append(locations, &pb.MigrateLocation{
				Storage: checkLocation.Storage,
				Bucket:  checkLocation.Bucket,
			})
		}
		consistencyChecks = append(consistencyChecks, &pb.ConsistencyCheck{
			Locations:  locations,
			Queued:     check.Queued,
			Completed:  check.Completed,
			Ready:      check.Ready,
			Consistent: check.Consistent,
		})
	}

	return &pb.ListConsistencyChecksResponse{
		Checks: consistencyChecks,
	}, nil
}

func (h *handlers) GetConsistencyCheckReport(ctx context.Context, req *pb.ConsistencyCheckRequest) (*pb.GetConsistencyCheckReportResponse, error) {
	if err := validate.StorageLocations(h.storages, req.Locations); err != nil {
		return nil, fmt.Errorf("unable to validate storage locations: %w", err)
	}

	checkLocations := make([]entity.ConsistencyCheckLocation, 0, len(req.Locations))
	for _, reqLocation := range req.Locations {
		checkLocations = append(checkLocations, entity.NewConsistencyCheckLocation(reqLocation.Storage, reqLocation.Bucket))
	}

	checkID := entity.NewConsistencyCheckID(checkLocations...)
	checkStatus, err := h.checkSvc.GetConsistencyCheckStatus(ctx, checkID)
	if err != nil {
		return nil, fmt.Errorf("unable to get consistency check status: %w", err)
	}

	return &pb.GetConsistencyCheckReportResponse{
		Check: &pb.ConsistencyCheck{
			Locations:  req.Locations,
			Queued:     checkStatus.Queued,
			Completed:  checkStatus.Completed,
			Ready:      checkStatus.Ready,
			Consistent: checkStatus.Consistent,
		},
	}, nil
}

func (h *handlers) GetConsistencyCheckReportEntries(ctx context.Context, req *pb.GetConsistencyCheckReportEntriesRequest) (*pb.GetConsistencyCheckReportEntriesResponse, error) {
	if err := validate.StorageLocations(h.storages, req.Locations); err != nil {
		return nil, fmt.Errorf("unable to validate storage locations: %w", err)
	}

	checkLocations := make([]entity.ConsistencyCheckLocation, 0, len(req.Locations))
	for _, reqLocation := range req.Locations {
		checkLocations = append(checkLocations, entity.NewConsistencyCheckLocation(reqLocation.Storage, reqLocation.Bucket))
	}

	checkID := entity.NewConsistencyCheckID(checkLocations...)
	reportPage, err := h.checkSvc.GetConsistencyCheckReportEntries(ctx, checkID, req.Cursor, req.PageSize)
	if err != nil {
		return nil, fmt.Errorf("unable to get consistency sets page: %w", err)
	}

	entries := make([]*pb.ConsistencyCheckReportEntry, 0, len(reportPage.Entries))
	for _, reportEntry := range reportPage.Entries {
		storageEntries := make([]*pb.ConsistencyCheckStorageEntry, 0, len(reportEntry.StorageEntries))
		for _, entry := range reportEntry.StorageEntries {
			storageEntries = append(storageEntries, &pb.ConsistencyCheckStorageEntry{
				Storage:   entry.Storage,
				VersionId: entry.VersionID,
			})
		}
		entries = append(entries, &pb.ConsistencyCheckReportEntry{
			Object:         reportEntry.Object,
			Etag:           reportEntry.Etag,
			StorageEntries: storageEntries,
		})
	}

	return &pb.GetConsistencyCheckReportEntriesResponse{
		Entries: entries,
		Cursor:  reportPage.Cursor,
	}, nil
}

func (h *handlers) DeleteConsistencyCheckReport(ctx context.Context, req *pb.ConsistencyCheckRequest) (*emptypb.Empty, error) {
	if err := validate.StorageLocations(h.storages, req.Locations); err != nil {
		return nil, fmt.Errorf("unable to validate storage locations: %w", err)
	}

	checkLocations := make([]entity.ConsistencyCheckLocation, 0, len(req.Locations))
	for _, reqLocation := range req.Locations {
		checkLocations = append(checkLocations, entity.NewConsistencyCheckLocation(reqLocation.Storage, reqLocation.Bucket))
	}

	checkID := entity.NewConsistencyCheckID(checkLocations...)
	if err := h.checkSvc.DeleteConsistencyCheck(ctx, checkID); err != nil {
		return nil, fmt.Errorf("unable to delete consistency check: %w", err)
	}

	return nil, nil
}

func (h *handlers) GetAppVersion(_ context.Context, _ *emptypb.Empty) (*pb.GetAppVersionResponse, error) {
	return &pb.GetAppVersionResponse{
		Version: h.appInfo.Version,
		Commit:  h.appInfo.Commit,
		Date:    h.appInfo.Date,
	}, nil
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
	client, err := h.s3clients.GetByName(ctx, req.User, req.From)
	if err != nil {
		return nil, err
	}
	buckets, err := client.S3().ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	res := &pb.ListBucketsForReplicationResponse{}

	for _, bucket := range buckets {
		bucketRoutingPolicyID := entity.NewBucketRoutingPolicyID(req.User, bucket.Name)
		_, err = h.policySvc.GetRoutingPolicy(ctx, bucketRoutingPolicyID)
		if errors.Is(err, dom.ErrRoutingBlock) {
			continue
		}
		replicationID := entity.ReplicationStatusID{
			User:        req.User,
			FromStorage: req.From,
			FromBucket:  bucket.Name,
			ToStorage:   req.To,
			ToBucket:    bucket.Name,
		}
		exists, err := h.policySvc.IsReplicationPolicyExists(ctx, replicationID)
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
	lock, err := h.userLocker.Lock(ctx, req.User, store.WithDuration(time.Second*5), store.WithRetry(true))
	if err != nil {
		return nil, err
	}
	defer lock.Release(ctx)
	err = lock.Do(ctx, time.Second*5, func() error {
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
		client, err := h.s3clients.GetByName(ctx, req.User, req.From)
		if err != nil {
			return err
		}
		// validate agentURL
		if err = h.validateAgentURL(ctx, req.From, req.AgentUrl); err != nil {
			return err
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
			bucketRoutingPolicyID := entity.NewBucketRoutingPolicyID(req.User, bucket)
			route, err := h.policySvc.GetRoutingPolicy(ctx, bucketRoutingPolicyID)
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
			replicationID := entity.ReplicationStatusID{
				User:        req.User,
				FromStorage: req.From,
				FromBucket:  bucket,
				ToStorage:   req.To,
				ToBucket:    bucket,
			}
			err = h.policySvc.AddBucketReplicationPolicy(ctx, replicationID, req.AgentUrl)
			if err != nil {
				if errors.Is(err, dom.ErrAlreadyExists) {
					continue
				}
				return err
			}
			// create bucket notification for agent:
			err = h.createAgentBucketNotification(ctx, replicationID, req.AgentUrl)
			if err != nil {
				return err
			}
			task := tasks.BucketCreatePayload{
				Bucket:   bucket,
				Location: "",
			}
			task.SetReplicationID(entity.IDFromBucketReplication(replicationID))
			err = h.queueSvc.EnqueueTask(ctx, task)
			if err != nil {
				return err
			}
		}
		return nil
	})

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
	client, err := h.s3clients.GetByName(ctx, req.User, req.From)
	if err != nil {
		return err
	}
	buckets, err := client.S3().ListBuckets(ctx)
	if err != nil {
		return err
	}
	userReplicationPolicy := entity.NewUserReplicationPolicy(req.From, req.To)
	err = h.policySvc.AddUserReplicationPolicy(ctx, req.User, userReplicationPolicy)
	if err != nil && !errors.Is(err, dom.ErrAlreadyExists) {
		return err
	}
	for _, bucket := range buckets {
		replicationID := entity.ReplicationStatusID{
			User:        req.User,
			FromStorage: req.From,
			FromBucket:  bucket.Name,
			ToStorage:   req.To,
			ToBucket:    bucket.Name,
		}
		err = h.policySvc.AddBucketReplicationPolicy(ctx, replicationID, nil)
		if err != nil {
			if errors.Is(err, dom.ErrAlreadyExists) {
				continue
			}
			return err
		}
		task := tasks.BucketCreatePayload{
			Bucket: bucket.Name,
		}
		task.SetReplicationID(entity.IDFromBucketReplication(replicationID))
		err = h.queueSvc.EnqueueTask(ctx, task)
		if err != nil {
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
	res := make([]*pb.Replication, 0, len(replications))
	for k, v := range replications {
		res = append(res, replicationToPb(k, v))
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
		for _, to := range policies.Destinations {
			res = append(res, &pb.UserReplication{
				User: user,
				From: policies.FromStorage,
				To:   to.Storage,
			})
		}
	}
	return &pb.ListUserReplicationsResponse{Replications: res}, nil
}

func (h *handlers) DeleteUserReplication(ctx context.Context, req *pb.DeleteUserReplicationRequest) (*emptypb.Empty, error) {
	lock, err := h.userLocker.Lock(ctx, req.User, store.WithDuration(time.Second), store.WithRetry(true))
	if err != nil {
		return nil, err
	}
	defer lock.Release(ctx)
	err = lock.Do(ctx, time.Second, func() error {
		userReplicationPolicy := entity.NewUserReplicationPolicy(req.From, req.To)
		err = h.policySvc.DeleteUserReplication(ctx, req.User, userReplicationPolicy)
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
			err = h.versionSvc.DeleteBucketMeta(ctx, meta.ToDest(req.To, ""), bucket)
			if err != nil {
				zerolog.Ctx(ctx).Err(err).Msg("unable to delete bucket version metadata")
			}
			err = h.storageSvc.CleanLastListedObj(ctx, req.From, req.To, bucket, bucket)
			if err != nil {
				zerolog.Ctx(ctx).Err(err).Msg("unable to delete bucket obj list metadata")
			}
			err = h.notificationSvc.DeleteBucketNotification(ctx, req.From, req.User, bucket)
			if err != nil {
				zerolog.Ctx(ctx).Err(err).Msg("unable to delete agent bucket notification")
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (h *handlers) PauseReplication(ctx context.Context, req *pb.ReplicationRequest) (*emptypb.Empty, error) {
	replicationID := entity.ReplicationStatusID{
		User:        req.User,
		FromStorage: req.From,
		FromBucket:  req.Bucket,
		ToStorage:   req.To,
		ToBucket:    req.ToBucket,
	}
	err := h.policySvc.PauseReplication(ctx, replicationID)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (h *handlers) ResumeReplication(ctx context.Context, req *pb.ReplicationRequest) (*emptypb.Empty, error) {
	replicationID := entity.ReplicationStatusID{
		User:        req.User,
		FromStorage: req.From,
		FromBucket:  req.Bucket,
		ToStorage:   req.To,
		ToBucket:    req.ToBucket,
	}
	err := h.policySvc.ResumeReplication(ctx, replicationID)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (h *handlers) DeleteReplication(ctx context.Context, req *pb.ReplicationRequest) (*emptypb.Empty, error) {
	lock, err := h.userLocker.Lock(ctx, req.User, store.WithDuration(time.Second), store.WithRetry(true))
	if err != nil {
		return nil, err
	}
	defer lock.Release(ctx)
	err = lock.Do(ctx, time.Second, func() error {
		replicationID := entity.ReplicationStatusID{
			User:        req.User,
			FromStorage: req.From,
			FromBucket:  req.Bucket,
			ToStorage:   req.To,
			ToBucket:    req.ToBucket,
		}
		err = h.policySvc.DeleteReplication(ctx, replicationID)
		if err != nil {
			return fmt.Errorf("%w: unable to delete replication policy", err)
		}
		err = h.versionSvc.DeleteBucketMeta(ctx, meta.ToDest(req.To, req.ToBucket), req.Bucket)
		if err != nil {
			return fmt.Errorf("%w: unable to delete version metadata", err)
		}
		err = h.storageSvc.CleanLastListedObj(ctx, req.From, req.To, req.Bucket, req.ToBucket)
		if err != nil {
			return fmt.Errorf("%w: unable to delete list obj metadata", err)
		}
		err = h.notificationSvc.DeleteBucketNotification(ctx, req.From, req.User, req.Bucket)
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Msg("unable to delete agent bucket notification")
		}
		return nil
	})
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

	res, err := h.rclone.Compare(ctx, req.ShowMatch, req.User, req.From, req.To, req.Bucket, req.ToBucket)
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
			replicationID := entity.ReplicationStatusID{
				User:        req.User,
				FromStorage: req.From,
				FromBucket:  req.Bucket,
				ToStorage:   req.To,
				ToBucket:    req.ToBucket,
			}
			m, err := h.policySvc.GetReplicationPolicyInfoExtended(ctx, replicationID)
			if err != nil {
				return err
			}
			pol := replicationToPb(replicationID, m)
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

func (h *handlers) AddBucketReplication(ctx context.Context, req *pb.AddBucketReplicationRequest) (*emptypb.Empty, error) {
	// validate
	if _, ok := h.storages.Storages[req.FromStorage]; !ok {
		return nil, fmt.Errorf("%w: unknown from storage %s", dom.ErrInvalidArg, req.FromStorage)
	}
	if _, ok := h.storages.Storages[req.ToStorage]; !ok {
		return nil, fmt.Errorf("%w: unknown to storage %s", dom.ErrInvalidArg, req.ToStorage)
	}
	if _, ok := h.storages.Storages[req.FromStorage].Credentials[req.User]; !ok {
		return nil, fmt.Errorf("%w: unknown user %s", dom.ErrInvalidArg, req.User)
	}
	lock, err := h.userLocker.Lock(ctx, req.User, store.WithDuration(time.Second*5), store.WithRetry(true))
	if err != nil {
		return nil, err
	}
	defer lock.Release(ctx)
	// obtain lock and try to add replication policy
	err = lock.Do(ctx, time.Second*5, func() error {
		client, err := h.s3clients.GetByName(ctx, req.User, req.FromStorage)
		if err != nil {
			return err
		}
		// validate agentURL
		if err = h.validateAgentURL(ctx, req.FromStorage, req.AgentUrl); err != nil {
			return err
		}

		// check if bucket exists in source
		ok, err := client.S3().BucketExists(ctx, req.FromBucket)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: unknown bucket %s", dom.ErrInvalidArg, req.FromBucket)
		}

		replicationID := entity.ReplicationStatusID{
			User:        req.User,
			FromStorage: req.FromStorage,
			FromBucket:  req.FromBucket,
			ToStorage:   req.ToStorage,
			ToBucket:    req.ToBucket,
		}
		// create policy:
		err = h.policySvc.AddBucketReplicationPolicy(ctx, replicationID, req.AgentUrl)
		if err != nil {
			return err
		}
		// create bucket notification for agent:
		err = h.createAgentBucketNotification(ctx, replicationID, req.AgentUrl)
		if err != nil {
			return err
		}
		// create task
		task := tasks.BucketCreatePayload{
			Bucket: req.FromBucket,
		}
		task.SetReplicationID(entity.IDFromBucketReplication(replicationID))
		err = h.queueSvc.EnqueueTask(ctx, task)
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

func (h *handlers) validateAgentURL(ctx context.Context, fromStorage string, agentURL *string) error {
	if agentURL == nil || *agentURL == "" {
		// agent is not set
		return nil
	}

	agents, err := h.agentClient.Ping(ctx)
	if err != nil {
		return err
	}
	for _, agent := range agents {
		if agent.URL != *agentURL {
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

func (h *handlers) createAgentBucketNotification(ctx context.Context, replicationID entity.ReplicationStatusID, agentURL *string) error {
	if agentURL == nil || *agentURL == "" {
		// agent is not set
		return nil
	}
	// create a topic and bucket notification for agent event source.
	err := h.notificationSvc.SubscribeToBucketNotifications(ctx, replicationID.FromStorage, replicationID.User, replicationID.FromBucket, *agentURL)
	if err != nil {
		cleanupErr := h.policySvc.DeleteReplication(context.Background(), replicationID)
		if cleanupErr != nil {
			zerolog.Ctx(ctx).Err(cleanupErr).Msgf("unable to cleanup replication policy for bucket %s", replicationID.FromBucket)
		}
		return err
	}
	return nil
}

func (h *handlers) GetReplication(ctx context.Context, req *pb.ReplicationRequest) (*pb.Replication, error) {
	replicationID := entity.ReplicationStatusID{
		User:        req.User,
		FromStorage: req.From,
		FromBucket:  req.Bucket,
		ToStorage:   req.To,
		ToBucket:    req.ToBucket,
	}
	status, err := h.policySvc.GetReplicationPolicyInfoExtended(ctx, replicationID)
	if err != nil {
		return nil, err
	}
	// Convert the status to the protobuf representation and return it
	return replicationToPb(replicationID, status), nil
}

func (h *handlers) SwitchBucket(ctx context.Context, req *pb.SwitchBucketRequest) (*emptypb.Empty, error) {
	// validate
	if err := validateSwitchRequest(req); err != nil {
		return nil, err
	}
	if _, ok := h.storages.Storages[req.ReplicationId.From]; !ok {
		return nil, fmt.Errorf("%w: unknown from storage %s", dom.ErrInvalidArg, req.ReplicationId.From)
	}
	if _, ok := h.storages.Storages[req.ReplicationId.To]; !ok {
		return nil, fmt.Errorf("%w: unknown to storage %s", dom.ErrInvalidArg, req.ReplicationId.To)
	}
	if _, ok := h.storages.Storages[req.ReplicationId.From].Credentials[req.ReplicationId.User]; !ok {
		return nil, fmt.Errorf("%w: unknown user %s", dom.ErrInvalidArg, req.ReplicationId.User)
	}
	if req.ReplicationId.ToBucket != req.ReplicationId.Bucket {
		// TODO: support replication to different bucket name in a separate PR.
		return nil, fmt.Errorf("%w: switch for replication to different bucket name is currently not supported", dom.ErrNotImplemented)
	}

	// obtain exclusive lock for the replication policy
	policyID := pbToReplicationID(req.ReplicationId)
	if err := validate.ReplicationStatusID(policyID); err != nil {
		return nil, err
	}
	lock, err := h.replicationStatusLocker.Lock(ctx, policyID, store.WithDuration(time.Second), store.WithRetry(true))
	if err != nil {
		return nil, err
	}
	defer lock.Release(ctx)
	err = lock.Do(ctx, time.Second, func() error {
		// persist switch metadata
		err = h.policySvc.SetDowntimeReplicationSwitch(ctx, policyID, pbToDowntimeOpts(req.DowntimeOpts))
		if err != nil {
			return fmt.Errorf("unable to store switch metadata: %w", err)
		}
		// create switch task
		err = h.queueSvc.EnqueueTask(ctx, tasks.SwitchWithDowntimePayload{
			ID: policyID,
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

func (h *handlers) SwitchBucketZeroDowntime(ctx context.Context, req *pb.SwitchBucketZeroDowntimeRequest) (*emptypb.Empty, error) {
	// validate
	if err := validateReplicationID(req.ReplicationId); err != nil {
		return nil, err
	}
	if _, ok := h.storages.Storages[req.ReplicationId.From]; !ok {
		return nil, fmt.Errorf("%w: unknown from storage %s", dom.ErrInvalidArg, req.ReplicationId.From)
	}
	if _, ok := h.storages.Storages[req.ReplicationId.To]; !ok {
		return nil, fmt.Errorf("%w: unknown to storage %s", dom.ErrInvalidArg, req.ReplicationId.To)
	}
	if _, ok := h.storages.Storages[req.ReplicationId.From].Credentials[req.ReplicationId.User]; !ok {
		return nil, fmt.Errorf("%w: unknown user %s", dom.ErrInvalidArg, req.ReplicationId.User)
	}
	if req.ReplicationId.ToBucket != req.ReplicationId.Bucket {
		// TODO: support replication to different bucket name in a separate PR.
		return nil, fmt.Errorf("%w: switch for replication to different bucket name is currently not supported", dom.ErrNotImplemented)
	}

	// obtain exclusive lock for the replication policy
	policyID := pbToReplicationID(req.ReplicationId)
	if err := validate.ReplicationStatusID(policyID); err != nil {
		return nil, err
	}

	lock, err := h.replicationStatusLocker.Lock(ctx, policyID, store.WithDuration(time.Second), store.WithRetry(true))
	if err != nil {
		return nil, err
	}
	defer lock.Release(ctx)
	err = lock.Do(ctx, time.Second, func() error {
		// persist switch metadata
		opts := &entity.ReplicationSwitchZeroDowntimeOpts{
			MultipartTTL: defaultZeroDowntimeMultipartTTL,
		}
		if req.MultipartTtl != nil && req.MultipartTtl.AsDuration() > 0 {
			opts.MultipartTTL = req.MultipartTtl.AsDuration()
		}

		err = h.policySvc.AddZeroDowntimeReplicationSwitch(ctx, policyID, opts)
		if err != nil {
			return fmt.Errorf("unable to store switch metadata: %w", err)
		}
		// create switch task
		err = h.queueSvc.EnqueueTask(ctx, tasks.ZeroDowntimeReplicationSwitchPayload{
			ID: policyID,
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

func (h *handlers) DeleteBucketSwitch(ctx context.Context, req *pb.ReplicationRequest) (*emptypb.Empty, error) {
	// validate
	if err := validateReplicationID(req); err != nil {
		return nil, err
	}
	// obtain exclusive lock for the replication policy
	policyID := pbToReplicationID(req)
	if err := validate.ReplicationStatusID(policyID); err != nil {
		return nil, err
	}
	lock, err := h.replicationStatusLocker.Lock(ctx, policyID, store.WithDuration(time.Second), store.WithRetry(true))
	if err != nil {
		return nil, err
	}
	defer lock.Release(ctx)
	err = lock.Do(ctx, time.Second, func() error {
		return h.policySvc.DeleteReplicationSwitch(ctx, policyID)
	})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (h *handlers) GetBucketSwitchStatus(ctx context.Context, req *pb.ReplicationRequest) (*pb.GetBucketSwitchStatusResponse, error) {
	// validate
	if err := validateReplicationID(req); err != nil {
		return nil, err
	}
	res, err := h.policySvc.GetReplicationSwitchInfo(ctx, pbToReplicationID(req))
	if err != nil {
		return nil, err
	}
	return toPbSwitchStatus(res)
}

func (h *handlers) ListReplicationSwitches(ctx context.Context, _ *emptypb.Empty) (*pb.ListSwitchResponse, error) {
	switches, err := h.policySvc.ListReplicationSwitchInfo(ctx)
	if err != nil {
		return nil, err
	}
	res := make([]*pb.GetBucketSwitchStatusResponse, len(switches))
	for i, sw := range switches {
		pb, err := toPbSwitchStatus(sw)
		if err != nil {
			return nil, err
		}
		res[i] = pb
	}
	return &pb.ListSwitchResponse{Switches: res}, nil
}
