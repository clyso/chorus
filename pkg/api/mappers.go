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
	"fmt"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/objstore"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

func tsToPb(ts *time.Time) *timestamppb.Timestamp {
	if ts == nil {
		return nil
	}
	return timestamppb.New(*ts)
}

func replicationToPb(id entity.UniversalReplicationID, value entity.ReplicationStatusExtended) *pb.Replication {
	return &pb.Replication{
		CreatedAt:     timestamppb.New(value.CreatedAt),
		IsPaused:      value.IsPaused,
		IsInitDone:    value.InitDone(),
		InitObjListed: toListed(value.InitMigration),
		InitObjDone:   int64(value.InitMigration.Done),
		Events:        toListed(value.EventMigration),
		EventsDone:    int64(value.EventMigration.Done),
		HasSwitch:     value.Switch != nil,
		IsArchived:    value.IsArchived,
		ArchivedAt:    tsToPb(value.ArchivedAt),
		Id:            replicationIDToPb(id),
		EventLag:      durationToPb(value.Latency()),
		SwitchInfo:    toPbSwitchStatus(value.Switch),
		EventSource:   eventSourceFromStatus(value.ReplicationStatus),
		WebhookUrl:    webhookURLFromStatus(value.ReplicationStatus),
	}
}

func webhookURLFromStatus(s *entity.ReplicationStatus) string {
	if s == nil {
		return ""
	}
	return s.AgentURL
}

func eventSourceFromStatus(s *entity.ReplicationStatus) pb.EventSource {
	if s == nil {
		return pb.EventSource_EVENT_SOURCE_PROXY
	}
	switch dom.EventSource(s.EventSource) {
	case dom.EventSourceS3Notification:
		return pb.EventSource_EVENT_SOURCE_S3_NOTIFICATION
	case dom.EventSourceWebhook:
		return pb.EventSource_EVENT_SOURCE_WEBHOOK
	case dom.EventSourceProxy:
		return pb.EventSource_EVENT_SOURCE_PROXY
	default:
		// backward compat: legacy replications without event_source
		if s.AgentURL != "" {
			return pb.EventSource_EVENT_SOURCE_S3_NOTIFICATION
		}
		return pb.EventSource_EVENT_SOURCE_PROXY
	}
}

func toListed(in entity.QueueStats) int64 {
	return int64(in.Unprocessed + in.Done)
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func pbToDowntimeOpts(in *pb.SwitchDowntimeOpts) *entity.ReplicationSwitchDowntimeOpts {
	if in == nil {
		return nil
	}
	return &entity.ReplicationSwitchDowntimeOpts{
		StartOnInitDone:     in.StartOnInitDone,
		Cron:                in.Cron,
		StartAt:             pbToTs(in.StartAt),
		MaxDuration:         pbToDuration(in.MaxDuration),
		MaxEventLag:         in.MaxEventLag,
		SkipBucketCheck:     in.SkipBucketCheck,
		ContinueReplication: in.ContinueReplication,
	}
}

func pbToTs(in *timestamppb.Timestamp) *time.Time {
	if in == nil {
		return nil
	}
	ts := in.AsTime()
	return &ts
}

func pbToDuration(in *durationpb.Duration) time.Duration {
	if in == nil {
		return 0
	}
	return in.AsDuration()
}

func toPbSwitchStatus(in *entity.ReplicationSwitchInfo) *pb.ReplicationSwitch {
	if in == nil {
		return nil
	}

	id := in.ReplicationID()
	fromBucket, toBucket := "", ""
	if bucketID, ok := id.AsBucketID(); ok {
		fromBucket = bucketID.FromBucket
		toBucket = bucketID.ToBucket
	}
	res := &pb.ReplicationSwitch{
		LastStatus:    toPbSwitchWithDowntimeStatus(in.LastStatus),
		ZeroDowntime:  in.IsZeroDowntime(),
		LastStartedAt: tsToPb(in.LastStartedAt),
		DoneAt:        tsToPb(in.DoneAt),
		History:       in.History,
		ReplicationId: &pb.ReplicationID{
			User:        id.User(),
			ToBucket:    strPtr(toBucket),
			FromStorage: id.FromStorage(),
			ToStorage:   id.ToStorage(),
			FromBucket:  strPtr(fromBucket),
		},
	}
	if in.IsZeroDowntime() {
		res.MultipartTtl = durationToPb(in.MultipartTTL)
	} else {
		res.DowntimeOpts = toPbDowntimeOpts(in.ReplicationSwitchDowntimeOpts)
	}
	return res
}

func toPbSwitchWithDowntimeStatus(in entity.ReplicationSwitchStatus) pb.ReplicationSwitch_Status {
	switch in {
	case entity.StatusInProgress:
		return pb.ReplicationSwitch_IN_PROGRESS
	case entity.StatusCheckInProgress:
		return pb.ReplicationSwitch_CHECK_IN_PROGRESS
	case entity.StatusSkipped:
		return pb.ReplicationSwitch_SKIPPED
	case entity.StatusError:
		return pb.ReplicationSwitch_ERROR
	case entity.StatusDone:
		return pb.ReplicationSwitch_DONE
	default:
		return pb.ReplicationSwitch_NOT_STARTED
	}
}

func durationToPb(in time.Duration) *durationpb.Duration {
	if in == 0 {
		return nil
	}
	return durationpb.New(in)
}

func toPbDowntimeOpts(in entity.ReplicationSwitchDowntimeOpts) *pb.SwitchDowntimeOpts {
	return &pb.SwitchDowntimeOpts{
		StartOnInitDone:     in.StartOnInitDone,
		Cron:                in.Cron,
		StartAt:             tsToPb(in.StartAt),
		MaxDuration:         durationToPb(in.MaxDuration),
		MaxEventLag:         in.MaxEventLag,
		SkipBucketCheck:     in.SkipBucketCheck,
		ContinueReplication: in.ContinueReplication,
	}
}

func toPbStorages(credsSvc objstore.CredsService) []*pb.Storage {
	storages := credsSvc.Storages()
	res := make([]*pb.Storage, 0, len(storages))
	for name, storType := range storages {
		storage := &pb.Storage{
			Name:     name,
			IsMain:   name == credsSvc.MainStorage(),
			Provider: pb.Storage_Type(pb.Storage_Type_value[string(storType)]),
		}
		users := credsSvc.ListUsers(name)
		switch storType {
		case dom.S3:
			for _, user := range users {
				creds, err := credsSvc.GetS3Credentials(name, user)
				if err != nil {
					continue
				}
				storage.Credentials = append(storage.Credentials, &pb.Credential{
					Alias:     user,
					AccessKey: creds.AccessKeyID,
				})
			}
			addr, err := credsSvc.GetS3Address(name)
			if err != nil {
				continue
			}
			storage.Address = addr.Address
		case dom.Swift:
			for _, user := range users {
				creds, err := credsSvc.GetSwiftCredentials(name, user)
				if err != nil {
					continue
				}
				storage.Credentials = append(storage.Credentials, &pb.Credential{
					Alias:     user,
					AccessKey: creds.Username,
				})
			}
			storConf, err := credsSvc.GetSwiftAddress(name)
			if err != nil {
				continue
			}
			storage.Address = fmt.Sprintf("[%s, Endpoint: %s]", storConf.AuthURL, storConf.StorageEndpointName)
		}
		slices.SortFunc(storage.Credentials, func(a, b *pb.Credential) int {
			if n := strings.Compare(a.Alias, b.Alias); n != 0 {
				return n
			}
			return strings.Compare(a.AccessKey, b.AccessKey)
		})
		res = append(res, storage)
	}
	return res
}

func pbToReplicationID(in *pb.ReplicationID) (entity.UniversalReplicationID, error) {
	if in == nil {
		return entity.UniversalReplicationID{}, fmt.Errorf("%w: replication ID is not set", dom.ErrInvalidArg)
	}
	fromBucket, toBucket := dereferStr(in.FromBucket), dereferStr(in.ToBucket)
	isUser := fromBucket == "" && toBucket == ""
	var result entity.UniversalReplicationID
	if isUser {
		// user replication
		result = entity.UniversalFromUserReplication(entity.NewUserReplicationPolicy(in.User, in.FromStorage, in.ToStorage))
	} else {
		// bucket replication
		result = entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
			User:        in.User,
			FromStorage: in.FromStorage,
			FromBucket:  fromBucket,
			ToStorage:   in.ToStorage,
			ToBucket:    toBucket,
		})
	}
	return result, result.Validate()
}

func replicationIDToPb(in entity.UniversalReplicationID) *pb.ReplicationID {
	fromBucket, toBucket := "", ""
	if bucketID, ok := in.AsBucketID(); ok {
		fromBucket = bucketID.FromBucket
		toBucket = bucketID.ToBucket
	}
	return &pb.ReplicationID{
		User:        in.User(),
		FromStorage: in.FromStorage(),
		ToStorage:   in.ToStorage(),
		FromBucket:  strPtr(fromBucket),
		ToBucket:    strPtr(toBucket),
	}
}

func dereferStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func toUserRoutingsPb(filter *pb.RoutingsRequest_Filter, routings map[string]string, blocks map[string]bool) []*pb.UserRouting {
	//nolint:prealloc // cannot preallocate because of the filtering
	var res []*pb.UserRouting
	for user, toStorage := range routings {
		if filter != nil && filter.User != nil && *filter.User != "" && user != *filter.User {
			continue
		}
		if filter != nil && filter.ToStorage != nil && *filter.ToStorage != "" && toStorage != *filter.ToStorage {
			continue
		}
		blocked := blocks[user]
		if filter != nil && filter.IsBlocked != nil && *filter.IsBlocked != blocked {
			continue
		}

		res = append(res, &pb.UserRouting{
			User:      user,
			ToStorage: toStorage,
			IsBlocked: blocked,
		})
	}
	for user, isBlocked := range blocks {
		if filter != nil && filter.User != nil && *filter.User != "" && user != *filter.User {
			continue
		}
		if filter != nil && filter.IsBlocked != nil && *filter.IsBlocked != isBlocked {
			continue
		}
		_, exists := routings[user]
		if !exists && isBlocked {
			// add blocked routing without target
			res = append(res, &pb.UserRouting{
				User:      user,
				ToStorage: "",
				IsBlocked: isBlocked,
			})
		}
	}
	return res
}

func toBucketRoutingsPb(filter *pb.RoutingsRequest_Filter, user string, routings map[string]string, blocks map[string]bool) []*pb.BucketRouting {
	//nolint:prealloc // cannot preallocate because of the filtering
	var res []*pb.BucketRouting
	for bucket, toStorage := range routings {
		if filter != nil && filter.Bucket != nil && *filter.Bucket != "" && bucket != *filter.Bucket {
			continue
		}
		if filter != nil && filter.ToStorage != nil && *filter.ToStorage != "" && toStorage != *filter.ToStorage {
			continue
		}
		blocked := blocks[bucket]
		if filter != nil && filter.IsBlocked != nil && *filter.IsBlocked != blocked {
			continue
		}
		res = append(res, &pb.BucketRouting{
			User:      user,
			Bucket:    bucket,
			ToStorage: toStorage,
			IsBlocked: blocked,
		})
	}
	for bucket, isBlocked := range blocks {
		if filter != nil && filter.Bucket != nil && *filter.Bucket != "" && bucket != *filter.Bucket {
			continue
		}
		if filter != nil && filter.IsBlocked != nil && *filter.IsBlocked != isBlocked {
			continue
		}
		_, exists := routings[bucket]
		if !exists && isBlocked {
			// add blocked routing without target
			res = append(res, &pb.BucketRouting{
				User:      user,
				Bucket:    bucket,
				ToStorage: "",
				IsBlocked: isBlocked,
			})
		}
	}
	return res
}
