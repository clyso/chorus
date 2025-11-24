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
		Opts:          replicationOptsToPb(value.ReplicationStatus),
		EventLag:      durationToPb(value.Latency()),
		SwitchInfo:    toPbSwitchStatus(value.Switch),
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
		return pb.ReplicationSwitch_InProgress
	case entity.StatusCheckInProgress:
		return pb.ReplicationSwitch_CheckInProgress
	case entity.StatusSkipped:
		return pb.ReplicationSwitch_Skipped
	case entity.StatusError:
		return pb.ReplicationSwitch_Error
	case entity.StatusDone:
		return pb.ReplicationSwitch_Done
	default:
		return pb.ReplicationSwitch_NotStarted
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

func toPbStorages(in objstore.Config) []*pb.Storage {
	res := make([]*pb.Storage, 0, len(in.Storages))
	for name, storConf := range in.Storages {
		storage := &pb.Storage{
			Name:     name,
			IsMain:   name == in.Main,
			Provider: pb.Storage_Type(pb.Storage_Type_value[string(storConf.Type)]),
		}
		switch storConf.Type {
		case dom.S3:
			for user, creds := range storConf.S3.Credentials {
				storage.Credentials = append(storage.Credentials, &pb.Credential{
					Alias:     user,
					AccessKey: creds.AccessKeyID,
				})
			}
			storage.Address = storConf.S3.Address
		case dom.Swift:
			for user, creds := range storConf.Swift.Credentials {
				storage.Credentials = append(storage.Credentials, &pb.Credential{
					Alias:     user,
					AccessKey: creds.Username,
				})
			}
			storage.Address = fmt.Sprintf("[%s, Endpoint: %s]", storConf.Swift.AuthURL, storConf.Swift.StorageEndpointName)
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

func pbToReplicationOpts(in *pb.ReplicationOpts) entity.ReplicationOptions {
	if in == nil {
		return entity.ReplicationOptions{}
	}
	return entity.ReplicationOptions{
		AgentURL: dereferStr(in.AgentUrl),
	}
}

func replicationOptsToPb(in *entity.ReplicationStatus) *pb.ReplicationOpts {
	if in == nil {
		return nil
	}
	return &pb.ReplicationOpts{
		AgentUrl: strPtr(in.AgentURL),
	}
}
