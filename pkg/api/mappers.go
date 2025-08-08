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
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/clyso/chorus/pkg/entity"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

func tsToPb(ts *time.Time) *timestamppb.Timestamp {
	if ts == nil {
		return nil
	}
	return timestamppb.New(*ts)
}

func replicationToPb(id entity.ReplicationStatusID, value entity.ReplicationStatus) *pb.Replication {
	return &pb.Replication{
		User:            id.User,
		Bucket:          id.FromBucket,
		From:            id.FromStorage,
		To:              id.ToStorage,
		ToBucket:        id.ToBucket,
		CreatedAt:       timestamppb.New(value.CreatedAt),
		IsPaused:        value.IsPaused,
		IsInitDone:      value.InitDone(),
		InitObjListed:   value.InitObjListed,
		InitObjDone:     value.InitObjDone,
		InitBytesListed: value.InitBytesListed,
		InitBytesDone:   value.InitBytesDone,
		Events:          value.Events,
		EventsDone:      value.EventsDone,
		InitDoneAt:      tsToPb(value.InitDoneAt),
		LastEmittedAt:   tsToPb(value.LastEmittedAt),
		LastProcessedAt: tsToPb(value.LastProcessedAt),
		AgentUrl:        strPtr(value.AgentURL),
		HasSwitch:       value.HasSwitch,
		IsArchived:      value.IsArchived,
		ArchivedAt:      tsToPb(value.ArchivedAt),
	}
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func pbToReplicationID(in *pb.ReplicationRequest) entity.ReplicationStatusID {
	return entity.ReplicationStatusID{
		User:        in.User,
		FromBucket:  in.Bucket,
		FromStorage: in.From,
		ToStorage:   in.To,
		ToBucket:    in.ToBucket,
	}
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

func toPbSwitchStatus(in entity.ReplicationSwitchInfo) (*pb.GetBucketSwitchStatusResponse, error) {
	id := in.ReplicationID()
	res := &pb.GetBucketSwitchStatusResponse{
		LastStatus:    toPbSwitchWithDowntimeStatus(in.LastStatus),
		ZeroDowntime:  in.IsZeroDowntime(),
		LastStartedAt: tsToPb(in.LastStartedAt),
		DoneAt:        tsToPb(in.DoneAt),
		History:       in.History,
		ReplicationId: &pb.ReplicationRequest{
			User:     id.User,
			Bucket:   id.FromBucket,
			From:     id.FromStorage,
			To:       id.ToStorage,
			ToBucket: id.ToBucket,
		},
	}
	if in.IsZeroDowntime() {
		res.MultipartTtl = durationToPb(in.MultipartTTL)
	} else {
		res.DowntimeOpts = toPbDowntimeOpts(in.ReplicationSwitchDowntimeOpts)
	}
	return res, nil
}

func toPbSwitchWithDowntimeStatus(in entity.ReplicationSwitchStatus) pb.GetBucketSwitchStatusResponse_Status {
	switch in {
	case entity.StatusInProgress:
		return pb.GetBucketSwitchStatusResponse_InProgress
	case entity.StatusCheckInProgress:
		return pb.GetBucketSwitchStatusResponse_CheckInProgress
	case entity.StatusSkipped:
		return pb.GetBucketSwitchStatusResponse_Skipped
	case entity.StatusError:
		return pb.GetBucketSwitchStatusResponse_Error
	case entity.StatusDone:
		return pb.GetBucketSwitchStatusResponse_Done
	default:
		return pb.GetBucketSwitchStatusResponse_NotStarted
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
