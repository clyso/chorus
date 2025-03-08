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

	"github.com/clyso/chorus/pkg/policy"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

func tsToPb(ts *time.Time) *timestamppb.Timestamp {
	if ts == nil {
		return nil
	}
	return timestamppb.New(*ts)
}

func replicationToPb(in policy.ReplicationPolicyStatusExtended) *pb.Replication {
	return &pb.Replication{
		User:            in.User,
		Bucket:          in.Bucket,
		From:            in.From,
		To:              in.To,
		ToBucket:        in.ToBucket,
		CreatedAt:       timestamppb.New(in.CreatedAt),
		IsPaused:        in.IsPaused,
		IsInitDone:      in.ReplicationPolicyStatus.InitDone(),
		InitObjListed:   in.InitObjListed,
		InitObjDone:     in.InitObjDone,
		InitBytesListed: in.InitBytesListed,
		InitBytesDone:   in.InitBytesDone,
		Events:          in.Events,
		EventsDone:      in.EventsDone,
		InitDoneAt:      tsToPb(in.InitDoneAt),
		LastEmittedAt:   tsToPb(in.LastEmittedAt),
		LastProcessedAt: tsToPb(in.LastProcessedAt),
		AgentUrl:        strPtr(in.AgentURL),
		HasSwitch:       in.HasSwitch,
	}
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func pbToReplicationID(in *pb.ReplicationRequest) policy.ReplicationID {
	return policy.ReplicationID{
		User:     in.User,
		Bucket:   in.Bucket,
		From:     in.From,
		To:       in.To,
		ToBucket: in.ToBucket,
	}
}

func pbToDowntimeOpts(in *pb.SwitchDowntimeOpts) *policy.SwitchDowntimeOpts {
	if in == nil {
		return nil
	}
	return &policy.SwitchDowntimeOpts{
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

func toPbSwitchStatus(in policy.SwitchInfo) (*pb.GetBucketSwitchStatusResponse, error) {
	id, err := in.ReplicationID()
	if err != nil {
		return nil, err
	}
	res := &pb.GetBucketSwitchStatusResponse{
		LastStatus:    toPbSwitchWithDowntimeStatus(in.LastStatus),
		ZeroDowntime:  in.IsZeroDowntime(),
		LastStartedAt: tsToPb(in.LastStartedAt),
		DoneAt:        tsToPb(in.DoneAt),
		History:       in.History,
		ReplicationId: &pb.ReplicationRequest{
			User:     id.User,
			Bucket:   id.Bucket,
			From:     id.From,
			To:       id.To,
			ToBucket: id.ToBucket,
		},
	}
	if in.IsZeroDowntime() {
		res.MultipartTtl = durationToPb(in.MultipartTTL)
	} else {
		res.DowntimeOpts = toPbDowntimeOpts(in.SwitchDowntimeOpts)
	}
	return res, nil
}

func toPbSwitchWithDowntimeStatus(in policy.SwitchWithDowntimeStatus) pb.GetBucketSwitchStatusResponse_Status {
	switch in {
	case policy.StatusInProgress:
		return pb.GetBucketSwitchStatusResponse_InProgress
	case policy.StatusCheckInProgress:
		return pb.GetBucketSwitchStatusResponse_CheckInProgress
	case policy.StatusSkipped:
		return pb.GetBucketSwitchStatusResponse_Skipped
	case policy.StatusError:
		return pb.GetBucketSwitchStatusResponse_Error
	case policy.StatusDone:
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

func toPbDowntimeOpts(in policy.SwitchDowntimeOpts) *pb.SwitchDowntimeOpts {
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
