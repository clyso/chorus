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
	"github.com/clyso/chorus/pkg/policy"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
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
		CreatedAt:       timestamppb.New(in.CreatedAt),
		IsPaused:        in.IsPaused,
		IsInitDone:      in.ListingStarted && in.InitObjDone >= in.InitObjListed,
		InitObjListed:   in.InitObjListed,
		InitObjDone:     in.InitObjDone,
		InitBytesListed: in.InitBytesListed,
		InitBytesDone:   in.InitBytesDone,
		Events:          in.Events,
		EventsDone:      in.EventsDone,
		LastEmittedAt:   tsToPb(in.LastEmittedAt),
		LastProcessedAt: tsToPb(in.LastProcessedAt),
		AgentUrl:        strPtr(in.AgentURL),
		SwitchStatus:    switchStatusToPb(in.SwitchStatus),
	}
}

func switchStatusToPb(status policy.SwitchStatus) pb.Replication_SwitchEnum {
	switch status {
	case policy.InProgress:
		return pb.Replication_InProgress
	case policy.Done:
		return pb.Replication_Done
	default:
		return pb.Replication_NotStarted
	}
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
