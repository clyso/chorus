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
	}
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
