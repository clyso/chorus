package api

import (
	"fmt"
	"time"

	"github.com/adhocore/gronx"

	"github.com/clyso/chorus/pkg/dom"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

func validateSwitchRequest(req *pb.SwitchBucketRequest) error {
	if err := validateReplicationID(req.ReplicationId); err != nil {
		return fmt.Errorf("invalid replication_id: %w", err)
	}
	return validateSwitchDonwtimeOpts(req.DowntimeOpts)
}

func validateReplicationID(in *pb.ReplicationRequest) error {
	if in == nil {
		return fmt.Errorf("%w: replication_id is required", dom.ErrInvalidArg)
	}
	if in.User == "" {
		return fmt.Errorf("%w: user is required", dom.ErrInvalidArg)
	}
	if in.Bucket == "" {
		return fmt.Errorf("%w: bucket is required", dom.ErrInvalidArg)
	}
	if in.From == "" {
		return fmt.Errorf("%w: from is required", dom.ErrInvalidArg)
	}
	if in.To == "" {
		return fmt.Errorf("%w: to is required", dom.ErrInvalidArg)
	}
	return nil
}

func validateSwitchDonwtimeOpts(in *pb.SwitchDowntimeOpts) error {
	if in == nil {
		return nil
	}
	if in.Cron != nil && in.StartAt != nil {
		return fmt.Errorf("%w: either cron or start_at can be set, but not both", dom.ErrInvalidArg)
	}
	if in.Cron != nil && !gronx.IsValid(*in.Cron) {
		return fmt.Errorf("%w: invalid cron expression %q", dom.ErrInvalidArg, *in.Cron)
	}
	if in.StartAt != nil && in.StartAt.AsTime().Before(time.Now()) {
		return fmt.Errorf("%w: start_at is in the past according to server time: %q", dom.ErrInvalidArg, time.Now().Format(time.RFC3339))
	}
	return nil
}
