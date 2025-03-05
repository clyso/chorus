package api

import (
	"fmt"
	"time"

	"github.com/adhocore/gronx"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

func validateSwitchRequest(req *pb.SwitchBucketRequest) error {
	if err := validateReplicationID(req.ReplicationId); err != nil {
		return fmt.Errorf("invalid replication_id: %w", err)
	}
	if req.NoDowntime {
		if req.DowntimeOpts != nil {
			return fmt.Errorf("no_downtime and downtime_window are mutually exclusive")
		}
	}
	return validateSwitchDonwtimeOpts(req.DowntimeOpts)
}

func validateReplicationID(in *pb.ReplicationRequest) error {
	if in == nil {
		return fmt.Errorf("replication_id is required")
	}
	if in.User == "" {
		return fmt.Errorf("user is required")
	}
	if in.Bucket == "" {
		return fmt.Errorf("bucket is required")
	}
	if in.From == "" {
		return fmt.Errorf("from is required")
	}
	if in.To == "" {
		return fmt.Errorf("to is required")
	}
	return nil
}

func validateSwitchDonwtimeOpts(in *pb.SwitchDowntimeOpts) error {
	if in == nil {
		return nil
	}
	if in.Cron != nil && in.StartAt != nil {
		return fmt.Errorf("either cron or start_at can be set, but not both")
	}
	if in.Cron != nil && !gronx.IsValid(*in.Cron) {
		return fmt.Errorf("invalid cron expression %q", *in.Cron)
	}
	if in.StartAt != nil && in.StartAt.AsTime().Before(time.Now()) {
		return fmt.Errorf("start_at is in the past according to server time: %q", time.Now().Format(time.RFC3339))
	}
	return nil
}
