package api

import (
	"testing"
	"time"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestValidateSwitchDowntimeOpts(t *testing.T) {
	tests := []struct {
		name    string
		input   *pb.SwitchDowntimeOpts
		wantErr string
	}{
		{
			name:    "nil input",
			input:   nil,
			wantErr: "",
		},
		{
			name: "valid empty config",
			input: &pb.SwitchDowntimeOpts{
				StartOnInitDone: false,
			},
			wantErr: "",
		},
		{
			name: "both cron and start_at set",
			input: &pb.SwitchDowntimeOpts{
				Cron:    stringPtr("0 * * * *"),
				StartAt: timestamppb.New(time.Now().Add(time.Hour)),
			},
			wantErr: "either cron or start_at can be set, but not both",
		},
		{
			name: "invalid cron expression",
			input: &pb.SwitchDowntimeOpts{
				Cron: stringPtr("invalid cron"),
			},
			wantErr: `invalid cron expression "invalid cron"`,
		},
		{
			name: "valid cron expression",
			input: &pb.SwitchDowntimeOpts{
				Cron: stringPtr("0 0 * * *"), // daily at midnight
			},
			wantErr: "",
		},
		{
			name: "start_at in past",
			input: &pb.SwitchDowntimeOpts{
				StartAt: timestamppb.New(time.Now().Add(-time.Hour)),
			},
			wantErr: "start_at is in the past according to server time",
		},
		{
			name: "start_at in future",
			input: &pb.SwitchDowntimeOpts{
				StartAt: timestamppb.New(time.Now().Add(time.Hour)),
			},
			wantErr: "",
		},
		{
			name: "complete valid config",
			input: &pb.SwitchDowntimeOpts{
				StartOnInitDone: true,
				Cron:            stringPtr("0 0 * * *"),
				MaxDuration:     durationpb.New(time.Hour),
				MaxEventLag:     uint32Ptr(1000),
			},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSwitchDonwtimeOpts(tt.input)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestValidateReplicationID(t *testing.T) {
	tests := []struct {
		name    string
		input   *pb.ReplicationRequest
		wantErr string
	}{
		{
			name:    "nil input",
			input:   nil,
			wantErr: "replication_id is required",
		},
		{
			name:    "empty user",
			input:   &pb.ReplicationRequest{Bucket: "b", From: "f", To: "t"},
			wantErr: "user is required",
		},
		{
			name:    "empty bucket",
			input:   &pb.ReplicationRequest{User: "u", From: "f", To: "t"},
			wantErr: "bucket is required",
		},
		{
			name:    "empty from",
			input:   &pb.ReplicationRequest{User: "u", Bucket: "b", To: "t"},
			wantErr: "from is required",
		},
		{
			name:    "empty to",
			input:   &pb.ReplicationRequest{User: "u", Bucket: "b", From: "f"},
			wantErr: "to is required",
		},
		{
			name:    "valid minimal",
			input:   &pb.ReplicationRequest{User: "u", Bucket: "b", From: "f", To: "t"},
			wantErr: "",
		},
		{
			name:    "valid with to_bucket",
			input:   &pb.ReplicationRequest{User: "u", Bucket: "b", From: "f", To: "t", ToBucket: stringPtr("tb")},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateReplicationID(tt.input)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
			}
		})
	}
}

func TestValidateSwitchRequest(t *testing.T) {
	tests := []struct {
		name    string
		input   *pb.SwitchBucketRequest
		wantErr string
	}{
		{
			name: "invalid replication_id",
			input: &pb.SwitchBucketRequest{
				ReplicationId: &pb.ReplicationRequest{},
			},
			wantErr: "invalid replication_id: user is required",
		},
		{
			name: "valid minimal with downtime",
			input: &pb.SwitchBucketRequest{
				ReplicationId: &pb.ReplicationRequest{
					User:   "u",
					Bucket: "b",
					From:   "f",
					To:     "t",
				},
			},
			wantErr: "",
		},
		{
			name: "no_downtime with downtime_window",
			input: &pb.SwitchBucketRequest{
				ReplicationId: &pb.ReplicationRequest{
					User:   "u",
					Bucket: "b",
					From:   "f",
					To:     "t",
				},
				DowntimeOpts: &pb.SwitchDowntimeOpts{},
			},
			wantErr: "no_downtime and downtime_window are mutually exclusive",
		},
		{
			name: "valid no_downtime",
			input: &pb.SwitchBucketRequest{
				ReplicationId: &pb.ReplicationRequest{
					User:   "u",
					Bucket: "b",
					From:   "f",
					To:     "t",
				},
			},
			wantErr: "",
		},
		{
			name: "invalid downtime_window",
			input: &pb.SwitchBucketRequest{
				ReplicationId: &pb.ReplicationRequest{
					User:   "u",
					Bucket: "b",
					From:   "f",
					To:     "t",
				},
				DowntimeOpts: &pb.SwitchDowntimeOpts{
					Cron:    stringPtr("0 * * * *"),
					StartAt: timestamppb.New(time.Now().Add(time.Hour)),
				},
			},
			wantErr: "either cron or start_at can be set, but not both",
		},
		{
			name: "valid complete with downtime",
			input: &pb.SwitchBucketRequest{
				ReplicationId: &pb.ReplicationRequest{
					User:   "u",
					Bucket: "b",
					From:   "f",
					To:     "t",
				},
				DowntimeOpts: &pb.SwitchDowntimeOpts{
					StartAt: timestamppb.New(time.Now().Add(time.Hour)),
				},
			},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSwitchRequest(tt.input)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

// Helper functions to create pointers
func stringPtr(s string) *string {
	return &s
}

func uint32Ptr(n uint32) *uint32 {
	return &n
}
