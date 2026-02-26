package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
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

// Helper functions to create pointers
func stringPtr(s string) *string {
	return &s
}

func uint32Ptr(n uint32) *uint32 {
	return &n
}
