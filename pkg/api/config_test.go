package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWebhookConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		conf    WebhookConfig
		wantErr string
	}{
		{
			name: "disabled",
			conf: WebhookConfig{Enabled: false},
		},
		{
			name: "enabled with defaults (shared ports)",
			conf: WebhookConfig{Enabled: true},
		},
		{
			name: "enabled with both separate ports",
			conf: WebhookConfig{Enabled: true, GrpcPort: 9680, HttpPort: 9681},
		},
		{
			name:    "grpcPort set without httpPort",
			conf:    WebhookConfig{Enabled: true, GrpcPort: 9680},
			wantErr: "httpPort must be set when grpcPort is set",
		},
		{
			name:    "httpPort set without grpcPort",
			conf:    WebhookConfig{Enabled: true, HttpPort: 9681},
			wantErr: "grpcPort must be set when httpPort is set",
		},
		{
			name: "disabled ignores invalid ports",
			conf: WebhookConfig{Enabled: false, GrpcPort: 9680},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.conf.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}
