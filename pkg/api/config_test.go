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
		{
			name: "baseUrl with trailing slash is valid",
			conf: WebhookConfig{Enabled: true, BaseURL: "http://localhost:9671/"},
		},
		{
			name: "enabled with baseUrl",
			conf: WebhookConfig{Enabled: true, BaseURL: "http://localhost:9671"},
		},
		{
			name:    "baseUrl without scheme",
			conf:    WebhookConfig{Enabled: true, BaseURL: "localhost:9671"},
			wantErr: "baseUrl must include scheme",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			err := tt.conf.Validate()
			if tt.wantErr == "" {
				r.NoError(err)
			} else {
				r.ErrorContains(err, tt.wantErr)
			}
		})
	}
}

func TestWebhookConfig_S3NotificationURL(t *testing.T) {
	tests := []struct {
		name    string
		conf    WebhookConfig
		storage string
		want    string
		wantErr string
	}{
		{
			name:    "disabled webhook",
			conf:    WebhookConfig{Enabled: false, BaseURL: "http://localhost:9671"},
			storage: "main",
			wantErr: "webhook must be enabled",
		},
		{
			name:    "empty baseUrl",
			conf:    WebhookConfig{Enabled: true},
			storage: "main",
			wantErr: "webhook baseUrl is required",
		},
		{
			name:    "valid",
			conf:    WebhookConfig{Enabled: true, BaseURL: "http://localhost:9671"},
			storage: "main",
			want:    "http://localhost:9671/webhook/main/s3-notifications",
		},
		{
			name:    "valid with custom storage",
			conf:    WebhookConfig{Enabled: true, BaseURL: "https://chorus.example.com"},
			storage: "my-s3",
			want:    "https://chorus.example.com/webhook/my-s3/s3-notifications",
		},
		{
			name:    "trailing slash trimmed",
			conf:    WebhookConfig{Enabled: true, BaseURL: "http://localhost:9671/"},
			storage: "main",
			want:    "http://localhost:9671/webhook/main/s3-notifications",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			got, err := tt.conf.S3NotificationURL(tt.storage)
			if tt.wantErr == "" {
				r.NoError(err)
				r.Equal(tt.want, got)
			} else {
				r.ErrorContains(err, tt.wantErr)
			}
		})
	}
}
