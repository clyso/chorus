package util

import (
	"testing"
)

func TestParseBytes(t *testing.T) {

	tests := []struct {
		name    string
		args    string
		want    int64
		wantErr bool
	}{
		{
			args:    "1Mi",
			want:    1048576,
			wantErr: false,
		},
		{
			args:    "69MiB",
			want:    72351744,
			wantErr: false,
		},
		{
			args:    "150.5Mi",
			want:    157810688,
			wantErr: false,
		},
		{
			args:    "1M",
			want:    1000000,
			wantErr: false,
		},
		{
			args:    "69MB",
			want:    69000000,
			wantErr: false,
		},
		{
			args:    "150.5M",
			want:    150500000,
			wantErr: false,
		},
		{
			args:    "1Ki",
			want:    1024,
			wantErr: false,
		},
		{
			args:    "100500KiB",
			want:    102912000,
			wantErr: false,
		},
		{
			args:    "69.5KiB",
			want:    71168,
			wantErr: false,
		},
		{
			args:    "1K",
			want:    1000,
			wantErr: false,
		},
		{
			args:    "100500KB",
			want:    100500000,
			wantErr: false,
		},
		{
			args:    "69.5KB",
			want:    69500,
			wantErr: false,
		},
		{
			args:    "1Gi",
			want:    1073741824,
			wantErr: false,
		},
		{
			args:    "1.5GiB",
			want:    1610612736,
			wantErr: false,
		},
		{
			args:    "3Gi",
			want:    3221225472,
			wantErr: false,
		},
		{
			args:    "1G",
			want:    1000000000,
			wantErr: false,
		},
		{
			args:    "1.5GB",
			want:    1500000000,
			wantErr: false,
		},
		{
			args:    "3G",
			want:    3000000000,
			wantErr: false,
		},
		{
			args:    "1234",
			want:    1234,
			wantErr: false,
		},
		{
			args:    "-3Gi",
			want:    0,
			wantErr: true,
		},
		{
			args:    "-3GiB",
			want:    0,
			wantErr: true,
		},
		{
			args:    ";q3o4if",
			want:    0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.args, func(t *testing.T) {
			got, err := ParseBytes(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseBytes() got = %v, want %v", got, tt.want)
			}
		})
	}
}
