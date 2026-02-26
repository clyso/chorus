package swift

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStorage_Validate(t *testing.T) {
	tests := []struct {
		name    string
		in      Storage
		wantErr bool
	}{
		{
			name: "valid",
			in: Storage{
				Credentials: map[string]Credentials{
					"user": {
						Username:   "username",
						Password:   "password",
						DomainName: "domain",
						TenantName: "tenant",
					},
				},
				StorageAddress: StorageAddress{
					StorageEndpointName: "endpoint",
					AuthURL:             "http://auth.url",
				},
			},
			wantErr: false,
		},
		{
			name: "no credentials",
			in: Storage{
				Credentials: map[string]Credentials{},
				StorageAddress: StorageAddress{
					StorageEndpointName: "endpoint",
					AuthURL:             "http://auth.url",
				},
			},
			wantErr: false,
		},
		{
			name: "no username",
			in: Storage{
				Credentials: map[string]Credentials{
					"user": {
						Username:   "",
						Password:   "password",
						DomainName: "domain",
						TenantName: "tenant",
					},
				},
				StorageAddress: StorageAddress{
					StorageEndpointName: "endpoint",
					AuthURL:             "http://auth.url",
				},
			},
			wantErr: true,
		},
		{
			name: "no password",
			in: Storage{
				Credentials: map[string]Credentials{
					"user": {
						Username:   "username",
						Password:   "",
						DomainName: "domain",
						TenantName: "tenant",
					},
				},
				StorageAddress: StorageAddress{
					StorageEndpointName: "endpoint",
					AuthURL:             "http://auth.url",
				},
			},
			wantErr: true,
		},
		{
			name: "no storage endpoint name",
			in: Storage{
				Credentials: map[string]Credentials{
					"user": {
						Username:   "username",
						Password:   "password",
						DomainName: "domain",
						TenantName: "tenant",
					},
				},
				StorageAddress: StorageAddress{
					StorageEndpointName: "",
					AuthURL:             "http://auth.url",
				},
			},
			wantErr: true,
		},
		{
			name: "no auth URL",
			in: Storage{
				Credentials: map[string]Credentials{
					"user": {
						Username:   "username",
						Password:   "password",
						DomainName: "domain",
						TenantName: "tenant",
					},
				},
				StorageAddress: StorageAddress{
					StorageEndpointName: "endpoint",
					AuthURL:             "",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.in.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Storage.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultStorageEndpointType(t *testing.T) {
	r := require.New(t)
	s := Storage{
		Credentials: map[string]Credentials{
			"user": {
				Username:   "username",
				Password:   "password",
				DomainName: "domain",
				TenantName: "tenant",
			},
		},
		StorageAddress: StorageAddress{
			StorageEndpointName: "endpoint",
			AuthURL:             "http://auth.url",
		},
	}
	r.NoError(s.Validate())
	r.Equal(defaultEndpointType, s.StorageEndpointType, "expected default StorageEndpointType to be set")

	s.StorageEndpointType = "customType"
	r.NoError(s.Validate())
	r.Equal("customType", s.StorageEndpointType, "expected StorageEndpointType to remain unchanged")
	r.NotEqual(defaultEndpointType, s.StorageEndpointType, "expected StorageEndpointType to not be default")
}
