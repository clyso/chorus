package s3

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStorageConfig_ValidateAddress(t *testing.T) {
	t.Run("Add http", func(t *testing.T) {
		r := require.New(t)

		s := Storage{
			StorageAddress: StorageAddress{
				Address:  "clyso.com",
				Provider: "Other",
			},
			Credentials: map[string]CredentialsV4{"user": {"1", "2"}},
		}
		r.NoError(s.Validate())
		r.EqualValues("http://clyso.com", s.Address)
	})
	t.Run("Add https", func(t *testing.T) {
		r := require.New(t)
		s := Storage{
			StorageAddress: StorageAddress{
				IsSecure: true, Address: "clyso.com", Provider: "Other",
			},
			Credentials: map[string]CredentialsV4{"user": {"1", "2"}}}
		r.NoError(s.Validate())
		r.EqualValues("https://clyso.com", s.Address)
	})

	t.Run("Already http", func(t *testing.T) {
		r := require.New(t)
		s := Storage{
			StorageAddress: StorageAddress{
				Address: "http://clyso.com", Provider: "Other",
			},
			Credentials: map[string]CredentialsV4{"user": {"1", "2"}}}
		r.NoError(s.Validate())
		r.EqualValues("http://clyso.com", s.Address)
	})
	t.Run("Already https", func(t *testing.T) {
		r := require.New(t)

		s := Storage{
			StorageAddress: StorageAddress{
				IsSecure: true, Address: "https://clyso.com", Provider: "Other",
			},
			Credentials: map[string]CredentialsV4{"user": {"1", "2"}}}
		r.NoError(s.Validate())
		r.EqualValues("https://clyso.com", s.Address)
	})

	t.Run("Invalid http", func(t *testing.T) {
		r := require.New(t)

		s := Storage{
			StorageAddress: StorageAddress{
				Address: "https://clyso.com", Provider: "Other",
			},
			Credentials: map[string]CredentialsV4{"user": {"1", "2"}}}
		r.Error(s.Validate())
	})
	t.Run("Invalid https", func(t *testing.T) {
		r := require.New(t)

		s := Storage{
			StorageAddress: StorageAddress{
				IsSecure: true, Address: "http://clyso.com", Provider: "Other",
			},
			Credentials: map[string]CredentialsV4{"user": {"1", "2"}}}
		r.Error(s.Validate())
	})
	t.Run("Invalid url", func(t *testing.T) {
		r := require.New(t)

		s := Storage{
			StorageAddress: StorageAddress{
				IsSecure: true, Address: "http:/clyso.com", Provider: "Other",
			},
			Credentials: map[string]CredentialsV4{"user": {"1", "2"}}}
		r.Error(s.Validate())
	})
}

func TestStorageConfig_ValidateTimeout(t *testing.T) {
	t.Run("default set when not provided", func(t *testing.T) {
		r := require.New(t)
		s := Storage{
			StorageAddress: StorageAddress{
				Address: "clyso.com", Provider: "Other",
			},
			Credentials: map[string]CredentialsV4{"user": {"1", "2"}}}
		r.NoError(s.Validate())
		r.EqualValues(defaultHttpTimeout, s.HttpTimeout)
	})
	t.Run("default ignored if set", func(t *testing.T) {
		r := require.New(t)
		http := defaultHttpTimeout + 123
		s := Storage{
			StorageAddress: StorageAddress{
				HttpTimeout: http, Address: "clyso.com", Provider: "Other",
			},
			Credentials: map[string]CredentialsV4{"user": {"1", "2"}}}
		r.NoError(s.Validate())
		r.EqualValues(http, s.HttpTimeout)
		r.NotEqualValues(defaultHttpTimeout, s.HttpTimeout)
	})
}

func TestStorage_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  Storage
		wantErr bool
	}{
		{
			name: "valid",
			config: Storage{
				Credentials: map[string]CredentialsV4{
					"user": {
						AccessKeyID:     "id",
						SecretAccessKey: "key",
					},
				},
				StorageAddress: StorageAddress{
					Address:       "clyso.com",
					Provider:      "Ceph",
					DefaultRegion: "",
					HttpTimeout:   0,
					IsSecure:      false,
				},
			},
			wantErr: false,
		},
		{
			name: "no credentials",
			config: Storage{
				Credentials: map[string]CredentialsV4{},
				StorageAddress: StorageAddress{
					Address:       "clyso.com",
					Provider:      "Ceph",
					DefaultRegion: "",
					HttpTimeout:   0,
					IsSecure:      false,
				},
			},
			wantErr: false,
		},
		{
			name: "no access key",
			config: Storage{
				Credentials: map[string]CredentialsV4{
					"user": {
						AccessKeyID:     "",
						SecretAccessKey: "key",
					},
				},
				StorageAddress: StorageAddress{
					Address:       "clyso.com",
					Provider:      "Ceph",
					DefaultRegion: "",
					HttpTimeout:   0,
					IsSecure:      false,
				},
			},
			wantErr: true,
		},
		{
			name: "no secret key",
			config: Storage{
				Credentials: map[string]CredentialsV4{
					"user": {
						AccessKeyID:     "id",
						SecretAccessKey: "",
					},
				},
				StorageAddress: StorageAddress{
					Address:       "clyso.com",
					Provider:      "Ceph",
					DefaultRegion: "",
					HttpTimeout:   0,
					IsSecure:      false,
				},
			},
			wantErr: true,
		},
		{
			name: "no address",
			config: Storage{
				Credentials: map[string]CredentialsV4{
					"user": {
						AccessKeyID:     "id",
						SecretAccessKey: "key",
					},
				},
				StorageAddress: StorageAddress{
					Address:       "",
					Provider:      "Ceph",
					DefaultRegion: "",
					HttpTimeout:   0,
					IsSecure:      false,
				},
			},
			wantErr: true,
		},
		{
			name: "no provider",
			config: Storage{
				Credentials: map[string]CredentialsV4{
					"user": {
						AccessKeyID:     "id",
						SecretAccessKey: "key",
					},
				},
				StorageAddress: StorageAddress{
					Address:       "clyso.com",
					Provider:      "",
					DefaultRegion: "",
					HttpTimeout:   0,
					IsSecure:      false,
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.config.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Storage.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
