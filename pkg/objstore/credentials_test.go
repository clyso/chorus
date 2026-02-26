package objstore

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/swift"
	"github.com/clyso/chorus/pkg/testutil"
)

func Test_credsSvc_Disabled(t *testing.T) {
	r := require.New(t)
	ctx := t.Context()
	s3Stor, swiftStor := "s3storage", "swiftstorage"
	conf := &Config{
		Main: s3Stor,
		Storages: map[string]Storage{
			s3Stor: {
				CommonConfig: CommonConfig{Type: dom.S3},
				S3:           &validS3,
			},
			swiftStor: {
				CommonConfig: CommonConfig{Type: dom.Swift},
				Swift:        &validSwift,
			},
		},
		DynamicCredentials: DynamicCredentialsConfig{
			Enabled: false,
		},
	}
	r.False(conf.DynamicCredentials.Enabled)
	s, err := NewCredsSvc(ctx, conf, nil)
	r.NoError(err)

	// HasUser returns from config
	t.Run("HasUser", func(t *testing.T) {
		r := require.New(t)
		err := s.HasUser(s3Stor, validUser)
		r.NoError(err)
		err = s.HasUser(swiftStor, validUser)
		r.NoError(err)

		// unknown storage
		err = s.HasUser("unknownstorage", validUser)
		r.Error(err)

		// unknown user
		err = s.HasUser(s3Stor, "unknownuser")
		r.Error(err)
		err = s.HasUser(swiftStor, "unknownuser")
		r.Error(err)
	})

	t.Run("GetCreds", func(t *testing.T) {
		r := require.New(t)
		s3Creds, err := s.GetS3Credentials(s3Stor, validUser)
		r.NoError(err)
		r.NoError(s3Creds.Validate())

		swiftCreds, err := s.GetSwiftCredentials(swiftStor, validUser)
		r.NoError(err)
		r.NoError(swiftCreds.Validate())

		// unknown storage
		_, err = s.GetS3Credentials("unknownstorage", validUser)
		r.Error(err)
		_, err = s.GetSwiftCredentials("unknownstorage", validUser)
		r.Error(err)

		// unknown user
		_, err = s.GetS3Credentials(s3Stor, "unknownuser")
		r.Error(err)
		_, err = s.GetSwiftCredentials(swiftStor, "unknownuser")
		r.Error(err)
	})

	t.Run("SetCreds", func(t *testing.T) {
		r := require.New(t)
		validS3Creds := s3.CredentialsV4{
			AccessKeyID:     "new_access_key",
			SecretAccessKey: "new_secret_key",
		}
		r.NoError(validS3Creds.Validate())
		err := s.SetS3Credentials(ctx, s3Stor, validUser, validS3Creds)
		r.Error(err, "cannot update creds when dynamic credentials disabled")

		err = s.SetS3Credentials(ctx, s3Stor, "new_user", validS3Creds)
		r.Error(err, "cannot add creds when dynamic credentials disabled")
	})

	t.Run("FindS3Credentials", func(t *testing.T) {
		r := require.New(t)
		creds := validS3.Credentials[validUser]

		user, gotCreds, err := s.FindS3Credentials(s3Stor, creds.AccessKeyID)
		r.NoError(err)
		r.EqualValues(creds, gotCreds)
		r.Equal(validUser, user)

		// unknown access key
		_, _, err = s.FindS3Credentials(s3Stor, "unknown_access_key")
		r.Error(err)
		// swift storage
		_, _, err = s.FindS3Credentials(swiftStor, creds.AccessKeyID)
		r.Error(err)
		// secret key match is not supported
		_, _, err = s.FindS3Credentials(s3Stor, creds.SecretAccessKey)
		r.Error(err)
	})

	t.Run("GetS3Address", func(t *testing.T) {
		r := require.New(t)
		addr, err := s.GetS3Address(s3Stor)
		r.NoError(err)
		expectedAddr := validS3.StorageAddress
		r.EqualValues(expectedAddr, addr)

		// unknown storage
		_, err = s.GetS3Address("unknownstorage")
		r.Error(err)

		//swift storage
		_, err = s.GetS3Address(swiftStor)
		r.Error(err)
	})

	t.Run("GetSwiftAddress", func(t *testing.T) {
		r := require.New(t)
		addr, err := s.GetSwiftAddress(swiftStor)
		r.NoError(err)
		expectedAddr := validSwift.StorageAddress
		r.EqualValues(expectedAddr, addr)
		// unknown storage
		_, err = s.GetSwiftAddress("unknownstorage")
		r.Error(err)
		// s3 storage
		_, err = s.GetSwiftAddress(s3Stor)
		r.Error(err)
	})
}

func Test_credsSvc_Enabled(t *testing.T) {
	var (
		validS3Creds = s3.CredentialsV4{
			AccessKeyID:     "dynamic_access_key",
			SecretAccessKey: "dynamic_secret_key",
		}
		validSwiftCreds = swift.Credentials{
			Username:   "dynamic_swift_user",
			Password:   "dynamic_swift_password",
			DomainName: "dynamic_domain",
			TenantName: "dynamic_tenant",
		}
	)
	c := testutil.SetupRedis(t)
	s3Stor, swiftStor := "s3storage", "swiftstorage"
	pollInterval := 100 * time.Millisecond
	conf := &Config{
		Main: s3Stor,
		Storages: map[string]Storage{
			s3Stor: {
				CommonConfig: CommonConfig{Type: dom.S3},
				S3:           &validS3,
			},
			swiftStor: {
				CommonConfig: CommonConfig{Type: dom.Swift},
				Swift:        &validSwift,
			},
		},
		DynamicCredentials: DynamicCredentialsConfig{
			Enabled:           true,
			DisableEncryption: false,
			MasterPassword:    "superseretlongpassword",
			PollInterval:      pollInterval,
		},
	}
	testEncryption := map[string]bool{
		"encrypted": true,
		"plaintext": false,
	}
	for name, enabled := range testEncryption {
		t.Run(name, func(t *testing.T) {
			r := require.New(t)
			ctx := t.Context()
			r.NoError(c.FlushAll(ctx).Err())
			conf.DynamicCredentials.DisableEncryption = !enabled
			conf.DynamicCredentials.MasterPassword = "superseretlongpassword"

			// create two instances to emlulate multiple replicas
			svcA, err := NewCredsSvc(ctx, conf, c)
			r.NoError(err)
			conf.DynamicCredentials.DisableEncryption = !enabled
			conf.DynamicCredentials.MasterPassword = "superseretlongpassword"
			svcB, err := NewCredsSvc(ctx, conf, c)
			r.NoError(err)

			// veryfy existing creds from config
			r.NoError(svcA.HasUser(s3Stor, validUser))
			r.NoError(svcA.HasUser(swiftStor, validUser))
			r.NoError(svcB.HasUser(s3Stor, validUser))
			r.NoError(svcB.HasUser(swiftStor, validUser))

			// unknown user should not exist
			r.Error(svcA.HasUser(s3Stor, "unknownuser"))
			r.Error(svcA.HasUser(swiftStor, "unknownuser"))
			r.Error(svcB.HasUser(s3Stor, "unknownuser"))
			r.Error(svcB.HasUser(swiftStor, "unknownuser"))

			// unknown storage
			r.Error(svcA.HasUser("unknownstorage", validUser))
			r.Error(svcB.HasUser("unknownstorage", validUser))

			newUser := "newuser"
			r.NoError(validS3Creds.Validate())
			r.NoError(validSwiftCreds.Validate())
			// cannot set creds for unknown storage
			r.Error(svcA.SetS3Credentials(ctx, "unknownstorage", newUser, validS3Creds))
			r.Error(svcA.SetSwiftCredentials(ctx, "unknownstorage", newUser, validSwiftCreds))
			r.Error(svcB.SetS3Credentials(ctx, "unknownstorage", newUser, validS3Creds))
			r.Error(svcB.SetSwiftCredentials(ctx, "unknownstorage", newUser, validSwiftCreds))

			// cannot set S3 creds on Swift storage and vice versa
			r.Error(svcA.SetS3Credentials(ctx, swiftStor, newUser, validS3Creds))
			r.Error(svcA.SetSwiftCredentials(ctx, s3Stor, newUser, validSwiftCreds))
			r.Error(svcB.SetS3Credentials(ctx, swiftStor, newUser, validS3Creds))
			r.Error(svcB.SetSwiftCredentials(ctx, s3Stor, newUser, validSwiftCreds))

			// cannot update users from config
			r.Error(svcA.SetS3Credentials(ctx, s3Stor, validUser, validS3Creds))
			r.Error(svcA.SetSwiftCredentials(ctx, swiftStor, validUser, validSwiftCreds))
			r.Error(svcB.SetS3Credentials(ctx, s3Stor, validUser, validS3Creds))
			r.Error(svcB.SetSwiftCredentials(ctx, swiftStor, validUser, validSwiftCreds))

			// cannot set invalid creds
			r.Error(svcA.SetS3Credentials(ctx, s3Stor, newUser, s3.CredentialsV4{}))
			r.Error(svcA.SetSwiftCredentials(ctx, swiftStor, newUser, swift.Credentials{}))
			r.Error(svcB.SetS3Credentials(ctx, s3Stor, newUser, s3.CredentialsV4{}))
			r.Error(svcB.SetSwiftCredentials(ctx, swiftStor, newUser, swift.Credentials{}))

			// check that there are existing users from config
			s3Users := svcA.ListUsers(s3Stor)
			r.Len(s3Users, 1)
			r.Contains(s3Users, validUser)
			swiftUsers := svcA.ListUsers(swiftStor)
			r.Len(swiftUsers, 1)
			r.Contains(swiftUsers, validUser)
			s3Users = svcB.ListUsers(s3Stor)
			r.Len(s3Users, 1)
			r.Contains(s3Users, validUser)
			swiftUsers = svcB.ListUsers(swiftStor)
			r.Len(swiftUsers, 1)
			r.Contains(swiftUsers, validUser)
			// check that HasUser returns ok for existing users
			r.NoError(svcA.HasUser(s3Stor, validUser))
			r.NoError(svcA.HasUser(swiftStor, validUser))
			r.NoError(svcB.HasUser(s3Stor, validUser))
			r.NoError(svcB.HasUser(swiftStor, validUser))
			// check that HasUser returns error for new user
			r.Error(svcA.HasUser(s3Stor, newUser))
			r.Error(svcA.HasUser(swiftStor, newUser))
			r.Error(svcB.HasUser(s3Stor, newUser))
			r.Error(svcB.HasUser(swiftStor, newUser))

			// set new S3 creds via svcA
			newS3Creds := s3.CredentialsV4{
				AccessKeyID:     "new_access_key",
				SecretAccessKey: "new_secret_key",
			}
			r.NoError(svcA.SetS3Credentials(ctx, s3Stor, newUser, newS3Creds))

			// get new S3 creds via svcA
			r.Eventually(func() bool {
				creds, err := svcA.GetS3Credentials(s3Stor, newUser)
				if err != nil {
					return false
				}
				return creds.AccessKeyID == newS3Creds.AccessKeyID && creds.SecretAccessKey == newS3Creds.SecretAccessKey
			}, pollInterval*2, pollInterval/2)

			// get new S3 creds via svcB
			r.Eventually(func() bool {
				creds, err := svcB.GetS3Credentials(s3Stor, newUser)
				if err != nil {
					return false
				}
				return creds.AccessKeyID == newS3Creds.AccessKeyID && creds.SecretAccessKey == newS3Creds.SecretAccessKey
			}, pollInterval*2, pollInterval/2)

			// check that new S3 creds cannot be accessed as Swift creds
			_, err = svcA.GetSwiftCredentials(swiftStor, newUser)
			r.Error(err)
			_, err = svcB.GetSwiftCredentials(swiftStor, newUser)
			r.Error(err)
			_, err = svcA.GetSwiftCredentials(s3Stor, newUser)
			r.Error(err)
			_, err = svcB.GetSwiftCredentials(s3Stor, newUser)
			r.Error(err)
			// check that new creds cannot be accessed as S3 creds on Swift storage
			_, err = svcA.GetS3Credentials(swiftStor, newUser)
			r.Error(err)
			_, err = svcB.GetS3Credentials(swiftStor, newUser)
			r.Error(err)
			// test HasUser for new user
			r.NoError(svcA.HasUser(s3Stor, newUser))
			r.NoError(svcB.HasUser(s3Stor, newUser))
			r.Error(svcA.HasUser(swiftStor, newUser))
			r.Error(svcB.HasUser(swiftStor, newUser))
			// test ListUsers
			s3UsersA := svcA.ListUsers(s3Stor)
			r.Len(s3UsersA, 2)
			r.Contains(s3UsersA, newUser)
			s3UsersB := svcB.ListUsers(s3Stor)
			r.Len(s3UsersB, 2)
			r.Contains(s3UsersB, newUser)
			swiftUsersA := svcA.ListUsers(swiftStor)
			r.Len(swiftUsersA, 1)
			r.NotContains(swiftUsersA, newUser)
			swiftUsersB := svcB.ListUsers(swiftStor)
			r.Len(swiftUsersB, 1)
			r.NotContains(swiftUsersB, newUser)
			// test find existing by access key
			user, creds, err := svcA.FindS3Credentials(s3Stor, validS3.Credentials[validUser].AccessKeyID)
			r.NoError(err)
			r.Equal(validUser, user)
			r.EqualValues(validS3.Credentials[validUser], creds)
			user, creds, err = svcB.FindS3Credentials(s3Stor, validS3.Credentials[validUser].AccessKeyID)
			r.NoError(err)
			r.Equal(validUser, user)
			r.EqualValues(validS3.Credentials[validUser], creds)
			// test find new by access key
			user, creds, err = svcA.FindS3Credentials(s3Stor, newS3Creds.AccessKeyID)
			r.NoError(err)
			r.Equal(newUser, user)
			r.EqualValues(newS3Creds, creds)
			user, creds, err = svcB.FindS3Credentials(s3Stor, newS3Creds.AccessKeyID)
			r.NoError(err)
			r.Equal(newUser, user)
			r.EqualValues(newS3Creds, creds)
			// test cannot find new by secret key
			_, _, err = svcA.FindS3Credentials(s3Stor, newS3Creds.SecretAccessKey)
			r.Error(err)
			// test cannot find on Swift storage
			_, _, err = svcA.FindS3Credentials(swiftStor, newS3Creds.AccessKeyID)
			r.Error(err)

			// update new S3 creds via svcB
			updatedS3Creds := s3.CredentialsV4{
				AccessKeyID:     "updated_access_key",
				SecretAccessKey: "updated_secret_key",
			}
			r.NoError(svcB.SetS3Credentials(ctx, s3Stor, newUser, updatedS3Creds))
			// add new Swift creds via svcB
			newSwiftCreds := swift.Credentials{
				Username:   "new_swift_user",
				Password:   "new_swift_password",
				DomainName: "new_domain",
				TenantName: "new_tenant",
			}
			r.NoError(svcB.SetSwiftCredentials(ctx, swiftStor, newUser, newSwiftCreds))

			// get updated S3 creds via svcA
			r.Eventually(func() bool {
				creds, err := svcA.GetS3Credentials(s3Stor, newUser)
				if err != nil {
					return false
				}
				return creds.AccessKeyID == updatedS3Creds.AccessKeyID && creds.SecretAccessKey == updatedS3Creds.SecretAccessKey
			}, pollInterval*2, pollInterval/2)

			// get updated S3 creds via svcB
			r.Eventually(func() bool {
				creds, err := svcB.GetS3Credentials(s3Stor, newUser)
				if err != nil {
					return false
				}
				return creds.AccessKeyID == updatedS3Creds.AccessKeyID && creds.SecretAccessKey == updatedS3Creds.SecretAccessKey
			}, pollInterval*2, pollInterval/2)

			// get new Swift creds via svcA
			r.Eventually(func() bool {
				creds, err := svcA.GetSwiftCredentials(swiftStor, newUser)
				if err != nil {
					return false
				}
				return creds.Username == newSwiftCreds.Username && creds.Password == newSwiftCreds.Password &&
					creds.DomainName == newSwiftCreds.DomainName && creds.TenantName == newSwiftCreds.TenantName
			}, pollInterval*2, pollInterval/2)
			// get new Swift creds via svcB
			r.Eventually(func() bool {
				creds, err := svcB.GetSwiftCredentials(swiftStor, newUser)
				if err != nil {
					return false
				}
				return creds.Username == newSwiftCreds.Username && creds.Password == newSwiftCreds.Password &&
					creds.DomainName == newSwiftCreds.DomainName && creds.TenantName == newSwiftCreds.TenantName
			}, pollInterval*2, pollInterval/2)

			// validate that all credentilas from both services match with expected values
			// users from config:
			gotS3A, err := svcA.GetS3Credentials(s3Stor, validUser)
			r.NoError(err)
			r.EqualValues(validS3.Credentials[validUser], gotS3A, "svcA config s3 user match")
			gotS3B, err := svcB.GetS3Credentials(s3Stor, validUser)
			r.NoError(err)
			r.EqualValues(validS3.Credentials[validUser], gotS3B, "svcB config s3 user match")
			gotSwiftA, err := svcA.GetSwiftCredentials(swiftStor, validUser)
			r.NoError(err)
			r.EqualValues(validSwift.Credentials[validUser], gotSwiftA, "svcA config swift user match")
			gotSwiftB, err := svcB.GetSwiftCredentials(swiftStor, validUser)
			r.NoError(err)
			r.EqualValues(validSwift.Credentials[validUser], gotSwiftB, "svcB config swift user match")

			// updated s3 user:
			gotNewS3A, err := svcA.GetS3Credentials(s3Stor, newUser)
			r.NoError(err)
			r.EqualValues(updatedS3Creds, gotNewS3A, "svcA updated s3 user match")
			gotNewS3B, err := svcB.GetS3Credentials(s3Stor, newUser)
			r.NoError(err)
			r.EqualValues(updatedS3Creds, gotNewS3B, "svcB updated s3 user match")

			// new swift user:
			gotNewSwiftA, err := svcA.GetSwiftCredentials(swiftStor, newUser)
			r.NoError(err)
			r.EqualValues(newSwiftCreds, gotNewSwiftA, "svcA new swift user match")
			gotNewSwiftB, err := svcB.GetSwiftCredentials(swiftStor, newUser)
			r.NoError(err)
			r.EqualValues(newSwiftCreds, gotNewSwiftB, "svcB new swift user match")

			// verify HasUser for all users
			r.NoError(svcA.HasUser(s3Stor, validUser))
			r.NoError(svcA.HasUser(swiftStor, validUser))
			r.NoError(svcA.HasUser(s3Stor, newUser))
			r.NoError(svcA.HasUser(swiftStor, newUser))
			r.NoError(svcB.HasUser(s3Stor, validUser))
			r.NoError(svcB.HasUser(swiftStor, validUser))
			r.NoError(svcB.HasUser(s3Stor, newUser))
			r.NoError(svcB.HasUser(swiftStor, newUser))

			// verify ListUsers for both services
			s3UsersA = svcA.ListUsers(s3Stor)
			r.Len(s3UsersA, 2)
			r.Contains(s3UsersA, validUser)
			r.Contains(s3UsersA, newUser)
			swiftUsersA = svcA.ListUsers(swiftStor)
			r.Len(swiftUsersA, 2)
			r.Contains(swiftUsersA, validUser)
			r.Contains(swiftUsersA, newUser)
			s3UsersB = svcB.ListUsers(s3Stor)
			r.Len(s3UsersB, 2)
			r.Contains(s3UsersB, validUser)
			r.Contains(s3UsersB, newUser)
			swiftUsersB = svcB.ListUsers(swiftStor)
			r.Len(swiftUsersB, 2)
			r.Contains(swiftUsersB, validUser)
			r.Contains(swiftUsersB, newUser)

			// verify that creds are stored encrypted in Redis
			s3Key := credsToRedisField(dom.S3, s3Stor, newUser)
			rawCred, err := c.HGet(ctx, redisHashKey, s3Key).Result()
			r.NoError(err)
			if enabled {
				// encryption enabled - stored as encrypted and base64-encoded string
				// cannot be unmarshalled directly
				var storedCreds s3.CredentialsV4
				r.Error(json.Unmarshal([]byte(rawCred), &storedCreds))
				decrypted, err := svcA.decrypt([]byte(rawCred))
				r.NoError(err)
				r.NoError(json.Unmarshal(decrypted, &storedCreds))
				r.EqualValues(updatedS3Creds, storedCreds)
			} else {
				// encryption disabled - stored as plaintext json
				var storedCreds s3.CredentialsV4
				r.NoError(json.Unmarshal([]byte(rawCred), &storedCreds))
				r.EqualValues(updatedS3Creds, storedCreds)
			}
		})
	}
}

func TestDynamicCredentialsConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		conf    DynamicCredentialsConfig
		wantErr bool
	}{
		{
			name: "valid encryption enabled",
			conf: DynamicCredentialsConfig{
				Enabled:           true,
				DisableEncryption: false,
				MasterPassword:    "superseretlongpassword",
				PollInterval:      time.Second,
			},
			wantErr: false,
		},
		{
			name: "valid encryption disabled",
			conf: DynamicCredentialsConfig{
				Enabled:           true,
				DisableEncryption: true,
				MasterPassword:    "",
				PollInterval:      time.Second,
			},
			wantErr: false,
		},
		{
			name: "always valid if disabled",
			conf: DynamicCredentialsConfig{
				Enabled:           false,
				DisableEncryption: false,
				MasterPassword:    "",
				PollInterval:      0,
			},
			wantErr: false,
		},
		{
			name: "missing master password",
			conf: DynamicCredentialsConfig{
				Enabled:           true,
				DisableEncryption: false,
				MasterPassword:    "",
				PollInterval:      time.Second,
			},
			wantErr: true,
		},
		{
			name: "too short master password",
			conf: DynamicCredentialsConfig{
				Enabled:           true,
				DisableEncryption: false,
				MasterPassword:    "short",
				PollInterval:      time.Second,
			},
			wantErr: true,
		},
		{
			name: "poll interval not set",
			conf: DynamicCredentialsConfig{
				Enabled:           true,
				DisableEncryption: true,
				MasterPassword:    "validlongpassword",
				PollInterval:      0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &tt.conf
			if err := c.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("DynamicCredentialsConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_credsSvc_ValidateReplicationID(t *testing.T) {
	c := testutil.SetupRedis(t)
	pollInterval := 100 * time.Millisecond
	s3A, s3B, swiftA, swiftB := "s3storagea", "s3storageb", "swiftstoragea", "swiftstorageb"
	conf := &Config{
		DynamicCredentials: DynamicCredentialsConfig{
			Enabled:           true,
			DisableEncryption: true,
			MasterPassword:    "superseretlongpassword",
			PollInterval:      pollInterval,
		},
		Main: s3A,
		Storages: map[string]Storage{
			s3A: {
				CommonConfig: CommonConfig{Type: dom.S3},
				S3:           &validS3,
			},
			s3B: {
				CommonConfig: CommonConfig{Type: dom.S3},
				S3:           &validS3,
			},
			swiftA: {
				CommonConfig: CommonConfig{Type: dom.Swift},
				Swift:        &validSwift,
			},
			swiftB: {
				CommonConfig: CommonConfig{Type: dom.Swift},
				Swift:        &validSwift,
			},
		},
	}
	tests := []struct {
		name              string
		config            *Config
		dynamicS3Users    map[string]map[string]s3.CredentialsV4
		dynamicSwiftUsers map[string]map[string]swift.Credentials
		in                entity.UniversalReplicationID
		wantErr           bool
	}{
		{
			name:   "valid same bucket replication ID",
			config: conf,
			in: entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
				User:        validUser,
				FromStorage: s3A,
				FromBucket:  "bucketA",
				ToStorage:   s3A,
				ToBucket:    "bucketB",
			}),
			wantErr: false,
		},
		{
			name:   "valid different bucket replication ID",
			config: conf,
			in: entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
				User:        validUser,
				FromStorage: swiftA,
				FromBucket:  "bucketA",
				ToStorage:   swiftB,
				ToBucket:    "bucketA",
			}),
			wantErr: false,
		},
		{
			name:   "valid user replication",
			config: conf,
			in: entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
				User:        validUser,
				FromStorage: s3A,
				ToStorage:   s3B,
			}),
			wantErr: false,
		},
		// same valid but with dynamic users
		{
			name:   "valid same bucket replication ID with dynamic user",
			config: conf,
			dynamicS3Users: map[string]map[string]s3.CredentialsV4{
				s3A: {
					"dynamicuser": {
						AccessKeyID:     "dynamic_access_key",
						SecretAccessKey: "dynamic_secret_key",
					},
				},
			},
			in: entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
				User:        "dynamicuser",
				FromStorage: s3A,
				FromBucket:  "bucketA",
				ToStorage:   s3A,
				ToBucket:    "bucketB",
			}),
			wantErr: false,
		},
		{
			name:   "valid dynamic user level replication",
			config: conf,
			dynamicSwiftUsers: map[string]map[string]swift.Credentials{
				swiftA: {
					"dynamicuser": {
						Username:   "dynamic_swift_user",
						Password:   "dynamic_swift_password",
						DomainName: "dynamic_domain",
						TenantName: "dynamic_tenant",
					},
				},
				swiftB: {
					"dynamicuser": {
						Username:   "dynamic_swift_user",
						Password:   "dynamic_swift_password",
						DomainName: "dynamic_domain",
						TenantName: "dynamic_tenant",
					},
				},
			},
			in: entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
				User:        "dynamicuser",
				FromStorage: swiftA,
				ToStorage:   swiftB,
			}),
			wantErr: false,
		},
		// invalid for storage types missmatch
		{
			name:   "invalid storage type missmatch",
			config: conf,
			in: entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
				User:        validUser,
				FromStorage: s3A,
				ToStorage:   swiftA,
			}),
			wantErr: true,
		},
		// invalid unknown from storage
		{
			name:   "invalid unknown from storage",
			config: conf,
			in: entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
				User:        validUser,
				FromStorage: "unknownstorage",
				ToStorage:   s3B,
			}),
			wantErr: true,
		},
		// invalid unknown to storage
		{
			name:   "invalid unknown to storage",
			config: conf,
			in: entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
				User:        validUser,
				FromStorage: s3A,
				ToStorage:   "unknownstorage",
			}),
			wantErr: true,
		},
		// invalid unknown user
		{
			name:   "invalid unknown user",
			config: conf,
			in: entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
				User:        "unknownuser",
				FromStorage: s3A,
				ToStorage:   s3B,
			}),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			ctx := t.Context()
			r.NoError(c.FlushAll(ctx).Err())
			// create two instances to emlulate multiple replicas
			svcA, err := NewCredsSvc(ctx, conf, c)
			r.NoError(err)

			if tt.dynamicS3Users != nil {
				for stor, users := range tt.dynamicS3Users {
					for user, creds := range users {
						r.NoError(svcA.SetS3Credentials(ctx, stor, user, creds))
					}
				}
			}
			if tt.dynamicSwiftUsers != nil {
				for stor, users := range tt.dynamicSwiftUsers {
					for user, creds := range users {
						r.NoError(svcA.SetSwiftCredentials(ctx, stor, user, creds))
					}
				}
			}
			// wait for user lens to be updated
			if len(tt.dynamicS3Users) != 0 || len(tt.dynamicSwiftUsers) != 0 {
				r.Eventually(func() bool {
					for stor, users := range tt.dynamicS3Users {
						listed := svcA.ListUsers(stor)
						if len(listed) != len(users)+1 {
							return false
						}
					}
					for stor, users := range tt.dynamicSwiftUsers {
						listed := svcA.ListUsers(stor)
						if len(listed) != len(users)+1 {
							return false
						}
					}
					return true
				}, pollInterval*2, pollInterval/2)
			}

			err = svcA.ValidateReplicationID(tt.in)
			if tt.wantErr {
				r.Error(err)
			} else {
				r.NoError(err)
			}
		})
	}
}
