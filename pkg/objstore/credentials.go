// Copyright 2026 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objstore

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"golang.org/x/crypto/argon2"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/swift"
)

const (
	redisHashKey       = "chorus:dynamic_creds"
	redisHFieldVersion = "version"
	redisHFieldSalt    = "salt"
	minPasswordLength  = 12
)

type CredsService interface {
	FindS3Credentials(storage, accessKey string) (user string, cred s3.CredentialsV4, err error)
	MainStorage() string
	Storages() map[string]dom.StorageType
	GetS3Address(storage string) (s3.StorageAddress, error)
	GetSwiftAddress(storage string) (swift.StorageAddress, error)
	ValidateReplicationID(id entity.UniversalReplicationID) error
	HasUser(storage, user string) error
	ListUsers(storage string) []string

	GetSwiftCredentials(storage, user string) (swift.Credentials, error)
	GetS3Credentials(storage, user string) (s3.CredentialsV4, error)

	SetSwiftCredentials(ctx context.Context, storage, user string, cred swift.Credentials) error
	SetS3Credentials(ctx context.Context, storage, user string, cred s3.CredentialsV4) error
}

type DynamicCredentialsConfig struct {
	MasterPassword    string        `yaml:"masterPassword"`
	PollInterval      time.Duration `yaml:"pollInterval"`
	Enabled           bool          `yaml:"enabled"`
	DisableEncryption bool          `yaml:"disableEncryption"`
}

func (c *DynamicCredentialsConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if !c.DisableEncryption && c.MasterPassword == "" {
		return fmt.Errorf("%w: master password is required when dynamic credentials encryption is enabled", dom.ErrInvalidArg)
	}
	if !c.DisableEncryption && len(c.MasterPassword) < minPasswordLength {
		return fmt.Errorf("%w: master password must be at least %d characters long", dom.ErrInvalidArg, minPasswordLength)
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("%w: poll interval must be greater than zero", dom.ErrInvalidArg)
	}
	return nil
}

func NewCredsSvc(ctx context.Context, conf *Config, rc redis.UniversalClient) (*credsSvc, error) {
	if err := conf.DynamicCredentials.Validate(); err != nil {
		return nil, err
	}
	instance := &credsSvc{
		types:          conf.Types(),
		s3AccessKeyIdx: buildS3AccessKeyIdxFromConf(conf),
		config:         conf,
		client:         rc,
		version:        -1,
	}
	if !conf.DynamicCredentials.Enabled {
		// dynamic credentials disabled, return credentials from config only
		return instance, nil
	}
	if !conf.DynamicCredentials.DisableEncryption {
		// derive encryption key
		salt, err := getOrCreateSalt(ctx, rc)
		if err != nil {
			return nil, fmt.Errorf("failed to get or create salt: %w", err)
		}
		instance.key = deriveKey(conf.DynamicCredentials.MasterPassword, salt)
	}
	if conf.DynamicCredentials.DisableEncryption {
		zerolog.Ctx(ctx).Warn().Msg("dynamic credentials encryption is disabled, credentials will be stored in Redis in plaintext")
	}
	// fetch initial credentials from redis
	if err := instance.sync(ctx); err != nil {
		return nil, fmt.Errorf("failed to sync credentials: %w", err)
	}
	// start background sync goroutine
	go func() {
		ticker := time.NewTicker(conf.DynamicCredentials.PollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := instance.sync(ctx); err != nil {
					zerolog.Ctx(ctx).Error().Err(err).Msg("failed to sync credentials")
				}
			}
		}
	}()

	return instance, nil
}

var _ CredsService = (*credsSvc)(nil)

/*
Implements CredsService interface for dynamic credentials management.
If dynamic credentials are disabled, credentials are read from config only.
Otherwise, credentials are stored in Redis and cached locally with periodic sync.

Redis HASH structure:
- Key: "chorus:dynamic_creds"
- Fields:
  - "version": int64 - version of the credentials, incremented on each update
  - "salt": string - base64 encoded salt for encryption key derivation
  - "<storType>:<storage>:<user>": credentials encoded string value
  - value: if encryption is enabled: JSON serialized credentials -> encrypted with AES-GCM -> base64 encoded
  - value: if encryption is disabled: JSON serialized credentials

Credentials encryption:
- AES-GCM with 256-bit key derived from master password using Argon2id KDF with per-instance salt stored in Redis.

Local cache:
- Maps of storage:user to credentials for S3 and Swift, updated on sync if version changes in Redis.
*/
type credsSvc struct {
	client     redis.UniversalClient
	types      map[string]dom.StorageType
	config     *Config
	swiftCreds map[string]swift.Credentials
	s3Creds    map[string]s3.CredentialsV4
	// index of S3 credentials by access key for chorus Proxy Auth Middleware
	s3AccessKeyIdx map[string]s3CredsByAccessKey
	key            []byte
	version        int64
	sync.RWMutex
}

type s3CredsByAccessKey struct {
	user        string
	credentials s3.CredentialsV4
}

func (s *credsSvc) FindS3Credentials(storage, accessKey string) (user string, cred s3.CredentialsV4, err error) {
	if s.config.DynamicCredentials.Enabled {
		s.RLock()
		defer s.RUnlock()
	}
	key := s3accessKeyIdxKey(storage, accessKey)
	res, ok := s.s3AccessKeyIdx[key]
	if !ok {
		return "", s3.CredentialsV4{}, dom.ErrNotFound
	}
	return res.user, res.credentials, nil
}

func (s *credsSvc) Storages() map[string]dom.StorageType {
	return s.types
}

func (s *credsSvc) StorageType(storage string) (dom.StorageType, error) {
	storConf, ok := s.config.Storages[storage]
	if !ok {
		return "", fmt.Errorf("%w: storage %q not found", dom.ErrNotFound, storage)
	}
	return storConf.CommonConfig.Type, nil
}

func (s *credsSvc) GetS3Address(storage string) (s3.StorageAddress, error) {
	res, ok := s.config.S3Storages()[storage]
	if !ok {
		return s3.StorageAddress{}, dom.ErrNotFound
	}
	return res.StorageAddress, nil
}

func (s *credsSvc) GetSwiftAddress(storage string) (swift.StorageAddress, error) {
	res, ok := s.config.SwiftStorages()[storage]
	if !ok {
		return swift.StorageAddress{}, dom.ErrNotFound
	}
	return res.StorageAddress, nil
}

func (s *credsSvc) ListUsers(storage string) []string {
	storageConf, ok := s.config.Storages[storage]
	if !ok {
		return nil
	}
	users := storageConf.UserList()
	if !s.config.DynamicCredentials.Enabled {
		// dynamic credentials disabled, return users from config only
		return users
	}
	// add users from dynamic credentials cache
	s.RLock()
	defer s.RUnlock()

	switch storageConf.CommonConfig.Type {
	case dom.S3:
		for cacheKey := range s.s3Creds {
			stor, user := cacheKeyToCreds(cacheKey)
			if stor == storage {
				users = append(users, user)
			}
		}
	case dom.Swift:
		for cacheKey := range s.swiftCreds {
			stor, user := cacheKeyToCreds(cacheKey)
			if stor == storage {
				users = append(users, user)
			}
		}
	default:
		// should not happen due to prior validation of storage config
		panic(fmt.Sprintf("unsupported storage type %q", storageConf.CommonConfig.Type))
	}
	return users
}

func (s *credsSvc) MainStorage() string {
	return s.config.Main
}

func (s *credsSvc) ValidateReplicationID(id entity.UniversalReplicationID) error {
	if err := s.config.sameStorType(id.FromStorage(), id.ToStorage()); err != nil {
		// TODO: allow cross-type replication in the future?
		return err
	}
	fromStor, toStor := id.FromStorage(), id.ToStorage()
	user := id.User()
	if err := s.HasUser(fromStor, user); err != nil {
		return fmt.Errorf("%w: from storage %q user %q", err, fromStor, user)
	}
	if err := s.HasUser(toStor, user); err != nil {
		return fmt.Errorf("%w: to storage %q user %q", err, toStor, user)
	}
	return id.Validate()
}

func (s *credsSvc) HasUser(storage string, user string) error {
	err := s.config.Exists(storage, user)
	if !s.config.DynamicCredentials.Enabled {
		// dynamic credentials disabled, return result from config
		return err
	}
	if err == nil {
		// user exists in config, return true
		return nil
	}
	// check in dynamic credentials cache
	cacheKey := credsCacheKey(storage, user)
	s.RLock()
	defer s.RUnlock()
	if _, ok := s.s3Creds[cacheKey]; ok {
		return nil
	}
	if _, ok := s.swiftCreds[cacheKey]; ok {
		return nil
	}
	return dom.ErrNotFound
}

func (s *credsSvc) sync(ctx context.Context) error {
	if !s.config.DynamicCredentials.Enabled {
		return nil
	}
	// read redis version of creds and update local cache
	remoteVer, err := s.client.HGet(ctx, redisHashKey, redisHFieldVersion).Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("failed to get credentials version from redis: %w", err)
	}
	if s.version == remoteVer {
		// no changes
		return nil
	}
	allCreds, err := s.client.HGetAll(ctx, redisHashKey).Result()
	if err != nil {
		return fmt.Errorf("failed to get all credentials from redis: %w", err)
	}
	s3Creds := make(map[string]s3.CredentialsV4)
	swiftCreds := make(map[string]swift.Credentials)
	s3AccessKeyIdx := buildS3AccessKeyIdxFromConf(s.config)
	for key, value := range allCreds {
		if key == redisHFieldVersion {
			// set remote version:
			remoteVer, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse remote version from redis: %w", err)
			}
			continue
		}
		if key == redisHFieldSalt {
			// ignore salt field
			continue
		}
		storType, cacheKey, err := credsFromRedisField(key)
		if err != nil {
			return fmt.Errorf("failed to parse redis field %s: %w", key, err)
		}
		plaintext, err := s.decrypt([]byte(value))
		if err != nil {
			return fmt.Errorf("failed to decrypt credentials for %s: %w", cacheKey, err)
		}
		switch storType {
		case dom.S3:
			var cred s3.CredentialsV4
			if err := json.Unmarshal(plaintext, &cred); err != nil {
				return fmt.Errorf("failed to unmarshal S3 credentials for %s: %w", cacheKey, err)
			}
			s3Creds[cacheKey] = cred

			// update s3AccessKeyIdx
			stor, user := cacheKeyToCreds(cacheKey)
			s3AccessKeyIdxKey := s3accessKeyIdxKey(stor, cred.AccessKeyID)
			s3AccessKeyIdx[s3AccessKeyIdxKey] = s3CredsByAccessKey{
				user:        user,
				credentials: cred,
			}
		case dom.Swift:
			var cred swift.Credentials
			if err := json.Unmarshal(plaintext, &cred); err != nil {
				return fmt.Errorf("failed to unmarshal Swift credentials for %s: %w", cacheKey, err)
			}
			swiftCreds[cacheKey] = cred
		default:
			return fmt.Errorf("%w: unknown storage type %s", dom.ErrInvalidArg, storType)
		}
	}

	s.Lock()
	defer s.Unlock()
	s.s3Creds = s3Creds
	s.s3AccessKeyIdx = s3AccessKeyIdx
	s.swiftCreds = swiftCreds
	s.version = remoteVer

	return nil

}

func (s *credsSvc) GetS3Credentials(storage string, user string) (s3.CredentialsV4, error) {
	creds, err := s.getS3FromConfig(storage, user)
	if !s.config.DynamicCredentials.Enabled {
		// dynamic credentials disabled, return result from config
		return creds, err
	}
	if err == nil {
		// user exists in config, return creds
		return creds, nil
	}
	// check in dynamic credentials cache
	cacheKey := credsCacheKey(storage, user)
	s.RLock()
	defer s.RUnlock()
	cred, ok := s.s3Creds[cacheKey]
	if !ok {
		return s3.CredentialsV4{}, dom.ErrNotFound
	}
	return cred, nil
}

func (s *credsSvc) getS3FromConfig(storage string, user string) (s3.CredentialsV4, error) {
	stor, ok := s.config.S3Storages()[storage]
	if !ok {
		return s3.CredentialsV4{}, dom.ErrNotFound
	}
	cred, ok := stor.Credentials[user]
	if !ok {
		return s3.CredentialsV4{}, dom.ErrNotFound
	}
	return cred, nil
}

func (s *credsSvc) GetSwiftCredentials(storage string, user string) (swift.Credentials, error) {
	creds, err := s.getSwiftFromConfig(storage, user)
	if !s.config.DynamicCredentials.Enabled {
		// dynamic credentials disabled, return result from config
		return creds, err
	}
	if err == nil {
		// user exists in config, return creds
		return creds, nil
	}
	// check in dynamic credentials cache
	cacheKey := credsCacheKey(storage, user)
	s.RLock()
	defer s.RUnlock()
	cred, ok := s.swiftCreds[cacheKey]
	if !ok {
		return swift.Credentials{}, dom.ErrNotFound
	}
	return cred, nil
}

func (s *credsSvc) getSwiftFromConfig(storage string, user string) (swift.Credentials, error) {
	stor, ok := s.config.SwiftStorages()[storage]
	if !ok {
		return swift.Credentials{}, dom.ErrNotFound
	}
	cred, ok := stor.Credentials[user]
	if !ok {
		return swift.Credentials{}, dom.ErrNotFound
	}
	return cred, nil
}

func (s *credsSvc) SetS3Credentials(ctx context.Context, storage string, user string, cred s3.CredentialsV4) error {
	if err := cred.Validate(); err != nil {
		return fmt.Errorf("%w: invalid S3 credentials: %w", dom.ErrInvalidArg, err)
	}
	return s.storeCred(ctx, dom.S3, storage, user, cred)
}

func (s *credsSvc) storeCred(ctx context.Context, storType dom.StorageType, storage, user string, value any) error {
	if !s.config.DynamicCredentials.Enabled {
		return fmt.Errorf("%w: dynamic credentials are disabled", dom.ErrInvalidArg)
	}
	// cannot overwrite existing user in config
	if err := s.config.Exists(storage, user); err == nil {
		return fmt.Errorf("%w: cannot overwrite existing user %q in config for storage %q", dom.ErrInvalidArg, user, storage)
	}
	// check if storage exists in config
	switch storType {
	case dom.S3:
		if _, ok := s.config.S3Storages()[storage]; !ok {
			return fmt.Errorf("%w: storage %q not found in config", dom.ErrInvalidArg, storage)
		}
	case dom.Swift:
		if _, ok := s.config.SwiftStorages()[storage]; !ok {
			return fmt.Errorf("%w: storage %q not found in config", dom.ErrInvalidArg, storage)
		}
	default:
		return fmt.Errorf("%w: unknown storage type %q", dom.ErrInvalidArg, storType)
	}

	// encrypt credentials
	value, err := s.encrypt(value)
	if err != nil {
		return fmt.Errorf("failed to encrypt S3 credentials: %w", err)
	}
	key := credsToRedisField(storType, storage, user)
	// store in redis and increment version in a transaction
	tx := s.client.TxPipeline()
	tx.HSet(ctx, redisHashKey, key, value)
	tx.HIncrBy(ctx, redisHashKey, redisHFieldVersion, 1)
	if _, err := tx.Exec(ctx); err != nil {
		return fmt.Errorf("failed to set S3 credentials in redis: %w", err)
	}
	return nil
}

func (s *credsSvc) SetSwiftCredentials(ctx context.Context, storage string, user string, cred swift.Credentials) error {
	if err := cred.Validate(); err != nil {
		return fmt.Errorf("%w: invalid Swift credentials: %w", dom.ErrInvalidArg, err)
	}
	return s.storeCred(ctx, dom.Swift, storage, user, cred)
}

func getOrCreateSalt(ctx context.Context, rc redis.UniversalClient) ([]byte, error) {
	// generate crypto random 16byte salt:
	salt := make([]byte, 16)
	n, err := rand.Read(salt)
	if err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}
	if n != 16 {
		return nil, fmt.Errorf("failed to generate salt: insufficient bytes read")
	}
	// encode to base64:
	saltB64 := base64.StdEncoding.EncodeToString(salt)
	// try to set in redis if not exists:
	set, err := rc.HSetNX(ctx, redisHashKey, redisHFieldSalt, saltB64).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to set salt in redis: %w", err)
	}
	if set {
		// we set the salt, return generated one
		return salt, nil
	}
	// salt already exists, get it
	saltStr, err := rc.HGet(ctx, redisHashKey, redisHFieldSalt).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get salt from redis: %w", err)
	}
	// decode from base64
	salt, err = base64.StdEncoding.DecodeString(saltStr)
	if err != nil {
		return nil, fmt.Errorf("failed to decode salt from base64: %w", err)
	}
	return salt, nil
}

func deriveKey(masterPassword string, salt []byte) []byte {
	const (
		// Argon2id parameters
		memory     = 64 * 1024 // 64 MB
		iterations = 3
		keyLength  = 32 // 32 bytes = 256 bits
	)
	pwd := []byte(masterPassword)
	cpuNum := runtime.NumCPU()
	key := argon2.IDKey(pwd, salt, iterations, memory, uint8(cpuNum), keyLength)
	return key

}

func credsCacheKey(storage, user string) string {
	// cache key format: "<storage name>:<user name>"
	// e.g. "my_storage:alice"
	return storage + ":" + user
}

func cacheKeyToCreds(cacheKey string) (storage, user string) {
	parts := strings.SplitN(cacheKey, ":", 2)
	if len(parts) != 2 {
		panic(fmt.Sprintf("invalid cache key format: %s", cacheKey))
	}
	return parts[0], parts[1]
}

func credsToRedisField(storType dom.StorageType, storage, user string) string {
	// redis field format: "<storType S3|SWIFT>:<svc cache key: (<storage>:<user)>"
	// e.g. "S3:my_storage:alice"
	return fmt.Sprintf("%s:%s", storType, credsCacheKey(storage, user))
}

func credsFromRedisField(field string) (storType dom.StorageType, cacheKey string, err error) {
	parts := strings.SplitN(field, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("%w: invalid redis field format", dom.ErrInvalidArg)
	}
	storType, cacheKey = dom.StorageType(parts[0]), parts[1]
	return storType, cacheKey, nil
}

func s3accessKeyIdxKey(storage, accessKey string) string {
	return storage + ":" + accessKey
}

func (s *credsSvc) encrypt(credsStruct any) (string, error) {
	// marshal to JSON
	plaintext, err := json.Marshal(&credsStruct)
	if err != nil {
		return "", fmt.Errorf("failed to marshal credentials to JSON: %w", err)
	}
	if s.config.DynamicCredentials.DisableEncryption {
		return string(plaintext), nil
	}
	// encrypt with AES-GCM
	block, err := aes.NewCipher(s.key)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}
	nonce := make([]byte, aesgcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("failed to generate nonce: %w", err)
	}
	ciphertext := aesgcm.Seal(nonce, nonce, plaintext, nil)
	// base64 encode
	encoded := base64.StdEncoding.EncodeToString(ciphertext)
	return encoded, nil
}

func (s *credsSvc) decrypt(ciphertext []byte) ([]byte, error) {
	if s.config.DynamicCredentials.DisableEncryption {
		return ciphertext, nil
	}
	// base64 decode
	decoded, err := base64.StdEncoding.DecodeString(string(ciphertext))
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 ciphertext: %w", err)
	}
	ciphertext = decoded
	block, err := aes.NewCipher(s.key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}
	nonceSize := aesgcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("%w: ciphertext too short", dom.ErrInvalidArg)
	}
	nonce, ct := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := aesgcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt ciphertext: %w", err)
	}
	return plaintext, nil
}

func buildS3AccessKeyIdxFromConf(conf *Config) map[string]s3CredsByAccessKey {
	res := make(map[string]s3CredsByAccessKey)
	for storage, storConf := range conf.S3Storages() {
		for user, cred := range storConf.Credentials {
			key := s3accessKeyIdxKey(storage, cred.AccessKeyID)
			res[key] = s3CredsByAccessKey{
				user:        user,
				credentials: cred,
			}
		}
	}
	return res
}
