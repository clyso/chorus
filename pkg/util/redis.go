// Copyright 2025 Clyso GmbH
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

package util

import (
	"crypto/tls"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/config"
)

func NewRedis(conf *config.Redis, db int) redis.UniversalClient {
	opt := &redis.UniversalOptions{
		Addrs:            conf.GetAddresses(),
		DB:               db,
		Username:         conf.User,
		Password:         conf.Password,
		SentinelUsername: conf.Sentinel.User,
		SentinelPassword: conf.Sentinel.Password,
		MasterName:       conf.Sentinel.MasterName,
	}
	if conf.TLS.Enabled {
		opt.TLSConfig = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: conf.TLS.Insecure,
		}
	}
	return redis.NewUniversalClient(opt)
}

func NewRedisAsynq(conf *config.Redis, db int) asynq.RedisConnOpt {
	addresses := conf.GetAddresses()
	if len(addresses) == 1 {
		// Standalone
		opt := asynq.RedisClientOpt{Addr: addresses[0], Username: conf.User, Password: conf.Password, DB: db}
		if conf.TLS.Enabled {
			opt.TLSConfig = &tls.Config{
				MinVersion:         tls.VersionTLS12,
				InsecureSkipVerify: conf.TLS.Insecure,
			}
		}
		return &opt
	}

	if conf.Sentinel.MasterName != "" {
		// Sentinel
		opt := asynq.RedisFailoverClientOpt{
			MasterName:       conf.Sentinel.MasterName,
			SentinelAddrs:    addresses,
			SentinelPassword: conf.Sentinel.Password,
			Username:         conf.User,
			Password:         conf.Password,
			DB:               db,
		}
		if conf.TLS.Enabled {
			opt.TLSConfig = &tls.Config{
				MinVersion:         tls.VersionTLS12,
				InsecureSkipVerify: conf.TLS.Insecure,
			}
		}
		return &opt
	}
	// Cluster
	opt := asynq.RedisClusterClientOpt{
		Addrs:    addresses,
		Username: conf.User,
		Password: conf.Password,
	}
	if conf.TLS.Enabled {
		opt.TLSConfig = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: conf.TLS.Insecure,
		}
	}
	return &opt
}
