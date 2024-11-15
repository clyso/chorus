package util

import (
	"crypto/tls"

	"github.com/clyso/chorus/pkg/config"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

func NewRedis(conf *config.Redis, db int) redis.UniversalClient {
	if conf.Address != "" {
		conf.Addresses = append(conf.Addresses, conf.Address)
	}
	opt := &redis.UniversalOptions{
		Addrs:            conf.Addresses,
		DB:               db,
		Username:         conf.User,
		Password:         conf.Password,
		SentinelUsername: conf.Sentinel.User,
		SentinelPassword: conf.Sentinel.Password,
		MasterName:       conf.Sentinel.MasterName,
	}
	if conf.TLS.Enabled {
		opt.TLSConfig = &tls.Config{InsecureSkipVerify: conf.TLS.Insecure}
	}
	return redis.NewUniversalClient(opt)
}

func NewRedisAsynq(conf *config.Redis, db int) asynq.RedisConnOpt {
	if conf.Address != "" {
		conf.Addresses = append(conf.Addresses, conf.Address)
	}
	if len(conf.Addresses) == 1 {
		// Standalone
		opt := asynq.RedisClientOpt{Addr: conf.Addresses[0], Username: conf.User, Password: conf.Password, DB: db}
		if conf.TLS.Enabled {
			opt.TLSConfig = &tls.Config{InsecureSkipVerify: conf.TLS.Insecure}
		}
		return &opt
	}

	if conf.Sentinel.MasterName != "" {
		// Sentinel
		opt := asynq.RedisFailoverClientOpt{
			MasterName:       conf.Sentinel.MasterName,
			SentinelAddrs:    conf.Addresses,
			SentinelPassword: conf.Sentinel.Password,
			Username:         conf.User,
			Password:         conf.Password,
			DB:               db,
		}
		if conf.TLS.Enabled {
			opt.TLSConfig = &tls.Config{InsecureSkipVerify: conf.TLS.Insecure}
		}
		return &opt
	}
	// Cluster
	opt := asynq.RedisClusterClientOpt{
		Addrs:    conf.Addresses,
		Username: conf.User,
		Password: conf.Password,
	}
	if conf.TLS.Enabled {
		opt.TLSConfig = &tls.Config{InsecureSkipVerify: conf.TLS.Insecure}
	}
	return &opt
}
