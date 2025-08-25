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

package testutil

import (
	"context"
	"flag"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

var (
	extRedisAddr string
	extRedisDB   int
	useExtRedis  bool
)

func init() {
	flag.StringVar(&extRedisAddr, "ext_redis_addr", "localhost:6379", "redis address to use in testing")
	flag.IntVar(&extRedisDB, "ext_redis_db", 15, "redis db number to use in testing")
	flag.BoolVar(&useExtRedis, "ext_redis", false, "use external redis instance in tests instead of miniredis")
}

func SetupRedisAddr(t testing.TB) string {
	t.Helper()
	if useExtRedis {
		client := redis.NewClient(&redis.Options{Addr: extRedisAddr})
		err := client.FlushAll(context.Background()).Err()
		if err != nil {
			t.Fatalf("setup: failed to redis.FlushAll %s: %v", extRedisAddr, err)
		}
		return extRedisAddr
	} else {
		db := miniredis.RunT(t)
		return db.Addr()
	}
}

func SetupRedis(t testing.TB) (client redis.UniversalClient) {
	t.Helper()
	if useExtRedis {
		client = redis.NewClient(&redis.Options{Addr: extRedisAddr})
		err := client.FlushAll(context.Background()).Err()
		if err != nil {
			t.Fatalf("setup: failed to redis.FlushAll %s: %v", extRedisAddr, err)
		}
	} else {
		db := miniredis.RunT(t)
		client = redis.NewClient(&redis.Options{Addr: db.Addr()})
	}
	t.Cleanup(func() {
		client.Close()
	})
	return client
}
