/*
 * Copyright © 2024 Clyso GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rpc

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestAgent(t *testing.T) {
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	r := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = AgentServe(ctx, c, "agent1", "s1")
	}()
	go func() {
		_ = AgentServe(ctx, c, "agent2", "s2")
	}()
	go func() {
		_ = AgentServe(ctx, c, "agent3", "s3")
	}()
	tst := AgentClient{c}
	time.Sleep(50 * time.Millisecond)
	res, err := tst.Ping(ctx)
	r.NoError(err)
	res, err = tst.Ping(ctx)
	r.NoError(err)
	r.Len(res, 3)
	r.ElementsMatch(res, []AgentInfo{
		{URL: "agent1", FromStorage: "s1"},
		{URL: "agent2", FromStorage: "s2"},
		{URL: "agent3", FromStorage: "s3"},
	})
}
