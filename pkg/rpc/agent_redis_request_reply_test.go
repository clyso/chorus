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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/testutil"
)

func TestAgent(t *testing.T) {
	c := testutil.SetupRedis(t)
	r := require.New(t)
	ctx := t.Context()
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		_ = AgentServe(ctx, c, "agent1", "s1")
	}()
	go func() {
		defer wg.Done()
		_ = AgentServe(ctx, c, "agent2", "s2")
	}()
	go func() {
		defer wg.Done()
		_ = AgentServe(ctx, c, "agent3", "s3")
	}()
	t.Cleanup(func() {
		wg.Wait()
	})
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
