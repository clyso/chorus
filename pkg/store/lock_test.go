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

package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
)

func TestLock_DoWaitsForWorkAfterCancel(t *testing.T) {
	r := require.New(t)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	locker := NewUserLocker(client, 100*time.Millisecond)

	const (
		lockDuration = 200 * time.Millisecond
		refresh      = 50 * time.Millisecond
	)
	ctx, cancel := context.WithCancel(t.Context())
	lock, err := locker.Lock(ctx, "user", WithDuration(lockDuration))
	r.NoError(err)

	workErr := errors.New("work error")
	workStarted := make(chan struct{})
	finishWork := make(chan struct{})
	doErr := make(chan error, 1)
	go func() {
		doErr <- lock.Do(ctx, refresh, func() error {
			close(workStarted)
			<-finishWork
			return workErr
		})
	}()
	<-workStarted
	cancel()

	for range 5 {
		// miniredis expires keys only on fast-forward: without refresh the lock would expire here
		mr.FastForward(refresh)
		time.Sleep(2 * refresh)
		select {
		case err := <-doErr:
			t.Fatalf("Do returned before work finished: %v", err)
		default:
		}
		_, err := locker.Lock(t.Context(), "user", WithDuration(lockDuration))
		var rateLimitErr *dom.ErrRateLimitExceeded
		r.ErrorAs(err, &rateLimitErr, "lock must be held while work is running")
	}

	close(finishWork)
	select {
	case err := <-doErr:
		r.ErrorIs(err, workErr)
	case <-time.After(5 * time.Second):
		t.Fatal("Do did not return after work finished")
	}
}
