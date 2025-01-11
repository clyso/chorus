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

package policy

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test_policySvc_UserRoutingPolicy(t *testing.T) {
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	ctx := context.TODO()

	svc := NewService(c)

	u1, u2 := "u1", "u2"
	users := []string{u1, u2}
	b1, b2 := "b1", "b2"
	buckets := []string{b1, b2}
	s1, s2, s3, s4 := "s1", "s2", "s3", "s4"

	t.Run("returns not found", func(t *testing.T) {
		r := require.New(t)
		db.FlushAll()
		for _, u := range users {
			_, err := svc.GetUserRoutingPolicy(ctx, u)
			r.ErrorIs(err, dom.ErrNotFound)
			for _, b := range buckets {
				_, err = svc.getBucketRoutingPolicy(ctx, u, b)
				r.ErrorIs(err, dom.ErrNotFound)
			}
		}
	})

	t.Run("args must be non empty", func(t *testing.T) {
		r := require.New(t)
		_, err := svc.GetUserRoutingPolicy(ctx, "")
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.getBucketRoutingPolicy(ctx, "", "")
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.getBucketRoutingPolicy(ctx, "a", "")
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.getBucketRoutingPolicy(ctx, "", "a")
		r.ErrorIs(err, dom.ErrInvalidArg)

		err = svc.AddUserRoutingPolicy(ctx, "", "")
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.AddUserRoutingPolicy(ctx, "a", "")
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.AddUserRoutingPolicy(ctx, "", "a")
		r.ErrorIs(err, dom.ErrInvalidArg)

		err = svc.addBucketRoutingPolicy(ctx, "", "", "")
		r.ErrorIs(err, dom.ErrInvalidArg)

		err = svc.addBucketRoutingPolicy(ctx, "a", "a", "")
		r.ErrorIs(err, dom.ErrInvalidArg)

		err = svc.addBucketRoutingPolicy(ctx, "a", "", "a")
		r.ErrorIs(err, dom.ErrInvalidArg)

		err = svc.addBucketRoutingPolicy(ctx, "", "a", "a")
		r.ErrorIs(err, dom.ErrInvalidArg)
	})

	t.Run("add user policies", func(t *testing.T) {
		r := require.New(t)
		db.FlushAll()

		err := svc.AddUserRoutingPolicy(ctx, u1, s1)
		r.NoError(err)
		err = svc.AddUserRoutingPolicy(ctx, u2, s2)
		r.NoError(err)

		res, err := svc.GetUserRoutingPolicy(ctx, u1)
		r.NoError(err)
		r.EqualValues(s1, res)
		res, err = svc.GetUserRoutingPolicy(ctx, u2)
		r.NoError(err)
		r.EqualValues(s2, res)
		for _, u := range users {
			for _, b := range buckets {
				_, err = svc.getBucketRoutingPolicy(ctx, u, b)
				r.ErrorIs(err, dom.ErrNotFound)
			}
		}

		err = svc.addBucketRoutingPolicy(ctx, u1, b1, s3)
		r.NoError(err)
		err = svc.addBucketRoutingPolicy(ctx, u2, b2, s4)
		r.NoError(err)

		res, err = svc.GetUserRoutingPolicy(ctx, u1)
		r.NoError(err)
		r.EqualValues(s1, res)
		res, err = svc.GetUserRoutingPolicy(ctx, u2)
		r.NoError(err)
		r.EqualValues(s2, res)

		res, err = svc.getBucketRoutingPolicy(ctx, u1, b1)
		r.NoError(err)
		r.EqualValues(s3, res)
		res, err = svc.getBucketRoutingPolicy(ctx, u1, b2)
		r.ErrorIs(err, dom.ErrNotFound)
		res, err = svc.getBucketRoutingPolicy(ctx, u2, b2)
		r.NoError(err)
		r.EqualValues(s4, res)
		res, err = svc.getBucketRoutingPolicy(ctx, u2, b1)
		r.ErrorIs(err, dom.ErrNotFound)

		// cannot add policy if already exists
		err = svc.AddUserRoutingPolicy(ctx, u1, s1)
		r.ErrorIs(err, dom.ErrAlreadyExists)
		err = svc.AddUserRoutingPolicy(ctx, u2, s2)
		r.ErrorIs(err, dom.ErrAlreadyExists)
		err = svc.addBucketRoutingPolicy(ctx, u1, b1, s3)
		r.ErrorIs(err, dom.ErrAlreadyExists)
		err = svc.addBucketRoutingPolicy(ctx, u2, b2, s4)
		r.ErrorIs(err, dom.ErrAlreadyExists)
	})

	t.Run("add bucket policies", func(t *testing.T) {
		r := require.New(t)
		db.FlushAll()

		err := svc.addBucketRoutingPolicy(ctx, u1, b1, s3)
		r.NoError(err)
		err = svc.addBucketRoutingPolicy(ctx, u2, b2, s4)
		r.NoError(err)

		_, err = svc.GetUserRoutingPolicy(ctx, u1)
		r.ErrorIs(err, dom.ErrNotFound)
		_, err = svc.GetUserRoutingPolicy(ctx, u2)
		r.ErrorIs(err, dom.ErrNotFound)

		res, err := svc.getBucketRoutingPolicy(ctx, u1, b1)
		r.NoError(err)
		r.EqualValues(s3, res)
		res, err = svc.getBucketRoutingPolicy(ctx, u1, b2)
		r.ErrorIs(err, dom.ErrNotFound)
		res, err = svc.getBucketRoutingPolicy(ctx, u2, b2)
		r.NoError(err)
		r.EqualValues(s4, res)
		res, err = svc.getBucketRoutingPolicy(ctx, u2, b1)
		r.ErrorIs(err, dom.ErrNotFound)
	})

	t.Run("cannot add policy if already exists", func(t *testing.T) {
		r := require.New(t)
		db.FlushAll()

		err := svc.addBucketRoutingPolicy(ctx, u1, b1, s3)
		r.NoError(err)
		err = svc.addBucketRoutingPolicy(ctx, u1, b1, s3)
		r.ErrorIs(err, dom.ErrAlreadyExists)

		err = svc.AddUserRoutingPolicy(ctx, u1, s1)
		r.NoError(err)
		err = svc.AddUserRoutingPolicy(ctx, u1, s1)
		r.ErrorIs(err, dom.ErrAlreadyExists)
	})
}

func Test_policySvc_BucketReplicationPolicies(t *testing.T) {
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	ctx := context.TODO()

	svc := NewService(c)

	u1, u2 := "u1", "u2"
	users := []string{u1, u2}
	b1, b2 := "b1", "b2"
	buckets := []string{b1, b2}
	s1, s2, s3, s4 := "s1", "s2", "s3", "s4"

	t.Run("args must be valid", func(t *testing.T) {
		r := require.New(t)
		db.FlushAll()

		err := svc.AddUserReplicationPolicy(ctx, "", "a", "a", tasks.Priority3)
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.AddUserReplicationPolicy(ctx, "a", "", "a", tasks.Priority3)
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.AddUserReplicationPolicy(ctx, "a", "a", "", tasks.Priority3)
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.AddUserReplicationPolicy(ctx, "a", "a", "a", tasks.Priority(69))
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.AddUserReplicationPolicy(ctx, "a", "a", "a", tasks.Priority3)
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.AddUserReplicationPolicy(ctx, "a", "a", "b", tasks.Priority3)
		r.NoError(err)

		_, err = svc.GetUserReplicationPolicies(ctx, "")
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.GetUserReplicationPolicies(ctx, "a")
		r.NoError(err)

		err = svc.AddBucketReplicationPolicy(ctx, "", "a", "a", "a", nil, tasks.Priority3, nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.AddBucketReplicationPolicy(ctx, "a", "", "a", "a", nil, tasks.Priority3, nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.AddBucketReplicationPolicy(ctx, "a", "a", "", "a", nil, tasks.Priority3, nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.AddBucketReplicationPolicy(ctx, "a", "a", "a", "", nil, tasks.Priority3, nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.AddBucketReplicationPolicy(ctx, "a", "a", "a", "a", nil, tasks.Priority(69), nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.AddBucketReplicationPolicy(ctx, "a", "a", "a", "a", nil, tasks.Priority3, nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.AddBucketReplicationPolicy(ctx, "a", "a", "a", "b", nil, tasks.Priority3, nil)
		r.NoError(err)

		_, err = svc.GetBucketReplicationPolicies(ctx, "", "")
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.GetBucketReplicationPolicies(ctx, "a", "")
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.GetBucketReplicationPolicies(ctx, "", "a")
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.GetBucketReplicationPolicies(ctx, "a", "a")
		r.NoError(err)

		_, err = svc.GetReplicationPolicyInfo(ctx, "", "a", "a", "a", nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.GetReplicationPolicyInfo(ctx, "a", "", "a", "a", nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.GetReplicationPolicyInfo(ctx, "a", "a", "", "a", nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.GetReplicationPolicyInfo(ctx, "a", "a", "a", "", nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.GetReplicationPolicyInfo(ctx, "a", "a", "a", "b", nil)
		r.NoError(err)

		_, err = svc.IsReplicationPolicyPaused(ctx, "", "a", "a", "a", nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.IsReplicationPolicyPaused(ctx, "a", "", "a", "a", nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.IsReplicationPolicyPaused(ctx, "a", "a", "", "a", nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.IsReplicationPolicyPaused(ctx, "a", "a", "a", "", nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.IsReplicationPolicyPaused(ctx, "a", "a", "a", "b", nil)
		r.NoError(err)

		err = svc.IncReplInitObjListed(ctx, "", "a", "a", "a", nil, 0, time.Now())
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.IncReplInitObjListed(ctx, "a", "", "a", "a", nil, 0, time.Now())
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.IncReplInitObjListed(ctx, "a", "a", "", "a", nil, 0, time.Now())
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.IncReplInitObjListed(ctx, "a", "a", "a", "", nil, 0, time.Now())
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.IncReplInitObjListed(ctx, "a", "a", "a", "a", nil, -1, time.Now())
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.IncReplInitObjListed(ctx, "a", "a", "a", "b", nil, 0, time.Now())
		r.NoError(err)

		err = svc.IncReplInitObjDone(ctx, "", "a", "a", "a", nil, 0, time.Now())
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.IncReplInitObjDone(ctx, "a", "", "a", "a", nil, 0, time.Now())
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.IncReplInitObjDone(ctx, "a", "a", "", "a", nil, 0, time.Now())
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.IncReplInitObjDone(ctx, "a", "a", "a", "", nil, 0, time.Now())
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.IncReplInitObjDone(ctx, "a", "a", "a", "a", nil, -1, time.Now())
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.IncReplInitObjDone(ctx, "a", "a", "a", "b", nil, 0, time.Now())
		r.NoError(err)

		err = svc.IncReplEvents(ctx, "", "a", "a", "a", nil, time.Now())
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.IncReplEvents(ctx, "a", "", "a", "a", nil, time.Now())
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.IncReplEvents(ctx, "a", "a", "", "a", nil, time.Now())
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.IncReplEvents(ctx, "a", "a", "a", "", nil, time.Now())
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.IncReplEvents(ctx, "a", "a", "a", "b", nil, time.Now())
		r.NoError(err)

		err = svc.IncReplEventsDone(ctx, "", "a", "a", "a", nil, time.Now())
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.IncReplEventsDone(ctx, "a", "", "a", "a", nil, time.Now())
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.IncReplEventsDone(ctx, "a", "a", "", "a", nil, time.Now())
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.IncReplEventsDone(ctx, "a", "a", "a", "", nil, time.Now())
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.IncReplEventsDone(ctx, "a", "a", "a", "b", nil, time.Now())
		r.NoError(err)

		err = svc.PauseReplication(ctx, "", "a", "a", "a", nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.PauseReplication(ctx, "a", "", "a", "a", nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.PauseReplication(ctx, "a", "a", "", "a", nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.PauseReplication(ctx, "a", "a", "a", "", nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.PauseReplication(ctx, "a", "a", "a", "b", nil)
		r.NoError(err)

		err = svc.ResumeReplication(ctx, "", "a", "a", "a", nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.ResumeReplication(ctx, "a", "", "a", "a", nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.ResumeReplication(ctx, "a", "a", "", "a", nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.ResumeReplication(ctx, "a", "a", "a", "", nil)
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.ResumeReplication(ctx, "a", "a", "a", "b", nil)
		r.NoError(err)
	})

	t.Run("returns not found", func(t *testing.T) {
		r := require.New(t)
		db.FlushAll()
		for _, u := range users {
			_, err := svc.GetUserReplicationPolicies(ctx, u)
			r.ErrorIs(err, dom.ErrNotFound)
			for _, b := range buckets {
				_, err = svc.GetBucketReplicationPolicies(ctx, u, b)
				r.ErrorIs(err, dom.ErrNotFound)
				_, err = svc.GetReplicationPolicyInfo(ctx, u, b, s1, s2, nil)
				r.ErrorIs(err, dom.ErrNotFound)
				exists, err := svc.IsReplicationPolicyExists(ctx, u, b, s1, s2, nil)
				r.NoError(err)
				r.False(exists)
				_, err = svc.IsReplicationPolicyPaused(ctx, u, b, s1, s2, nil)
				r.ErrorIs(err, dom.ErrNotFound)
				err = svc.IncReplEvents(ctx, u, b, s1, s2, nil, time.Now())
				r.ErrorIs(err, dom.ErrNotFound)
				err = svc.IncReplEventsDone(ctx, u, b, s1, s2, nil, time.Now())
				r.ErrorIs(err, dom.ErrNotFound)
				err = svc.IncReplInitObjListed(ctx, u, b, s1, s2, nil, 5, time.Now())
				r.ErrorIs(err, dom.ErrNotFound)
				err = svc.IncReplInitObjDone(ctx, u, b, s3, s4, nil, 5, time.Now())
				r.ErrorIs(err, dom.ErrNotFound)
				err = svc.PauseReplication(ctx, u, b, s3, s4, nil)
				r.ErrorIs(err, dom.ErrNotFound)
				err = svc.ResumeReplication(ctx, u, b, s3, s4, nil)
				r.ErrorIs(err, dom.ErrNotFound)
				err = svc.ObjListStarted(ctx, u, b, s3, s4, nil)
				r.ErrorIs(err, dom.ErrNotFound)
			}
		}
		list, err := svc.ListReplicationPolicyInfo(ctx)
		r.NoError(err)
		r.Empty(list)
	})

	t.Run("add user repl policy", func(t *testing.T) {
		r := require.New(t)
		db.FlushAll()

		_, err := svc.GetUserReplicationPolicies(ctx, u1)
		r.ErrorIs(err, dom.ErrNotFound)
		list, err := svc.ListReplicationPolicyInfo(ctx)
		r.NoError(err)
		r.Empty(list)

		err = svc.AddUserReplicationPolicy(ctx, u1, s1, s2, tasks.Priority3)
		r.NoError(err)

		res, err := svc.GetUserReplicationPolicies(ctx, u1)
		r.NoError(err)
		r.EqualValues(s1, res.From)
		r.Len(res.To, 1)
		r.EqualValues(tasks.Priority3, res.To[s2])

		list, err = svc.ListReplicationPolicyInfo(ctx)
		r.NoError(err)
		r.Empty(list)

		err = svc.AddUserReplicationPolicy(ctx, u1, s1, s2, tasks.Priority3)
		r.ErrorIs(err, dom.ErrAlreadyExists)
		err = svc.AddUserReplicationPolicy(ctx, u1, s2, s1, tasks.Priority3)
		r.ErrorIs(err, dom.ErrInvalidArg)

		err = svc.AddUserReplicationPolicy(ctx, u2, s2, s1, tasks.Priority3)
		r.NoError(err)
		res, err = svc.GetUserReplicationPolicies(ctx, u2)
		r.NoError(err)
		r.EqualValues(s2, res.From)
		r.Len(res.To, 1)
		r.EqualValues(tasks.Priority3, res.To[s1])

		err = svc.AddUserReplicationPolicy(ctx, u1, s1, s3, tasks.Priority4)
		r.NoError(err)
		res, err = svc.GetUserReplicationPolicies(ctx, u1)
		r.NoError(err)
		r.EqualValues(s1, res.From)
		r.Len(res.To, 2)
		r.EqualValues(tasks.Priority3, res.To[s2])
		r.EqualValues(tasks.Priority4, res.To[s3])
	})

	t.Run("add bucket repl policy", func(t *testing.T) {
		r := require.New(t)
		db.FlushAll()

		_, err := svc.GetBucketReplicationPolicies(ctx, u1, b1)
		r.ErrorIs(err, dom.ErrNotFound)

		_, err = svc.GetReplicationPolicyInfo(ctx, u1, b1, s1, s2, nil)
		r.ErrorIs(err, dom.ErrNotFound)
		exists, err := svc.IsReplicationPolicyExists(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.False(exists)
		list, err := svc.ListReplicationPolicyInfo(ctx)
		r.NoError(err)
		r.Empty(list)

		err = svc.AddBucketReplicationPolicy(ctx, u1, b1, s1, s2, nil, tasks.Priority3, nil)
		r.NoError(err)

		res, err := svc.GetBucketReplicationPolicies(ctx, u1, b1)
		r.NoError(err)
		r.EqualValues(s1, res.From)
		r.Len(res.To, 1)
		r.EqualValues(tasks.Priority3, res.To[s2])

		info, err := svc.GetReplicationPolicyInfo(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.False(info.CreatedAt.IsZero())
		r.False(info.IsPaused)
		r.Zero(info.InitObjListed)
		r.Zero(info.InitObjDone)
		r.Zero(info.InitBytesListed)
		r.Zero(info.InitBytesDone)
		r.Zero(info.Events)
		r.Zero(info.EventsDone)

		exists, err = svc.IsReplicationPolicyExists(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.True(exists)

		list, err = svc.ListReplicationPolicyInfo(ctx)
		r.NoError(err)
		r.Len(list, 1)
		r.EqualValues(info, list[0].ReplicationPolicyStatus)
		r.EqualValues(u1, list[0].User)
		r.EqualValues(b1, list[0].Bucket)
		r.EqualValues(s1, list[0].From)
		r.EqualValues(s2, list[0].To)

		err = svc.AddBucketReplicationPolicy(ctx, u1, b1, s1, s2, nil, tasks.Priority3, nil)
		r.ErrorIs(err, dom.ErrAlreadyExists)
		err = svc.AddBucketReplicationPolicy(ctx, u1, b1, s2, s1, nil, tasks.Priority3, nil)
		r.ErrorIs(err, dom.ErrInvalidArg)

		err = svc.AddBucketReplicationPolicy(ctx, u2, b1, s2, s1, nil, tasks.PriorityDefault1, nil)
		r.NoError(err)
		res, err = svc.GetBucketReplicationPolicies(ctx, u2, b1)
		r.NoError(err)
		r.EqualValues(s2, res.From)
		r.Len(res.To, 1)
		r.EqualValues(tasks.PriorityDefault1, res.To[s1])

		err = svc.AddBucketReplicationPolicy(ctx, u1, b2, s2, s1, nil, tasks.PriorityHighest5, nil)
		r.NoError(err)
		res, err = svc.GetBucketReplicationPolicies(ctx, u1, b2)
		r.NoError(err)
		r.EqualValues(s2, res.From)
		r.Len(res.To, 1)
		r.EqualValues(tasks.PriorityHighest5, res.To[s1])

		err = svc.AddBucketReplicationPolicy(ctx, u1, b1, s1, s3, nil, tasks.Priority4, nil)
		r.NoError(err)
		res, err = svc.GetBucketReplicationPolicies(ctx, u1, b1)
		r.NoError(err)
		r.EqualValues(s1, res.From)
		r.Len(res.To, 2)
		r.EqualValues(tasks.Priority3, res.To[s2])
		r.EqualValues(tasks.Priority4, res.To[s3])

		list, err = svc.ListReplicationPolicyInfo(ctx)
		r.NoError(err)
		r.Len(list, 4)

		pol, err := svc.GetReplicationPolicyInfo(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.False(pol.ListingStarted)
		err = svc.ObjListStarted(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		pol, err = svc.GetReplicationPolicyInfo(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.True(pol.ListingStarted)

		err = svc.DeleteReplication(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		_, err = svc.GetReplicationPolicyInfo(ctx, u1, b1, s1, s2, nil)
		r.ErrorIs(err, dom.ErrNotFound)
		exists, err = svc.IsReplicationPolicyExists(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.False(exists)
		list, err = svc.ListReplicationPolicyInfo(ctx)
		r.NoError(err)
		r.Len(list, 3)

	})

	t.Run("counters", func(t *testing.T) {
		r := require.New(t)
		db.FlushAll()

		err := svc.AddBucketReplicationPolicy(ctx, u1, b1, s1, s2, nil, tasks.Priority3, nil)
		r.NoError(err)

		res, err := svc.GetBucketReplicationPolicies(ctx, u1, b1)
		r.NoError(err)
		r.EqualValues(s1, res.From)
		r.Len(res.To, 1)
		r.EqualValues(tasks.Priority3, res.To[s2])

		info, err := svc.GetReplicationPolicyInfo(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.False(info.CreatedAt.IsZero())
		r.False(info.IsPaused)
		r.Zero(info.InitObjListed)
		r.Zero(info.InitObjDone)
		r.Zero(info.InitBytesListed)
		r.Zero(info.InitBytesDone)
		r.Zero(info.Events)
		r.Zero(info.EventsDone)

		now := time.Now()
		err = svc.IncReplInitObjListed(ctx, u1, b1, s1, s2, nil, 69, now)
		r.NoError(err)
		infoUpd, err := svc.GetReplicationPolicyInfo(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.EqualValues(info.CreatedAt, infoUpd.CreatedAt)
		r.False(infoUpd.IsPaused)
		r.EqualValues(1, infoUpd.InitObjListed)
		r.Zero(infoUpd.InitObjDone)
		r.EqualValues(69, infoUpd.InitBytesListed)
		r.Zero(infoUpd.InitBytesDone)
		r.Zero(infoUpd.Events)
		r.Zero(infoUpd.EventsDone)
		r.Nil(infoUpd.InitDoneAt)

		err = svc.IncReplInitObjDone(ctx, u1, b1, s1, s2, nil, 13, now)
		r.NoError(err)
		infoUpd, err = svc.GetReplicationPolicyInfo(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.EqualValues(info.CreatedAt, infoUpd.CreatedAt)
		r.False(infoUpd.IsPaused)
		r.EqualValues(1, infoUpd.InitObjListed)
		r.EqualValues(1, infoUpd.InitObjDone)
		r.EqualValues(69, infoUpd.InitBytesListed)
		r.EqualValues(13, infoUpd.InitBytesDone)
		r.Zero(infoUpd.Events)
		r.Zero(infoUpd.EventsDone)
		r.NotNil(infoUpd.InitDoneAt)
		r.False(infoUpd.InitDoneAt.IsZero())
		r.NotNil(infoUpd.LastProcessedAt)
		r.EqualValues(now.UTC().UnixMicro(), infoUpd.LastProcessedAt.UTC().UnixMicro())

		before := now.Add(-time.Hour)

		err = svc.IncReplInitObjDone(ctx, u1, b1, s1, s2, nil, 7, before)
		r.NoError(err)
		infoUpd, err = svc.GetReplicationPolicyInfo(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.EqualValues(info.CreatedAt, infoUpd.CreatedAt)
		r.False(infoUpd.IsPaused)
		r.EqualValues(1, infoUpd.InitObjListed)
		r.EqualValues(2, infoUpd.InitObjDone)
		r.EqualValues(69, infoUpd.InitBytesListed)
		r.EqualValues(20, infoUpd.InitBytesDone)
		r.Zero(infoUpd.Events)
		r.Zero(infoUpd.EventsDone)
		r.NotNil(infoUpd.LastProcessedAt)
		r.EqualValues(now.UTC().UnixMicro(), infoUpd.LastProcessedAt.UTC().UnixMicro())

		err = svc.IncReplEvents(ctx, u1, b1, s1, s2, nil, now) //??
		r.NoError(err)
		infoUpd, err = svc.GetReplicationPolicyInfo(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.EqualValues(info.CreatedAt, infoUpd.CreatedAt)
		r.False(infoUpd.IsPaused)
		r.EqualValues(1, infoUpd.InitObjListed)
		r.EqualValues(2, infoUpd.InitObjDone)
		r.EqualValues(69, infoUpd.InitBytesListed)
		r.EqualValues(20, infoUpd.InitBytesDone)
		r.EqualValues(1, infoUpd.Events)
		r.Zero(infoUpd.EventsDone)

		after := now.Add(time.Minute)
		err = svc.IncReplEvents(ctx, u1, b1, s1, s2, nil, after)
		r.NoError(err)
		infoUpd, err = svc.GetReplicationPolicyInfo(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.EqualValues(info.CreatedAt, infoUpd.CreatedAt)
		r.False(infoUpd.IsPaused)
		r.EqualValues(1, infoUpd.InitObjListed)
		r.EqualValues(2, infoUpd.InitObjDone)
		r.EqualValues(69, infoUpd.InitBytesListed)
		r.EqualValues(20, infoUpd.InitBytesDone)
		r.EqualValues(2, infoUpd.Events)
		r.Zero(infoUpd.EventsDone)

		err = svc.IncReplEventsDone(ctx, u1, b1, s1, s2, nil, after)
		r.NoError(err)
		infoUpd, err = svc.GetReplicationPolicyInfo(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.EqualValues(info.CreatedAt, infoUpd.CreatedAt)
		r.False(infoUpd.IsPaused)
		r.EqualValues(1, infoUpd.InitObjListed)
		r.EqualValues(2, infoUpd.InitObjDone)
		r.EqualValues(69, infoUpd.InitBytesListed)
		r.EqualValues(20, infoUpd.InitBytesDone)
		r.EqualValues(2, infoUpd.Events)
		r.EqualValues(1, infoUpd.EventsDone)
		r.NotNil(infoUpd.LastProcessedAt)
		r.EqualValues(after.UnixMicro(), infoUpd.LastProcessedAt.UnixMicro())

		afterAfter := after.Add(time.Minute)
		err = svc.IncReplEventsDone(ctx, u1, b1, s1, s2, nil, afterAfter)
		r.NoError(err)
		infoUpd, err = svc.GetReplicationPolicyInfo(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.EqualValues(info.CreatedAt, infoUpd.CreatedAt)
		r.False(infoUpd.IsPaused)
		r.EqualValues(1, infoUpd.InitObjListed)
		r.EqualValues(2, infoUpd.InitObjDone)
		r.EqualValues(69, infoUpd.InitBytesListed)
		r.EqualValues(20, infoUpd.InitBytesDone)
		r.EqualValues(2, infoUpd.Events)
		r.EqualValues(2, infoUpd.EventsDone)
		r.NotNil(infoUpd.LastProcessedAt)
		r.EqualValues(afterAfter.UnixMicro(), infoUpd.LastProcessedAt.UnixMicro())
		r.NotNil(infoUpd.LastEmittedAt)
		r.NotNil(infoUpd.LastProcessedAt)
		r.True(infoUpd.LastEmittedAt.Before(*infoUpd.LastProcessedAt))
	})

	t.Run("pause", func(t *testing.T) {
		r := require.New(t)
		db.FlushAll()

		err := svc.AddBucketReplicationPolicy(ctx, u1, b1, s1, s2, nil, tasks.Priority3, nil)
		r.NoError(err)

		res, err := svc.GetBucketReplicationPolicies(ctx, u1, b1)
		r.NoError(err)
		r.EqualValues(s1, res.From)
		r.Len(res.To, 1)
		r.EqualValues(tasks.Priority3, res.To[s2])

		info, err := svc.GetReplicationPolicyInfo(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.False(info.CreatedAt.IsZero())
		r.False(info.IsPaused)
		r.Zero(info.InitObjListed)
		r.Zero(info.InitObjDone)
		r.Zero(info.InitBytesListed)
		r.Zero(info.InitBytesDone)
		r.Zero(info.Events)
		r.Zero(info.EventsDone)

		err = svc.PauseReplication(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)

		info, err = svc.GetReplicationPolicyInfo(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.True(info.IsPaused)
		r.Zero(info.InitObjListed)
		r.Zero(info.InitObjDone)
		r.Zero(info.InitBytesListed)
		r.Zero(info.InitBytesDone)
		r.Zero(info.Events)
		r.Zero(info.EventsDone)

		err = svc.ResumeReplication(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)

		info, err = svc.GetReplicationPolicyInfo(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.False(info.IsPaused)
		r.Zero(info.InitObjListed)
		r.Zero(info.InitObjDone)
		r.Zero(info.InitBytesListed)
		r.Zero(info.InitBytesDone)
		r.Zero(info.Events)
		r.Zero(info.EventsDone)
	})

	t.Run("delete user repl", func(t *testing.T) {
		r := require.New(t)
		db.FlushAll()

		err := svc.AddUserReplicationPolicy(ctx, u1, s1, s2, tasks.Priority3)
		r.NoError(err)

		res, err := svc.GetUserReplicationPolicies(ctx, u1)
		r.NoError(err)
		r.EqualValues(s1, res.From)
		r.Len(res.To, 1)
		r.EqualValues(tasks.Priority3, res.To[s2])

		err = svc.AddUserReplicationPolicy(ctx, u1, s1, s3, tasks.Priority4)
		r.NoError(err)

		res, err = svc.GetUserReplicationPolicies(ctx, u1)
		r.NoError(err)
		r.EqualValues(s1, res.From)
		r.Len(res.To, 2)
		r.EqualValues(tasks.Priority3, res.To[s2])
		r.EqualValues(tasks.Priority4, res.To[s3])

		err = svc.DeleteUserReplication(ctx, u1, s1, s2)
		r.NoError(err)

		res, err = svc.GetUserReplicationPolicies(ctx, u1)
		r.NoError(err)
		r.EqualValues(s1, res.From)
		r.Len(res.To, 1)
		r.EqualValues(tasks.Priority4, res.To[s3])

		err = svc.DeleteUserReplication(ctx, u1, s1, s2)
		r.ErrorIs(err, dom.ErrNotFound)

		err = svc.DeleteUserReplication(ctx, u1, s1, s3)
		r.NoError(err)

		res, err = svc.GetUserReplicationPolicies(ctx, u1)
		r.ErrorIs(err, dom.ErrNotFound)

		err = svc.AddBucketReplicationPolicy(ctx, u1, b1, s1, s2, nil, tasks.Priority4, nil)
		r.NoError(err)
		err = svc.AddBucketReplicationPolicy(ctx, u1, b2, s1, s2, nil, tasks.Priority4, nil)
		r.NoError(err)
		err = svc.AddBucketReplicationPolicy(ctx, u1, b1, s1, s3, nil, tasks.Priority4, nil)
		r.NoError(err)
		err = svc.AddBucketReplicationPolicy(ctx, u2, b1, s1, s3, nil, tasks.Priority4, nil)
		r.NoError(err)

		exists, err := svc.IsReplicationPolicyExists(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.True(exists)
		exists, err = svc.IsReplicationPolicyExists(ctx, u1, b2, s1, s2, nil)
		r.NoError(err)
		r.True(exists)
		exists, err = svc.IsReplicationPolicyExists(ctx, u1, b1, s1, s3, nil)
		r.NoError(err)
		r.True(exists)
		exists, err = svc.IsReplicationPolicyExists(ctx, u2, b1, s1, s3, nil)
		r.NoError(err)
		r.True(exists)

		deleted, err := svc.DeleteBucketReplicationsByUser(ctx, u1, s1, s2)
		r.NoError(err)
		r.NotEmpty(deleted)

		exists, err = svc.IsReplicationPolicyExists(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.False(exists)
		exists, err = svc.IsReplicationPolicyExists(ctx, u1, b2, s1, s2, nil)
		r.NoError(err)
		r.False(exists)
		exists, err = svc.IsReplicationPolicyExists(ctx, u1, b1, s1, s3, nil)
		r.NoError(err)
		r.True(exists)
		exists, err = svc.IsReplicationPolicyExists(ctx, u2, b1, s1, s3, nil)
		r.NoError(err)
		r.True(exists)
	})

	t.Run("replication switch", func(t *testing.T) {
		r := require.New(t)
		db.FlushAll()

		_, err := svc.GetReplicationSwitch(ctx, u1, b1)
		r.ErrorIs(err, dom.ErrNotFound)
		_, err = svc.GetRoutingPolicy(ctx, u1, b1)
		r.ErrorIs(err, dom.ErrNotFound)
		_, err = svc.GetBucketReplicationPolicies(ctx, u1, b1)
		r.ErrorIs(err, dom.ErrNotFound)
		_, err = svc.GetReplicationPolicyInfo(ctx, u1, b1, s1, s2, nil)
		r.ErrorIs(err, dom.ErrNotFound)

		err = svc.addBucketRoutingPolicy(ctx, u1, b1, s1)
		r.NoError(err)
		err = svc.AddBucketReplicationPolicy(ctx, u1, b1, s1, s2, nil, tasks.Priority3, nil)
		r.NoError(err)
		err = svc.AddBucketReplicationPolicy(ctx, u1, b1, s1, s3, nil, tasks.Priority4, nil)
		r.NoError(err)

		res, err := svc.GetBucketReplicationPolicies(ctx, u1, b1)
		r.NoError(err)
		r.EqualValues(s1, res.From)
		r.Len(res.To, 2)
		r.EqualValues(tasks.Priority3, res.To[s2])
		r.EqualValues(tasks.Priority4, res.To[s3])

		info, err := svc.GetReplicationPolicyInfo(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.False(info.CreatedAt.IsZero())
		r.False(info.IsPaused)
		r.Zero(info.InitObjListed)
		r.Zero(info.InitObjDone)
		r.Zero(info.InitBytesListed)
		r.Zero(info.InitBytesDone)
		r.Zero(info.Events)
		r.Zero(info.EventsDone)
		r.EqualValues(NotStarted, info.SwitchStatus)

		exists, err := svc.IsReplicationPolicyExists(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.True(exists)

		_, err = svc.GetReplicationSwitch(ctx, u1, b1)
		r.ErrorIs(err, dom.ErrNotFound)

		inProgress, err := svc.IsReplicationSwitchInProgress(ctx, u1, b1)
		r.NoError(err)
		r.False(inProgress)

		err = svc.DoReplicationSwitch(ctx, u1, b1, s2)
		r.Error(err)

		r.NoError(svc.ObjListStarted(ctx, u1, b1, s1, s2, nil))

		err = svc.DoReplicationSwitch(ctx, u1, b1, s2)
		r.Error(err)

		r.NoError(svc.ObjListStarted(ctx, u1, b1, s1, s3, nil))

		err = svc.DoReplicationSwitch(ctx, u1, b1, s2)
		r.NoError(err)

		rs, err := svc.GetReplicationSwitch(ctx, u1, b1)
		r.NoError(err)
		r.EqualValues(res.To, rs.GetOldFollowers())
		r.EqualValues(s1, rs.OldMain)
		r.False(rs.IsDone)

		rp, err := svc.GetRoutingPolicy(ctx, u1, b1)
		r.NoError(err)
		r.EqualValues(s2, rp)
		replP, err := svc.GetBucketReplicationPolicies(ctx, u1, b1)
		r.NoError(err)
		r.EqualValues(s2, replP.From)
		r.Len(replP.To, 1)
		_, ok := replP.To[s3]
		r.True(ok)

		info, err = svc.GetReplicationPolicyInfo(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.EqualValues(InProgress, info.SwitchStatus)

		err = svc.ReplicationSwitchDone(ctx, u1, b1)
		r.NoError(err)
		rs, err = svc.GetReplicationSwitch(ctx, u1, b1)
		r.NoError(err)
		r.True(rs.IsDone)

		info, err = svc.GetReplicationPolicyInfo(ctx, u1, b1, s1, s2, nil)
		r.NoError(err)
		r.EqualValues(Done, info.SwitchStatus)
	})
}

func TestReplicationSwitch_GetOldFollowers(t *testing.T) {
	r := require.New(t)
	followers := map[string]tasks.Priority{
		"f1": tasks.Priority3,
		"f2": tasks.Priority2,
		"f3": tasks.PriorityDefault1,
	}

	s := ReplicationSwitch{}
	r.Empty(s.OldFollowers)
	r.Empty(s.GetOldFollowers())

	s.SetOldFollowers(followers)
	r.NotEmpty(s.OldFollowers)
	r.NotEmpty(s.GetOldFollowers())
	r.EqualValues(followers, s.GetOldFollowers())
}
