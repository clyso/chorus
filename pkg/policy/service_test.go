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
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/tasks"
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

		err = svc.addBucketRoutingPolicy(ctx, "", "", "", false)
		r.ErrorIs(err, dom.ErrInvalidArg)

		err = svc.addBucketRoutingPolicy(ctx, "a", "a", "", false)
		r.ErrorIs(err, dom.ErrInvalidArg)

		err = svc.addBucketRoutingPolicy(ctx, "a", "", "a", false)
		r.ErrorIs(err, dom.ErrInvalidArg)

		err = svc.addBucketRoutingPolicy(ctx, "", "a", "a", false)
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

		err = svc.addBucketRoutingPolicy(ctx, u1, b1, s3, false)
		r.NoError(err)
		err = svc.addBucketRoutingPolicy(ctx, u2, b2, s4, false)
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
		_, err = svc.getBucketRoutingPolicy(ctx, u1, b2)
		r.ErrorIs(err, dom.ErrNotFound)
		res, err = svc.getBucketRoutingPolicy(ctx, u2, b2)
		r.NoError(err)
		r.EqualValues(s4, res)
		_, err = svc.getBucketRoutingPolicy(ctx, u2, b1)
		r.ErrorIs(err, dom.ErrNotFound)

		// cannot add policy if already exists
		err = svc.AddUserRoutingPolicy(ctx, u1, s1)
		r.ErrorIs(err, dom.ErrAlreadyExists)
		err = svc.AddUserRoutingPolicy(ctx, u2, s2)
		r.ErrorIs(err, dom.ErrAlreadyExists)
		err = svc.addBucketRoutingPolicy(ctx, u1, b1, s3, false)
		r.ErrorIs(err, dom.ErrAlreadyExists)
		err = svc.addBucketRoutingPolicy(ctx, u2, b2, s4, false)
		r.ErrorIs(err, dom.ErrAlreadyExists)
	})

	t.Run("add bucket policies", func(t *testing.T) {
		r := require.New(t)
		db.FlushAll()

		err := svc.addBucketRoutingPolicy(ctx, u1, b1, s3, false)
		r.NoError(err)
		err = svc.addBucketRoutingPolicy(ctx, u2, b2, s4, false)
		r.NoError(err)

		_, err = svc.GetUserRoutingPolicy(ctx, u1)
		r.ErrorIs(err, dom.ErrNotFound)
		_, err = svc.GetUserRoutingPolicy(ctx, u2)
		r.ErrorIs(err, dom.ErrNotFound)

		res, err := svc.getBucketRoutingPolicy(ctx, u1, b1)
		r.NoError(err)
		r.EqualValues(s3, res)
		_, err = svc.getBucketRoutingPolicy(ctx, u1, b2)
		r.ErrorIs(err, dom.ErrNotFound)
		res, err = svc.getBucketRoutingPolicy(ctx, u2, b2)
		r.NoError(err)
		r.EqualValues(s4, res)
		_, err = svc.getBucketRoutingPolicy(ctx, u2, b1)
		r.ErrorIs(err, dom.ErrNotFound)
	})

	t.Run("cannot add policy if already exists", func(t *testing.T) {
		r := require.New(t)
		db.FlushAll()

		err := svc.addBucketRoutingPolicy(ctx, u1, b1, s3, false)
		r.NoError(err)
		err = svc.addBucketRoutingPolicy(ctx, u1, b1, s3, false)
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

		wrongReplicationIDs := []entity.ReplicationStatusID{
			{
				User:        "",
				FromStorage: "a",
				ToStorage:   "a",
				FromBucket:  "a",
				ToBucket:    "",
			},
			{
				User:        "a",
				FromStorage: "",
				ToStorage:   "a",
				FromBucket:  "a",
				ToBucket:    "",
			},
			{
				User:        "a",
				FromStorage: "a",
				ToStorage:   "",
				FromBucket:  "a",
				ToBucket:    "",
			},
			{
				User:        "a",
				FromStorage: "a",
				ToStorage:   "a",
				FromBucket:  "a",
				ToBucket:    "",
			},
			{
				User:        "a",
				FromStorage: "a",
				ToStorage:   "a",
				FromBucket:  "",
				ToBucket:    "",
			},
		}

		rightReplicationID := entity.ReplicationStatusID{
			User:        "a",
			FromStorage: "a",
			ToStorage:   "b",
			FromBucket:  "a",
			ToBucket:    "",
		}

		for _, replicationID := range wrongReplicationIDs {
			err = svc.AddBucketReplicationPolicy(ctx, replicationID, tasks.Priority3, nil)
			r.ErrorIs(err, dom.ErrInvalidArg)
			err = svc.AddBucketReplicationPolicy(ctx, replicationID, tasks.Priority(69), nil)
			r.ErrorIs(err, dom.ErrInvalidArg)
			_, err = svc.GetReplicationPolicyInfo(ctx, replicationID)
			r.ErrorIs(err, dom.ErrInvalidArg)
			_, err = svc.IsReplicationPolicyPaused(ctx, replicationID)
			r.ErrorIs(err, dom.ErrInvalidArg)
			err = svc.IncReplInitObjListed(ctx, replicationID, 0, time.Now())
			r.ErrorIs(err, dom.ErrInvalidArg)
			err = svc.IncReplInitObjDone(ctx, replicationID, 0, time.Now())
			r.ErrorIs(err, dom.ErrInvalidArg)
			err = svc.IncReplEvents(ctx, replicationID, time.Now())
			r.ErrorIs(err, dom.ErrInvalidArg)
			err = svc.IncReplEventsDone(ctx, replicationID, time.Now())
			r.ErrorIs(err, dom.ErrInvalidArg)
			err = svc.PauseReplication(ctx, replicationID)
			r.ErrorIs(err, dom.ErrInvalidArg)
			err = svc.ResumeReplication(ctx, replicationID)
			r.ErrorIs(err, dom.ErrInvalidArg)
		}

		err = svc.AddBucketReplicationPolicy(ctx, rightReplicationID, tasks.Priority3, nil)
		r.NoError(err)
		_, err = svc.GetReplicationPolicyInfo(ctx, rightReplicationID)
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.IsReplicationPolicyPaused(ctx, rightReplicationID)
		r.NoError(err)
		err = svc.IncReplInitObjListed(ctx, rightReplicationID, 0, time.Now())
		r.NoError(err)
		err = svc.IncReplInitObjDone(ctx, rightReplicationID, 0, time.Now())
		r.NoError(err)
		err = svc.IncReplEvents(ctx, rightReplicationID, time.Now())
		r.NoError(err)
		err = svc.IncReplEventsDone(ctx, rightReplicationID, time.Now())
		r.NoError(err)
		err = svc.PauseReplication(ctx, rightReplicationID)
		r.NoError(err)
		err = svc.ResumeReplication(ctx, rightReplicationID)
		r.NoError(err)

		_, err = svc.GetBucketReplicationPolicies(ctx, "", "")
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.GetBucketReplicationPolicies(ctx, "a", "")
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.GetBucketReplicationPolicies(ctx, "", "a")
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.GetBucketReplicationPolicies(ctx, "a", "a")
		r.NoError(err)
	})

	t.Run("returns not found", func(t *testing.T) {
		r := require.New(t)
		db.FlushAll()
		for _, u := range users {
			_, err := svc.GetUserReplicationPolicies(ctx, u)
			r.ErrorIs(err, dom.ErrNotFound)
			for _, b := range buckets {
				replicationID12 := entity.ReplicationStatusID{
					User:        u,
					FromStorage: s1,
					ToStorage:   s2,
					FromBucket:  b,
					ToBucket:    "",
				}
				replicationID34 := entity.ReplicationStatusID{
					User:        u,
					FromStorage: s3,
					ToStorage:   s4,
					FromBucket:  b,
					ToBucket:    "",
				}
				_, err = svc.GetBucketReplicationPolicies(ctx, u, b)
				r.ErrorIs(err, dom.ErrNotFound)
				_, err = svc.GetReplicationPolicyInfo(ctx, replicationID12)
				r.ErrorIs(err, dom.ErrNotFound)
				exists, err := svc.IsReplicationPolicyExists(ctx, replicationID12)
				r.NoError(err)
				r.False(exists)
				_, err = svc.IsReplicationPolicyPaused(ctx, replicationID12)
				r.ErrorIs(err, dom.ErrNotFound)
				err = svc.IncReplEvents(ctx, replicationID12, time.Now())
				r.ErrorIs(err, dom.ErrNotFound)
				err = svc.IncReplEventsDone(ctx, replicationID12, time.Now())
				r.ErrorIs(err, dom.ErrNotFound)
				err = svc.IncReplInitObjListed(ctx, replicationID12, 5, time.Now())
				r.ErrorIs(err, dom.ErrNotFound)
				err = svc.IncReplInitObjDone(ctx, replicationID34, 5, time.Now())
				r.ErrorIs(err, dom.ErrNotFound)
				err = svc.PauseReplication(ctx, replicationID34)
				r.ErrorIs(err, dom.ErrNotFound)
				err = svc.ResumeReplication(ctx, replicationID34)
				r.ErrorIs(err, dom.ErrNotFound)
				err = svc.ObjListStarted(ctx, replicationID34)
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
		r.EqualValues(tasks.Priority3, res.To[ReplicationPolicyDest(s2)])

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
		r.EqualValues(tasks.Priority3, res.To[ReplicationPolicyDest(s1)])

		err = svc.AddUserReplicationPolicy(ctx, u1, s1, s3, tasks.Priority4)
		r.NoError(err)
		res, err = svc.GetUserReplicationPolicies(ctx, u1)
		r.NoError(err)
		r.EqualValues(s1, res.From)
		r.Len(res.To, 2)
		r.EqualValues(tasks.Priority3, res.To[ReplicationPolicyDest(s2)])
		r.EqualValues(tasks.Priority4, res.To[ReplicationPolicyDest(s3)])
	})

	t.Run("add bucket repl policy", func(t *testing.T) {
		r := require.New(t)
		db.FlushAll()

		replicationIDu1s1s2 := entity.ReplicationStatusID{
			User:        u1,
			FromStorage: s1,
			ToStorage:   s2,
			FromBucket:  b1,
			ToBucket:    b1,
		}
		replicationIDu1s1s3 := entity.ReplicationStatusID{
			User:        u1,
			FromStorage: s1,
			ToStorage:   s2,
			FromBucket:  b1,
			ToBucket:    b1,
		}
		replicationIDu1s2s1 := entity.ReplicationStatusID{
			User:        u1,
			FromStorage: s2,
			ToStorage:   s1,
			FromBucket:  b1,
			ToBucket:    b1,
		}
		replicationIDu1s2s1b2 := entity.ReplicationStatusID{
			User:        u1,
			FromStorage: s2,
			ToStorage:   s1,
			FromBucket:  b2,
			ToBucket:    b2,
		}
		replicationIDu2s2s1 := entity.ReplicationStatusID{
			User:        u2,
			FromStorage: s2,
			ToStorage:   s1,
			FromBucket:  b1,
			ToBucket:    b1,
		}
		_, err := svc.GetBucketReplicationPolicies(ctx, u1, b1)
		r.ErrorIs(err, dom.ErrNotFound)

		_, err = svc.GetReplicationPolicyInfo(ctx, replicationIDu1s1s2)
		r.ErrorIs(err, dom.ErrNotFound)
		exists, err := svc.IsReplicationPolicyExists(ctx, replicationIDu1s1s2)
		r.NoError(err)
		r.False(exists)
		list, err := svc.ListReplicationPolicyInfo(ctx)
		r.NoError(err)
		r.Empty(list)

		err = svc.AddBucketReplicationPolicy(ctx, replicationIDu1s1s2, tasks.Priority3, nil)
		r.NoError(err)

		res, err := svc.GetBucketReplicationPolicies(ctx, u1, b1)
		r.NoError(err)
		r.EqualValues(s1, res.From)
		r.Len(res.To, 1)
		r.EqualValues(tasks.Priority3, res.To[ReplicationPolicyDest(s2)])

		info, err := svc.GetReplicationPolicyInfo(ctx, replicationIDu1s1s2)
		r.NoError(err)
		r.False(info.CreatedAt.IsZero())
		r.False(info.IsPaused)
		r.Zero(info.InitObjListed)
		r.Zero(info.InitObjDone)
		r.Zero(info.InitBytesListed)
		r.Zero(info.InitBytesDone)
		r.Zero(info.Events)
		r.Zero(info.EventsDone)

		exists, err = svc.IsReplicationPolicyExists(ctx, replicationIDu1s1s2)
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

		err = svc.AddBucketReplicationPolicy(ctx, replicationIDu1s1s2, tasks.Priority3, nil)
		r.ErrorIs(err, dom.ErrAlreadyExists)
		err = svc.AddBucketReplicationPolicy(ctx, replicationIDu1s2s1, tasks.Priority3, nil)
		r.ErrorIs(err, dom.ErrInvalidArg)

		err = svc.AddBucketReplicationPolicy(ctx, replicationIDu2s2s1, tasks.PriorityDefault1, nil)
		r.NoError(err)
		res, err = svc.GetBucketReplicationPolicies(ctx, u2, b1)
		r.NoError(err)
		r.EqualValues(s2, res.From)
		r.Len(res.To, 1)
		r.EqualValues(tasks.PriorityDefault1, res.To[ReplicationPolicyDest(s1)])

		err = svc.AddBucketReplicationPolicy(ctx, replicationIDu1s2s1b2, tasks.PriorityHighest5, nil)
		r.NoError(err)
		res, err = svc.GetBucketReplicationPolicies(ctx, u1, b2)
		r.NoError(err)
		r.EqualValues(s2, res.From)
		r.Len(res.To, 1)
		r.EqualValues(tasks.PriorityHighest5, res.To[ReplicationPolicyDest(s1)])

		err = svc.AddBucketReplicationPolicy(ctx, replicationIDu1s1s3, tasks.Priority4, nil)
		r.NoError(err)
		res, err = svc.GetBucketReplicationPolicies(ctx, u1, b1)
		r.NoError(err)
		r.EqualValues(s1, res.From)
		r.Len(res.To, 2)
		r.EqualValues(tasks.Priority3, res.To[ReplicationPolicyDest(s2)])
		r.EqualValues(tasks.Priority4, res.To[ReplicationPolicyDest(s3)])

		list, err = svc.ListReplicationPolicyInfo(ctx)
		r.NoError(err)
		r.Len(list, 4)

		pol, err := svc.GetReplicationPolicyInfo(ctx, replicationIDu1s1s2)
		r.NoError(err)
		r.False(pol.ListingStarted)
		err = svc.ObjListStarted(ctx, replicationIDu1s1s2)
		r.NoError(err)
		pol, err = svc.GetReplicationPolicyInfo(ctx, replicationIDu1s1s2)
		r.NoError(err)
		r.True(pol.ListingStarted)

		err = svc.DeleteReplication(ctx, replicationIDu1s1s2)
		r.NoError(err)
		_, err = svc.GetReplicationPolicyInfo(ctx, replicationIDu1s1s2)
		r.ErrorIs(err, dom.ErrNotFound)
		exists, err = svc.IsReplicationPolicyExists(ctx, replicationIDu1s1s2)
		r.NoError(err)
		r.False(exists)
		list, err = svc.ListReplicationPolicyInfo(ctx)
		r.NoError(err)
		r.Len(list, 3)

	})

	t.Run("counters", func(t *testing.T) {
		r := require.New(t)
		db.FlushAll()

		replicationID12 := entity.ReplicationStatusID{
			User:        u1,
			FromStorage: s1,
			ToStorage:   s2,
			FromBucket:  b1,
			ToBucket:    "",
		}
		err := svc.AddBucketReplicationPolicy(ctx, replicationID12, tasks.Priority3, nil)
		r.NoError(err)

		res, err := svc.GetBucketReplicationPolicies(ctx, u1, b1)
		r.NoError(err)
		r.EqualValues(s1, res.From)
		r.Len(res.To, 1)
		r.EqualValues(tasks.Priority3, res.To[ReplicationPolicyDest(s2)])

		info, err := svc.GetReplicationPolicyInfo(ctx, replicationID12)
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
		err = svc.IncReplInitObjListed(ctx, replicationID12, 69, now)
		r.NoError(err)
		infoUpd, err := svc.GetReplicationPolicyInfo(ctx, replicationID12)
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

		err = svc.IncReplInitObjDone(ctx, replicationID12, 13, now)
		r.NoError(err)
		infoUpd, err = svc.GetReplicationPolicyInfo(ctx, replicationID12)
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

		err = svc.IncReplInitObjDone(ctx, replicationID12, 7, before)
		r.NoError(err)
		infoUpd, err = svc.GetReplicationPolicyInfo(ctx, replicationID12)
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

		err = svc.IncReplEvents(ctx, replicationID12, now) //??
		r.NoError(err)
		infoUpd, err = svc.GetReplicationPolicyInfo(ctx, replicationID12)
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
		err = svc.IncReplEvents(ctx, replicationID12, after)
		r.NoError(err)
		infoUpd, err = svc.GetReplicationPolicyInfo(ctx, replicationID12)
		r.NoError(err)
		r.EqualValues(info.CreatedAt, infoUpd.CreatedAt)
		r.False(infoUpd.IsPaused)
		r.EqualValues(1, infoUpd.InitObjListed)
		r.EqualValues(2, infoUpd.InitObjDone)
		r.EqualValues(69, infoUpd.InitBytesListed)
		r.EqualValues(20, infoUpd.InitBytesDone)
		r.EqualValues(2, infoUpd.Events)
		r.Zero(infoUpd.EventsDone)

		err = svc.IncReplEventsDone(ctx, replicationID12, after)
		r.NoError(err)
		infoUpd, err = svc.GetReplicationPolicyInfo(ctx, replicationID12)
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
		err = svc.IncReplEventsDone(ctx, replicationID12, afterAfter)
		r.NoError(err)
		infoUpd, err = svc.GetReplicationPolicyInfo(ctx, replicationID12)
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

		replicationID12 := entity.ReplicationStatusID{
			User:        u1,
			FromStorage: s1,
			ToStorage:   s2,
			FromBucket:  b1,
			ToBucket:    "",
		}
		err := svc.AddBucketReplicationPolicy(ctx, replicationID12, tasks.Priority3, nil)
		r.NoError(err)

		res, err := svc.GetBucketReplicationPolicies(ctx, u1, b1)
		r.NoError(err)
		r.EqualValues(s1, res.From)
		r.Len(res.To, 1)
		r.EqualValues(tasks.Priority3, res.To[ReplicationPolicyDest(s2)])

		info, err := svc.GetReplicationPolicyInfo(ctx, replicationID12)
		r.NoError(err)
		r.False(info.CreatedAt.IsZero())
		r.False(info.IsPaused)
		r.Zero(info.InitObjListed)
		r.Zero(info.InitObjDone)
		r.Zero(info.InitBytesListed)
		r.Zero(info.InitBytesDone)
		r.Zero(info.Events)
		r.Zero(info.EventsDone)

		err = svc.PauseReplication(ctx, replicationID12)
		r.NoError(err)

		info, err = svc.GetReplicationPolicyInfo(ctx, replicationID12)
		r.NoError(err)
		r.True(info.IsPaused)
		r.Zero(info.InitObjListed)
		r.Zero(info.InitObjDone)
		r.Zero(info.InitBytesListed)
		r.Zero(info.InitBytesDone)
		r.Zero(info.Events)
		r.Zero(info.EventsDone)

		err = svc.ResumeReplication(ctx, replicationID12)
		r.NoError(err)

		info, err = svc.GetReplicationPolicyInfo(ctx, replicationID12)
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
		r.EqualValues(tasks.Priority3, res.To[ReplicationPolicyDest(s2)])

		err = svc.AddUserReplicationPolicy(ctx, u1, s1, s3, tasks.Priority4)
		r.NoError(err)

		res, err = svc.GetUserReplicationPolicies(ctx, u1)
		r.NoError(err)
		r.EqualValues(s1, res.From)
		r.Len(res.To, 2)
		r.EqualValues(tasks.Priority3, res.To[ReplicationPolicyDest(s2)])
		r.EqualValues(tasks.Priority4, res.To[ReplicationPolicyDest(s3)])

		err = svc.DeleteUserReplication(ctx, u1, s1, s2)
		r.NoError(err)

		res, err = svc.GetUserReplicationPolicies(ctx, u1)
		r.NoError(err)
		r.EqualValues(s1, res.From)
		r.Len(res.To, 1)
		r.EqualValues(tasks.Priority4, res.To[ReplicationPolicyDest(s3)])

		err = svc.DeleteUserReplication(ctx, u1, s1, s2)
		r.ErrorIs(err, dom.ErrNotFound)

		err = svc.DeleteUserReplication(ctx, u1, s1, s3)
		r.NoError(err)

		res, err = svc.GetUserReplicationPolicies(ctx, u1)
		r.ErrorIs(err, dom.ErrNotFound)

		replicationIDs := []entity.ReplicationStatusID{
			{
				User:        u1,
				FromStorage: s1,
				ToStorage:   s2,
				FromBucket:  b1,
				ToBucket:    b1,
			},
			{
				User:        u1,
				FromStorage: s1,
				ToStorage:   s2,
				FromBucket:  b2,
				ToBucket:    b2,
			},
			{
				User:        u1,
				FromStorage: s1,
				ToStorage:   s3,
				FromBucket:  b1,
				ToBucket:    b1,
			},
			{
				User:        u2,
				FromStorage: s1,
				ToStorage:   s3,
				FromBucket:  b1,
				ToBucket:    b1,
			},
		}

		for _, replicationID := range replicationIDs {
			err = svc.AddBucketReplicationPolicy(ctx, replicationID, tasks.Priority4, nil)
			r.NoError(err)
			exists, err := svc.IsReplicationPolicyExists(ctx, replicationID)
			r.NoError(err)
			r.True(exists)
		}

		deleted, err := svc.DeleteBucketReplicationsByUser(ctx, u1, s1, s2)
		r.NoError(err)
		r.NotEmpty(deleted)

		exists, err := svc.IsReplicationPolicyExists(ctx, replicationIDs[0])
		r.NoError(err)
		r.False(exists)
		exists, err = svc.IsReplicationPolicyExists(ctx, replicationIDs[1])
		r.NoError(err)
		r.False(exists)

	})
}

func Test_CustomDestBucket(t *testing.T) {
	r := require.New(t)
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	ctx := context.TODO()

	svc := NewService(c)

	// setup
	user := "user"
	srcBuck, dstBuck := "b1", "b2"
	stor := "stor"
	stor2 := "stor2"
	r.NoError(svc.AddUserRoutingPolicy(ctx, user, stor), "route to main storage")
	replicationIDMatchingSrcDest := entity.ReplicationStatusID{
		User:        user,
		FromStorage: stor,
		ToStorage:   stor,
		FromBucket:  srcBuck,
		ToBucket:    srcBuck,
	}

	replicationIDDifferentBuckets := entity.ReplicationStatusID{
		User:        user,
		FromStorage: stor,
		ToStorage:   stor,
		FromBucket:  srcBuck,
		ToBucket:    dstBuck,
	}

	replicationIDDifferentSrcDest := entity.ReplicationStatusID{
		User:        user,
		FromStorage: stor,
		ToStorage:   stor2,
		FromBucket:  srcBuck,
		ToBucket:    dstBuck,
	}

	// validate policy creation
	err := svc.AddBucketReplicationPolicy(ctx, replicationIDMatchingSrcDest, tasks.Priority3, nil)
	r.ErrorIs(err, dom.ErrInvalidArg, "repl to same storage and bucket is not allowed")

	err = svc.AddBucketReplicationPolicy(ctx, replicationIDDifferentBuckets, tasks.Priority3, nil)
	r.NoError(err, dom.ErrInvalidArg, "repl to same storage but different bucket is allowed")

	err = svc.AddBucketReplicationPolicy(ctx, replicationIDDifferentSrcDest, tasks.Priority2, nil)
	r.NoError(err, dom.ErrInvalidArg, "repl to different storage and different bucket is allowed")

	err = svc.AddBucketReplicationPolicy(ctx, replicationIDDifferentSrcDest, tasks.Priority2, nil)
	r.Error(err, "already exists")

	// check replication lookup
	rps, err := svc.GetBucketReplicationPolicies(ctx, user, srcBuck)
	r.NoError(err)
	r.EqualValues(stor, rps.From)
	r.Len(rps.To, 2)
	r.EqualValues(tasks.Priority3, rps.To[ReplicationPolicyDest(stor+":"+dstBuck)], "custom bucket is in destination")
	r.EqualValues(tasks.Priority2, rps.To[ReplicationPolicyDest(stor2+":"+dstBuck)], "custom bucket is in destination")

	route, err := svc.GetRoutingPolicy(ctx, user, srcBuck)
	r.NoError(err)
	r.EqualValues(stor, route, "route to main for src bucket")
	_, err = svc.GetRoutingPolicy(ctx, user, dstBuck)
	r.ErrorIs(err, dom.ErrRoutingBlock, "for dst bucket routing is blocked")

	_, err = svc.GetReplicationPolicyInfo(ctx, replicationIDDifferentBuckets)
	r.NoError(err, "info created")

	list, err := svc.ListReplicationPolicyInfo(ctx)
	r.NoError(err)
	r.Len(list, 2)
	for _, policy := range list {
		r.EqualValues(stor, policy.From)
		r.EqualValues(user, policy.User)
		r.EqualValues(srcBuck, policy.Bucket)
		r.NotNil(policy.ToBucket)
		if policy.To == stor {
			r.EqualValues(dstBuck, policy.ToBucket)
		} else if policy.To == stor2 {
			r.EqualValues(dstBuck, policy.ToBucket)
		} else {
			r.Fail("invalid policy dest storage")
		}
	}

	ok, err := svc.IsReplicationPolicyExists(ctx, replicationIDDifferentBuckets)
	r.NoError(err)
	r.True(ok)

	// check pause/resume:
	ok, err = svc.IsReplicationPolicyPaused(ctx, replicationIDDifferentBuckets)
	r.NoError(err)
	r.False(ok)
	err = svc.PauseReplication(ctx, replicationIDDifferentBuckets)
	r.NoError(err)
	ok, err = svc.IsReplicationPolicyPaused(ctx, replicationIDDifferentBuckets)
	r.NoError(err)
	r.True(ok)
	err = svc.ResumeReplication(ctx, replicationIDDifferentBuckets)
	r.NoError(err)
	ok, err = svc.IsReplicationPolicyPaused(ctx, replicationIDDifferentBuckets)
	r.NoError(err)
	r.False(ok)

	// check replication counters
	info, err := svc.GetReplicationPolicyInfo(ctx, replicationIDDifferentBuckets)
	r.NoError(err)
	r.False(info.CreatedAt.IsZero())
	r.False(info.IsPaused)
	r.Zero(info.InitObjListed)
	r.Zero(info.InitObjDone)
	r.Zero(info.InitBytesListed)
	r.Zero(info.InitBytesDone)
	r.Zero(info.Events)
	r.Zero(info.EventsDone)
	r.Nil(info.LastEmittedAt)
	r.Nil(info.LastProcessedAt)

	eventTime := time.Now()
	err = svc.IncReplInitObjListed(ctx, replicationIDDifferentBuckets, 69, eventTime)
	r.NoError(err)
	err = svc.IncReplInitObjDone(ctx, replicationIDDifferentBuckets, 69, eventTime)
	r.NoError(err)

	err = svc.IncReplEvents(ctx, replicationIDDifferentBuckets, eventTime)
	r.NoError(err)
	err = svc.IncReplEvents(ctx, replicationIDDifferentBuckets, eventTime)
	r.NoError(err)
	err = svc.IncReplEventsDone(ctx, replicationIDDifferentBuckets, eventTime)
	r.NoError(err)

	info, err = svc.GetReplicationPolicyInfo(ctx, replicationIDDifferentBuckets)
	r.NoError(err)
	r.EqualValues(1, info.InitObjListed)
	r.EqualValues(1, info.InitObjDone)
	r.EqualValues(69, info.InitBytesListed)
	r.EqualValues(69, info.InitBytesDone)
	r.EqualValues(2, info.Events)
	r.EqualValues(1, info.EventsDone)
	r.NotNil(info.LastEmittedAt)
	r.NotNil(info.LastProcessedAt)

	// delete replication
	err = svc.DeleteReplication(ctx, replicationIDDifferentBuckets)
	r.NoError(err)

	// verify deletion
	info, err = svc.GetReplicationPolicyInfo(ctx, replicationIDDifferentBuckets)
	r.ErrorIs(err, dom.ErrNotFound)

	list, err = svc.ListReplicationPolicyInfo(ctx)
	r.NoError(err)
	r.Len(list, 1)
	r.EqualValues(stor2, list[0].To)

	ok, err = svc.IsReplicationPolicyExists(ctx, replicationIDDifferentBuckets)
	r.NoError(err)
	r.False(ok)

	route, err = svc.GetRoutingPolicy(ctx, user, dstBuck)
	r.NoError(err, "routing block removed")
	r.EqualValues(stor, route)
}

func TestReplicationID(t *testing.T) {
	tests := []struct {
		name    string
		in      entity.ReplicationStatusID
		wantErr bool
	}{
		// Valid cases
		{
			name: "Valid basic replication ID",
			in: entity.ReplicationStatusID{
				User:        "user1",
				FromBucket:  "bucket1",
				FromStorage: "source1",
				ToStorage:   "dest1",
			},
			wantErr: false,
		},
		{
			name: "Valid with ToBucket",
			in: entity.ReplicationStatusID{
				User:        "user2",
				FromBucket:  "bucket2",
				FromStorage: "source2",
				ToStorage:   "dest2",
				ToBucket:    "target2",
			},
			wantErr: false,
		},
		{
			name: "Valid with special characters (no colons)",
			in: entity.ReplicationStatusID{
				User:        "user-3_special",
				FromBucket:  "bucket.3-special",
				FromStorage: "src_3",
				ToStorage:   "dst-3",
			},
			wantErr: false,
		},

		// Error cases: Missing required fields
		{
			name: "Empty User",
			in: entity.ReplicationStatusID{
				FromBucket:  "bucket",
				FromStorage: "from",
				ToStorage:   "to",
			},
			wantErr: true,
		},
		{
			name: "Empty Bucket",
			in: entity.ReplicationStatusID{
				User:        "user",
				FromStorage: "from",
				ToStorage:   "to",
			},
			wantErr: true,
		},
		{
			name: "Empty From",
			in: entity.ReplicationStatusID{
				User:       "user",
				FromBucket: "bucket",
				ToStorage:  "to",
			},
			wantErr: true,
		},
		{
			name: "Empty To",
			in: entity.ReplicationStatusID{
				User:        "user",
				FromBucket:  "bucket",
				FromStorage: "from",
			},
			wantErr: true,
		},

		// Error cases: ToBucket validation
		{
			name: "Empty ToBucket",
			in: entity.ReplicationStatusID{
				User:        "user",
				FromBucket:  "bucket",
				FromStorage: "from",
				ToStorage:   "to",
				ToBucket:    "",
			},
			wantErr: true,
		},
		{
			name: "ToBucket same as Bucket",
			in: entity.ReplicationStatusID{
				User:        "user",
				FromBucket:  "bucket",
				FromStorage: "from",
				ToStorage:   "to",
				ToBucket:    "bucket",
			},
			wantErr: true,
		},

		// Error cases: Colon in fields
		{
			name: "Colon in User",
			in: entity.ReplicationStatusID{
				User:        "user:1",
				FromBucket:  "bucket",
				FromStorage: "from",
				ToStorage:   "to",
			},
			wantErr: true,
		},
		{
			name: "Colon in Bucket",
			in: entity.ReplicationStatusID{
				User:        "user",
				FromBucket:  "bucket:1",
				FromStorage: "from",
				ToStorage:   "to",
			},
			wantErr: true,
		},
		{
			name: "Colon in From",
			in: entity.ReplicationStatusID{
				User:        "user",
				FromBucket:  "bucket",
				FromStorage: "from:1",
				ToStorage:   "to",
			},
			wantErr: true,
		},
		{
			name: "Colon in To",
			in: entity.ReplicationStatusID{
				User:        "user",
				FromBucket:  "bucket",
				FromStorage: "from",
				ToStorage:   "to:1",
			},
			wantErr: true,
		},
		{
			name: "Colon in ToBucket",
			in: entity.ReplicationStatusID{
				User:        "user",
				FromBucket:  "bucket",
				FromStorage: "from",
				ToStorage:   "to",
				ToBucket:    "target:1",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)

			err := tt.in.Validate()
			if tt.wantErr {
				r.Error(err)
				return
			} else {
				r.NoError(err)
			}
			got, err := ReplicationIDFromStr(tt.in.String())
			r.NoError(err)
			r.EqualValues(tt.in, got)
			r.EqualValues(tt.in.String(), got.String())
			r.EqualValues(tt.in.StatusKey(), got.StatusKey())
			r.EqualValues(tt.in.RoutingKey(), got.RoutingKey())
			r.EqualValues(tt.in.SwitchKey(), got.SwitchKey())
			r.EqualValues(tt.in.SwitchHistoryKey(), got.SwitchHistoryKey())
		})
	}
}
