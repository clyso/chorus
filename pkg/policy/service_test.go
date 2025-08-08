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
)

func Test_policySvc_UserRoutingPolicy(t *testing.T) {
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	ctx := context.TODO()

	svc := NewService(c, nil)

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
				_, err = svc.getBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(u, b))
				r.ErrorIs(err, dom.ErrNotFound)
			}
		}
	})

	t.Run("args must be non empty", func(t *testing.T) {
		r := require.New(t)
		_, err := svc.GetUserRoutingPolicy(ctx, "")
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.getBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID("", ""))
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.getBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID("a", ""))
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.getBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID("", "a"))
		r.ErrorIs(err, dom.ErrInvalidArg)

		err = svc.AddUserRoutingPolicy(ctx, "", "")
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.AddUserRoutingPolicy(ctx, "a", "")
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.AddUserRoutingPolicy(ctx, "", "a")
		r.ErrorIs(err, dom.ErrInvalidArg)

		err = svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID("", ""), "", false)
		r.ErrorIs(err, dom.ErrInvalidArg)

		err = svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID("a", "a"), "", false)
		r.ErrorIs(err, dom.ErrInvalidArg)

		err = svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID("a", ""), "a", false)
		r.ErrorIs(err, dom.ErrInvalidArg)

		err = svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID("", "a"), "a", false)
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
				_, err = svc.getBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(u, b))
				r.ErrorIs(err, dom.ErrNotFound)
			}
		}

		err = svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(u1, b1), s3, false)
		r.NoError(err)
		err = svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(u2, b2), s4, false)
		r.NoError(err)

		res, err = svc.GetUserRoutingPolicy(ctx, u1)
		r.NoError(err)
		r.EqualValues(s1, res)
		res, err = svc.GetUserRoutingPolicy(ctx, u2)
		r.NoError(err)
		r.EqualValues(s2, res)

		res, err = svc.getBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(u1, b1))
		r.NoError(err)
		r.EqualValues(s3, res)
		_, err = svc.getBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(u1, b2))
		r.ErrorIs(err, dom.ErrNotFound)
		res, err = svc.getBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(u2, b2))
		r.NoError(err)
		r.EqualValues(s4, res)
		_, err = svc.getBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(u2, b1))
		r.ErrorIs(err, dom.ErrNotFound)

		// cannot add policy if already exists
		err = svc.AddUserRoutingPolicy(ctx, u1, s1)
		r.ErrorIs(err, dom.ErrAlreadyExists)
		err = svc.AddUserRoutingPolicy(ctx, u2, s2)
		r.ErrorIs(err, dom.ErrAlreadyExists)
		err = svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(u1, b1), s3, false)
		r.ErrorIs(err, dom.ErrAlreadyExists)
		err = svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(u2, b2), s4, false)
		r.ErrorIs(err, dom.ErrAlreadyExists)
	})

	t.Run("add bucket policies", func(t *testing.T) {
		r := require.New(t)
		db.FlushAll()

		err := svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(u1, b1), s3, false)
		r.NoError(err)
		err = svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(u2, b2), s4, false)
		r.NoError(err)

		_, err = svc.GetUserRoutingPolicy(ctx, u1)
		r.ErrorIs(err, dom.ErrNotFound)
		_, err = svc.GetUserRoutingPolicy(ctx, u2)
		r.ErrorIs(err, dom.ErrNotFound)

		res, err := svc.getBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(u1, b1))
		r.NoError(err)
		r.EqualValues(s3, res)
		_, err = svc.getBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(u1, b2))
		r.ErrorIs(err, dom.ErrNotFound)
		res, err = svc.getBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(u2, b2))
		r.NoError(err)
		r.EqualValues(s4, res)
		_, err = svc.getBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(u2, b1))
		r.ErrorIs(err, dom.ErrNotFound)
	})

	t.Run("cannot add policy if already exists", func(t *testing.T) {
		r := require.New(t)
		db.FlushAll()

		err := svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(u1, b1), s3, false)
		r.NoError(err)
		err = svc.AddBucketRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(u1, b1), s3, false)
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

	svc := NewService(c, nil)

	u1, u2 := "u1", "u2"
	users := []string{u1, u2}
	b1, b2 := "b1", "b2"
	buckets := []string{b1, b2}
	s1, s2, s3, s4 := "s1", "s2", "s3", "s4"

	t.Run("args must be valid", func(t *testing.T) {
		r := require.New(t)
		db.FlushAll()

		err := svc.AddUserReplicationPolicy(ctx, "", entity.NewUserReplicationPolicy("a", "a"))
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.AddUserReplicationPolicy(ctx, "a", entity.NewUserReplicationPolicy("", "a"))
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.AddUserReplicationPolicy(ctx, "a", entity.NewUserReplicationPolicy("a", ""))
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.AddUserReplicationPolicy(ctx, "a", entity.NewUserReplicationPolicy("a", "a"))
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.AddUserReplicationPolicy(ctx, "a", entity.NewUserReplicationPolicy("a", "a"))
		r.ErrorIs(err, dom.ErrInvalidArg)
		err = svc.AddUserReplicationPolicy(ctx, "a", entity.NewUserReplicationPolicy("a", "b"))
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
			ToBucket:    "a",
		}

		for _, replicationID := range wrongReplicationIDs {
			err = svc.AddBucketReplicationPolicy(ctx, replicationID, nil)
			r.ErrorIs(err, dom.ErrInvalidArg)
			err = svc.AddBucketReplicationPolicy(ctx, replicationID, nil)
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

		err = svc.AddBucketReplicationPolicy(ctx, rightReplicationID, nil)
		r.NoError(err)
		_, err = svc.GetReplicationPolicyInfo(ctx, rightReplicationID)
		r.NoError(err)
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

		_, err = svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID("", ""))
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID("a", ""))
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID("", "a"))
		r.ErrorIs(err, dom.ErrInvalidArg)
		_, err = svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID("a", "a"))
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
					ToBucket:    b,
				}
				replicationID34 := entity.ReplicationStatusID{
					User:        u,
					FromStorage: s3,
					ToStorage:   s4,
					FromBucket:  b,
					ToBucket:    b,
				}
				_, err = svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(u, b))
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

		err = svc.AddUserReplicationPolicy(ctx, u1, entity.NewUserReplicationPolicy(s1, s2))
		r.NoError(err)

		res, err := svc.GetUserReplicationPolicies(ctx, u1)
		r.NoError(err)
		r.EqualValues(s1, res.FromStorage)
		r.Len(res.Destinations, 1)
		r.EqualValues(entity.NewUserReplicationPolicyDestination(s2), res.Destinations[0])

		list, err = svc.ListReplicationPolicyInfo(ctx)
		r.NoError(err)
		r.Empty(list)

		err = svc.AddUserReplicationPolicy(ctx, u1, entity.NewUserReplicationPolicy(s1, s2))
		r.ErrorIs(err, dom.ErrAlreadyExists)
		err = svc.AddUserReplicationPolicy(ctx, u1, entity.NewUserReplicationPolicy(s2, s1))
		r.ErrorIs(err, dom.ErrInvalidArg)

		err = svc.AddUserReplicationPolicy(ctx, u2, entity.NewUserReplicationPolicy(s2, s1))
		r.NoError(err)
		res, err = svc.GetUserReplicationPolicies(ctx, u2)
		r.NoError(err)
		r.EqualValues(s2, res.FromStorage)
		r.Len(res.Destinations, 1)
		r.EqualValues(entity.NewUserReplicationPolicyDestination(s1), res.Destinations[0])

		err = svc.AddUserReplicationPolicy(ctx, u1, entity.NewUserReplicationPolicy(s1, s3))
		r.NoError(err)
		res, err = svc.GetUserReplicationPolicies(ctx, u1)
		r.NoError(err)
		r.EqualValues(s1, res.FromStorage)
		r.Len(res.Destinations, 2)
		r.Contains(res.Destinations, entity.NewUserReplicationPolicyDestination(s2))
		r.Contains(res.Destinations, entity.NewUserReplicationPolicyDestination(s3))
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
			ToStorage:   s3,
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
		_, err := svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(u1, b1))
		r.ErrorIs(err, dom.ErrNotFound)

		_, err = svc.GetReplicationPolicyInfo(ctx, replicationIDu1s1s2)
		r.ErrorIs(err, dom.ErrNotFound)
		exists, err := svc.IsReplicationPolicyExists(ctx, replicationIDu1s1s2)
		r.NoError(err)
		r.False(exists)
		list, err := svc.ListReplicationPolicyInfo(ctx)
		r.NoError(err)
		r.Empty(list)

		err = svc.AddBucketReplicationPolicy(ctx, replicationIDu1s1s2, nil)
		r.NoError(err)

		res, err := svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(u1, b1))
		r.NoError(err)
		r.EqualValues(s1, res.FromStorage)
		r.Len(res.Destinations, 1)
		r.EqualValues(entity.NewBucketReplicationPolicyDestination(s2, b1), res.Destinations[0])

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
		for id, status := range list {
			r.EqualValues(info, status)
			r.EqualValues(u1, id.User)
			r.EqualValues(b1, id.FromBucket)
			r.EqualValues(s1, id.FromStorage)
			r.EqualValues(s2, id.ToStorage)
		}

		err = svc.AddBucketReplicationPolicy(ctx, replicationIDu1s1s2, nil)
		r.ErrorIs(err, dom.ErrAlreadyExists)
		err = svc.AddBucketReplicationPolicy(ctx, replicationIDu1s2s1, nil)
		r.ErrorIs(err, dom.ErrInvalidArg)

		err = svc.AddBucketReplicationPolicy(ctx, replicationIDu2s2s1, nil)
		r.NoError(err)
		res, err = svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(u2, b1))
		r.NoError(err)
		r.EqualValues(s2, res.FromStorage)
		r.Len(res.Destinations, 1)
		r.EqualValues(entity.NewBucketReplicationPolicyDestination(s1, b1), res.Destinations[0])

		err = svc.AddBucketReplicationPolicy(ctx, replicationIDu1s2s1b2, nil)
		r.NoError(err)
		res, err = svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(u1, b2))
		r.NoError(err)
		r.EqualValues(s2, res.FromStorage)
		r.Len(res.Destinations, 1)
		r.EqualValues(entity.NewBucketReplicationPolicyDestination(s1, b2), res.Destinations[0])

		err = svc.AddBucketReplicationPolicy(ctx, replicationIDu1s1s3, nil)
		r.NoError(err)
		res, err = svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(u1, b1))
		r.NoError(err)
		r.EqualValues(s1, res.FromStorage)
		r.Len(res.Destinations, 2)
		r.Contains(res.Destinations, entity.NewBucketReplicationPolicyDestination(s2, b1))
		r.Contains(res.Destinations, entity.NewBucketReplicationPolicyDestination(s3, b1))

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
			ToBucket:    b1,
		}
		err := svc.AddBucketReplicationPolicy(ctx, replicationID12, nil)
		r.NoError(err)

		res, err := svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(u1, b1))
		r.NoError(err)
		r.EqualValues(s1, res.FromStorage)
		r.Len(res.Destinations, 1)
		r.EqualValues(entity.NewBucketReplicationPolicyDestination(s2, b1), res.Destinations[0])

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
			ToBucket:    b1,
		}
		err := svc.AddBucketReplicationPolicy(ctx, replicationID12, nil)
		r.NoError(err)

		res, err := svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(u1, b1))
		r.NoError(err)
		r.EqualValues(s1, res.FromStorage)
		r.Len(res.Destinations, 1)
		r.EqualValues(entity.NewBucketReplicationPolicyDestination(s2, b1), res.Destinations[0])

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

		userReplicationPolicy1 := entity.NewUserReplicationPolicy(s1, s2)
		err := svc.AddUserReplicationPolicy(ctx, u1, userReplicationPolicy1)
		r.NoError(err)

		res, err := svc.GetUserReplicationPolicies(ctx, u1)
		r.NoError(err)
		r.EqualValues(s1, res.FromStorage)
		r.Len(res.Destinations, 1)
		r.EqualValues(entity.NewUserReplicationPolicyDestination(s2), res.Destinations[0])

		userReplicationPolicy2 := entity.NewUserReplicationPolicy(s1, s3)
		err = svc.AddUserReplicationPolicy(ctx, u1, userReplicationPolicy2)
		r.NoError(err)

		res, err = svc.GetUserReplicationPolicies(ctx, u1)
		r.NoError(err)
		r.EqualValues(s1, res.FromStorage)
		r.Len(res.Destinations, 2)
		r.Contains(res.Destinations, entity.NewUserReplicationPolicyDestination(s2))
		r.Contains(res.Destinations, entity.NewUserReplicationPolicyDestination(s3))

		err = svc.DeleteUserReplication(ctx, u1, userReplicationPolicy1)
		r.NoError(err)

		res, err = svc.GetUserReplicationPolicies(ctx, u1)
		r.NoError(err)
		r.EqualValues(s1, res.FromStorage)
		r.Len(res.Destinations, 1)
		r.EqualValues(entity.NewUserReplicationPolicyDestination(s3), res.Destinations[0])

		err = svc.DeleteUserReplication(ctx, u1, userReplicationPolicy1)
		r.ErrorIs(err, dom.ErrNotFound)

		err = svc.DeleteUserReplication(ctx, u1, userReplicationPolicy2)
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
			err = svc.AddBucketReplicationPolicy(ctx, replicationID, nil)
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
		exists, err = svc.IsReplicationPolicyExists(ctx, replicationIDs[2])
		r.NoError(err)
		r.True(exists)
		exists, err = svc.IsReplicationPolicyExists(ctx, replicationIDs[3])
		r.NoError(err)
		r.True(exists)
	})
}

func Test_CustomDestBucket(t *testing.T) {
	r := require.New(t)
	db := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: db.Addr()})
	ctx := context.TODO()

	svc := NewService(c, nil)

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
	err := svc.AddBucketReplicationPolicy(ctx, replicationIDMatchingSrcDest, nil)
	r.ErrorIs(err, dom.ErrInvalidArg, "repl to same storage and bucket is not allowed")

	err = svc.AddBucketReplicationPolicy(ctx, replicationIDDifferentBuckets, nil)
	r.NoError(err, "repl to same storage but different bucket is allowed")

	err = svc.AddBucketReplicationPolicy(ctx, replicationIDDifferentSrcDest, nil)
	r.NoError(err, "repl to different storage and different bucket is allowed")

	err = svc.AddBucketReplicationPolicy(ctx, replicationIDDifferentSrcDest, nil)
	r.Error(err, "already exists")

	// check replication lookup
	rps, err := svc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(user, srcBuck))
	r.NoError(err)
	r.EqualValues(stor, rps.FromStorage)
	r.Len(rps.Destinations, 2)
	r.Contains(rps.Destinations, entity.NewBucketReplicationPolicyDestination(stor, dstBuck), "custom bucket is in destination")
	r.Contains(rps.Destinations, entity.NewBucketReplicationPolicyDestination(stor2, dstBuck), "custom bucket is in destination")

	route, err := svc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(user, srcBuck))
	r.NoError(err)
	r.EqualValues(stor, route, "route to main for src bucket")
	_, err = svc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(user, dstBuck))
	r.ErrorIs(err, dom.ErrRoutingBlock, "for dst bucket routing is blocked")

	_, err = svc.GetReplicationPolicyInfo(ctx, replicationIDDifferentBuckets)
	r.NoError(err, "info created")

	list, err := svc.ListReplicationPolicyInfo(ctx)
	r.NoError(err)
	r.Len(list, 2)
	for id := range list {
		r.EqualValues(stor, id.FromStorage)
		r.EqualValues(user, id.User)
		r.EqualValues(srcBuck, id.FromBucket)
		r.NotNil(id.ToBucket)
		if id.ToStorage == stor {
			r.EqualValues(dstBuck, id.ToBucket)
		} else if id.ToStorage == stor2 {
			r.EqualValues(dstBuck, id.ToBucket)
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
	for id := range list {
		r.EqualValues(stor2, id.ToStorage)
	}

	ok, err = svc.IsReplicationPolicyExists(ctx, replicationIDDifferentBuckets)
	r.NoError(err)
	r.False(ok)

	route, err = svc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(user, dstBuck))
	r.NoError(err, "routing block removed")
	r.EqualValues(stor, route)
}
