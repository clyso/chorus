package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/testutil"
)

func TestBucketReplicationStatusStore(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	ctx := t.Context()

	store := NewBucketReplicationStatusStore(c)
	// same bucket policy
	policy1 := entity.BucketReplicationPolicy{
		User:        "u1",
		FromStorage: "s1",
		FromBucket:  "b1",
		ToStorage:   "s2",
		ToBucket:    "b1",
	}
	// get list add achieve delete

	// initial get - empty
	_, err := store.GetOp(ctx, policy1).Get()
	r.ErrorIs(err, dom.ErrNotFound)
	// initial list - empty
	_, err = store.List(ctx, policy1.User)
	r.ErrorIs(err, dom.ErrNotFound)

	// add
	status := entity.ReplicationStatus{
		AgentURL: "123",
	}
	statusCreated := time.Now()
	err = store.AddOp(ctx, policy1, status).Get()
	r.NoError(err)

	// get after add
	got, err := store.GetOp(ctx, policy1).Get()
	r.NoError(err)
	r.Equal(status.AgentURL, got.AgentURL)
	r.Nil(got.ArchivedAt)
	r.False(got.IsArchived)
	r.WithinDuration(got.CreatedAt, statusCreated, time.Second)

	// list after add
	list, err := store.List(ctx, policy1.User)
	r.NoError(err)
	r.Len(list, 1)
	r.Equal(status.AgentURL, list[policy1].AgentURL)
	r.Nil(list[policy1].ArchivedAt)
	r.False(list[policy1].IsArchived)
	r.WithinDuration(list[policy1].CreatedAt, statusCreated, time.Second)

	// archive
	statusArchived := time.Now()
	err = store.ArchieveOp(ctx, policy1).Get()
	r.NoError(err)
	// get after archive
	got, err = store.GetOp(ctx, policy1).Get()
	r.NoError(err)
	r.True(got.IsArchived)
	r.NotNil(got.ArchivedAt)
	r.WithinDuration(got.CreatedAt, statusCreated, time.Second)
	r.WithinDuration(*got.ArchivedAt, statusArchived, time.Second)
	r.Equal(status.AgentURL, got.AgentURL)
	// achievedAt after CreatedAt
	r.True(got.ArchivedAt.After(got.CreatedAt) || got.ArchivedAt.Equal(got.CreatedAt))
	// list after archive
	list, err = store.List(ctx, policy1.User)
	r.NoError(err)
	r.Len(list, 1)
	r.True(list[policy1].IsArchived)
	r.NotNil(list[policy1].ArchivedAt)
	r.WithinDuration(list[policy1].CreatedAt, statusCreated, time.Second)
	r.Equal(status.AgentURL, list[policy1].AgentURL)

	// add same user
	policy2 := entity.BucketReplicationPolicy{
		User:        "u1",
		FromStorage: "s1",
		FromBucket:  "b1",
		ToStorage:   "s3",
		ToBucket:    "b1",
	}
	// not exists yet
	_, err = store.GetOp(ctx, policy2).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	// add
	status2 := entity.ReplicationStatus{
		AgentURL: "",
	}
	err = store.AddOp(ctx, policy2, status2).Get()
	r.NoError(err)

	// get after add
	got, err = store.GetOp(ctx, policy2).Get()
	r.NoError(err)
	r.Equal(status2.AgentURL, got.AgentURL)
	r.Nil(got.ArchivedAt)
	r.False(got.IsArchived)
	r.False(got.CreatedAt.IsZero())
	// list after add
	list, err = store.List(ctx, policy1.User)
	r.NoError(err)
	r.Len(list, 2)
	r.True(list[policy1].IsArchived)
	r.False(list[policy2].IsArchived)
	r.Equal(status.AgentURL, list[policy1].AgentURL)
	r.Equal(status2.AgentURL, list[policy2].AgentURL)
	r.NotNil(list[policy1].ArchivedAt)
	r.Nil(list[policy2].ArchivedAt)

	// add different user
	policy3 := entity.BucketReplicationPolicy{
		User:        "u2",
		FromStorage: "s1",
		FromBucket:  "b2",
		ToStorage:   "s2",
		ToBucket:    "b2",
	}
	// not exists yet
	_, err = store.GetOp(ctx, policy3).Get()
	r.ErrorIs(err, dom.ErrNotFound)
	// list for u2 empty
	_, err = store.List(ctx, policy3.User)
	r.ErrorIs(err, dom.ErrNotFound)

	// add
	status3 := entity.ReplicationStatus{
		AgentURL: "asdf",
	}
	err = store.AddOp(ctx, policy3, status3).Get()
	r.NoError(err)
	// get after add
	got, err = store.GetOp(ctx, policy3).Get()
	r.NoError(err)
	r.Equal(status3.AgentURL, got.AgentURL)
	r.Nil(got.ArchivedAt)
	r.False(got.IsArchived)

	// list for u2 after add
	list, err = store.List(ctx, policy3.User)
	r.NoError(err)
	r.Len(list, 1)
	r.Equal(status3.AgentURL, list[policy3].AgentURL)
	r.Nil(list[policy3].ArchivedAt)
	r.False(list[policy3].IsArchived)

	// list for u1 still the same
	list, err = store.List(ctx, policy1.User)
	r.NoError(err)
	r.Len(list, 2)
	r.Equal(status.AgentURL, list[policy1].AgentURL)
	r.Equal(status2.AgentURL, list[policy2].AgentURL)

	// delete policy1
	err = store.DeleteOp(ctx, policy1).Get()
	r.NoError(err)
	_, err = store.GetOp(ctx, policy1).Get()
	r.ErrorIs(err, dom.ErrNotFound)
	// list for u1 only policy2 left
	list, err = store.List(ctx, policy1.User)
	r.NoError(err)
	r.Len(list, 1)
	r.Equal(status2.AgentURL, list[policy2].AgentURL)
	// list for u2 still the same
	list, err = store.List(ctx, policy3.User)
	r.NoError(err)
	r.Len(list, 1)
	r.Equal(status3.AgentURL, list[policy3].AgentURL)
	// delete policy1 again - idempotent
	err = store.DeleteOp(ctx, policy1).Get()
	r.NoError(err)

	// delete policy2
	err = store.DeleteOp(ctx, policy2).Get()
	r.NoError(err)
	_, err = store.GetOp(ctx, policy2).Get()
	r.ErrorIs(err, dom.ErrNotFound)
	// list for u1 empty now
	list, err = store.List(ctx, policy1.User)
	r.ErrorIs(err, dom.ErrNotFound)

	// list for u2 still the same
	list, err = store.List(ctx, policy3.User)
	r.NoError(err)
	r.Len(list, 1)
	r.Equal(status3.AgentURL, list[policy3].AgentURL)

	// delete policy3
	err = store.DeleteOp(ctx, policy3).Get()
	r.NoError(err)
	_, err = store.GetOp(ctx, policy3).Get()
	r.ErrorIs(err, dom.ErrNotFound)
	// list for u2 empty now
	list, err = store.List(ctx, policy3.User)
	r.ErrorIs(err, dom.ErrNotFound)

	// key should be fully removed - check redis directly
	keys, err := c.Keys(ctx, "*").Result()
	r.NoError(err)
	r.Len(keys, 0)

}

func TestUserReplicationStatusStore(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	ctx := t.Context()

	store := NewUserReplicationStatusStore(c)
	policy1 := entity.UserReplicationPolicy{
		User:        "u1",
		FromStorage: "s1",
		ToStorage:   "s2",
	}
	// get list add achieve delete

	// initial get - empty
	_, err := store.GetOp(ctx, policy1).Get()
	r.ErrorIs(err, dom.ErrNotFound)
	// initial list - empty
	_, err = store.List(ctx)
	r.ErrorIs(err, dom.ErrNotFound)

	// add
	status := entity.ReplicationStatus{
		AgentURL: "123",
	}
	statusCreated := time.Now()
	err = store.AddOp(ctx, policy1, status).Get()
	r.NoError(err)

	// get after add
	got, err := store.GetOp(ctx, policy1).Get()
	r.NoError(err)
	r.Equal(status.AgentURL, got.AgentURL)
	r.Nil(got.ArchivedAt)
	r.False(got.IsArchived)
	r.WithinDuration(got.CreatedAt, statusCreated, time.Second)

	// list after add
	list, err := store.List(ctx)
	r.NoError(err)
	r.Len(list, 1)
	r.Equal(status.AgentURL, list[policy1].AgentURL)
	r.Nil(list[policy1].ArchivedAt)
	r.False(list[policy1].IsArchived)
	r.WithinDuration(list[policy1].CreatedAt, statusCreated, time.Second)

	// archive
	statusArchived := time.Now()
	err = store.ArchieveOp(ctx, policy1).Get()
	r.NoError(err)
	// get after archive
	got, err = store.GetOp(ctx, policy1).Get()
	r.NoError(err)
	r.True(got.IsArchived)
	r.NotNil(got.ArchivedAt)
	r.WithinDuration(got.CreatedAt, statusCreated, time.Second)
	r.WithinDuration(*got.ArchivedAt, statusArchived, time.Second)
	r.Equal(status.AgentURL, got.AgentURL)
	// achievedAt after CreatedAt
	r.True(got.ArchivedAt.After(got.CreatedAt) || got.ArchivedAt.Equal(got.CreatedAt))
	// list after archive
	list, err = store.List(ctx)
	r.NoError(err)
	r.Len(list, 1)
	r.True(list[policy1].IsArchived)
	r.NotNil(list[policy1].ArchivedAt)
	r.WithinDuration(list[policy1].CreatedAt, statusCreated, time.Second)
	r.Equal(status.AgentURL, list[policy1].AgentURL)

	// add same user
	policy2 := entity.UserReplicationPolicy{
		User:        "u1",
		FromStorage: "s1",
		ToStorage:   "s3",
	}
	// not exists yet
	_, err = store.GetOp(ctx, policy2).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	// add
	status2 := entity.ReplicationStatus{
		AgentURL: "",
	}
	err = store.AddOp(ctx, policy2, status2).Get()
	r.NoError(err)

	// get after add
	got, err = store.GetOp(ctx, policy2).Get()
	r.NoError(err)
	r.Equal(status2.AgentURL, got.AgentURL)
	r.Nil(got.ArchivedAt)
	r.False(got.IsArchived)
	r.False(got.CreatedAt.IsZero())
	// list after add
	list, err = store.List(ctx)
	r.NoError(err)
	r.Len(list, 2)
	r.True(list[policy1].IsArchived)
	r.False(list[policy2].IsArchived)
	r.Equal(status.AgentURL, list[policy1].AgentURL)
	r.Equal(status2.AgentURL, list[policy2].AgentURL)
	r.NotNil(list[policy1].ArchivedAt)
	r.Nil(list[policy2].ArchivedAt)

	// add different user
	policy3 := entity.UserReplicationPolicy{
		User:        "u2",
		FromStorage: "s1",
		ToStorage:   "s2",
	}
	// not exists yet
	_, err = store.GetOp(ctx, policy3).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	// add
	status3 := entity.ReplicationStatus{
		AgentURL: "asdf",
	}
	err = store.AddOp(ctx, policy3, status3).Get()
	r.NoError(err)
	// get after add
	got, err = store.GetOp(ctx, policy3).Get()
	r.NoError(err)
	r.Equal(status3.AgentURL, got.AgentURL)
	r.Nil(got.ArchivedAt)
	r.False(got.IsArchived)

	// list for u2 after add
	list, err = store.List(ctx)
	r.NoError(err)
	r.Len(list, 3)
	r.Equal(status3.AgentURL, list[policy3].AgentURL)
	r.Nil(list[policy3].ArchivedAt)
	r.False(list[policy3].IsArchived)

	r.Equal(status.AgentURL, list[policy1].AgentURL)
	r.Equal(status2.AgentURL, list[policy2].AgentURL)

	// delete policy1
	err = store.DeleteOp(ctx, policy1).Get()
	r.NoError(err)
	_, err = store.GetOp(ctx, policy1).Get()
	r.ErrorIs(err, dom.ErrNotFound)
	// list
	list, err = store.List(ctx)
	r.NoError(err)
	r.Len(list, 2)
	r.Equal(status2.AgentURL, list[policy2].AgentURL)
	r.Equal(status3.AgentURL, list[policy3].AgentURL)
	// delete policy1 again - idempotent
	err = store.DeleteOp(ctx, policy1).Get()
	r.NoError(err)

	// delete policy2
	err = store.DeleteOp(ctx, policy2).Get()
	r.NoError(err)
	_, err = store.GetOp(ctx, policy2).Get()
	r.ErrorIs(err, dom.ErrNotFound)
	// list
	list, err = store.List(ctx)
	r.NoError(err)
	r.Len(list, 1)
	r.Equal(status3.AgentURL, list[policy3].AgentURL)

	// delete policy3
	err = store.DeleteOp(ctx, policy3).Get()
	r.NoError(err)
	_, err = store.GetOp(ctx, policy3).Get()
	r.ErrorIs(err, dom.ErrNotFound)
	// list  empty now
	list, err = store.List(ctx)
	r.ErrorIs(err, dom.ErrNotFound)

	// key should be fully removed - check redis directly
	keys, err := c.Keys(ctx, "*").Result()
	r.NoError(err)
	r.Len(keys, 0)

}
