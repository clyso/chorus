package store

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/testutil"
)

func TestNewUserReplicationPolicyStore(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	ctx := t.Context()

	user := "test_user"
	fromStorage := "storage_0"
	toStorage := "storage_1"
	store := NewUserReplicationPolicyStore(c)

	//not found
	_, err := store.GetOp(ctx, user).Get()
	r.ErrorIs(err, dom.ErrNotFound)
	_, err = store.Get(ctx, user)
	r.ErrorIs(err, dom.ErrNotFound)

	//add
	err = store.AddOp(ctx, entity.NewUserReplicationPolicy(user, fromStorage, toStorage)).Get()
	r.NoError(err)

	// now exists
	got, err := store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.Len(got, 1)
	r.Equal(user, got[0].User)
	r.Equal(fromStorage, got[0].FromStorage)
	r.Equal(toStorage, got[0].ToStorage)
	got2, err := store.Get(ctx, user)
	r.NoError(err)
	r.Len(got2, 1)
	r.EqualValues(got[0], got2[0])

	// add idempotent
	err = store.AddOp(ctx, entity.NewUserReplicationPolicy(user, fromStorage, toStorage)).Get()
	r.NoError(err)

	got, err = store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.Len(got, 1)
	r.Equal(user, got[0].User)
	r.Equal(fromStorage, got[0].FromStorage)
	r.Equal(toStorage, got[0].ToStorage)

	// adding a replication with different toStorage should work
	err = store.AddOp(ctx, entity.NewUserReplicationPolicy(user, fromStorage, "storage_2")).Get()
	r.NoError(err)

	got, err = store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.Len(got, 2)
	r.Equal(user, got[0].User)
	r.Equal(fromStorage, got[0].FromStorage)
	r.Equal(toStorage, got[0].ToStorage)
	r.Equal(user, got[1].User)
	r.Equal(fromStorage, got[1].FromStorage)
	r.Equal("storage_2", got[1].ToStorage)

	// add replication to a different user
	user2 := "test_user_2"
	user2Rep := entity.NewUserReplicationPolicy(user2, fromStorage, toStorage)
	err = store.AddOp(ctx, user2Rep).Get()
	r.NoError(err)

	// original user is unchanged
	got, err = store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.Len(got, 2)
	r.Equal(user, got[0].User)
	r.Equal(fromStorage, got[0].FromStorage)
	r.Equal(toStorage, got[0].ToStorage)
	r.Equal(user, got[1].User)
	r.Equal(fromStorage, got[1].FromStorage)
	r.Equal("storage_2", got[1].ToStorage)

	// new user has its replication
	got, err = store.GetOp(ctx, user2).Get()
	r.NoError(err)
	r.Len(got, 1)
	r.Equal(user2, got[0].User)
	r.Equal(fromStorage, got[0].FromStorage)
	r.Equal(toStorage, got[0].ToStorage)

	//remove new user replication
	err = store.RemoveOp(ctx, user2Rep).Get()
	r.NoError(err)
	_, err = store.GetOp(ctx, user2).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	// original user is unchanged
	got, err = store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.Len(got, 2)
	r.Equal(user, got[0].User)
	r.Equal(fromStorage, got[0].FromStorage)
	r.Equal(toStorage, got[0].ToStorage)
	r.Equal(user, got[1].User)
	r.Equal(fromStorage, got[1].FromStorage)
	r.Equal("storage_2", got[1].ToStorage)

	// remove original user second replication
	err = store.RemoveOp(ctx, entity.NewUserReplicationPolicy(user, fromStorage, "storage_2")).Get()
	r.NoError(err)

	got, err = store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.Len(got, 1)
	r.Equal(user, got[0].User)
	r.Equal(fromStorage, got[0].FromStorage)
	r.Equal(toStorage, got[0].ToStorage)

	// remove original user last replication
	err = store.RemoveOp(ctx, entity.NewUserReplicationPolicy(user, fromStorage, toStorage)).Get()
	r.NoError(err)
	_, err = store.GetOp(ctx, user).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	// remove idempotent
	err = store.RemoveOp(ctx, entity.NewUserReplicationPolicy(user, fromStorage, toStorage)).Get()
	r.NoError(err)
	_, err = store.GetOp(ctx, user).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	// check no keys in redis left:
	keys, err := c.Keys(ctx, "*").Result()
	r.NoError(err)
	r.Len(keys, 0)
}

func TestNewUserReplicationPolicyStore_Remove(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	ctx := t.Context()
	store := NewUserReplicationPolicyStore(c)

	policyU1S1_1 := entity.NewUserReplicationPolicy("user1", "s1", "s2")
	policyU1S1_2 := entity.NewUserReplicationPolicy("user1", "s1", "s3")

	policyU2S1_1 := entity.NewUserReplicationPolicy("user2", "s1", "s2")
	policyU2S1_2 := entity.NewUserReplicationPolicy("user2", "s1", "s3")

	policyU3S2_1 := entity.NewUserReplicationPolicy("user3", "s2", "s1")
	policyU3S2_2 := entity.NewUserReplicationPolicy("user3", "s2", "s3")

	allPolicies := []entity.UserReplicationPolicy{
		policyU1S1_1, policyU1S1_2,
		policyU2S1_1, policyU2S1_2,
		policyU3S2_1, policyU3S2_2,
	}
	// add all policies
	for _, p := range allPolicies {
		err := store.AddOp(ctx, p).Get()
		r.NoError(err)
	}

	// check that polices have the same ID
	u1s1Policies := []entity.UserReplicationPolicy{policyU1S1_1, policyU1S1_2}
	dest := u1s1Policies[0].LookupID()
	for _, p := range u1s1Policies {
		r.EqualValues(dest, p.LookupID())
	}

	u1s2Policies := []entity.UserReplicationPolicy{policyU2S1_1, policyU2S1_2}
	dest = u1s2Policies[0].LookupID()
	for _, p := range u1s2Policies {
		r.EqualValues(dest, p.LookupID())
	}

	u2s1Policies := []entity.UserReplicationPolicy{policyU3S2_1, policyU3S2_2}
	dest = u2s1Policies[0].LookupID()
	for _, p := range u2s1Policies {
		r.EqualValues(dest, p.LookupID())
	}

	groupedPolicies := [][]entity.UserReplicationPolicy{
		u1s1Policies,
		u1s2Policies,
		u2s1Policies,
	}
	for i := range allPolicies {
		for _, sameUser := range groupedPolicies {
			if len(sameUser) == 0 {
				continue
			}
			dest := sameUser[0].LookupID()
			got, err := store.GetOp(ctx, dest).Get()
			r.NoError(err)
			r.Len(got, len(sameUser))
			r.ElementsMatch(sameUser, got)

		}
		// remove one policy
		groupID := i % len(groupedPolicies)
		// remove last
		policyToRemove := groupedPolicies[groupID][len(groupedPolicies[groupID])-1]
		groupedPolicies[groupID] = groupedPolicies[groupID][:len(groupedPolicies[groupID])-1]
		err := store.RemoveOp(ctx, policyToRemove).Get()
		r.NoError(err)
	}

	// no policies:
	for _, p := range allPolicies {
		_, err := store.GetOp(ctx, p.LookupID()).Get()
		r.ErrorIs(err, dom.ErrNotFound)
	}
	// check no keys in redis left:
	keys, err := c.Keys(ctx, "*").Result()
	r.NoError(err)
	r.Len(keys, 0)
}

func TestNewUserReplicationPolicyStore_WithExec(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	ctx := t.Context()

	user := "test_user"
	fromStorage := "storage_0"
	toStorage := "storage_1"
	store := NewUserReplicationPolicyStore(c)
	tx := store.TxExecutor()
	storeTx := store.WithExecutor(tx)

	// create replication
	err := store.AddOp(ctx, entity.NewUserReplicationPolicy(user, fromStorage, toStorage)).Get()
	r.NoError(err)

	getExistingResult := storeTx.GetOp(ctx, user)
	getNonExistingResult := storeTx.GetOp(ctx, "non_existing_user")
	addNonExistingResult := storeTx.AddOp(ctx, entity.NewUserReplicationPolicy("non_existing_user", "s0", "s1"))
	getCreatedResult := storeTx.GetOp(ctx, "non_existing_user")

	// commit
	_ = tx.Exec(ctx)
	// now all results are available
	existing, err := getExistingResult.Get()
	r.NoError(err)
	r.Len(existing, 1)
	r.Equal(user, existing[0].User)
	r.Equal(fromStorage, existing[0].FromStorage)
	r.Equal(toStorage, existing[0].ToStorage)
	_, err = getNonExistingResult.Get()
	r.ErrorIs(err, dom.ErrNotFound)
	err = addNonExistingResult.Get()
	r.NoError(err)
	created, err := getCreatedResult.Get()
	r.NoError(err)
	r.Len(created, 1)
	r.Equal("non_existing_user", created[0].User)
	r.Equal("s0", created[0].FromStorage)
	r.Equal("s1", created[0].ToStorage)

}

func TestNewBucketReplicationPolicyStore(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	store := NewBucketReplicationPolicyStore(c)
	ctx := t.Context()

	s1, s2 := "storage_1", "storage_2"
	user1 := "user1"
	bucket1 := "bucket1"
	policy1 := entity.NewBucketRepliationPolicy(user1, s1, bucket1, s2, bucket1)
	r.Equal(user1, policy1.User)
	r.Equal(bucket1, policy1.FromBucket)
	r.Equal(s1, policy1.FromStorage)
	r.Equal(s2, policy1.ToStorage)
	r.Equal(bucket1, policy1.ToBucket)

	// getting a non-existing policy fails
	_, err := store.GetOp(ctx, entity.BucketReplicationPolicyID{User: "asdf", FromBucket: "qwerty"}).Get()
	r.ErrorIs(err, dom.ErrNotFound)
	_, err = store.Get(ctx, entity.BucketReplicationPolicyID{User: "asdf", FromBucket: "qwerty"})
	r.ErrorIs(err, dom.ErrNotFound)

	existsForUser, err := store.ExistsForUser(ctx, user1)
	r.NoError(err)
	r.False(existsForUser)

	destInUse, err := store.IsDestinationInUse(ctx, policy1)
	r.NoError(err)
	r.False(destInUse)

	// add policy1
	err = store.AddOp(ctx, policy1).Get()
	r.NoError(err)

	// get policy1
	got, err := store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user1, FromBucket: bucket1}).Get()
	r.NoError(err)
	r.Len(got, 1)
	r.EqualValues(policy1, got[0])
	got2, err := store.Get(ctx, entity.BucketReplicationPolicyID{User: user1, FromBucket: bucket1})
	r.NoError(err)
	r.Len(got2, 1)
	r.EqualValues(policy1, got2[0])

	existsForUser, err = store.ExistsForUser(ctx, user1)
	r.NoError(err)
	r.True(existsForUser)

	destInUse, err = store.IsDestinationInUse(ctx, policy1)
	r.NoError(err)
	r.True(destInUse)

	// get with wrong user fails
	_, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: "user2", FromBucket: bucket1}).Get()
	r.ErrorIs(err, dom.ErrNotFound)
	// get with wrong bucket fails
	_, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user1, FromBucket: "bucket2"}).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	// adding the same policy again should be fine
	err = store.AddOp(ctx, policy1).Get()
	r.NoError(err)
	// get policy1
	got, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user1, FromBucket: bucket1}).Get()
	r.NoError(err)
	r.Len(got, 1)
	r.EqualValues(policy1, got[0])

	// add a second policy for the same user/bucket but different toStorage
	s3 := "storage_3"
	policy2 := entity.NewBucketRepliationPolicy(user1, s1, bucket1, s3, bucket1)

	destInUse, err = store.IsDestinationInUse(ctx, policy2)
	r.NoError(err)
	r.False(destInUse)

	err = store.AddOp(ctx, policy2).Get()
	r.NoError(err)

	// get both policies
	got, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user1, FromBucket: bucket1}).Get()
	r.NoError(err)
	r.Len(got, 2)
	r.EqualValues(policy1, got[0])
	r.EqualValues(policy2, got[1])

	existsForUser, err = store.ExistsForUser(ctx, user1)
	r.NoError(err)
	r.True(existsForUser)

	destInUse, err = store.IsDestinationInUse(ctx, policy1)
	r.NoError(err)
	r.True(destInUse)

	destInUse, err = store.IsDestinationInUse(ctx, policy2)
	r.NoError(err)
	r.True(destInUse)

	// add a policy for the same storage but different toBucket
	bucket2 := "bucket2"
	policy3 := entity.NewBucketRepliationPolicy(user1, s1, bucket1, s2, bucket2)

	destInUse, err = store.IsDestinationInUse(ctx, policy3)
	r.NoError(err)
	r.False(destInUse)

	err = store.AddOp(ctx, policy3).Get()
	r.NoError(err)
	// get all three policies
	got, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user1, FromBucket: bucket1}).Get()
	r.NoError(err)
	r.Len(got, 3)
	r.EqualValues(policy1, got[0])
	r.EqualValues(policy3, got[1]) // order by toStorage, then toBucket
	r.EqualValues(policy2, got[2])

	existsForUser, err = store.ExistsForUser(ctx, user1)
	r.NoError(err)
	r.True(existsForUser)

	destInUse, err = store.IsDestinationInUse(ctx, policy1)
	r.NoError(err)
	r.True(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy2)
	r.NoError(err)
	r.True(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy3)
	r.NoError(err)
	r.True(destInUse)

	// add a policy for a different user
	// first check that no poicy for user2 exists
	user2 := "user2"
	existsForUser, err = store.ExistsForUser(ctx, user2)
	r.NoError(err)
	r.False(existsForUser)

	_, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user2, FromBucket: bucket1}).Get()
	r.ErrorIs(err, dom.ErrNotFound)
	// add policy for user2
	policy4 := entity.NewBucketRepliationPolicy(user2, s1, bucket1, s2, bucket1)
	destInUse, err = store.IsDestinationInUse(ctx, policy4)
	r.NoError(err)
	r.False(destInUse)
	err = store.AddOp(ctx, policy4).Get()
	r.NoError(err)
	// 3 policies for user1 has not changed
	got, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user1, FromBucket: bucket1}).Get()
	r.NoError(err)
	r.Len(got, 3)
	r.EqualValues(policy1, got[0])
	r.EqualValues(policy3, got[1]) // order by toStorage, then toBucket
	r.EqualValues(policy2, got[2])
	// user2 has its policy
	got, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user2, FromBucket: bucket1}).Get()
	r.NoError(err)
	r.Len(got, 1)
	r.EqualValues(policy4, got[0])

	existsForUser, err = store.ExistsForUser(ctx, user1)
	r.NoError(err)
	r.True(existsForUser)
	existsForUser, err = store.ExistsForUser(ctx, user2)
	r.NoError(err)
	r.True(existsForUser)

	destInUse, err = store.IsDestinationInUse(ctx, policy1)
	r.NoError(err)
	r.True(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy2)
	r.NoError(err)
	r.True(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy3)
	r.NoError(err)
	r.True(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy4)
	r.NoError(err)
	r.True(destInUse)

	// add a policy for a different fromBucket for user1

	// first check that no poicy for bucket2 exists
	_, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user1, FromBucket: bucket2}).Get()
	r.ErrorIs(err, dom.ErrNotFound)
	// add policy for bucket2
	policy5 := entity.NewBucketRepliationPolicy(user1, s1, bucket2, s2, bucket2)
	err = store.AddOp(ctx, policy5).Get()
	r.NoError(err)

	// 3 policies for user1/bucket1 has not changed
	got, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user1, FromBucket: bucket1}).Get()
	r.NoError(err)
	r.Len(got, 3)
	r.EqualValues(policy1, got[0])
	r.EqualValues(policy3, got[1]) // order by toStorage, then toBucket
	r.EqualValues(policy2, got[2])

	existsForUser, err = store.ExistsForUser(ctx, user1)
	r.NoError(err)
	r.True(existsForUser)
	existsForUser, err = store.ExistsForUser(ctx, user2)
	r.NoError(err)
	r.True(existsForUser)

	destInUse, err = store.IsDestinationInUse(ctx, policy1)
	r.NoError(err)
	r.True(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy2)
	r.NoError(err)
	r.True(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy3)
	r.NoError(err)
	r.True(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy4)
	r.NoError(err)
	r.True(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy5)
	r.NoError(err)
	r.True(destInUse)

	// policies for user2 also unchanged
	got, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user2, FromBucket: bucket1}).Get()
	r.NoError(err)
	r.Len(got, 1)
	r.EqualValues(policy4, got[0])

	// user1/bucket2 has its policy
	got, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user1, FromBucket: bucket2}).Get()
	r.NoError(err)
	r.Len(got, 1)
	r.EqualValues(policy5, got[0])

	// remove policy1
	err = store.RemoveOp(ctx, policy1).Get()
	r.NoError(err)

	// user1/bucket1 has 2 policies now
	got, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user1, FromBucket: bucket1}).Get()
	r.NoError(err)
	r.Len(got, 2)
	r.EqualValues(policy3, got[0]) // order by toStorage, then toBucket
	r.EqualValues(policy2, got[1])

	// user2 unchanged
	got, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user2, FromBucket: bucket1}).Get()
	r.NoError(err)
	r.Len(got, 1)
	r.EqualValues(policy4, got[0])
	// exists for user
	existsForUser, err = store.ExistsForUser(ctx, user1)
	r.NoError(err)
	r.True(existsForUser)
	existsForUser, err = store.ExistsForUser(ctx, user2)
	r.NoError(err)
	r.True(existsForUser)
	// destinations still in use except policy1
	destInUse, err = store.IsDestinationInUse(ctx, policy1)
	r.NoError(err)
	r.False(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy2)
	r.NoError(err)
	r.True(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy3)
	r.NoError(err)
	r.True(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy4)
	r.NoError(err)
	r.True(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy5)
	r.NoError(err)
	r.True(destInUse)

	// remove policy3
	err = store.RemoveOp(ctx, policy3).Get()
	r.NoError(err)

	// user1/bucket1 has 1 policy now
	got, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user1, FromBucket: bucket1}).Get()
	r.NoError(err)
	r.Len(got, 1)
	r.EqualValues(policy2, got[0])

	// user2 unchanged
	got, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user2, FromBucket: bucket1}).Get()
	r.NoError(err)
	r.Len(got, 1)
	r.EqualValues(policy4, got[0])
	// exists for user
	existsForUser, err = store.ExistsForUser(ctx, user1)
	r.NoError(err)
	r.True(existsForUser)
	existsForUser, err = store.ExistsForUser(ctx, user2)
	r.NoError(err)
	r.True(existsForUser)
	// destinations still in use except policy1 and policy3
	destInUse, err = store.IsDestinationInUse(ctx, policy1)
	r.NoError(err)
	r.False(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy2)
	r.NoError(err)
	r.True(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy3)
	r.NoError(err)
	r.False(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy4)
	r.NoError(err)
	r.True(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy5)
	r.NoError(err)
	r.False(destInUse)

	// remove policy4
	err = store.RemoveOp(ctx, policy4).Get()
	r.NoError(err)
	// user2 has no policies now
	_, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user2, FromBucket: bucket1}).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	// user1/bucket1 unchanged
	got, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user1, FromBucket: bucket1}).Get()
	r.NoError(err)
	r.Len(got, 1)
	r.EqualValues(policy2, got[0])
	// exists only for user1
	existsForUser, err = store.ExistsForUser(ctx, user1)
	r.NoError(err)
	r.True(existsForUser)
	existsForUser, err = store.ExistsForUser(ctx, user2)
	r.NoError(err)
	r.False(existsForUser)
	// destinations still in use except policy1 and policy3 and policy4
	destInUse, err = store.IsDestinationInUse(ctx, policy1)
	r.NoError(err)
	r.False(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy2)
	r.NoError(err)
	r.True(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy3)
	r.NoError(err)
	r.False(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy4)
	r.NoError(err)
	r.False(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy5)
	r.NoError(err)
	r.False(destInUse)

	// remove policy2
	err = store.RemoveOp(ctx, policy2).Get()
	r.NoError(err)
	// user1/bucket1 has no policies now
	got, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user1, FromBucket: bucket1}).Get()
	r.ErrorIs(err, dom.ErrNotFound)
	// user1/bucket2 unchanged
	_, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user2, FromBucket: bucket1}).Get()
	r.ErrorIs(err, dom.ErrNotFound, got)

	// remove idempotent
	err = store.RemoveOp(ctx, policy2).Get()
	r.NoError(err)
	_, err = store.GetOp(ctx, entity.BucketReplicationPolicyID{User: user1, FromBucket: bucket1}).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	got, err = store.GetOp(ctx, policy5.LookupID()).Get()
	r.NoError(err)
	r.Len(got, 1)
	r.EqualValues(policy5, got[0])

	// exists only for user1
	existsForUser, err = store.ExistsForUser(ctx, user1)
	r.NoError(err)
	r.True(existsForUser)
	existsForUser, err = store.ExistsForUser(ctx, user2)
	r.NoError(err)
	r.False(existsForUser)
	// destinations still in use except policy1 and policy3 and policy4
	destInUse, err = store.IsDestinationInUse(ctx, policy1)
	r.NoError(err)
	r.False(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy2)
	r.NoError(err)
	r.False(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy3)
	r.NoError(err)
	r.False(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy4)
	r.NoError(err)
	r.False(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy5)
	r.NoError(err)
	r.False(destInUse)

	// remove last policy
	err = store.RemoveOp(ctx, policy5).Get()
	r.NoError(err)
	_, err = store.GetOp(ctx, policy5.LookupID()).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	existsForUser, err = store.ExistsForUser(ctx, user1)
	r.NoError(err)
	r.False(existsForUser)
	existsForUser, err = store.ExistsForUser(ctx, user2)
	r.NoError(err)
	r.False(existsForUser)
	// destinations still in use except policy1 and policy3 and policy4
	destInUse, err = store.IsDestinationInUse(ctx, policy1)
	r.NoError(err)
	r.False(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy2)
	r.NoError(err)
	r.False(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy3)
	r.NoError(err)
	r.False(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy4)
	r.NoError(err)
	r.False(destInUse)
	destInUse, err = store.IsDestinationInUse(ctx, policy5)
	r.NoError(err)
	r.False(destInUse)

	// check no keys in redis left:
	keys, err := c.Keys(ctx, "*").Result()
	r.NoError(err)
	r.Len(keys, 0)
}

func TestNewBucketReplicationPolicyStoreRemove(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	ctx := t.Context()

	store := NewBucketReplicationPolicyStore(c)

	policyU1B1_1 := entity.NewBucketRepliationPolicy("u1", "s1", "b1", "s2", "b1") //same bucket
	policyU1B1_2 := entity.NewBucketRepliationPolicy("u1", "s1", "b1", "s1", "b2") //same storage, bucket different
	policyU1B1_3 := entity.NewBucketRepliationPolicy("u1", "s1", "b1", "s3", "b2") //storage and bucket different

	policyU2B1_1 := entity.NewBucketRepliationPolicy("u2", "s1", "b1", "s2", "b1") //same bucket
	policyU2B1_2 := entity.NewBucketRepliationPolicy("u2", "s1", "b1", "s1", "b2") //same storage, bucket different
	policyU2B1_3 := entity.NewBucketRepliationPolicy("u2", "s1", "b1", "s3", "b2") //storage and bucket different

	policyU1B2_1 := entity.NewBucketRepliationPolicy("u1", "s1", "b2", "s2", "b1") //same bucket
	policyU1B2_2 := entity.NewBucketRepliationPolicy("u1", "s1", "b2", "s1", "b1") //same storage, bucket different
	policyU1B2_3 := entity.NewBucketRepliationPolicy("u1", "s1", "b2", "s3", "b2") //storage and bucket different

	policyU2B2_1 := entity.NewBucketRepliationPolicy("u2", "s2", "b2", "s2", "b1") //same bucket
	policyU2B2_2 := entity.NewBucketRepliationPolicy("u2", "s2", "b2", "s1", "b2") //same storage, bucket different
	policyU2B2_3 := entity.NewBucketRepliationPolicy("u2", "s2", "b2", "s3", "b2") //storage and bucket different

	// add all policies
	allPolicies := []entity.BucketReplicationPolicy{
		policyU1B1_1, policyU1B1_2, policyU1B1_3,
		policyU2B1_1, policyU2B1_2, policyU2B1_3,
		policyU1B2_1, policyU1B2_2, policyU1B2_3,
		policyU2B2_1, policyU2B2_2, policyU2B2_3,
	}
	for _, p := range allPolicies {
		err := store.AddOp(ctx, p).Get()
		r.NoError(err)
	}
	// check that polices have the same ID
	u1b1Policies := []entity.BucketReplicationPolicy{policyU1B1_1, policyU1B1_2, policyU1B1_3}
	dest := u1b1Policies[0].LookupID()
	for _, p := range u1b1Policies {
		r.EqualValues(dest, p.LookupID())
	}

	u2b1Policies := []entity.BucketReplicationPolicy{policyU2B1_1, policyU2B1_2, policyU2B1_3}
	dest = u2b1Policies[0].LookupID()
	for _, p := range u2b1Policies {
		r.EqualValues(dest, p.LookupID())
	}

	u1b2Policies := []entity.BucketReplicationPolicy{policyU1B2_1, policyU1B2_2, policyU1B2_3}
	dest = u1b2Policies[0].LookupID()
	for _, p := range u1b2Policies {
		r.EqualValues(dest, p.LookupID())
	}
	u2b2Policies := []entity.BucketReplicationPolicy{policyU2B2_1, policyU2B2_2, policyU2B2_3}
	dest = u2b2Policies[0].LookupID()
	for _, p := range u2b2Policies {
		r.EqualValues(dest, p.LookupID())
	}
	groupedPolicies := [][]entity.BucketReplicationPolicy{
		u1b1Policies,
		u2b1Policies,
		u1b2Policies,
		u2b2Policies,
	}
	for i := range allPolicies {
		for _, sameSrc := range groupedPolicies {
			if len(sameSrc) == 0 {
				continue
			}
			dest := sameSrc[0].LookupID()
			got, err := store.GetOp(ctx, dest).Get()
			r.NoError(err)
			r.Len(got, len(sameSrc))
			r.ElementsMatch(sameSrc, got)

		}
		// remove one policy
		groupID := i % len(groupedPolicies)
		// remove last
		policyToRemove := groupedPolicies[groupID][len(groupedPolicies[groupID])-1]
		groupedPolicies[groupID] = groupedPolicies[groupID][:len(groupedPolicies[groupID])-1]
		err := store.RemoveOp(ctx, policyToRemove).Get()
		r.NoError(err)
	}
	// no policies:
	for _, p := range allPolicies {
		_, err := store.GetOp(ctx, p.LookupID()).Get()
		r.ErrorIs(err, dom.ErrNotFound)
	}
	// check no keys in redis left:
	keys, err := c.Keys(ctx, "*").Result()
	r.NoError(err)
	r.Len(keys, 0)
}

func TestNewBucketReplicationPolicyStore_WithExec(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	ctx := t.Context()

	store := NewBucketReplicationPolicyStore(c)
	tx := store.TxExecutor()
	storeTx := store.WithExecutor(tx)

	// create replication
	policy := entity.NewBucketRepliationPolicy("user1", "s1", "b1", "s2", "b2")
	err := store.AddOp(ctx, policy).Get()
	r.NoError(err)

	getExistingResult := storeTx.GetOp(ctx, entity.BucketReplicationPolicyID{User: "user1", FromBucket: "b1"})
	getNonExistingResult := storeTx.GetOp(ctx, entity.BucketReplicationPolicyID{User: "non_exist_user", FromBucket: "b1"})
	policy2 := entity.NewBucketRepliationPolicy("non_exist_user", "s1", "b1", "s2", "b2")
	addNonExistingResult := storeTx.AddOp(ctx, policy2)
	getCreatedResult := storeTx.GetOp(ctx, entity.BucketReplicationPolicyID{User: "non_exist_user", FromBucket: "b1"})

	// commit
	_ = tx.Exec(ctx)
	// now all results are available
	existing, err := getExistingResult.Get()
	r.NoError(err)
	r.Len(existing, 1)
	r.EqualValues(policy, existing[0])
	_, err = getNonExistingResult.Get()
	r.ErrorIs(err, dom.ErrNotFound)
	err = addNonExistingResult.Get()
	r.NoError(err)
	created, err := getCreatedResult.Get()
	r.NoError(err)
	r.Len(created, 1)
	r.EqualValues(policy2, created[0])
}
