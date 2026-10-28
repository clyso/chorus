package store

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/testutil"
)

func TestReplicationGenerationStore(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	ctx := t.Context()
	store := NewReplicationGenerationStore(c)

	userPolicy := entity.NewUserReplicationPolicy("user", "main", "follower")
	bucketPolicy := entity.NewBucketReplicationPolicy("user", "main", "bucket", "follower", "other-bucket")
	userID := entity.UniversalFromUserReplication(userPolicy)
	bucketID := entity.UniversalFromBucketReplication(bucketPolicy)

	for _, id := range []entity.UniversalReplicationID{userID, bucketID} {
		gen, err := store.Get(ctx, id)
		r.NoError(err)
		r.Zero(gen)
	}
	userGens, err := store.GetUserOp(ctx, "user").Get()
	r.NoError(err)
	r.Empty(userGens)
	bucketGens, err := store.GetBucketOp(ctx, bucketPolicy.LookupID()).Get()
	r.NoError(err)
	r.Empty(bucketGens)

	gen, err := store.IncrementOp(ctx, userID).Get()
	r.NoError(err)
	r.EqualValues(1, gen)
	gen, err = store.IncrementOp(ctx, userID).Get()
	r.NoError(err)
	r.EqualValues(2, gen)
	gen, err = store.IncrementOp(ctx, bucketID).Get()
	r.NoError(err)
	r.EqualValues(1, gen)

	gen, err = store.Get(ctx, userID)
	r.NoError(err)
	r.EqualValues(2, gen)
	gen, err = store.Get(ctx, bucketID)
	r.NoError(err)
	r.EqualValues(1, gen)

	userGens, err = store.GetUserOp(ctx, "user").Get()
	r.NoError(err)
	r.Equal(map[string]int64{"follower": 2}, userGens)
	bucketGens, err = store.GetBucketOp(ctx, bucketPolicy.LookupID()).Get()
	r.NoError(err)
	r.Equal(map[entity.BucketReplicationPolicyDestination]int64{bucketPolicy.Destination(): 1}, bucketGens)

	userGens, err = store.GetUserOp(ctx, "other-user").Get()
	r.NoError(err)
	r.Empty(userGens)
	bucketGens, err = store.GetBucketOp(ctx, entity.NewBucketReplicationPolicyID("user", "other-bucket")).Get()
	r.NoError(err)
	r.Empty(bucketGens)
}
