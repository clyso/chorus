package store

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/testutil"
)

func TestNewUserRoutingStore(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	ctx := t.Context()

	user := "test_user"
	toStorage := "storage_1"
	store := NewUserRoutingStore(c)

	_, err := store.GetOp(ctx, user).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	err = store.SetOp(ctx, user, toStorage).Get()
	r.NoError(err)

	got, err := store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.Equal(toStorage, got)

	// set idempotent
	err = store.SetOp(ctx, user, toStorage).Get()
	r.NoError(err)

	got, err = store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.Equal(toStorage, got)

	// overwrite with different storage
	newStorage := "storage_2"
	err = store.SetOp(ctx, user, newStorage).Get()
	r.NoError(err)
	got, err = store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.Equal(newStorage, got)

	// block user
	err = store.BlockOp(ctx, user).Get()
	r.NoError(err)

	_, err = store.GetOp(ctx, user).Get()
	r.ErrorIs(err, dom.ErrRoutingBlock)

	// block idempotent
	err = store.BlockOp(ctx, user).Get()
	r.NoError(err)
	_, err = store.GetOp(ctx, user).Get()
	r.ErrorIs(err, dom.ErrRoutingBlock)

	// unblock user
	err = store.UnblockOp(ctx, user).Get()
	r.NoError(err)
	got, err = store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.Equal(newStorage, got)

	// unblock idempotent
	err = store.UnblockOp(ctx, user).Get()
	r.NoError(err)
	got, err = store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.Equal(newStorage, got)

	// different user still not exists
	_, err = store.GetOp(ctx, "asdf").Get()
	r.ErrorIs(err, dom.ErrNotFound)

	// add route for different user
	err = store.SetOp(ctx, "asdf", "storage_asdf").Get()
	r.NoError(err)

	got, err = store.GetOp(ctx, "asdf").Get()
	r.NoError(err)
	r.Equal("storage_asdf", got)

	// original user still the same
	got, err = store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.Equal(newStorage, got)

	// block different user
	err = store.BlockOp(ctx, "asdf").Get()
	r.NoError(err)
	_, err = store.GetOp(ctx, "asdf").Get()
	r.ErrorIs(err, dom.ErrRoutingBlock)

	// original user still the same
	got, err = store.GetOp(ctx, user).Get()
	r.NoError(err)
	r.Equal(newStorage, got)
}

func TestNewUserRoutingStore_WithExec(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	ctx := t.Context()

	user := "test_user"
	toStorage := "storage_1"
	store := NewUserRoutingStore(c)
	tx := store.TxExecutor()
	storeTx := NewUserRoutingStore(c).WithExecutor(tx)

	// empty
	_, err := store.GetOp(ctx, user).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	getNotExists := storeTx.GetOp(ctx, user)
	setOp := storeTx.SetOp(ctx, user, toStorage)
	getExists := storeTx.GetOp(ctx, user)
	blockOp := storeTx.BlockOp(ctx, user)
	getBlocked := storeTx.GetOp(ctx, user)

	// tx not executed yet - so no results
	getVal, err := getNotExists.Get()
	r.NoError(err)
	r.Equal("", getVal)
	isSetErr := setOp.Get()
	r.NoError(isSetErr)

	getVal, err = getExists.Get()
	r.NoError(err)
	r.Equal("", getVal)
	isBlockErr := blockOp.Get()
	r.NoError(isBlockErr)
	getVal, err = getBlocked.Get()
	r.NoError(err)
	r.Equal("", getVal)

	// commit tx
	_ = tx.Exec(ctx)
	// now results should be available

	getVal, err = getNotExists.Get()
	r.ErrorIs(err, dom.ErrNotFound)
	isSetErr = setOp.Get()
	r.NoError(isSetErr)
	getVal, err = getExists.Get()
	r.NoError(err)
	r.Equal(toStorage, getVal)
	blockErr := blockOp.Get()
	r.NoError(blockErr)
	_, err = getBlocked.Get()
	r.ErrorIs(err, dom.ErrRoutingBlock)

}

func TestNewBucketRoutingStore(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	ctx := t.Context()

	user := "test_user"
	bucket := "test_bucket"
	id := entity.NewBucketRoutingPolicyID(user, bucket)
	toStorage := "storage_1"
	store := NewBucketRoutingStore(c)

	_, err := store.GetOp(ctx, id).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	err = store.SetOp(ctx, id, toStorage).Get()
	r.NoError(err)
	got, err := store.GetOp(ctx, id).Get()
	r.NoError(err)
	r.Equal(toStorage, got)

	// set idempotent
	err = store.SetOp(ctx, id, toStorage).Get()
	r.NoError(err)
	got, err = store.GetOp(ctx, id).Get()
	r.NoError(err)
	r.Equal(toStorage, got)

	// overwrite with different storage
	updatedToStorage := "storage_2"
	err = store.SetOp(ctx, id, updatedToStorage).Get()
	r.NoError(err)
	got, err = store.GetOp(ctx, id).Get()
	r.NoError(err)
	r.Equal(updatedToStorage, got)

	// block bucket
	err = store.BlockOp(ctx, id).Get()
	r.NoError(err)
	_, err = store.GetOp(ctx, id).Get()
	r.ErrorIs(err, dom.ErrRoutingBlock)
	// block idempotent
	err = store.BlockOp(ctx, id).Get()
	r.NoError(err)
	_, err = store.GetOp(ctx, id).Get()
	r.ErrorIs(err, dom.ErrRoutingBlock)

	// unblock bucket
	err = store.UnblockOp(ctx, id).Get()
	r.NoError(err)
	got, err = store.GetOp(ctx, id).Get()
	r.NoError(err)
	r.Equal(updatedToStorage, got)
	// unblock idempotent
	err = store.UnblockOp(ctx, id).Get()
	r.NoError(err)
	got, err = store.GetOp(ctx, id).Get()
	r.NoError(err)
	r.Equal(updatedToStorage, got)

	// different bucket still not exists
	otherID := entity.NewBucketRoutingPolicyID(user, "asdf")
	otherStorage := "storage_asdf"
	_, err = store.GetOp(ctx, otherID).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	// add route for different bucket
	err = store.SetOp(ctx, otherID, otherStorage).Get()
	r.NoError(err)
	got, err = store.GetOp(ctx, otherID).Get()
	r.NoError(err)
	r.Equal(otherStorage, got)

	// original bucket still the same
	got, err = store.GetOp(ctx, id).Get()
	r.NoError(err)
	r.Equal(updatedToStorage, got)

	// block different bucket
	err = store.BlockOp(ctx, otherID).Get()
	r.NoError(err)
	_, err = store.GetOp(ctx, otherID).Get()
	r.ErrorIs(err, dom.ErrRoutingBlock)
	// original bucket still the same
	got, err = store.GetOp(ctx, id).Get()
	r.NoError(err)
	r.Equal(updatedToStorage, got)

}

func TestNewBucketRoutingStore_WithExec(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	ctx := t.Context()

	user := "test_user"
	bucket := "test_bucket"
	toStorage := "storage_1"
	id := entity.NewBucketRoutingPolicyID(user, bucket)
	store := NewBucketRoutingStore(c)
	tx := store.TxExecutor()
	storeTx := NewBucketRoutingStore(c).WithExecutor(tx)

	// empty
	_, err := store.GetOp(ctx, id).Get()
	r.ErrorIs(err, dom.ErrNotFound)

	getNotExists := storeTx.GetOp(ctx, id)
	setOp := storeTx.SetOp(ctx, id, toStorage)
	getExists := storeTx.GetOp(ctx, id)
	blockOp := storeTx.BlockOp(ctx, id)
	getBlocked := storeTx.GetOp(ctx, id)

	// tx not executed yet - so no results
	getVal, err := getNotExists.Get()
	r.NoError(err)
	r.Equal("", getVal)
	isSetErr := setOp.Get()
	r.NoError(isSetErr)
	getVal, err = getExists.Get()
	r.NoError(err)
	r.Equal("", getVal)
	isBlockErr := blockOp.Get()
	r.NoError(isBlockErr)
	getVal, err = getBlocked.Get()
	r.NoError(err)
	r.Equal("", getVal)

	// commit tx
	_ = tx.Exec(ctx)
	// now results should be available
	getVal, err = getNotExists.Get()
	r.ErrorIs(err, dom.ErrNotFound)
	isSetErr = setOp.Get()
	r.NoError(isSetErr)
	getVal, err = getExists.Get()
	r.NoError(err)
	r.Equal(toStorage, getVal)
	blockErr := blockOp.Get()
	r.NoError(blockErr)
	_, err = getBlocked.Get()
	r.ErrorIs(err, dom.ErrRoutingBlock)

}
