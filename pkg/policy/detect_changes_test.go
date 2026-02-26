package policy

import (
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/testutil"
)

func TestCheckSchemaCompatibility(t *testing.T) {
	c := testutil.SetupRedis(t)
	r := require.New(t)
	ctx := t.Context()

	t.Run("compatible empty", func(t *testing.T) {
		c.FlushAll(ctx)

		err := CheckSchemaCompatibility(ctx, "v0.6.0", c)
		r.NoError(err)
		// check that schema version is set
		v, err := c.Get(ctx, schemaVersionKey).Int()
		r.NoError(err)
		r.Equal(currentSchemaVersion, v)
		//repeat should be ok
		err = CheckSchemaCompatibility(ctx, "v0.6.0", c)
		r.NoError(err)
	})

	t.Run("has legacy route key", func(t *testing.T) {
		c.FlushAll(ctx)
		err := c.Set(ctx, "p:route:ceph", "value", 0).Err()
		r.NoError(err)

		err = CheckSchemaCompatibility(ctx, "v0.6.0", c)
		r.Error(err)
		r.ErrorIs(err, dom.ErrInternal)
	})

	t.Run("has legacy repl key", func(t *testing.T) {
		c.FlushAll(ctx)
		err := c.ZAdd(ctx, "p:repl:from", redis.Z{
			Score:  5,
			Member: "to:buck",
		}).Err()
		r.NoError(err)

		err = CheckSchemaCompatibility(ctx, "v0.6.0", c)
		r.Error(err)
		r.ErrorIs(err, dom.ErrInternal)
	})

	t.Run("has schema verstion 2", func(t *testing.T) {
		c.FlushAll(ctx)
		// add at least one key
		err := c.ZAdd(ctx, "p:repl:from", redis.Z{
			Score:  5,
			Member: "to:buck",
		}).Err()
		r.NoError(err)

		err = c.Set(ctx, schemaVersionKey, 2, 0).Err()
		r.NoError(err)

		err = CheckSchemaCompatibility(ctx, "", c)
		r.Error(err)
		r.ErrorIs(err, dom.ErrInternal)
		r.Contains(err.Error(), "v0.6.0")
	})
}
