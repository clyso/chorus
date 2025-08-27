package meta

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/testutil"
)

func Test_svc_GetMigrationCosts(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)
	var s MigrationCostService = NewMigrationCostService(c)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	from, to := "from1", "to1"

	_, err := s.GetMigrationCosts(ctx, from, to)
	r.ErrorIs(err, dom.ErrNotFound)

	err = s.MigrationCostsIncBucket(ctx, from, to)
	r.NoError(err)
	m, err := s.GetMigrationCosts(ctx, from, to)
	r.NoError(err)
	r.EqualValues(1, m.BucketsNum())
	r.EqualValues(0, m.ObjectsNum())
	r.EqualValues(0, m.ObjectsSize())
	r.EqualValues(true, m.Done())

	err = s.MigrationCostsIncBucket(ctx, from, to)
	r.NoError(err)
	m, err = s.GetMigrationCosts(ctx, from, to)
	r.NoError(err)
	r.EqualValues(2, m.BucketsNum())
	r.EqualValues(0, m.ObjectsNum())
	r.EqualValues(0, m.ObjectsSize())
	r.EqualValues(true, m.Done())

	err = s.MigrationCostsIncJob(ctx, from, to)
	r.NoError(err)
	m, err = s.GetMigrationCosts(ctx, from, to)
	r.NoError(err)
	r.EqualValues(2, m.BucketsNum())
	r.EqualValues(0, m.ObjectsNum())
	r.EqualValues(0, m.ObjectsSize())
	r.EqualValues(false, m.Done())

	err = s.MigrationCostsIncJobDone(ctx, from, to)
	r.NoError(err)
	m, err = s.GetMigrationCosts(ctx, from, to)
	r.NoError(err)
	r.EqualValues(2, m.BucketsNum())
	r.EqualValues(0, m.ObjectsNum())
	r.EqualValues(0, m.ObjectsSize())
	r.EqualValues(true, m.Done())

	err = s.MigrationCostsIncObj(ctx, from, to)
	r.NoError(err)
	m, err = s.GetMigrationCosts(ctx, from, to)
	r.NoError(err)
	r.EqualValues(2, m.BucketsNum())
	r.EqualValues(1, m.ObjectsNum())
	r.EqualValues(0, m.ObjectsSize())
	r.EqualValues(true, m.Done())

	err = s.MigrationCostsIncObj(ctx, from, to)
	r.NoError(err)
	m, err = s.GetMigrationCosts(ctx, from, to)
	r.NoError(err)
	r.EqualValues(2, m.BucketsNum())
	r.EqualValues(2, m.ObjectsNum())
	r.EqualValues(0, m.ObjectsSize())
	r.EqualValues(true, m.Done())

	err = s.MigrationCostsIncSize(ctx, from, to, 69)
	r.NoError(err)
	m, err = s.GetMigrationCosts(ctx, from, to)
	r.NoError(err)
	r.EqualValues(2, m.BucketsNum())
	r.EqualValues(2, m.ObjectsNum())
	r.EqualValues(69, m.ObjectsSize())
	r.EqualValues(true, m.Done())

	err = s.MigrationCostsIncSize(ctx, from, to, 420)
	r.NoError(err)
	m, err = s.GetMigrationCosts(ctx, from, to)
	r.NoError(err)
	r.EqualValues(2, m.BucketsNum())
	r.EqualValues(2, m.ObjectsNum())
	r.EqualValues(489, m.ObjectsSize())
	r.EqualValues(true, m.Done())

	lo, err := s.MigrationCostsLastObjGet(ctx, from, to, "buck", "pref")
	r.NoError(err)
	r.Empty(lo)

	err = s.MigrationCostsLastObjSet(ctx, from, to, "buck", "pref", "someObj")
	r.NoError(err)
	lo, err = s.MigrationCostsLastObjGet(ctx, from, to, "buck", "pref")
	r.NoError(err)
	r.EqualValues("someObj", lo)
}
