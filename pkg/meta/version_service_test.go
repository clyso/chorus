package meta

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_Version_svc(t *testing.T) {
	r := require.New(t)
	red := miniredis.RunT(t)

	c := redis.NewClient(&redis.Options{
		Addr: red.Addr(),
	})

	s := NewVersionService(c)

	obj := dom.Object{
		Bucket:  "bucket",
		Name:    "obj",
		Version: "",
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	vers, err := s.GetObj(ctx, obj)
	r.NoError(err)
	r.Empty(vers)
	vers, err = s.GetTags(ctx, obj)
	r.NoError(err)
	r.Empty(vers)
	vers, err = s.GetACL(ctx, obj)
	r.NoError(err)
	r.Empty(vers)

	stor := "stor1"
	err = s.UpdateIfGreater(ctx, obj, stor, -1)
	r.Error(err)
	err = s.UpdateIfGreater(ctx, obj, stor, 0)
	r.Error(err)
	err = s.UpdateIfGreater(ctx, obj, stor, 69)
	r.NoError(err)
	vers, err = s.GetObj(ctx, obj)
	r.NoError(err)
	r.EqualValues(69, vers[stor])
	err = s.UpdateIfGreater(ctx, obj, stor, 67)
	r.NoError(err)
	vers, err = s.GetObj(ctx, obj)
	r.NoError(err)
	r.EqualValues(69, vers[stor])
	err = s.UpdateIfGreater(ctx, obj, stor, 0)
	r.Error(err)
	err = s.UpdateIfGreater(ctx, obj, stor, 420)
	r.NoError(err)
	vers, err = s.GetObj(ctx, obj)
	r.NoError(err)
	r.EqualValues(420, vers[stor])

	incVer, err := s.IncrementObj(ctx, obj, stor)
	r.NoError(err)
	r.EqualValues(421, incVer)
	vers, err = s.GetObj(ctx, obj)
	r.NoError(err)
	r.EqualValues(421, vers[stor])

	stor2 := "stor2"
	incVer2, err := s.IncrementObj(ctx, obj, stor2)
	r.NoError(err)
	r.EqualValues(1, incVer2)
	vers, err = s.GetObj(ctx, obj)
	r.NoError(err)
	r.Len(vers, 2)
	r.EqualValues(421, vers[stor])
	r.EqualValues(1, vers[stor2])

	vers, err = s.GetTags(ctx, obj)
	r.NoError(err)
	r.Empty(vers)
	vers, err = s.GetACL(ctx, obj)
	r.NoError(err)
	r.Empty(vers)

	err = s.DeleteObjAll(ctx, obj)
	r.NoError(err)
	vers, err = s.GetObj(ctx, obj)
	r.NoError(err)
	r.Empty(vers)
	vers, err = s.GetTags(ctx, obj)
	r.NoError(err)
	r.Empty(vers)
	vers, err = s.GetACL(ctx, obj)
	r.NoError(err)
	r.Empty(vers)

	err = s.UpdateACLIfGreater(ctx, obj, stor, -1)
	r.Error(err)
	err = s.UpdateACLIfGreater(ctx, obj, stor, 0)
	r.Error(err)
	err = s.UpdateACLIfGreater(ctx, obj, stor, 33)
	r.NoError(err)
	vers, err = s.GetACL(ctx, obj)
	r.NoError(err)
	r.EqualValues(33, vers[stor])
	err = s.UpdateACLIfGreater(ctx, obj, stor, 32)
	r.NoError(err)
	vers, err = s.GetACL(ctx, obj)
	r.NoError(err)
	r.EqualValues(33, vers[stor])
	err = s.UpdateACLIfGreater(ctx, obj, stor, 0)
	r.Error(err)
	err = s.UpdateACLIfGreater(ctx, obj, stor, 55)
	r.NoError(err)
	vers, err = s.GetACL(ctx, obj)
	r.NoError(err)
	r.EqualValues(55, vers[stor])

	aclVer, err := s.IncrementACL(ctx, obj, stor)
	r.NoError(err)
	r.EqualValues(56, aclVer)
	vers, err = s.GetACL(ctx, obj)
	r.NoError(err)
	r.Len(vers, 1)
	r.EqualValues(56, vers[stor])

	aclVer2, err := s.IncrementACL(ctx, obj, stor2)
	r.NoError(err)
	r.EqualValues(1, aclVer2)
	vers, err = s.GetACL(ctx, obj)
	r.NoError(err)
	r.Len(vers, 2)
	r.EqualValues(1, vers[stor2])

	err = s.UpdateTagsIfGreater(ctx, obj, stor, -1)
	r.Error(err)
	err = s.UpdateTagsIfGreater(ctx, obj, stor, 0)
	r.Error(err)
	err = s.UpdateTagsIfGreater(ctx, obj, stor, 33)
	r.NoError(err)
	vers, err = s.GetTags(ctx, obj)
	r.NoError(err)
	r.EqualValues(33, vers[stor])
	err = s.UpdateTagsIfGreater(ctx, obj, stor, 32)
	r.NoError(err)
	vers, err = s.GetTags(ctx, obj)
	r.NoError(err)
	r.EqualValues(33, vers[stor])
	err = s.UpdateTagsIfGreater(ctx, obj, stor, 0)
	r.Error(err)
	err = s.UpdateTagsIfGreater(ctx, obj, stor, 55)
	r.NoError(err)
	vers, err = s.GetTags(ctx, obj)
	r.NoError(err)
	r.EqualValues(55, vers[stor])

	tagVer, err := s.IncrementTags(ctx, obj, stor)
	r.NoError(err)
	r.EqualValues(56, tagVer)
	vers, err = s.GetTags(ctx, obj)
	r.NoError(err)
	r.Len(vers, 1)
	r.EqualValues(56, vers[stor])

	tagVer2, err := s.IncrementTags(ctx, obj, stor2)
	r.NoError(err)
	r.EqualValues(1, tagVer2)
	vers, err = s.GetTags(ctx, obj)
	r.NoError(err)
	r.Len(vers, 2)
	r.EqualValues(1, vers[stor2])

	err = s.DeleteObjAll(ctx, obj)
	r.NoError(err)
	vers, err = s.GetObj(ctx, obj)
	r.NoError(err)
	r.Empty(vers)
	vers, err = s.GetTags(ctx, obj)
	r.NoError(err)
	r.Empty(vers)
	vers, err = s.GetACL(ctx, obj)
	r.NoError(err)
	r.Empty(vers)
}
