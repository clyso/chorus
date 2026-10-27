package objstore

import (
	"context"
	"errors"
	"iter"
	"testing"

	"github.com/stretchr/testify/require"
)

// listStub lists objects as a storage does for the listing prefix.
type listStub struct {
	Common
	listed []CommonObjectInfo
	err    error
	prefix string
}

func (s *listStub) ListObjects(_ context.Context, _ string, opts ...func(o *commonListOptions)) iter.Seq2[CommonObjectInfo, error] {
	listOpts := &commonListOptions{}
	for _, opt := range opts {
		opt(listOpts)
	}
	s.prefix = listOpts.prefix
	return func(yield func(CommonObjectInfo, error) bool) {
		for _, object := range s.listed {
			if !yield(object, nil) {
				return
			}
		}
		if s.err != nil {
			yield(CommonObjectInfo{}, s.err)
		}
	}
}

func TestObjectVersions(t *testing.T) {
	r := require.New(t)
	stub := &listStub{
		listed: []CommonObjectInfo{
			{Key: "a", VersionID: "a2"},
			{Key: "a", VersionID: "a1"},
			{Key: "a/b", VersionID: "a/b1"},
			{Key: "ab", VersionID: "ab1"},
		},
		err: errors.New("list error"),
	}

	var versions []string
	var listErr error
	for object, err := range ObjectVersions(t.Context(), stub, "bucket", "a") {
		if err != nil {
			listErr = err
			continue
		}
		versions = append(versions, object.VersionID)
	}
	r.Equal("a", stub.prefix)
	r.Equal([]string{"a2", "a1"}, versions)
	r.ErrorIs(listErr, stub.err)
}
