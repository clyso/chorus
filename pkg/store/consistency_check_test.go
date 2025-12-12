package store

import (
	"context"
	"testing"

	"github.com/clyso/chorus/pkg/entity"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
)

var _ = Describe("Consistency checker stores", func() {

	const (
		CMinSize = 0
		CMaxSize = 10000

		CMinStringLength = 0
		CMaxStringLength = 10

		CMinKeyCount = 1
		CMaxKeyCount = 100

		CMinLocationCount = 2
		CMaxLocationCount = 10

		CGenAlphabet = "abcdefghijklmopqrspuvwxyz/."
	)

	AfterEach(func() {
		ctx := context.WithoutCancel(testCtx)

		err := testRedisClient.FlushAll(ctx).Err()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Should remove all matching records", func() {
		ctx := context.WithoutCancel(testCtx)

		store := NewConsistencyCheckSetStore(testRedisClient)

		locationCount := testRnd.Int64InRange(CMinLocationCount, CMaxLocationCount)
		locations := make([]entity.ConsistencyCheckLocation, 0, locationCount)
		for i := int64(0); i < locationCount; i++ {
			storage := testRnd.VarLengthStringFromAlphabet(CGenAlphabet, CMinStringLength, CMaxStringLength)
			bucket := testRnd.VarLengthStringFromAlphabet(CGenAlphabet, CMinStringLength, CMaxStringLength)
			locations = append(locations, entity.NewConsistencyCheckLocation(storage, bucket))
		}
		checkID := entity.NewConsistencyCheckID(locations...)

		keyCount := testRnd.Int64InRange(CMinKeyCount, CMaxKeyCount)
		ids := make([]entity.ConsistencyCheckSetID, 0, keyCount)
		for i := int64(0); i < keyCount; i++ {
			object := testRnd.VarLengthStringFromAlphabet(CGenAlphabet, CMinStringLength, CMaxStringLength)
			etag := testRnd.VarLengthStringFromAlphabet(CGenAlphabet, CMinStringLength, CMaxStringLength)
			size := uint64(testRnd.Int64InRange(CMinSize, CMaxSize))
			ids = append(ids, entity.NewEtagConsistencyCheckSetID(checkID, object, size, etag))
		}

		for i := 0; i < int(locationCount)-1; i++ {
			location := locations[i]
			for _, key := range ids {
				err := store.Add(ctx, key, entity.NewConsistencyCheckSetEntry(location), uint8(locationCount))
				Expect(err).NotTo(HaveOccurred())
			}
		}

		keys, err := testRedisClient.Keys(ctx, "*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(keys).To(HaveLen(int(keyCount)))

		lastLocation := locations[locationCount-1]
		for idx, key := range ids {
			err := store.Add(ctx, key, entity.NewConsistencyCheckSetEntry(lastLocation), uint8(locationCount))
			Expect(err).NotTo(HaveOccurred())
			keys, err := testRedisClient.Keys(ctx, "*").Result()
			Expect(keys).To(HaveLen(int(keyCount) - idx - 1))
		}
	})
})

func TestConsistencyCheckIDToTokensConverter(t *testing.T) {
	type args struct {
	}
	tests := []struct {
		name    string
		id      entity.ConsistencyCheckID
		wantErr bool
	}{
		{
			name: "buffer error case",
			id: entity.ConsistencyCheckID{
				Locations: []entity.ConsistencyCheckLocation{
					{Storage: "f1", Bucket: "restart1"},
					{Storage: "main", Bucket: "restart1"},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			got, err := ConsistencyCheckIDToTokensConverter(tt.id)
			if tt.wantErr {
				r.Error(err)
				return
			}
			r.NoError(err)
			gotID, err := TokensToConsistencyCheckIDConverter(got)
			r.NoError(err)
			r.EqualValues(tt.id, gotID)
		})
	}
}
