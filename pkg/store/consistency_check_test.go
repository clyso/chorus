package store

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/clyso/chorus/pkg/entity"
)

var _ = Describe("Consistency checker stores", func() {

	const (
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
			ids = append(ids, entity.NewConsistencyCheckSetID(checkID, object, etag))
		}

		for i := 0; i < int(locationCount)-1; i++ {
			location := locations[i]
			for _, key := range ids {
				err := store.Add(ctx, key, entity.NewConsistencyCheckSetEntry(location.Storage), uint8(locationCount))
				Expect(err).NotTo(HaveOccurred())
			}
		}

		keys, err := testRedisClient.Keys(ctx, "*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(keys).To(HaveLen(int(keyCount)))

		lastLocation := locations[locationCount-1]
		for idx, key := range ids {
			err := store.Add(ctx, key, entity.NewConsistencyCheckSetEntry(lastLocation.Storage), uint8(locationCount))
			Expect(err).NotTo(HaveOccurred())
			keys, err := testRedisClient.Keys(ctx, "*").Result()
			Expect(keys).To(HaveLen(int(keyCount) - idx - 1))
		}
	})
})
