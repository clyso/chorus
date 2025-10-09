package store

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Common stores", func() {

	const (
		CMinStringLength = 1
		CMaxStringLength = 100

		CMinKeyCount = 0
		CMaxKeyCount = 1000

		CGenAlphabet = "abcdefghijklmopqrspuvwxyz/."
	)

	var ()

	AfterEach(func() {
		ctx := context.WithoutCancel(testCtx)

		err := testRedisClient.FlushAll(ctx).Err()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Should remove all keys", func() {
		ctx := context.WithoutCancel(testCtx)

		keyPrefix := testRnd.VarLengthStringFromAlphabet(CGenAlphabet, CMinStringLength, CMaxStringLength)
		store := NewRedisIDCommonStore(testRedisClient, keyPrefix, StringToSingleTokenConverter, SingleTokenToStringConverter)

		recordCount := testRnd.Int64InRange(CMinKeyCount, CMaxKeyCount)

		for i := range recordCount {
			id := testRnd.VarLengthStringFromAlphabet(CGenAlphabet, CMinStringLength, CMaxStringLength)
			key, err := store.MakeKey(id)
			Expect(err).NotTo(HaveOccurred())
			_, err = testRedisClient.Set(ctx, key, i, 0).Result()
			Expect(err).NotTo(HaveOccurred())
		}

		keys, err := testRedisClient.Keys(ctx, "*").Result()
		storedKeyCount := len(keys)
		Expect(err).NotTo(HaveOccurred())
		Expect(storedKeyCount).To(And(
			BeNumerically("<=", recordCount),
			BeNumerically(">=", CMinKeyCount),
		))

		affected, err := store.DropIDs(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(affected).To(BeNumerically("==", storedKeyCount))

		keys, err = testRedisClient.Keys(ctx, "*").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(keys).To(HaveLen(0))
	})
})
