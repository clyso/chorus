package gen

import (
	"bytes"
	"slices"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var treeGen *TreeGenerator[*GeneratedS3Object]

func TestEnv(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Test Env Suite")
}

var _ = Describe("Tree generation", func() {
	It("Generated content with the same seed should match", func() {
		seed := GinkgoRandomSeed()

		objGen1 := NewS3ObjectGenerator()
		treeGen1, err := NewTreeGenerator[*GeneratedS3Object](
			WithRandomSeed[*GeneratedS3Object](seed),
			WithObjectGenerator[*GeneratedS3Object](objGen1),
		)
		Expect(err).NotTo(HaveOccurred())

		objGen2 := NewS3ObjectGenerator()
		treeGen2, err := NewTreeGenerator[*GeneratedS3Object](
			WithRandomSeed[*GeneratedS3Object](seed),
			WithObjectGenerator[*GeneratedS3Object](objGen2),
		)
		Expect(err).NotTo(HaveOccurred())

		tree1, err := treeGen1.Generate()
		Expect(err).NotTo(HaveOccurred())
		tree2, err := treeGen2.Generate()
		Expect(err).NotTo(HaveOccurred())

		depthIter1 := tree1.DepthFirstIterator()
		depthIter2 := tree2.DepthFirstIterator()

		for depthIter1.HasNext() {
			item1, err := depthIter1.Next()
			Expect(err).NotTo(HaveOccurred())
			item2, err := depthIter2.Next()
			Expect(err).NotTo(HaveOccurred())

			Expect(item1.name).To(Equal(item2.name))
			Expect(item1.fullPath).To(Equal(item2.fullPath))
			Expect(item1.contentSeed).To(Equal(item2.contentSeed))
			Expect(item1.versionCount).To(Equal(item2.versionCount))
			Expect(item1.contentLengthRange).NotTo(BeNil())
			Expect(item2.contentLengthRange).NotTo(BeNil())
			Expect(item1.contentLengthRange.Min).To(Equal(item2.contentLengthRange.Min))
			Expect(item1.contentLengthRange.Max).To(Equal(item2.contentLengthRange.Max))

			for idx, reader1 := range item1.ContentReaderIterator() {
				reader2 := item2.GetVersionContentReader(idx)
				var buffer1 bytes.Buffer
				_, err = buffer1.ReadFrom(reader1)
				Expect(err).NotTo(HaveOccurred())

				var buffer2 bytes.Buffer
				_, err = buffer2.ReadFrom(reader2)
				Expect(err).NotTo(HaveOccurred())

				Expect(buffer1).To(Equal(buffer2))
			}
		}

		Expect(depthIter1.HasNext()).To(BeFalse())
		Expect(depthIter2.HasNext()).To(BeFalse())

		item1, err := depthIter1.Next()
		Expect(item1).To(BeNil())
		Expect(err).To(HaveOccurred())
		item2, err := depthIter2.Next()
		Expect(item2).To(BeNil())
		Expect(err).To(HaveOccurred())
	})

	It("Content reader is repeatable", func() {
		seed := GinkgoRandomSeed()

		objGen := NewS3ObjectGenerator()
		treeGen, err := NewTreeGenerator[*GeneratedS3Object](
			WithRandomSeed[*GeneratedS3Object](seed),
			WithObjectGenerator[*GeneratedS3Object](objGen),
			WithForceTargetDepth[*GeneratedS3Object](),
			WithDepthRange[*GeneratedS3Object](5, 5),
			WithWidthRange[*GeneratedS3Object](2, 5),
		)
		Expect(err).NotTo(HaveOccurred())

		tree, err := treeGen.Generate()
		Expect(err).NotTo(HaveOccurred())

		for item := range tree.DepthFirstIterator().Must() {
			for i := uint64(0); i < item.versionCount; i++ {
				reader1 := item.GetVersionContentReader(i)
				reader2 := item.GetVersionContentReader(i)

				var buffer1 bytes.Buffer
				var buffer2 bytes.Buffer

				_, err = buffer1.ReadFrom(reader1)
				Expect(err).NotTo(HaveOccurred())
				_, err = buffer2.ReadFrom(reader2)
				Expect(err).NotTo(HaveOccurred())

				Expect(buffer1.Bytes()).To(Equal(buffer2.Bytes()))
			}
		}
	})

	It("Iterators returning same collection", func() {
		seed := GinkgoRandomSeed()

		objGen := NewS3ObjectGenerator()
		treeGen, err := NewTreeGenerator[*GeneratedS3Object](
			WithObjectGenerator[*GeneratedS3Object](objGen),
			WithRandomSeed[*GeneratedS3Object](seed),
		)
		Expect(err).NotTo(HaveOccurred())

		tree, err := treeGen.Generate()
		Expect(err).NotTo(HaveOccurred())

		depthIter := tree.DepthFirstIterator()
		depthCollection := slices.Collect(depthIter.Must())

		widthIter := tree.WidthFirstIterator()
		widthCollection := slices.Collect(widthIter.Must())

		sortFunc := func(a, b *GeneratedS3Object) int {
			if a.fullPath == b.fullPath {
				return 0
			}
			if a.fullPath > b.fullPath {
				return -1
			}
			return 1
		}

		slices.SortFunc(depthCollection, sortFunc)
		slices.SortFunc(widthCollection, sortFunc)

		Expect(depthCollection).To(Equal(widthCollection))
	})
})
