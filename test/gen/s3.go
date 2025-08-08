// Copyright 2025 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gen

import (
	"context"
	"fmt"
	"io"
	"iter"
	"strings"

	"github.com/minio/minio-go/v7"
)

func GenerateS3SafeCharacters() []rune {
	runes := []rune{}
	for i := 'a'; i <= 'z'; i++ {
		runes = append(runes, i)
	}
	for i := 'A'; i <= 'Z'; i++ {
		runes = append(runes, i)
	}
	for i := '0'; i <= '9'; i++ {
		runes = append(runes, i)
	}
	for _, ch := range "!-_.*'()" {
		runes = append(runes, ch)
	}
	return runes
}

func GenerateS3SpecialHandlingCharacters() []rune {
	runes := []rune{}
	for i := 0; i < 31; i++ {
		runes = append(runes, rune(i))
	}
	runes = append(runes, rune(127))
	for _, ch := range "&$@=;/:+,?" {
		runes = append(runes, ch)
	}
	return runes
}

func GenerateS3AvoidCharacters() []rune {
	runes := []rune{}
	for i := 128; i < 255; i++ {
		runes = append(runes, rune(i))
	}
	for _, ch := range "\\{}^%[]`\"<>~#|" {
		runes = append(runes, ch)
	}
	return runes
}

type GeneratedS3ObjectContentReader struct {
	rnd           *Rnd
	contentLength uint64
	read          uint64
}

func NewGeneratedS3ObjectContentReader(seed int64, contentLengthRange *GeneratorRange) *GeneratedS3ObjectContentReader {
	rnd := NewRnd(seed)
	var contentLength uint64
	if contentLengthRange != nil {
		contentLength = uint64(rnd.Int64InRange(contentLengthRange.Min, contentLengthRange.Max))
	}
	return &GeneratedS3ObjectContentReader{
		rnd:           NewRnd(seed),
		contentLength: contentLength,
	}
}

func (r *GeneratedS3ObjectContentReader) Len() uint64 {
	return r.contentLength
}

func (r *GeneratedS3ObjectContentReader) Read(p []byte) (int, error) {
	if r.read == r.contentLength {
		return 0, io.EOF
	}
	bufferLen := len(p)
	if bufferLen == 0 {
		return 0, nil
	}
	readRemain := r.contentLength - r.read
	var toRead uint64
	if bufferLen > int(readRemain) {
		toRead = readRemain
	} else {
		toRead = uint64(bufferLen)
	}

	r.rnd.Read(p[:toRead])
	r.read += toRead

	return int(toRead), nil
}

type GeneratedS3Object struct {
	name               string
	fullPath           string
	contentLengthRange *GeneratorRange
	contentSeed        int64
	versionCount       uint64
}

func (r *GeneratedS3Object) GetFullPath() string {
	return r.fullPath
}

func (r *GeneratedS3Object) GetVersionCount() uint64 {
	return r.versionCount
}

func (r *GeneratedS3Object) ContentReaderIterator() iter.Seq2[uint64, *GeneratedS3ObjectContentReader] {
	return func(yield func(uint64, *GeneratedS3ObjectContentReader) bool) {
		for i := uint64(0); i < r.versionCount; i++ {
			if !yield(i, r.GetVersionContentReader(i)) {
				return
			}
		}
	}
}

func (r *GeneratedS3Object) GetVersionContentReader(versionIdx uint64) *GeneratedS3ObjectContentReader {
	contentSeed := r.contentSeed + int64(versionIdx)
	return NewGeneratedS3ObjectContentReader(contentSeed, r.contentLengthRange)
}

func (r *GeneratedS3Object) GetFirstVersionContentReader() *GeneratedS3ObjectContentReader {
	return r.GetVersionContentReader(0)
}

func (r *GeneratedS3Object) GetLastVersionContentReader() *GeneratedS3ObjectContentReader {
	return r.GetVersionContentReader(r.versionCount - 1)
}

func (r *GeneratedS3Object) GetContentReader() *GeneratedS3ObjectContentReader {
	return r.GetFirstVersionContentReader()
}

type S3ObjectGeneratorOptions func(gen *S3ObjectGenerator)

func WithContentLengthhRange(min uint32, max uint32) S3ObjectGeneratorOptions {
	return func(gen *S3ObjectGenerator) {
		gen.contentLenghtRange = &GeneratorRange{
			Min: int64(min),
			Max: int64(max),
		}
	}
}

func WithVersionRange(min uint32, max uint32) S3ObjectGeneratorOptions {
	return func(gen *S3ObjectGenerator) {
		gen.versioned = true
		gen.versionRange = &GeneratorRange{
			Min: int64(min),
			Max: int64(max),
		}
	}
}

func WithVersioned() S3ObjectGeneratorOptions {
	return func(gen *S3ObjectGenerator) {
		gen.versioned = true
	}
}

func WithNameLengthRange(min uint32, max uint32) S3ObjectGeneratorOptions {
	return func(gen *S3ObjectGenerator) {
		gen.nameLengthRange = &GeneratorRange{
			Min: int64(min),
			Max: int64(max),
		}
	}
}

func WithNameCharacters(chars []rune) S3ObjectGeneratorOptions {
	return func(gen *S3ObjectGenerator) {
		gen.nameGenerationCharacters = chars
	}
}

type S3ObjectGenerator struct {
	contentLenghtRange       *GeneratorRange
	versioned                bool
	versionRange             *GeneratorRange
	nameLengthRange          *GeneratorRange
	nameGenerationCharacters []rune
}

func NewS3ObjectGenerator(opts ...S3ObjectGeneratorOptions) *S3ObjectGenerator {
	gen := &S3ObjectGenerator{}
	for _, opt := range opts {
		opt(gen)
	}
	gen.setDefaults()
	return gen
}

func (r *S3ObjectGenerator) Generate(rnd *Rnd, nodeType TreeNodeType, parentData *GeneratedS3Object) (*GeneratedS3Object, error) {
	switch nodeType {
	case CRootTreeNodeType:
		return r.generateRoot(), nil
	case CJointTreeNodeType:
		return r.generateJoint(rnd, parentData), nil
	case CLeafTreeNodeType:
		return r.generateLeaf(rnd, parentData), nil
	}
	return nil, nil
}

func (r *S3ObjectGenerator) setDefaults() {
	if r.contentLenghtRange == nil {
		r.contentLenghtRange = &GeneratorRange{
			Min: 1024,
			Max: 2048,
		}
	}
	if r.nameLengthRange == nil {
		r.nameLengthRange = &GeneratorRange{
			Min: 5,
			Max: 10,
		}
	}
	if r.versionRange == nil {
		r.versionRange = &GeneratorRange{
			Min: 1,
			Max: 10,
		}
	}
	if r.nameGenerationCharacters == nil {
		r.nameGenerationCharacters = GenerateS3SafeCharacters()
	}
}

func (r *S3ObjectGenerator) generateRoot() *GeneratedS3Object {
	return &GeneratedS3Object{
		fullPath: "/",
		contentLengthRange: &GeneratorRange{
			Min: 0,
			Max: 0,
		},
	}
}

func (r *S3ObjectGenerator) generateJoint(rnd *Rnd, parentData *GeneratedS3Object) *GeneratedS3Object {
	name := r.generateName(rnd)

	return &GeneratedS3Object{
		name:               name,
		fullPath:           parentData.fullPath + name + "/",
		contentLengthRange: &GeneratorRange{},
		versionCount:       1,
	}
}

func (r *S3ObjectGenerator) generateLeaf(rnd *Rnd, parentData *GeneratedS3Object) *GeneratedS3Object {
	name := r.generateName(rnd)

	var versionCount int64
	if r.versioned {
		versionCount = rnd.Int64InRange(r.versionRange.Min, r.versionRange.Max)
	} else {
		versionCount = 1
	}

	return &GeneratedS3Object{
		name:               name,
		fullPath:           parentData.fullPath + name,
		contentLengthRange: r.contentLenghtRange,
		contentSeed:        rnd.Int64(),
		versionCount:       uint64(versionCount),
	}
}

func (r *S3ObjectGenerator) generateName(rnd *Rnd) string {
	nameLength := rnd.Int64InRange(r.nameLengthRange.Min, r.nameLengthRange.Max)
	alphabetLenght := len(r.nameGenerationCharacters)
	var nameBuilder strings.Builder
	for i := int64(0); i < nameLength; i++ {
		nameRuneIdx := rnd.Int64InRange(0, int64(alphabetLenght)-1)
		nameRune := r.nameGenerationCharacters[nameRuneIdx]
		_, _ = nameBuilder.WriteRune(nameRune)
	}
	return nameBuilder.String()
}

type S3Filler struct {
	tree   *Tree[*GeneratedS3Object]
	client *minio.Client
}

func NewS3Filler(tree *Tree[*GeneratedS3Object], client *minio.Client) *S3Filler {
	return &S3Filler{
		tree:   tree,
		client: client,
	}
}

func (r *S3Filler) Fill(ctx context.Context, bucket string) error {
	for item := range r.tree.DepthFirstIterator().Must() {
		for _, reader := range item.ContentReaderIterator() {
			_, err := r.client.PutObject(ctx, bucket, item.fullPath, reader, int64(reader.Len()), minio.PutObjectOptions{})
			if err != nil {
				return fmt.Errorf("unable to upload object: %w", err)
			}
		}
	}

	return nil
}
