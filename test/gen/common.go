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
	"bytes"
	"context"
	"errors"
	"io"
	"iter"
	"time"
)

const (
	CTestGenSeed    = 811509576612567777
	CUseTestGenSeed = false
)

type ObjectInfo struct {
	LastModified time.Time
	Key          string
	VersionID    string
	Etag         string
	Size         uint64
}

type ContentFiller interface {
	FillBucket(ctx context.Context, bucket string) error
	FillBucketWithLastVersions(ctx context.Context, bucket string) error
}

type GeneratorRange struct {
	Min int64
	Max int64
}

func readersHaveSameContent(left io.Reader, right io.Reader) bool {
	leftBuffer := make([]byte, 512)
	rightBuffer := make([]byte, 512)
	for {
		_, leftErr := left.Read(leftBuffer)
		_, rightErr := right.Read(rightBuffer)

		if errors.Is(leftErr, io.EOF) && errors.Is(rightErr, io.EOF) {
			return true
		}
		if leftErr != nil || rightErr != nil {
			return false
		}
		if !bytes.Equal(leftBuffer, rightBuffer) {
			return false
		}
	}
}

func GenerateCommonSafeCharacters() []rune {
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

type GeneratedObjectContentReader struct {
	rnd           *Rnd
	contentLength uint64
	read          uint64
}

func NewGeneratedObjectContentReader(seed int64, contentLengthRange *GeneratorRange) *GeneratedObjectContentReader {
	rnd := NewRnd(seed)
	var contentLength uint64
	if contentLengthRange != nil {
		contentLength = uint64(rnd.Int64InRange(contentLengthRange.Min, contentLengthRange.Max))
	}
	return &GeneratedObjectContentReader{
		rnd:           NewRnd(seed),
		contentLength: contentLength,
	}
}

func (r *GeneratedObjectContentReader) Len() uint64 {
	return r.contentLength
}

func (r *GeneratedObjectContentReader) Read(p []byte) (int, error) {
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

type GeneratedObject struct {
	contentLengthRange *GeneratorRange
	name               string
	fullPath           string
	contentSeed        int64
	versionCount       uint64
	aType              TreeNodeType
}

func (r *GeneratedObject) GetFullPath() string {
	return r.fullPath
}

func (r *GeneratedObject) GetVersionCount() uint64 {
	return r.versionCount
}

func (r *GeneratedObject) GetNodeType() TreeNodeType {
	return r.aType
}

func (r *GeneratedObject) ContentReaderIterator() iter.Seq2[uint64, *GeneratedObjectContentReader] {
	return func(yield func(uint64, *GeneratedObjectContentReader) bool) {
		for i := uint64(0); i < r.versionCount; i++ {
			if !yield(i, r.GetVersionContentReader(i)) {
				return
			}
		}
	}
}

func (r *GeneratedObject) GetVersionContentReader(versionIdx uint64) *GeneratedObjectContentReader {
	contentSeed := r.contentSeed + int64(versionIdx)
	return NewGeneratedObjectContentReader(contentSeed, r.contentLengthRange)
}

func (r *GeneratedObject) GetFirstVersionContentReader() *GeneratedObjectContentReader {
	return r.GetVersionContentReader(0)
}

func (r *GeneratedObject) GetLastVersionContentReader() *GeneratedObjectContentReader {
	return r.GetVersionContentReader(r.versionCount - 1)
}

func (r *GeneratedObject) GetContentReader() *GeneratedObjectContentReader {
	return r.GetFirstVersionContentReader()
}

type CommonObjectGeneratorOptions func(gen *CommonObjectGenerator)

func WithContentLengthhRange(min uint32, max uint32) CommonObjectGeneratorOptions {
	return func(gen *CommonObjectGenerator) {
		gen.contentLenghtRange = &GeneratorRange{
			Min: int64(min),
			Max: int64(max),
		}
	}
}

func WithVersionRange(min uint32, max uint32) CommonObjectGeneratorOptions {
	return func(gen *CommonObjectGenerator) {
		gen.versioned = true
		gen.versionRange = &GeneratorRange{
			Min: int64(min),
			Max: int64(max),
		}
	}
}

func WithVersioned() CommonObjectGeneratorOptions {
	return func(gen *CommonObjectGenerator) {
		gen.versioned = true
	}
}

func WithNameLengthRange(min uint32, max uint32) CommonObjectGeneratorOptions {
	return func(gen *CommonObjectGenerator) {
		gen.nameLengthRange = &GeneratorRange{
			Min: int64(min),
			Max: int64(max),
		}
	}
}

func WithNameCharacters(chars []rune) CommonObjectGeneratorOptions {
	return func(gen *CommonObjectGenerator) {
		gen.nameGenerationCharacters = chars
	}
}

type CommonObjectGenerator struct {
	contentLenghtRange       *GeneratorRange
	versionRange             *GeneratorRange
	nameLengthRange          *GeneratorRange
	nameGenerationCharacters []rune
	versioned                bool
}

func NewCommonObjectGenerator(opts ...CommonObjectGeneratorOptions) *CommonObjectGenerator {
	gen := &CommonObjectGenerator{}
	for _, opt := range opts {
		opt(gen)
	}
	gen.setDefaults()
	return gen
}

func (r *CommonObjectGenerator) Generate(rnd *Rnd, nodeType TreeNodeType, parentData *GeneratedObject) (*GeneratedObject, error) {
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

func (r *CommonObjectGenerator) setDefaults() {
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
		r.nameGenerationCharacters = GenerateCommonSafeCharacters()
	}
}

func (r *CommonObjectGenerator) generateRoot() *GeneratedObject {
	return &GeneratedObject{
		fullPath: "/",
		aType:    CRootTreeNodeType,
		contentLengthRange: &GeneratorRange{
			Min: 0,
			Max: 0,
		},
	}
}

func (r *CommonObjectGenerator) generateJoint(rnd *Rnd, parentData *GeneratedObject) *GeneratedObject {
	name := r.generateName(rnd)

	return &GeneratedObject{
		name:               name,
		aType:              CJointTreeNodeType,
		fullPath:           parentData.fullPath + name + "/",
		contentLengthRange: &GeneratorRange{},
		versionCount:       1,
	}
}

func (r *CommonObjectGenerator) generateLeaf(rnd *Rnd, parentData *GeneratedObject) *GeneratedObject {
	name := r.generateName(rnd)

	var versionCount int64
	if r.versioned {
		versionCount = rnd.Int64InRange(r.versionRange.Min, r.versionRange.Max)
	} else {
		versionCount = 1
	}

	return &GeneratedObject{
		name:               name,
		aType:              CLeafTreeNodeType,
		fullPath:           parentData.fullPath + name,
		contentLengthRange: r.contentLenghtRange,
		contentSeed:        rnd.Int64(),
		versionCount:       uint64(versionCount),
	}
}

func (r *CommonObjectGenerator) generateName(rnd *Rnd) string {
	return rnd.VarLengthStringFromRunes(r.nameGenerationCharacters, r.nameLengthRange.Min, r.nameLengthRange.Max)
}
