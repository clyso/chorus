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
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
)

const (
	CRootTreeNodeType TreeNodeType = iota + 1
	CJointTreeNodeType
	CLeafTreeNodeType
)

var (
	ErrEmptyIterator = errors.New("iterator is empty")
)

type TreeNodeType int

type WidthFirstTreeNodeIterator[T any] struct {
	queue *Queue[*TreeNode[T]]
}

func (r *WidthFirstTreeNodeIterator[T]) HasNext() bool {
	return !r.queue.Empty()
}

func (r *WidthFirstTreeNodeIterator[T]) Next() (*TreeNode[T], error) {
	if r.queue.Empty() {
		// var noVal T
		return nil, ErrEmptyIterator
	}

	currentNode, err := r.queue.Dequeue()
	if err != nil {
		// var noVal T
		return nil, fmt.Errorf("unable to get next node from queue: %w", err)
	}

	if currentNode == nil {
		return nil, errors.New("queue returned nil value")
	}

	nextNode := currentNode.leftChild
	for nextNode != nil {
		r.queue.Enqueue(nextNode)
		nextNode = nextNode.rightNeighbour
	}

	return currentNode, nil
}

func (r *WidthFirstTreeNodeIterator[T]) Must() iter.Seq[*TreeNode[T]] {
	return func(yield func(*TreeNode[T]) bool) {
		for r.HasNext() {
			next, err := r.Next()
			if err != nil {
				panic(err)
			}
			if !yield(next) {
				return
			}
		}
	}
}

type WidthFirstTreeValueIterator[T any] struct {
	WidthFirstTreeNodeIterator[T]
}

func (r *WidthFirstTreeValueIterator[T]) Next() (T, error) {
	next, err := r.WidthFirstTreeNodeIterator.Next()
	if err != nil {
		var noVal T
		return noVal, err
	}

	return next.data, nil
}

func (r *WidthFirstTreeValueIterator[T]) Must() iter.Seq[T] {
	return func(yield func(T) bool) {
		for r.WidthFirstTreeNodeIterator.HasNext() {
			next, err := r.Next()
			if err != nil {
				panic(err)
			}
			if !yield(next) {
				return
			}
		}
	}
}

type DepthFirstTreeNodeIterator[T any] struct {
	stack *Stack[*TreeNode[T]]
}

func (r *DepthFirstTreeNodeIterator[T]) HasNext() bool {
	return !r.stack.Empty()
}

func (r *DepthFirstTreeNodeIterator[T]) Next() (*TreeNode[T], error) {
	if r.stack.Empty() {
		return nil, ErrEmptyIterator
	}

	currentNode, err := r.stack.Pop()
	if err != nil {
		return nil, fmt.Errorf("unable to get next node from stack: %w", err)
	}

	if currentNode == nil {
		return nil, errors.New("stack returned nil value")
	}

	nextNode := currentNode.leftChild
	for nextNode != nil {
		r.stack.Push(nextNode)
		nextNode = nextNode.rightNeighbour
	}

	return currentNode, nil
}

func (r *DepthFirstTreeNodeIterator[T]) Must() iter.Seq[*TreeNode[T]] {
	return func(yield func(*TreeNode[T]) bool) {
		for r.HasNext() {
			next, err := r.Next()
			if err != nil {
				panic(err)
			}
			if !yield(next) {
				return
			}
		}
	}
}

type DepthFirstTreeValueIterator[T any] struct {
	DepthFirstTreeNodeIterator[T]
}

func (r *DepthFirstTreeValueIterator[T]) Next() (T, error) {
	next, err := r.DepthFirstTreeNodeIterator.Next()
	if err != nil {
		var noVal T
		return noVal, err
	}

	return next.data, nil
}

func (r *DepthFirstTreeValueIterator[T]) Must() iter.Seq[T] {
	return func(yield func(T) bool) {
		for r.HasNext() {
			next, err := r.Next()
			if err != nil {
				panic(err)
			}
			if !yield(next) {
				return
			}
		}
	}
}

type TreeNode[T any] struct {
	data           T
	rightNeighbour *TreeNode[T]
	leftChild      *TreeNode[T]
}

func (r *TreeNode[T]) Data() T {
	return r.data
}

type Tree[T any] struct {
	root *TreeNode[T]
}

func NewTree[T any](root *TreeNode[T]) *Tree[T] {
	return &Tree[T]{
		root: root,
	}
}

func (r *Tree[T]) DepthFirstNodeIterator() *DepthFirstTreeNodeIterator[T] {
	stack := NewStack[*TreeNode[T]]()
	if r.root != nil {
		stack.Push(r.root)
	}
	return &DepthFirstTreeNodeIterator[T]{
		stack: stack,
	}
}

func (r *Tree[T]) WidthFirstNodeIterator() *WidthFirstTreeNodeIterator[T] {
	queue := NewQueue[*TreeNode[T]]()
	if r.root != nil {
		queue.Enqueue(r.root)
	}
	return &WidthFirstTreeNodeIterator[T]{
		queue: queue,
	}
}

func (r *Tree[T]) DepthFirstValueIterator() *DepthFirstTreeValueIterator[T] {
	stack := NewStack[*TreeNode[T]]()
	if r.root != nil {
		stack.Push(r.root)
	}
	return &DepthFirstTreeValueIterator[T]{
		DepthFirstTreeNodeIterator: DepthFirstTreeNodeIterator[T]{
			stack: stack,
		},
	}
}

func (r *Tree[T]) WidthFirstValueIterator() *WidthFirstTreeValueIterator[T] {
	queue := NewQueue[*TreeNode[T]]()
	if r.root != nil {
		queue.Enqueue(r.root)
	}
	return &WidthFirstTreeValueIterator[T]{
		WidthFirstTreeNodeIterator: WidthFirstTreeNodeIterator[T]{
			queue: queue,
		},
	}
}

type ObjectGenerator[T any] interface {
	Generate(rnd *Rnd, nodeType TreeNodeType, parentData T) (T, error)
}

type DummyObjectGenerator[T any] struct{}

func NewDummyObjectGenerator[T any]() *DummyObjectGenerator[T] {
	return &DummyObjectGenerator[T]{}
}

func (r *DummyObjectGenerator[T]) Generate(_ *Rnd, _ TreeNodeType, _ T) (T, error) {
	var noVal T
	return noVal, nil
}

type TreeGeneratorOption[T any] interface {
	apply(*TreeGenerator[T])
}

type TreeGeneratorWithRndOption[T any] struct {
	rnd *Rnd
}

func WithRnd[T any](rnd *Rnd) TreeGeneratorOption[T] {
	return &TreeGeneratorWithRndOption[T]{
		rnd: rnd,
	}
}

//nolint:unused // detected as unused, but it is used in tree generator
func (r *TreeGeneratorWithRndOption[T]) apply(gen *TreeGenerator[T]) {
	gen.rnd = r.rnd
}

type TreeGeneratorWithRandomSeedOption[T any] struct {
	seed int64
}

func WithRandomSeed[T any](seed int64) TreeGeneratorOption[T] {
	return &TreeGeneratorWithRandomSeedOption[T]{
		seed: seed,
	}
}

//nolint:unused // detected as unused, but it is used in tree generator
func (r *TreeGeneratorWithRandomSeedOption[T]) apply(gen *TreeGenerator[T]) {
	gen.rnd = NewRnd(r.seed)
}

type TreeGeneratorWithForceTargetDepthOption[T any] struct{}

func WithForceTargetDepth[T any]() TreeGeneratorOption[T] {
	return &TreeGeneratorWithForceTargetDepthOption[T]{}
}

//nolint:unused // detected as unused, but it is used in tree generator
func (r *TreeGeneratorWithForceTargetDepthOption[T]) apply(gen *TreeGenerator[T]) {
	gen.forceTargetDepth = true
}

type TreeGeneratorWithDepthRangeOption[T any] struct {
	depthRange *GeneratorRange
}

func WithDepthRange[T any](min uint32, max uint32) TreeGeneratorOption[T] {
	return &TreeGeneratorWithDepthRangeOption[T]{
		depthRange: &GeneratorRange{
			Min: int64(min),
			Max: int64(max),
		},
	}
}

//nolint:unused // detected as unused, but it is used in tree generator
func (r *TreeGeneratorWithDepthRangeOption[T]) apply(gen *TreeGenerator[T]) {
	gen.depthRange = r.depthRange
}

type TreeGeneratorWithWidthRangeOption[T any] struct {
	widthRange *GeneratorRange
}

func WithWidthRange[T any](min uint32, max uint32) TreeGeneratorOption[T] {
	return &TreeGeneratorWithWidthRangeOption[T]{
		widthRange: &GeneratorRange{
			Min: int64(min),
			Max: int64(max),
		},
	}
}

//nolint:unused // detected as unused, but it is used in tree generator
func (r *TreeGeneratorWithWidthRangeOption[T]) apply(gen *TreeGenerator[T]) {
	gen.widthRange = r.widthRange
}

type TreeGeneratorWithObjectGeneratorOption[T any] struct {
	generator ObjectGenerator[T]
}

func WithObjectGenerator[T any](generator ObjectGenerator[T]) TreeGeneratorOption[T] {
	return &TreeGeneratorWithObjectGeneratorOption[T]{
		generator: generator,
	}
}

//nolint:unused // detected as unused, but it is used in tree generator
func (r *TreeGeneratorWithObjectGeneratorOption[T]) apply(gen *TreeGenerator[T]) {
	gen.objectGenerator = r.generator
}

type TreeGenerationTask[T any] struct {
	node         *TreeNode[T]
	currentDepth uint64
}

type TreeGenerator[T any] struct {
	forceTargetDepth bool
	depthRange       *GeneratorRange
	widthRange       *GeneratorRange
	objectGenerator  ObjectGenerator[T]
	rnd              *Rnd
}

func NewTreeGenerator[T any](opts ...TreeGeneratorOption[T]) (*TreeGenerator[T], error) {
	gen := &TreeGenerator[T]{}
	for _, opt := range opts {
		opt.apply(gen)
	}

	if err := gen.setDefaults(); err != nil {
		return nil, fmt.Errorf("unable to set defaults: %w", err)
	}

	return gen, nil
}

func (r *TreeGenerator[T]) Generate() (*Tree[T], error) {
	var noVal T
	rootData, err := r.objectGenerator.Generate(r.rnd, CRootTreeNodeType, noVal)
	if err != nil {
		return nil, fmt.Errorf("unable to generate node data: %w", err)
	}

	rootNode := &TreeNode[T]{
		data: rootData,
	}
	generationStack := NewStack[TreeGenerationTask[T]]()
	generationStack.Push(TreeGenerationTask[T]{
		node: rootNode,
	})
	targetDepth := r.rnd.Int64InRange(r.depthRange.Min, r.depthRange.Max)

	for !generationStack.Empty() {
		generationTask, err := generationStack.Pop()
		if err != nil {
			return nil, fmt.Errorf("unable to pop task from stack: %w", err)
		}

		childDepth := generationTask.currentDepth + 1

		var childrenCount int64
		var forceTargetDepthChildIndex int64
		if r.forceTargetDepth && r.widthRange.Min == 0 {
			childrenCount = r.rnd.Int64InRange(1, r.widthRange.Max)
		} else {
			childrenCount = r.rnd.Int64InRange(r.widthRange.Min, r.widthRange.Max)
		}

		if r.forceTargetDepth {
			forceTargetDepthChildIndex = r.rnd.Int64InRange(0, childrenCount-1)
		}

		var prevChild *TreeNode[T]
		for i := int64(0); i < childrenCount; i++ {
			var nodeType TreeNodeType
			if childDepth < uint64(targetDepth) &&
				(r.forceTargetDepth && forceTargetDepthChildIndex == i || r.rnd.Bool()) {
				nodeType = CJointTreeNodeType
			} else {
				nodeType = CLeafTreeNodeType
			}

			data, err := r.objectGenerator.Generate(r.rnd, nodeType, generationTask.node.data)
			if err != nil {
				return nil, fmt.Errorf("unable to generate node data: %w", err)
			}
			node := &TreeNode[T]{
				data:           data,
				rightNeighbour: prevChild,
			}

			prevChild = node

			if nodeType == CLeafTreeNodeType {
				continue
			}

			generationStack.Push(TreeGenerationTask[T]{
				node:         node,
				currentDepth: childDepth,
			})
		}

		generationTask.node.leftChild = prevChild
	}

	return NewTree(rootNode), nil
}

func (r *TreeGenerator[T]) setDefaults() error {
	if r.depthRange == nil {
		r.depthRange = &GeneratorRange{
			Min: 5,
			Max: 5,
		}
	}
	if r.widthRange == nil {
		r.widthRange = &GeneratorRange{
			Min: 5,
			Max: 5,
		}
	}
	if r.objectGenerator == nil {
		r.objectGenerator = NewDummyObjectGenerator[T]()
	}
	if r.rnd == nil {
		var seed [8]byte
		_, err := crand.Read(seed[:])
		if err != nil {
			return fmt.Errorf("unable to read random bytes for seed: %w", err)
		}
		r.rnd = NewRnd(int64(binary.NativeEndian.Uint64(seed[:])))
	}
	return nil
}

type TreeRandomElementPicker[T any] struct {
	root   *TreeNode[T]
	joints []*TreeNode[T]
	leafs  []*TreeNode[T]
	rnd    *Rnd
}

func NewTreeRandomElementPicker[T any](tree *Tree[T], rnd *Rnd) *TreeRandomElementPicker[T] {
	picker := &TreeRandomElementPicker[T]{
		root:   tree.root,
		joints: []*TreeNode[T]{},
		leafs:  []*TreeNode[T]{},
		rnd:    rnd,
	}

	if tree.root == nil || tree.root.leftChild == nil {
		return picker
	}

	rootlessTree := NewTree(tree.root.leftChild)

	for node := range rootlessTree.DepthFirstNodeIterator().Must() {
		if node.leftChild == nil {
			picker.leafs = append(picker.leafs, node)
		} else {
			picker.joints = append(picker.joints, node)
		}
	}

	return picker
}

func (r *TreeRandomElementPicker[T]) RootNode() *TreeNode[T] {
	return r.root
}

func (r *TreeRandomElementPicker[T]) RootValue() T {
	return r.root.data
}

func (r *TreeRandomElementPicker[T]) RandomJointSubtree() *Tree[T] {
	node := r.randomNode(r.joints)
	if node == nil {
		return nil
	}
	return NewTree(node)
}

func (r *TreeRandomElementPicker[T]) RandomJointNode() *TreeNode[T] {
	node := r.randomNode(r.joints)
	return node
}

func (r *TreeRandomElementPicker[T]) RandomJointValue() T {
	node := r.randomNode(r.joints)
	if node == nil {
		var noVal T
		return noVal
	}
	return node.data
}

func (r *TreeRandomElementPicker[T]) RandomLeafNode() *TreeNode[T] {
	node := r.randomNode(r.leafs)
	return node
}

func (r *TreeRandomElementPicker[T]) RandomLeafValue() T {
	node := r.randomNode(r.leafs)
	if node == nil {
		var noVal T
		return noVal
	}
	return node.data
}

func (r *TreeRandomElementPicker[T]) randomNode(collection []*TreeNode[T]) *TreeNode[T] {
	if len(collection) == 0 {
		return nil
	}
	if len(collection) == 1 {
		return collection[0]
	}
	idx := r.rnd.IntInRange(0, len(r.joints)-1)
	return collection[idx]
}
