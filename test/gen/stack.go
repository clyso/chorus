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

import "errors"

var (
	ErrEmptyStack = errors.New("stack is empty")
)

type StackNode[T any] struct {
	data T
	next *StackNode[T]
}

type Stack[T any] struct {
	head  *StackNode[T]
	count uint64
}

func NewStack[T any]() *Stack[T] {
	return &Stack[T]{}
}

func (r *Stack[T]) Push(data T) {
	newNode := &StackNode[T]{
		data: data,
		next: r.head,
	}
	r.head = newNode
	r.count++
}

func (r *Stack[T]) Pop() (T, error) {
	if r.head == nil {
		var noVal T
		return noVal, ErrEmptyStack
	}
	val := r.head.data
	r.head = r.head.next
	r.count--
	return val, nil
}

func (r *Stack[T]) Peek() (T, error) {
	if r.head == nil {
		var noVal T
		return noVal, ErrEmptyStack
	}
	return r.head.data, nil
}

func (r *Stack[T]) Len() uint64 {
	return r.count
}

func (r *Stack[T]) Empty() bool {
	return r.head == nil
}
