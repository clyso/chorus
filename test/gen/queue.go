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
	"errors"
)

var (
	ErrEmptyQueue = errors.New("queue is empty")
)

type QueueNode[T any] struct {
	data T
	next *QueueNode[T]
}

type Queue[T any] struct {
	head  *QueueNode[T]
	tail  *QueueNode[T]
	count uint64
}

func NewQueue[T any]() *Queue[T] {
	return &Queue[T]{}
}

func (r *Queue[T]) Enqueue(data T) {
	node := &QueueNode[T]{
		data: data,
	}
	if r.head == nil && r.tail == nil {
		r.head = node
		r.tail = node
		return
	}
	r.tail.next = node
	r.tail = node
	r.count++
}

func (r *Queue[T]) Dequeue() (T, error) {
	if r.head == nil {
		var noVal T
		return noVal, ErrEmptyQueue
	}
	data := r.head.data
	r.head = r.head.next
	if r.head == nil {
		r.tail = nil
	}
	r.count--
	return data, nil
}

func (r *Queue[T]) Len() uint64 {
	return r.count
}

func (r *Queue[T]) Empty() bool {
	return r.head == nil
}
