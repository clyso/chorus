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
	"math/rand"
)

type Rnd struct {
	rnd        *rand.Rand
	boolSource int64
	boolRemain int
}

func NewRnd(seed int64) *Rnd {
	return &Rnd{
		rnd: rand.New(rand.NewSource(seed)),
	}
}

func (r *Rnd) Int64InRange(min int64, max int64) int64 {
	return r.rnd.Int63n(max-min+1) + min
}

func (r *Rnd) IntInRange(min int, max int) int {
	return r.rnd.Intn(max-min+1) + min
}

func (r *Rnd) Bool() bool {
	if r.boolRemain == 0 {
		r.boolSource = r.rnd.Int63()
		r.boolRemain = 63
	}

	val := r.boolSource & 0x1
	r.boolSource >>= 1
	r.boolRemain--

	return val == 1
}

func (r *Rnd) Int64() int64 {
	return r.rnd.Int63()
}

func (r *Rnd) Read(p []byte) {
	r.rnd.Read(p)
}
