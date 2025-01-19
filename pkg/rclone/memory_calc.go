/*
 * Copyright © 2023 Clyso GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rclone

import (
	"github.com/clyso/chorus/pkg/util"
)

const MB = 1000000

//nolint:unused // TODO: Unless the global var is exported, it will be marked by linter as unused https://golangci-lint.run/usage/linters/#unused
var benchmark = [7][2]int64{
	{7 * MB, 9 * MB},
	{35 * MB, 17 * MB},
	{170 * MB, 27 * MB},
	{250 * MB, 39 * MB},
	{300 * MB, 44 * MB},
	{1000 * MB, 47 * MB},
	{2000 * MB, 50 * MB},
}

func NewMemoryCalculator(conf MemoryCalc) *MemCalculator {
	memMul := conf.Mul
	if memMul <= 0 {
		memMul = 1.
	}
	memConst, _ := util.ParseBytes(conf.Const)
	return &MemCalculator{
		memMul:   memMul,
		memConst: memConst,
	}
}

type MemCalculator struct {
	memMul   float64
	memConst int64
}

func (m MemCalculator) calcMemFromFileSize(fileSize int64) int64 {
	var res int64
	switch {
	case fileSize < 2*MB:
		res = 2 * MB
	case fileSize < 50*MB:
		res = fileSize
	default:
		res = 50 * MB
	}
	return int64(m.memMul*float64(res)) + m.memConst
}
