package rclone

import (
	"github.com/clyso/chorus/pkg/util"
)

const MB = 1000000

var benchmark = [][]int64{
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
	if fileSize < 2*MB {
		res = 2 * MB
	} else if fileSize < 50*MB {
		res = fileSize
	} else {
		res = 50 * MB
	}
	return int64(m.memMul*float64(res)) + m.memConst
}
