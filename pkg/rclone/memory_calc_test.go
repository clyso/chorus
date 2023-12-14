package rclone

import (
	"testing"
)

func Test_calcMemFromFileSize(t *testing.T) {
	t.Skip()
	tests := []struct {
		name     string
		fileSize int64
		want     int64
	}{
		{
			name:     "zero",
			fileSize: 0,
			want:     0 + 2*MB,
		},
		{
			name:     "zero",
			fileSize: 2,
			want:     2 + 2*MB,
		},
		{
			name:     "1mb",
			fileSize: benchmark[0][0],
			want:     benchmark[0][1],
		},
		{
			name:     "100mb",
			fileSize: 100 * MB,
			want:     27 * MB,
		},
		{
			name:     "max",
			fileSize: 100_0000 * MB,
			want:     benchmark[len(benchmark)-1][1],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &MemCalculator{memMul: 1.}
			if got := c.calcMemFromFileSize(tt.fileSize); got != tt.want {
				t.Errorf("calcMemFromFileSize() = %v, want %v", got, tt.want)
			}
		})
	}
}
