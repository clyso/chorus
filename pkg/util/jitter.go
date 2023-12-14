package util

import (
	"math/rand"
	"time"
)

func DurationJitter(min, max time.Duration) time.Duration {
	maxSec := int(max.Seconds())
	if maxSec == 0 {
		maxSec = 1
	}
	minSec := int(min.Seconds())
	sec := rand.Intn(maxSec-minSec+1) + minSec
	return time.Duration(sec) * time.Second
}
