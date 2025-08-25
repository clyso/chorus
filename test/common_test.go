package test

import (
	"bytes"
	"math/rand"
)

type testObj struct {
	name   string
	data   []byte
	bucket string
}

func getTestObj(name, bucket string) testObj {
	return testObj{
		name:   name,
		data:   bytes.Repeat([]byte(name[len(name)-1:]), rand.Intn(1<<20)+32*1024),
		bucket: bucket,
	}
}
