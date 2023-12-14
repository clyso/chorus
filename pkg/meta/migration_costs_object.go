package meta

import (
	"fmt"
	"strconv"
	"time"
)

type MigrationCosts interface {
	Done() bool
	BucketsNum() int64
	ObjectsNum() int64
	ObjectsSize() int64
	StartedAt() *time.Time

	String() string
}

type tMap map[string]string

func (m tMap) GetInt(key string) int {
	str, ok := m[key]
	if !ok {
		return 0
	}
	res, _ := strconv.Atoi(str)
	return res
}

func (m tMap) GetInt64(key string) int64 {
	str, ok := m[key]
	if !ok {
		return 0
	}
	res, _ := strconv.Atoi(str)
	return int64(res)
}

func (m tMap) GetBool(key string) bool {
	str, ok := m[key]
	if !ok {
		return false
	}
	res, _ := strconv.ParseBool(str)
	return res
}

func (m tMap) GetTime(key string) *time.Time {
	str, ok := m[key]
	if !ok {
		return nil
	}
	ts, err := strconv.Atoi(str)
	if err != nil {
		return nil
	}
	res := time.UnixMilli(int64(ts))
	return &res
}

var _ MigrationCosts = &migrationCosts{}

type migrationCosts struct {
	src tMap
}

func (m migrationCosts) Done() bool {
	return m.src.GetInt("jobStarted") == m.src.GetInt("jobDone")
}

func (m migrationCosts) BucketsNum() int64 {
	return m.src.GetInt64("bucketNum")
}

func (m migrationCosts) ObjectsNum() int64 {
	return m.src.GetInt64("objNum")
}

func (m migrationCosts) ObjectsSize() int64 {
	return m.src.GetInt64("objSize")
}

func (m migrationCosts) String() string {
	return fmt.Sprintf("%+v", m.src)
}

func (m migrationCosts) StartedAt() *time.Time {
	return m.src.GetTime("startedAt")
}
