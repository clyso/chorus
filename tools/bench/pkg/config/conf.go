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

package config

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	_ = 1 << (10 * iota)
	KiB
	MiB
	GiB
	TiB
)
const (
	_dbName         = "chorus-bench.db" //var/lib/chorus/bench.db
	_bucketName     = "bench-bucket9"
	_api            = "10.82.220.13:32270"
	_objSize        = 99 * KiB
	_objTotal       = 100_000 //0_000
	_measureEvery   = 30      //00
	_parallelWrites = 5
	_listMax        = 1_000
)

type Config struct {
	DB             string
	Bucket         string
	Api            string
	AccessKey      string
	SecretKey      string
	ObjSize        int64
	TotalObj       int64
	MeasureEvery   int64
	ParallelWrites int64
	ListMax        int64
	StartedTs      int64
	LastCount      int64
	_lastAdjusted  time.Time
	_m             sync.Mutex
}

func (c *Config) IncInterval() {
	c._m.Lock()
	defer c._m.Unlock()
	if c._lastAdjusted.IsZero() || time.Now().Sub(c._lastAdjusted) > time.Second*30 {
		c.MeasureEvery += 100
		c._lastAdjusted = time.Now()
		logrus.Infof("bench interval increased on 100 and equals %d", c.MeasureEvery)
	}
}

func (c *Config) DecInterval() {
	c._m.Lock()
	defer c._m.Unlock()
	if c.MeasureEvery-50 < _measureEvery {
		return
	}
	if c._lastAdjusted.IsZero() || time.Now().Sub(c._lastAdjusted) > time.Second*30 {
		c.MeasureEvery -= 50
		c._lastAdjusted = time.Now()
		logrus.Infof("bench interval decreased to 50 and equals %d", c.MeasureEvery)
	}
}

func Get() *Config {
	return &Config{
		DB:             getStr("CB_DB", _dbName, false),
		Bucket:         getStr("CB_BUCKET", _bucketName, false),
		Api:            getStr("CB_API", _api, false),
		AccessKey:      getStr("CB_ACCESS", "", true),
		SecretKey:      getStr("CB_SECRET", "", true),
		ObjSize:        getInt("CB_OBJ_SIZE", _objSize),
		TotalObj:       getInt("CB_OBJ_NUM", _objTotal),
		MeasureEvery:   getInt("CB_BENCH_EVERY", _measureEvery),
		ParallelWrites: getInt("CB_WRITE_N", _parallelWrites),
		ListMax:        getInt("CB_LIST_MAX", _listMax),
		StartedTs:      time.Now().Unix(),
	}
}

func getStr(envName, defaultVal string, needMask bool) string {
	envVal := os.Getenv(envName)
	if envVal == "" {
		logrus.Infof("env: %s not set: use default %s", envName, defaultVal)
		return defaultVal
	}
	logrus.Infof("env: use val from %s %s", envName, mask(envVal, needMask))
	return envVal
}

func getInt(envName string, defaultVal int64) int64 {
	envVal := os.Getenv(envName)
	if envVal == "" {
		logrus.Infof("env: %s not set: use default %d", envName, defaultVal)
		return defaultVal
	}
	val, err := strconv.Atoi(envVal)
	if err != nil {
		panic(fmt.Errorf("%v: unable to cast env var %s - %s to int", err, envName, envVal))
	}
	logrus.Infof("env: use val from %s %d", envName, val)
	return int64(val)
}

func mask(s string, do bool) string {
	if !do {
		return s
	}
	var sb strings.Builder
	for i := 0; i < len(s); i++ {
		if len(s) > 4 && (i == 0 || i == len(s)-1) {
			sb.WriteRune(rune(s[i]))
			continue
		}
		sb.WriteString("*")
	}
	return sb.String()
}
