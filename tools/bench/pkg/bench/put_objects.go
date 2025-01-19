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

package bench

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	mclient "github.com/minio/minio-go/v7"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/util"
	"github.com/clyso/chorus/tools/bench/pkg/config"
	"github.com/clyso/chorus/tools/bench/pkg/db"
	"github.com/clyso/chorus/tools/bench/pkg/dump"
)

type Prev struct {
	Bucket  string
	ObjSize int64
	ObjNum  int64
}

func PutObjects(ctx context.Context, conf *config.Config, kv *db.DB, proxy s3client.Client) (<-chan struct{}, <-chan error, error) {
	logger := logrus.WithFields(logrus.Fields{
		"bucket":         conf.Bucket,
		"obj_size":       util.ByteCountIEC(conf.ObjSize),
		"total_objects":  conf.TotalObj,
		"parallel_write": conf.ParallelWrites,
	})
	objBytes, err := createBytes(conf.ObjSize)
	if err != nil {
		return nil, nil, err
	}

	fileName := benchFileName("PUT", conf)
	writeFileCh := make(chan dump.BenchEvent, conf.ParallelWrites)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		// spawn file writer worker
		select {
		case <-ctx.Done():
			return nil
		case writeErr := <-dump.ToCSVNonBlocking(conf, fileName, []string{"COUNT", "START_TS_US", "END_TS_US", "DURATION_US", "EVENTS", "EVENTS_DONE", "EVENTS_LAG"}, writeFileCh):
			return writeErr
		}
	})

	done := make(chan error, 1)
	benchmarkQueue := make(chan struct{}, 100)
	cntAtomic := atomic.Int64{}
	cntAtomic.Store(conf.LastCount)
	for i := 0; i < int(conf.ParallelWrites); i++ {
		// spawn put object workers:
		g.Go(func() error {
			cnt := cntAtomic.Add(1)
			errCount := 0
			prevQueue := len(benchmarkQueue)
			for cnt <= conf.TotalObj {
				if ctx.Err() != nil {
					logrus.Info("put Object: ctx cancelled: stop")
					return ctx.Err()
				}
				objName := benchObjName(cnt)
				logger := logger.WithFields(logrus.Fields{"obj": objName, "count": cnt})

				startTs := time.Now().UnixMicro()
				_, err = proxy.S3().PutObject(ctx, conf.Bucket, objName, bytes.NewReader(objBytes), conf.ObjSize, mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
				endTs := time.Now().UnixMicro()
				if err != nil {
					errCount++
					logger.WithError(err).WithField("err_count", errCount).Error("writer: write obj error")
					if errCount > 10 {
						return err
					}
					continue
				}
				repl := GetReplication()
				errCount = 0
				err = kv.PutInt(db.ObjCount, cnt)
				if err != nil {
					logger.WithError(err).Error("writer: persist counter error")
				}
				select {
				case <-ctx.Done():
					return nil
				case writeFileCh <- dump.BenchEvent{
					Count: cnt,
					Data: []string{
						strconv.Itoa(int(cnt)),
						strconv.Itoa(int(startTs)),
						strconv.Itoa(int(endTs)),
						strconv.Itoa(int(endTs - startTs)),
						strconv.Itoa(int(repl.Events)),
						strconv.Itoa(int(repl.EventsDone)),
						strconv.Itoa(int(repl.Events - repl.EventsDone)),
					},
				}:
				}

				// start benchmark event
				if cnt%conf.MeasureEvery == 0 {
					select {
					case <-ctx.Done():
						return nil
					case benchmarkQueue <- struct{}{}:
					}

					currQueue := len(benchmarkQueue)
					logger.Infof("writer: EVENTS IN QUEUE %d, measure every %d objects", currQueue, conf.MeasureEvery)
					// adjust benchmark frequency if there are too much objects in the queue
					if currQueue >= prevQueue && currQueue > 1 {
						logrus.Info("try inc bench interval")
						conf.IncInterval()
						prevQueue = currQueue
					}
					if currQueue == 0 {
						logrus.Info("try dec bench interval")
						conf.DecInterval()
					}
				}
				cnt = cntAtomic.Add(1)
			}
			return nil
		})
	}

	go func() {
		defer close(done)
		defer close(writeFileCh)
		defer close(benchmarkQueue)
		<-ctx.Done()
	}()

	return benchmarkQueue, done, nil
}

var rndReader = rand.New(rand.NewSource(time.Now().UnixNano()))

func createBytes(size int64) ([]byte, error) {
	lr := io.LimitReader(rndReader, size)
	// make a buffer to keep chunks that are read
	buf := make([]byte, size)
	_, err := lr.Read(buf)
	return buf, err
}

func benchFileName(name string, conf *config.Config) string {
	return fmt.Sprintf("bench_%s_%s_P%d_%d.csv", name, strings.ReplaceAll(util.ByteCountIEC(conf.ObjSize), " ", ""), conf.ParallelWrites, conf.StartedTs)
}

func benchObjName(count int64) string {
	return fmt.Sprintf("bench_obj_%08d", count)
}
