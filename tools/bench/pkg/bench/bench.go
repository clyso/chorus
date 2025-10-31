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
	"context"
	"io"
	"strconv"
	"strings"
	"time"

	mclient "github.com/minio/minio-go/v7"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/tools/bench/pkg/config"
	"github.com/clyso/chorus/tools/bench/pkg/db"
	"github.com/clyso/chorus/tools/bench/pkg/dump"
)

func Benchmark(ctx context.Context, conf *config.Config, kv *db.DB, main, proxy s3client.Client, queue <-chan struct{}) <-chan error {
	done := make(chan error, 1)
	go func() {
		defer close(done)
		g, ctx := errgroup.WithContext(ctx)
		listProxyCh := make(chan int64, 1)
		defer close(listProxyCh)
		g.Go(func() error {
			return listenListBenchmark(ctx, "proxy", conf, proxy, listProxyCh)
		})
		listMainCh := make(chan int64, 1)
		defer close(listMainCh)
		g.Go(func() error {
			return listenListBenchmark(ctx, "main", conf, main, listMainCh)
		})
		getProxyCh := make(chan int64, 1)
		defer close(getProxyCh)
		g.Go(func() error {
			return listenGetBenchmark(ctx, "proxy", conf, proxy, getProxyCh)
		})
		getMainCh := make(chan int64, 1)
		defer close(getMainCh)
		g.Go(func() error {
			return listenGetBenchmark(ctx, "main", conf, main, getMainCh)
		})

		for {
			select {
			case <-ctx.Done():
				logrus.WithError(ctx.Err()).Info("bench: ctx cancelled: stop")
				return
			case _, more := <-queue:
				if !more {
					logrus.Info("bench: queue closed: stop")
					return
				}

				count, err := kv.GetInt(db.ObjCount)
				if err != nil {
					logrus.WithError(err).Error("bench: db get count err")
					done <- err
					return
				}
				// start all benchmarks in parallel
				getProxyCh <- count
				getMainCh <- count
				listProxyCh <- count
				listMainCh <- count
				logrus.Infof("bench for count %d started", count)
			}
		}

	}()
	return done
}

func listenListBenchmark(ctx context.Context, name string, conf *config.Config, client s3client.Client, listen <-chan int64) error {
	writeFileCh := make(chan dump.BenchEvent)
	defer close(writeFileCh)
	writeFileDone := dump.ToCSVNonBlocking(conf, benchFileName(name+"_LIST", conf), []string{"COUNT", "LIST_COUNT", "START_TS_US", "END_TS_US", "DURATION_US", "EVENTS", "EVENTS_DONE", "EVENTS_LAG"}, writeFileCh)
	for {
		select {
		case writeErr := <-writeFileDone:
			logrus.WithError(writeErr).Info("list bench: write file done")
			return writeErr
		case count, more := <-listen:
			if !more {
				logrus.Info("list bench: events closed: stop")
				return nil
			}
			repl := GetReplication()
			before := time.Now().UnixMicro()
			countRes, err := countObjects(ctx, client, conf.Bucket, conf.ListMax)
			after := time.Now().UnixMicro()
			if err != nil {
				logrus.WithError(err).Errorf("list object count %d error", count)
				return err
			}
			writeFileCh <- dump.BenchEvent{
				Count: count,
				Data: []string{
					strconv.Itoa(int(count)),
					strconv.Itoa(int(countRes)),
					strconv.Itoa(int(before)),
					strconv.Itoa(int(after)),
					strconv.Itoa(int(after - before)),
					strconv.Itoa(int(repl.Events)),
					strconv.Itoa(int(repl.EventsDone)),
					strconv.Itoa(int(repl.Events - repl.EventsDone)),
				},
			}
		}
	}
}

func listenGetBenchmark(ctx context.Context, name string, conf *config.Config, client s3client.Client, listen <-chan int64) error {
	writeFileCh := make(chan dump.BenchEvent)
	defer close(writeFileCh)
	writeFileDone := dump.ToCSVNonBlocking(conf, benchFileName(name+"_GET", conf), []string{"COUNT", "START_TS_US", "END_TS_US", "DURATION_US", "EVENTS", "EVENTS_DONE", "EVENTS_LAG"}, writeFileCh)

	for {
		select {
		case writeErr := <-writeFileDone:
			logrus.WithError(writeErr).Info("get bench: write file done")
			return writeErr
		case count, more := <-listen:
			if !more {
				logrus.Info("get bench: events closed: stop")
				return nil
			}
			repl := GetReplication()
			before := time.Now().UnixMicro()
			obj, err := client.S3().GetObject(ctx, conf.Bucket, benchObjName(count), mclient.GetObjectOptions{})
			_, _ = io.ReadAll(obj)
			_ = obj.Close()
			after := time.Now().UnixMicro()
			if err != nil {
				logrus.WithError(err).Errorf("get object %s error", benchObjName(count))
				return err
			}
			writeFileCh <- dump.BenchEvent{
				Count: count,
				Data: []string{
					strconv.Itoa(int(count)),
					strconv.Itoa(int(before)),
					strconv.Itoa(int(after)),
					strconv.Itoa(int(after - before)),
					strconv.Itoa(int(repl.Events)),
					strconv.Itoa(int(repl.EventsDone)),
					strconv.Itoa(int(repl.Events - repl.EventsDone)),
				},
			}
		}
	}
}

func countObjects(ctx context.Context, c s3client.Client, bucket string, max int64) (int64, error) {
	var res int64
	objCh := c.S3().ListObjects(ctx, bucket, mclient.ListObjectsOptions{MaxKeys: int(max)})
	for {
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		case obj, more := <-objCh:
			if !more || res >= max {
				return res, nil
			}
			if obj.Err != nil {
				logrus.WithError(obj.Err).Errorf("list count obj err on %d", res)
				if strings.Contains(obj.Err.Error(), "header you provided implies functionality that is not implemented") {
					return res, nil
				}
				return res, obj.Err
			}
			res++
		}
	}
}
