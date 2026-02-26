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
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	mclient "github.com/minio/minio-go/v7"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/bench/pkg/config"
	"github.com/clyso/chorus/tools/bench/pkg/db"
)

func Start(conf *config.Config) error {
	logrus.SetLevel(logrus.InfoLevel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGTERM)
	go func() {
		sig := <-signals
		logrus.Info("received shutdown signal", sig.String())
		cancel()
	}()

	conn, err := connectApi(ctx, conf.Api)
	if err != nil {
		return fmt.Errorf("%w: unable to connect to api %s", err, conf.Api)
	}
	apiClient := pb.NewPolicyClient(conn)
	defer conn.Close()

	kv, err := db.New(conf.DB, false)
	if err != nil {
		return fmt.Errorf("%w: unable to open db", err)
	}
	defer kv.Close()

	ctx, main, proxy, err := createS3Clients(ctx, conf)
	if err != nil {
		return fmt.Errorf("%w: unable to create s3 clients bucket", err)
	}

	err = prepareBucket(ctx, conf, proxy, apiClient)
	if err != nil {
		return fmt.Errorf("%w: unable to prepare bucket", err)
	}

	// try recover benchmark state to resume
	err = restoreState(conf, kv)
	if err != nil {
		return fmt.Errorf("%w: unable to restore state", err)
	}

	// subscribe to replication status in Chorus api
	watchDone, err := WatchReplicationMeta(ctx, conf, apiClient)
	if err != nil {
		return fmt.Errorf("%w: unable to watch api", err)
	}

	// start writing objects to proxy
	benchmarkQueue, putDone, err := PutObjects(ctx, conf, kv, proxy)
	if err != nil {
		return fmt.Errorf("%w: unable to put objects", err)
	}

	// start benchmark worker:
	benchDone := Benchmark(ctx, conf, kv, main, proxy, benchmarkQueue)

	select {
	case <-ctx.Done(): // terminated by signal
		logrus.Info("received ctx cancel signal, wait for workers...")
		<-putDone
		logrus.Info("write done")
		<-benchDone
		logrus.Info("benchmarks done")
	case err = <-watchDone: // terminated by api watch error
		logrus.WithError(err).Error("watch api done, canceling workers")
		cancel()
		<-putDone
		logrus.Info("write done")
		<-benchDone
		logrus.Info("benchmarks done")
	case err = <-putDone: // terminated by write obj done/error
		logrus.WithError(err).Error("write done, canceling workers")
		cancel()
		<-benchDone
		logrus.Info("benchmarks done")
	case err = <-benchDone: // terminated by benchmarks error
		logrus.WithError(err).Error("benchmark api done, canceling workers")
		cancel()
		<-putDone
		logrus.Info("write done")
	}

	logrus.Info("exiting... waiting 1s for cleanup...")
	time.Sleep(time.Second * 1)

	if errors.Is(err, context.Canceled) {
		err = nil
	}
	return err
}

func connectApi(ctx context.Context, url string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, url,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		//grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.Config{MaxDelay: time.Second}}),
		//grpc.WithInsecure(),
	)
}

func restoreState(conf *config.Config, kv *db.DB) error {
	// recover previous values if presented
	prevBucket, err := kv.Get(db.Bucket)
	if err != nil {
		return err
	}
	prevSize, err := kv.GetInt(db.ObjSize)
	if err != nil {
		return err
	}
	prevParallel, err := kv.GetInt(db.Parallel)
	if err != nil {
		return err
	}

	if conf.Bucket == prevBucket && conf.ObjSize == prevSize && conf.ParallelWrites == prevParallel {
		logrus.Info("same bucket and obj size, recover last obj count")
		conf.LastCount, err = kv.GetInt(db.ObjCount)
		if err != nil {
			return err
		}
		started, err := kv.GetInt(db.Started)
		if err != nil {
			return err
		}
		if started != 0 {
			conf.StartedTs = started
		}
	}

	// update benchmark meta
	err = kv.Put(db.Bucket, conf.Bucket)
	if err != nil {
		return err
	}
	err = kv.PutInt(db.ObjSize, conf.ObjSize)
	if err != nil {
		return err
	}
	err = kv.PutInt(db.ObjTotal, conf.TotalObj)
	if err != nil {
		return err
	}
	err = kv.PutInt(db.Started, conf.StartedTs)
	if err != nil {
		return err
	}

	return kv.PutInt(db.Parallel, conf.ParallelWrites)
}

func prepareBucket(ctx context.Context, conf *config.Config, proxy s3client.Client, apiClient pb.PolicyClient) error {
	exists, err := proxy.S3().BucketExists(ctx, conf.Bucket)
	if err != nil {
		return err
	}
	if !exists {
		logrus.Infof("creating bucket %s...", conf.Bucket)
		err = proxy.S3().MakeBucket(ctx, conf.Bucket, mclient.MakeBucketOptions{})
		if err != nil {
			return err
		}
		time.Sleep(time.Second)
	}
	logrus.Infof("bucket %s exists", conf.Bucket)
	_, err = apiClient.AddReplication(ctx, &pb.AddReplicationRequest{
		Id: &pb.ReplicationID{
			FromStorage: "one",
			ToStorage:   "two",
			FromBucket:  &conf.Bucket,
			ToBucket:    &conf.Bucket,
			User:        "admin",
		},
	})
	return err
}

func createS3Clients(ctx context.Context, conf *config.Config) (newCtx context.Context, main s3client.Client, proxy s3client.Client, err error) {
	user := "user"
	newCtx = xctx.SetUser(ctx, user)
	proxy, err = s3client.NewClient(ctx, metrics.NewS3Service(false), s3.StorageAddress{
		Address:     "chorus-dev.clyso.cloud",
		Provider:    "Ceph",
		HttpTimeout: time.Minute * 2,
		IsSecure:    true,
	}, s3.CredentialsV4{
		AccessKeyID:     conf.AccessKey,
		SecretAccessKey: conf.SecretKey,
	}, "proxy", user)
	if err != nil {
		return
	}
	main, err = s3client.NewClient(ctx, metrics.NewS3Service(false), s3.StorageAddress{
		Address:     "s3.clyso.com",
		Provider:    "Ceph",
		HttpTimeout: time.Minute * 2,
		IsSecure:    true,
	}, s3.CredentialsV4{
		AccessKeyID:     conf.AccessKey,
		SecretAccessKey: conf.SecretKey,
	}, "main", user)
	if err != nil {
		return
	}
	return
}
