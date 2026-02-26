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
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/bench/pkg/config"
)

var _repl *pb.Replication
var _replM sync.RWMutex

func WatchReplicationMeta(ctx context.Context, conf *config.Config, apiClient pb.PolicyClient) (<-chan error, error) {
	req := &pb.ReplicationID{
		User:        "admin",
		FromBucket:  &conf.Bucket,
		ToBucket:    &conf.Bucket,
		FromStorage: "one",
		ToStorage:   "two",
	}
	stream, err := apiClient.StreamReplication(ctx, req)
	if err != nil {
		return nil, err
	}
	_repl, err = stream.Recv()
	if err != nil {
		return nil, err
	}
	done := make(chan error, 1)
	go func() {
		defer close(done)
		for {
			if ctx.Err() != nil {
				logrus.Info("api watcher: ctx cancelled: stop")
				_ = stream.CloseSend()
				return
			}
			res, err := stream.Recv()
			if err != nil {
				if ctx.Err() != nil {
					logrus.Info("api watcher: ctx cancelled: stop")
					_ = stream.CloseSend()
					return
				}
				logrus.WithError(err).Error("received replication stream error. Reconnect...")
				_ = stream.CloseSend()
				stream, err = apiClient.StreamReplication(ctx, req)
				if err != nil {
					done <- err
					return
				}
				continue
			}
			_replM.Lock()
			_repl = res
			_replM.Unlock()
			logrus.Debug("api watcher: updated")
		}
	}()
	return done, nil
}

func GetReplication() *pb.Replication {
	_replM.RLock()
	defer _replM.RUnlock()
	res := proto.Clone(_repl)
	return res.(*pb.Replication)
}
