package bench

import (
	"context"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/bench/pkg/config"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"sync"
)

var _repl *pb.Replication
var _replM sync.RWMutex

func WatchReplicationMeta(ctx context.Context, conf *config.Config, apiClient pb.ChorusClient) (<-chan error, error) {
	req := &pb.ReplicationRequest{
		User:   "admin",
		Bucket: conf.Bucket,
		From:   "one",
		To:     "two",
	}
	stream, err := apiClient.StreamBucketReplication(ctx, req)
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
				stream, err = apiClient.StreamBucketReplication(ctx, req)
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
