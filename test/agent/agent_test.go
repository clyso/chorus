//go:build agent

package agent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

const waitInterval = 5 * time.Second
const retryInterval = 100 * time.Millisecond

func Test_api_storages(t *testing.T) {
	t.Skip()
	bucket := "init-bucketasdf"
	r := require.New(t)

	err := mainClient.RemoveAllBucketNotification(tstCtx, bucket)
	r.NoError(err)

	r.Eventually(func() bool {
		agents, err := apiClient.GetAgents(tstCtx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		return len(agents.Agents) != 0
	}, waitInterval, retryInterval)

	agents, err := apiClient.GetAgents(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(agents.Agents, 1)
	r.EqualValues(agents.Agents[0].Storage, "main")

	repl, err := apiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Empty(repl.Replications)

	_, err = apiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		User:            user,
		From:            "main",
		To:              "f1",
		Buckets:         []string{bucket},
		IsForAllBuckets: false,
		AgentUrl:        &agents.Agents[0].Url,
	})
	r.NoError(err)

	r.Eventually(func() bool {
		ex, err := f1Client.BucketExists(tstCtx, bucket)
		return err == nil && ex
	}, waitInterval, retryInterval)
	time.Sleep(time.Second * 3)

	for {
		src, err := mpMainClient.ListObjects(bucket, "", "", "", 1_000)
		r.NoError(err)
		dest, err := mpF1Client.ListObjects(bucket, "", "", "", 1_000)
		r.NoError(err)
		t.Log(len(src.Contents), len(dest.Contents))
		time.Sleep(time.Second * 3)
	}

}
