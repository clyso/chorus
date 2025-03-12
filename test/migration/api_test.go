package migration

import (
	"testing"

	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

func Test_api_storages(t *testing.T) {
	r := require.New(t)
	res, err := apiClient.GetStorages(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(res.Storages, 3)
}

func Test_api_proxy_creds(t *testing.T) {
	r := require.New(t)
	res, err := apiClient.GetProxyCredentials(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Contains(res.Address, "localhost")
	r.Len(res.Credentials, 1)
	r.EqualValues(res.Credentials[0].Alias, user)
	r.NotEmpty(res.Credentials[0].AccessKey)
	r.NotEmpty(res.Credentials[0].SecretKey)
	r.EqualValues(res.Credentials[0].AccessKey, proxyConf.Storage.Storages[proxyConf.Auth.UseStorage].Credentials[user].AccessKeyID)
	r.EqualValues(res.Credentials[0].SecretKey, proxyConf.Storage.Storages[proxyConf.Auth.UseStorage].Credentials[user].SecretAccessKey)
}

func Test_api_list_replications(t *testing.T) {
	r := require.New(t)
	err := proxyClient.MakeBucket(tstCtx, "replications", mclient.MakeBucketOptions{})
	r.NoError(err)
	defer func() {
		proxyClient.RemoveBucket(tstCtx, "replications")
	}()
	_, err = apiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		User:            user,
		From:            "main",
		To:              "f1",
		Buckets:         []string{"replications"},
		IsForAllBuckets: false,
	})
	r.NoError(err)
	defer func() {
		apiClient.DeleteReplication(tstCtx, &pb.ReplicationRequest{
			User:   user,
			From:   "main",
			To:     "f1",
			Bucket: "replications",
		})
	}()
	res, err := apiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(res.Replications, 1)
	r.EqualValues("replications", res.Replications[0].Bucket)
	r.EqualValues(user, res.Replications[0].User)
	r.EqualValues("main", res.Replications[0].From)
	r.EqualValues("f1", res.Replications[0].To)
}

func Test_api_get_replication(t *testing.T) {
	r := require.New(t)
	err := proxyClient.MakeBucket(tstCtx, "replications", mclient.MakeBucketOptions{})
	r.NoError(err)
	defer func() {
		proxyClient.RemoveBucket(tstCtx, "replications")
	}()
	_, err = apiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		User:            user,
		From:            "main",
		To:              "f1",
		Buckets:         []string{"replications"},
		IsForAllBuckets: false,
	})
	r.NoError(err)
	defer func() {
		apiClient.DeleteReplication(tstCtx, &pb.ReplicationRequest{
			User:   user,
			From:   "main",
			To:     "f1",
			Bucket: "replications",
		})
	}()
	res, err := apiClient.GetReplication(tstCtx, &pb.ReplicationRequest{
		User:   user,
		From:   "main",
		To:     "f1",
		Bucket: "replications",
	})
	r.NoError(err)
	r.EqualValues("replications", res.Bucket)
	r.EqualValues(user, res.User)
	r.EqualValues("main", res.From)
	r.EqualValues("f1", res.To)
}