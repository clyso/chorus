package migration

import (
	"net/http"
	"net/http/httputil"
	"testing"

	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/test/env"
)

func Test_api_storages(t *testing.T) {
	e, _, _ := env.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	r := require.New(t)
	res, err := e.ApiClient.GetStorages(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(res.Storages, 3)
	resp, err := http.Get(e.UrlHttpApi + "/storage")
	r.NoError(err)
	r.EqualValues(http.StatusOK, resp.StatusCode)
	r.Positive(resp.ContentLength)

	_, err = httputil.DumpResponse(resp, true)
	r.NoError(err)
}

func Test_api_proxy_creds(t *testing.T) {
	e, _, newProxyConf := env.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	r := require.New(t)
	res, err := e.ApiClient.GetProxyCredentials(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Contains(res.Address, "localhost")
	r.Len(res.Credentials, 1)
	r.EqualValues(res.Credentials[0].Alias, user)
	r.NotEmpty(res.Credentials[0].AccessKey)
	r.NotEmpty(res.Credentials[0].SecretKey)
	r.EqualValues(res.Credentials[0].AccessKey, newProxyConf.Storage.Storages[newProxyConf.Auth.UseStorage].Credentials[user].AccessKeyID)
	r.EqualValues(res.Credentials[0].SecretKey, newProxyConf.Storage.Storages[newProxyConf.Auth.UseStorage].Credentials[user].SecretAccessKey)
}

func Test_api_list_replications(t *testing.T) {
	e, _, _ := env.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	r := require.New(t)
	err := e.ProxyClient.MakeBucket(tstCtx, "replications", mclient.MakeBucketOptions{})
	r.NoError(err)
	_, err = e.ApiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		User:            user,
		From:            "main",
		To:              "f1",
		Buckets:         []string{"replications"},
		IsForAllBuckets: false,
	})
	r.NoError(err)
	res, err := e.ApiClient.ListReplications(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(res.Replications, 1)
	r.EqualValues("replications", res.Replications[0].Bucket)
	r.EqualValues(user, res.Replications[0].User)
	r.EqualValues("main", res.Replications[0].From)
	r.EqualValues("f1", res.Replications[0].To)
}

func Test_api_get_replication(t *testing.T) {
	e, _, _ := env.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	r := require.New(t)
	err := e.ProxyClient.MakeBucket(tstCtx, "replications", mclient.MakeBucketOptions{})
	r.NoError(err)
	defer func() {
		e.ProxyClient.RemoveBucket(tstCtx, "replications")
	}()
	_, err = e.ApiClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		User:            user,
		From:            "main",
		To:              "f1",
		Buckets:         []string{"replications"},
		IsForAllBuckets: false,
	})
	r.NoError(err)
	defer func() {
		e.ApiClient.DeleteReplication(tstCtx, &pb.ReplicationRequest{
			User:     user,
			From:     "main",
			To:       "f1",
			Bucket:   "replications",
			ToBucket: "replications",
		})
	}()
	res, err := e.ApiClient.GetReplication(tstCtx, &pb.ReplicationRequest{
		User:     user,
		From:     "main",
		To:       "f1",
		Bucket:   "replications",
		ToBucket: "replications",
	})
	r.NoError(err)
	r.EqualValues("replications", res.Bucket)
	r.EqualValues(user, res.User)
	r.EqualValues("main", res.From)
	r.EqualValues("f1", res.To)
}
