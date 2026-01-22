package migration

import (
	"net/http"
	"net/http/httputil"
	"testing"

	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/test/app"
)

func Test_api_storages(t *testing.T) {
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	r := require.New(t)
	res, err := e.ChorusClient.GetStorages(tstCtx, &emptypb.Empty{})
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
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	r := require.New(t)
	res, err := e.ChorusClient.GetProxyCredentials(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Contains(res.Address, "127.0.0.1")
	r.Len(res.Credentials, 1)
	r.EqualValues(res.Credentials[0].Alias, user)
	r.NotEmpty(res.Credentials[0].AccessKey)
	r.NotEmpty(res.Credentials[0].SecretKey)
	r.EqualValues(res.Credentials[0].AccessKey, proxyConf.Storage.S3Storages()[proxyConf.Auth.UseStorage].Credentials[user].AccessKeyID)
	r.EqualValues(res.Credentials[0].SecretKey, proxyConf.Storage.S3Storages()[proxyConf.Auth.UseStorage].Credentials[user].SecretAccessKey)
}

func Test_api_list_replications(t *testing.T) {
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	r := require.New(t)
	bucket := "replications"

	testRes, err := e.PolicyClient.TestProxy(tstCtx, &pb.TestProxyRequest{
		User:   user,
		Bucket: bucket,
	})
	r.NoError(err)
	r.EqualValues("main", testRes.RouteToStorage)
	r.Empty(testRes.Replications)

	err = e.ProxyClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{})
	r.NoError(err)
	_, err = e.PolicyClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		Id: &pb.ReplicationID{
			User:        user,
			FromStorage: "main",
			ToStorage:   "f1",
			FromBucket:  &bucket,
			ToBucket:    &bucket,
		},
	})
	r.NoError(err)
	res, err := e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	r.Len(res.Replications, 1)
	r.EqualValues("replications", *res.Replications[0].Id.FromBucket)
	r.EqualValues("replications", *res.Replications[0].Id.ToBucket)
	r.EqualValues(user, res.Replications[0].Id.User)
	r.EqualValues("main", res.Replications[0].Id.FromStorage)
	r.EqualValues("f1", res.Replications[0].Id.ToStorage)

	testRes, err = e.PolicyClient.TestProxy(tstCtx, &pb.TestProxyRequest{
		User:   user,
		Bucket: bucket,
	})
	r.NoError(err)
	r.EqualValues("main", testRes.RouteToStorage)
	r.Len(testRes.Replications, 1)
	r.EqualValues("f1", testRes.Replications[0].ToStorage)

	testRes, err = e.PolicyClient.TestProxy(tstCtx, &pb.TestProxyRequest{
		User:   user,
		Bucket: "asdfas",
	})
	r.NoError(err)
	r.EqualValues("main", testRes.RouteToStorage)
	r.Empty(testRes.Replications)
}

func Test_api_get_replication(t *testing.T) {
	e := app.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	r := require.New(t)
	bucket := "replications"
	err := e.ProxyClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{})
	r.NoError(err)
	_, err = e.PolicyClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		Id: &pb.ReplicationID{
			User:        user,
			FromStorage: "main",
			ToStorage:   "f1",
			FromBucket:  &bucket,
			ToBucket:    &bucket,
		},
	})
	r.NoError(err)
	defer func() {
		e.PolicyClient.DeleteReplication(tstCtx, &pb.ReplicationID{
			User:        user,
			FromStorage: "main",
			ToStorage:   "f1",
			FromBucket:  &bucket,
			ToBucket:    &bucket,
		})
	}()
	res, err := e.PolicyClient.GetReplication(tstCtx, &pb.ReplicationID{
		User:        user,
		FromStorage: "main",
		ToStorage:   "f1",
		FromBucket:  &bucket,
		ToBucket:    &bucket,
	})
	r.NoError(err)
	r.EqualValues("replications", *res.Id.FromBucket)
	r.EqualValues("replications", *res.Id.ToBucket)
	r.EqualValues(user, res.Id.User)
	r.EqualValues("main", res.Id.FromStorage)
	r.EqualValues("f1", res.Id.ToStorage)
}
