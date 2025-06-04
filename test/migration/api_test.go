package migration

import (
	"testing"

	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/clyso/chorus/pkg/dom"
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
	defer cleanup("replications")
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

func Test_api_CompareBucketErrorConditions(t *testing.T) {
	r := require.New(t)
	bucketSrc01 := "src-cb-test01"
	bucketDst01 := "dst-cb-test01"
	bucketSrc02 := "src-cb-test02"
	bucketDst02 := "dst-cb-test02"

	err := proxyClient.MakeBucket(tstCtx, bucketSrc01, mclient.MakeBucketOptions{})
	r.NoError(err)
	err = proxyClient.MakeBucket(tstCtx, bucketDst01, mclient.MakeBucketOptions{})
	r.NoError(err)
	err = proxyClient.MakeBucket(tstCtx, bucketSrc02, mclient.MakeBucketOptions{})
	r.NoError(err)
	err = proxyClient.MakeBucket(tstCtx, bucketDst02, mclient.MakeBucketOptions{})
	r.NoError(err)
	defer func() {
		proxyClient.RemoveBucket(tstCtx, bucketSrc01)
		proxyClient.RemoveBucket(tstCtx, bucketDst01)
		proxyClient.RemoveBucket(tstCtx, bucketSrc02)
		proxyClient.RemoveBucket(tstCtx, bucketDst02)
	}()

	t.Run("single replication", func(t *testing.T) {
		r := require.New(t)
		_, err = apiClient.AddBucketReplication(tstCtx, &pb.AddBucketReplicationRequest{
			User:        user,
			FromStorage: "main",
			ToStorage:   "f1",
			FromBucket:  bucketSrc01,
		})
		r.NoError(err)
		t.Cleanup(func() {
			apiClient.DeleteReplication(tstCtx, &pb.ReplicationRequest{
				User:   user,
				From:   "main",
				Bucket: bucketSrc01,
				To:     "f1",
			})
		})

		res, err := apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    bucketSrc01,
			From:      "main",
			To:        "f1",
			ShowMatch: true,
			User:      user,
		})
		r.NoError(err)
		r.True(res.IsMatch)

		res, err = apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    bucketSrc02,
			From:      "main",
			To:        "f1",
			ShowMatch: true,
			User:      user,
			ToBucket:  &bucketSrc01,
		})
		r.NoError(err)
		r.True(res.IsMatch)
	})

	t.Run("two replications with unambiguous destination buckets", func(t *testing.T) {
		r := require.New(t)
		_, err = apiClient.AddBucketReplication(tstCtx, &pb.AddBucketReplicationRequest{
			User:        user,
			FromStorage: "main",
			ToStorage:   "f1",
			FromBucket:  bucketSrc01,
		})
		r.NoError(err)
		t.Cleanup(func() {
			apiClient.DeleteReplication(tstCtx, &pb.ReplicationRequest{
				User:   user,
				From:   "main",
				Bucket: bucketSrc01,
				To:     "f1",
			})
		})

		_, err = apiClient.AddBucketReplication(tstCtx, &pb.AddBucketReplicationRequest{
			User:        user,
			FromStorage: "main",
			ToStorage:   "f2",
			FromBucket:  bucketSrc01,
		})
		r.NoError(err)
		t.Cleanup(func() {
			apiClient.DeleteReplication(tstCtx, &pb.ReplicationRequest{
				User:   user,
				From:   "main",
				Bucket: bucketSrc01,
				To:     "f2",
			})
		})

		res, err := apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    bucketSrc01,
			From:      "main",
			To:        "f1",
			ShowMatch: true,
			User:      user,
		})
		r.NoError(err)
		r.True(res.IsMatch)

		res, err = apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    bucketSrc01,
			From:      "main",
			To:        "f2",
			ShowMatch: true,
			User:      user,
		})
		r.NoError(err)
		r.True(res.IsMatch)
		res, err = apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    bucketSrc01,
			From:      "main",
			To:        "f1",
			ShowMatch: true,
			User:      user,
			ToBucket:  &bucketSrc01,
		})
		r.NoError(err)
		r.True(res.IsMatch)

		res, err = apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    bucketSrc01,
			From:      "main",
			To:        "f2",
			ShowMatch: true,
			User:      user,
			ToBucket:  &bucketSrc01,
		})
		r.NoError(err)
		r.True(res.IsMatch)
	})

	t.Run("two replications with ambiguous destination buckets", func(t *testing.T) {
		r := require.New(t)
		_, err = apiClient.AddBucketReplication(tstCtx, &pb.AddBucketReplicationRequest{
			User:        user,
			FromStorage: "main",
			ToStorage:   "f1",
			FromBucket:  bucketSrc01,
		})
		r.NoError(err)
		t.Cleanup(func() {
			apiClient.DeleteReplication(tstCtx, &pb.ReplicationRequest{
				User:   user,
				From:   "main",
				Bucket: bucketSrc01,
				To:     "f1",
			})
		})

		_, err = apiClient.AddBucketReplication(tstCtx, &pb.AddBucketReplicationRequest{
			User:        user,
			FromStorage: "main",
			ToStorage:   "f1",
			FromBucket:  bucketSrc01,
			ToBucket:    &bucketDst01,
		})
		r.NoError(err)
		t.Cleanup(func() {
			apiClient.DeleteReplication(tstCtx, &pb.ReplicationRequest{
				User:     user,
				From:     "main",
				Bucket:   bucketSrc01,
				To:       "f1",
				ToBucket: &bucketDst01,
			})
		})

		_, err := apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    bucketSrc01,
			From:      "main",
			To:        "f1",
			ShowMatch: true,
			User:      user,
		})
		r.Error(err)
		r.Equal(codes.FailedPrecondition, status.Code(err))
		r.Equal(dom.ErrAmbiguousDestination.Error(), status.Convert(err).Message())

		_, err = apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    bucketSrc01,
			From:      "main",
			To:        "f1",
			ShowMatch: true,
			User:      user,
			ToBucket:  &bucketDst01,
		})
		r.NoError(err)
	})

	t.Run("compare without replication", func(t *testing.T) {
		r := require.New(t)
		_, err := apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    bucketSrc01,
			From:      "main",
			To:        "f1",
			ShowMatch: true,
			User:      user,
		})
		r.Error(err)
		r.Equal(codes.FailedPrecondition, status.Code(err))
		r.Equal(dom.ErrUnknownDestination.Error(), status.Convert(err).Message())

		_, err = apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    bucketSrc01,
			From:      "main",
			To:        "f1",
			ShowMatch: true,
			User:      user,
			ToBucket:  &bucketDst01,
		})
		r.NoError(err)
	})

	t.Run("compare request does not match replications", func(t *testing.T) {
		r := require.New(t)
		_, err = apiClient.AddBucketReplication(tstCtx, &pb.AddBucketReplicationRequest{
			User:        user,
			FromStorage: "main",
			ToStorage:   "f1",
			FromBucket:  bucketSrc01,
		})
		r.NoError(err)
		t.Cleanup(func() {
			apiClient.DeleteReplication(tstCtx, &pb.ReplicationRequest{
				User:   user,
				From:   "main",
				Bucket: bucketSrc01,
				To:     "f1",
			})
		})
		_, err = apiClient.AddBucketReplication(tstCtx, &pb.AddBucketReplicationRequest{
			User:        user,
			FromStorage: "main",
			ToStorage:   "f2",
			FromBucket:  bucketSrc02,
		})
		r.NoError(err)
		t.Cleanup(func() {
			apiClient.DeleteReplication(tstCtx, &pb.ReplicationRequest{
				User:   user,
				From:   "main",
				Bucket: bucketSrc02,
				To:     "f2",
			})
		})

		_, err := apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    bucketSrc01,
			From:      "main",
			To:        "f2",
			ShowMatch: true,
			User:      user,
		})
		r.Error(err)
		r.Equal(codes.FailedPrecondition, status.Code(err))
		r.Equal(dom.ErrUnknownDestination.Error(), status.Convert(err).Message())

		_, err = apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    bucketSrc02,
			From:      "main",
			To:        "f1",
			ShowMatch: true,
			User:      user,
		})
		r.Error(err)
		r.Equal(codes.FailedPrecondition, status.Code(err))
		r.Equal(dom.ErrUnknownDestination.Error(), status.Convert(err).Message())

		_, err = apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    bucketSrc01,
			From:      "main",
			To:        "f1",
			ShowMatch: true,
			User:      user,
		})
		r.NoError(err)

		_, err = apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    bucketSrc02,
			From:      "main",
			To:        "f2",
			ShowMatch: true,
			User:      user,
		})
		r.NoError(err)
	})
	t.Run("compare with non-existent FromBucket", func(t *testing.T) {
		r := require.New(t)
		nonExistentBucket := "non-existent-bucket"
		res, err := apiClient.CompareBucket(tstCtx, &pb.CompareBucketRequest{
			Bucket:    nonExistentBucket,
			From:      "main",
			To:        "f1",
			ShowMatch: true,
			User:      user,
			ToBucket:  &nonExistentBucket,
		})
		r.NoError(err)
		r.False(res.IsMatch)
		r.Equal(0, len(res.Match))
		r.Equal(0, len(res.MissFrom))
		r.Equal(0, len(res.MissTo))
		r.Equal(0, len(res.Differ))
		r.Equal(0, len(res.Error))
	})
}
