package migration

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/clyso/chorus/pkg/s3"

	"github.com/clyso/chorus/pkg/objstore"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/test/app"
	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

func Test_Dynamic_creds(t *testing.T) {
	r := require.New(t)
	wc, err := app.DeepCopyStruct(workerConf)
	r.NoError(err)
	// enable dynamic creds with encryption
	dcInterval := 100 * time.Millisecond
	wc.Storage.DynamicCredentials = objstore.DynamicCredentialsConfig{
		MasterPassword:    "test-master-password",
		PollInterval:      dcInterval,
		Enabled:           true,
		DisableEncryption: false,
	}
	pc, err := app.DeepCopyStruct(proxyConf)
	r.NoError(err)
	pc.Storage.DynamicCredentials = wc.Storage.DynamicCredentials
	e := app.SetupEmbedded(t, wc, pc)
	tstCtx := t.Context()

	// verify existing storages and creds
	storResp, err := e.ChorusClient.GetStorages(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(storResp.Storages, 3)
	for _, st := range storResp.Storages {
		r.EqualValues(pb.Storage_S3, st.Provider)
		r.Len(st.Credentials, 1)
		cred := st.Credentials[0]
		r.EqualValues(user, cred.Alias)
	}

	// add new user to main storage dynamically
	newUser := "new-user"
	newCred := &pb.S3Credential{
		AccessKey: "new-access-key",
		SecretKey: "new-secret-key",
	}
	_, err = e.ChorusClient.SetUserCredentials(tstCtx, &pb.SetUserCredentialsRequest{
		Storage: "unknown-storage",
		User:    newUser,
		S3Cred:  newCred,
	})
	r.Error(err, "setting creds for unknown storage should fail")

	_, err = e.ChorusClient.SetUserCredentials(tstCtx, &pb.SetUserCredentialsRequest{
		Storage: "main",
		User:    "",
		S3Cred:  newCred,
	})
	r.Error(err, "user name is required")

	_, err = e.ChorusClient.SetUserCredentials(tstCtx, &pb.SetUserCredentialsRequest{
		Storage: "main",
		User:    newUser,
		S3Cred:  nil,
	})
	r.Error(err, "s3 credentials are required")

	_, err = e.ChorusClient.SetUserCredentials(tstCtx, &pb.SetUserCredentialsRequest{
		Storage: "main",
		User:    user,
		S3Cred:  newCred,
	})
	r.Error(err, "cannot overwrite user from yaml config")

	_, err = e.ChorusClient.SetUserCredentials(tstCtx, &pb.SetUserCredentialsRequest{
		Storage: "main",
		User:    newUser,
		S3Cred:  newCred,
	})
	r.NoError(err, "success")

	time.Sleep(dcInterval * 2)
	// verify new user creds are in effect
	storResp, err = e.ChorusClient.GetStorages(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(storResp.Storages, 3)
	for _, st := range storResp.Storages {
		r.EqualValues(pb.Storage_S3, st.Provider)
		if st.Name != "main" {
			r.Len(st.Credentials, 1)
		} else {
			r.Len(st.Credentials, 2)
			for _, cred := range st.Credentials {
				if cred.Alias == newUser {
					r.EqualValues(newCred.AccessKey, cred.AccessKey)
				} else {
					r.EqualValues(user, cred.Alias)
					r.NotEqual(newCred.AccessKey, cred.AccessKey)
				}
			}
		}
	}

	// cannot start migration because new user only in main storage
	_, err = e.PolicyClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		Id: &pb.ReplicationID{
			User:        newUser,
			FromStorage: "main",
			ToStorage:   "f1",
		},
		Opts: &pb.ReplicationOpts{},
	})
	r.Error(err, "cannot start migration with missing creds in target storage")

	// add creds for new user to f1 storage
	_, err = e.ChorusClient.SetUserCredentials(tstCtx, &pb.SetUserCredentialsRequest{
		Storage: "f1",
		User:    newUser,
		S3Cred:  newCred,
	})
	r.NoError(err, "success")

	time.Sleep(dcInterval * 2)
	// verify new user creds are in effect
	storResp, err = e.ChorusClient.GetStorages(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	r.Len(storResp.Storages, 3)
	mainAddr, f1addr := "", ""
	for _, st := range storResp.Storages {
		r.EqualValues(pb.Storage_S3, st.Provider)
		if st.Name == "f2" {
			r.Len(st.Credentials, 1)
		} else {
			// main and f1
			r.Len(st.Credentials, 2)
			if st.Name == "main" {
				mainAddr = st.Address
			} else if st.Name == "f1" {
				f1addr = st.Address
			}
			for _, cred := range st.Credentials {
				if cred.Alias == newUser {
					r.EqualValues(newCred.AccessKey, cred.AccessKey)
				} else {
					r.EqualValues(user, cred.Alias)
					r.NotEqual(newCred.AccessKey, cred.AccessKey)
				}
			}
		}
	}
	// create s3 clients with new user creds
	proxyAddr, err := e.ChorusClient.GetProxyCredentials(tstCtx, &emptypb.Empty{})
	r.NoError(err)
	proxyClient, _ := app.CreateClient(t, s3.StorageAddress{
		Address:  proxyAddr.Address,
		Provider: "Other",
		IsSecure: false,
	}, s3.CredentialsV4{
		AccessKeyID:     newCred.AccessKey,
		SecretAccessKey: newCred.SecretKey,
	})
	mainClient, _ := app.CreateClient(t, s3.StorageAddress{
		Address:  mainAddr,
		Provider: "Other",
		IsSecure: false,
	}, s3.CredentialsV4{
		AccessKeyID:     newCred.AccessKey,
		SecretAccessKey: newCred.SecretKey,
	})
	f1Client, _ := app.CreateClient(t, s3.StorageAddress{
		Address:  f1addr,
		Provider: "Other",
		IsSecure: false,
	}, s3.CredentialsV4{
		AccessKeyID:     newCred.AccessKey,
		SecretAccessKey: newCred.SecretKey,
	})

	// create 2 buckets with files in main storage
	b1, b2 := "dyncreds-bucket-1", "dyncreds-bucket-2"
	err = mainClient.MakeBucket(tstCtx, b1, mclient.MakeBucketOptions{})
	r.NoError(err)
	err = proxyClient.MakeBucket(tstCtx, b2, mclient.MakeBucketOptions{})
	r.NoError(err)
	ok, err := mainClient.BucketExists(tstCtx, b2)
	r.NoError(err)
	r.True(ok)
	ok, err = proxyClient.BucketExists(tstCtx, b1)
	r.NoError(err)
	r.True(ok)

	//upload 1 file to each bucket using main
	obj1 := getTestObj("obj1", b1)
	_, err = mainClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)
	obj2 := getTestObj("photo/sept/obj2", b2)
	_, err = mainClient.PutObject(tstCtx, obj2.bucket, obj2.name, bytes.NewReader(obj2.data), int64(len(obj2.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream"})
	r.NoError(err)

	// read objects using proxy
	pObj1, err := proxyClient.GetObject(tstCtx, obj1.bucket, obj1.name, mclient.GetObjectOptions{})
	r.NoError(err)
	pObj1Bytes, err := io.ReadAll(pObj1)
	r.NoError(err)
	r.True(bytes.Equal(obj1.data, pObj1Bytes))

	pObj2, err := proxyClient.GetObject(tstCtx, obj2.bucket, obj2.name, mclient.GetObjectOptions{})
	r.NoError(err)
	pObj2Bytes, err := io.ReadAll(pObj2)
	r.NoError(err)
	r.True(bytes.Equal(obj2.data, pObj2Bytes))

	// buckets not exist yet in f1
	buckets, err := f1Client.ListBuckets(tstCtx)
	r.NoError(err)
	r.Empty(buckets)

	// create replication for b1 from main to f1
	id1 := &pb.ReplicationID{
		FromBucket:  &b1,
		ToBucket:    &b1,
		FromStorage: "main",
		ToStorage:   "f1",
		User:        newUser,
	}

	ur, err := e.PolicyClient.ListReplications(tstCtx, &pb.ListReplicationsRequest{})
	r.NoError(err)
	r.Empty(ur.Replications)

	bfr, err := e.PolicyClient.AvailableBuckets(tstCtx, &pb.AvailableBucketsRequest{
		User:           newUser,
		FromStorage:    "main",
		ToStorage:      "f1",
		ShowReplicated: true,
	})
	r.NoError(err)
	r.Empty(bfr.ReplicatedBuckets)
	r.Len(bfr.Buckets, 2)
	r.ElementsMatch([]string{b1, b2}, bfr.Buckets)

	_, err = e.PolicyClient.AddReplication(tstCtx, &pb.AddReplicationRequest{
		Id: id1,
	})
	r.NoError(err)

	// get replication
	rep1, err := e.PolicyClient.GetReplication(tstCtx, id1)
	r.NoError(err)
	r.EqualValues(newUser, rep1.Id.User)

	r.Eventually(func() bool {
		rep1, err = e.PolicyClient.GetReplication(tstCtx, id1)
		if err != nil {
			return false
		}
		return rep1.IsInitDone
	}, e.WaitLong, e.RetryLong)

	diff := replicationDiff(t, e, id1)
	r.True(diff.IsMatch)
	r.Empty(diff.Differ)
	r.Empty(diff.MissFrom)
	r.Empty(diff.MissTo)

	// get obj1 from f1
	f1Obj1, err := f1Client.GetObject(tstCtx, obj1.bucket, obj1.name, mclient.GetObjectOptions{})
	r.NoError(err)
	f1Obj1Bytes, err := io.ReadAll(f1Obj1)
	r.NoError(err)
	r.True(bytes.Equal(obj1.data, f1Obj1Bytes))
	// obj2 should not exist yet in f1
	f1Obj2, err := f1Client.GetObject(tstCtx, obj2.bucket, obj2.name, mclient.GetObjectOptions{})
	r.NoError(err)
	_, err = f1Obj2.Stat()
	r.Error(err)
}
