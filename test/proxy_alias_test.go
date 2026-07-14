package test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	mclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/s3"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/test/app"
)

// newAliasProxyClient creates minio clients for the proxy signed with the
// given credential without any readiness checks.
func newAliasProxyClient(t *testing.T, proxyAddr string, cred s3.CredentialsV4) (*mclient.Client, *mclient.Core) {
	t.Helper()
	r := require.New(t)
	addr := strings.TrimPrefix(proxyAddr, "http://")
	mc, err := mclient.New(addr, &mclient.Options{
		Creds: credentials.NewStaticV4(cred.AccessKeyID, cred.SecretAccessKey, ""),
	})
	r.NoError(err)
	core, err := mclient.NewCore(addr, &mclient.Options{
		Creds: credentials.NewStaticV4(cred.AccessKeyID, cred.SecretAccessKey, ""),
	})
	r.NoError(err)
	return mc, core
}

// TestProxyAlias_CustomAuth verifies that a nested auth.custom credential
// authenticates the caller as (user, alias) and that requests are forwarded
// re-signed with the storage credential of the same alias. Also verifies that
// an unknown access key is rejected.
func TestProxyAlias_CustomAuth(t *testing.T) {
	customCred := s3.CredentialsV4{
		AccessKeyID:     "CUSTOMALIASACCESSKEY",
		SecretAccessKey: "customAliasSecretKey0000000000000000000",
	}
	// custom credential authenticates as user "test" alias "default":
	// the pair must exist in the main storage static config
	proxyConf.Auth.Custom = map[string]map[string]s3.CredentialsV4{
		"test": {"default": customCred},
	}
	t.Cleanup(func() { proxyConf.Auth.Custom = nil })

	e := app.SetupEmbedded(t, workerConf, proxyConf)
	e.CreateMainFollowerUserReplications(t)
	tstCtx := t.Context()
	r := require.New(t)
	bucket := "proxy-alias-custom"

	customClient, _ := newAliasProxyClient(t, proxyConf.Address, customCred)
	err := customClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{})
	r.NoError(err)

	// bucket is created on main and replicated to followers
	r.Eventually(func() bool {
		for _, c := range []*mclient.Client{e.MainClient, e.F1Client, e.F2Client} {
			ok, err := c.BucketExists(tstCtx, bucket)
			if err != nil || !ok {
				return false
			}
		}
		return true
	}, e.WaitShort, e.RetryShort)

	// object written with the custom alias key replicates to followers
	objName := "obj"
	payload := bytes.Repeat([]byte("c"), 1024)
	_, err = customClient.PutObject(tstCtx, bucket, objName, bytes.NewReader(payload), int64(len(payload)), mclient.PutObjectOptions{})
	r.NoError(err)
	r.Eventually(func() bool {
		for _, c := range []*mclient.Client{e.MainClient, e.F1Client, e.F2Client} {
			if _, err := c.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{}); err != nil {
				return false
			}
		}
		return true
	}, e.WaitShort, e.RetryShort)

	// unknown access key is rejected
	bogusClient, _ := newAliasProxyClient(t, proxyConf.Address, s3.CredentialsV4{
		AccessKeyID:     "BOGUSACCESSKEY123456",
		SecretAccessKey: "bogusSecretKey0000000000000000000000000",
	})
	err = bogusClient.MakeBucket(tstCtx, "proxy-alias-bogus", mclient.MakeBucketOptions{})
	r.Error(err)
	errResp := mclient.ToErrorResponse(err)
	r.EqualValues("InvalidAccessKeyId", errResp.Code)
}

// TestProxyAlias_DynamicCredentials verifies the dynamic path: a second alias
// added via the management API authenticates at the proxy after the poll
// interval, replication stays keyed by user, and multipart uploads work under
// an alias.
func TestProxyAlias_DynamicCredentials(t *testing.T) {
	prevWorkerDC := workerConf.Storage.DynamicCredentials
	prevProxyDC := proxyConf.Storage.DynamicCredentials
	dc := objstore.DynamicCredentialsConfig{
		Enabled:           true,
		DisableEncryption: true,
		PollInterval:      300 * time.Millisecond,
	}
	workerConf.Storage.DynamicCredentials = dc
	proxyConf.Storage.DynamicCredentials = dc
	t.Cleanup(func() {
		workerConf.Storage.DynamicCredentials = prevWorkerDC
		proxyConf.Storage.DynamicCredentials = prevProxyDC
	})

	e := app.SetupEmbedded(t, workerConf, proxyConf)
	e.CreateMainFollowerUserReplications(t)
	tstCtx := t.Context()
	r := require.New(t)
	user := "test"
	alias := "laptop"

	// add a second alias for the user on every storage via the management API
	aliasCreds := map[string]s3.CredentialsV4{
		"main": {AccessKeyID: "LAPTOPMAINACCESSKEY0", SecretAccessKey: "laptopMainSecretKey00000000000000000000"},
		"f1":   {AccessKeyID: "LAPTOPF1ACCESSKEY000", SecretAccessKey: "laptopF1SecretKey0000000000000000000000"},
		"f2":   {AccessKeyID: "LAPTOPF2ACCESSKEY000", SecretAccessKey: "laptopF2SecretKey0000000000000000000000"},
	}
	for storage, cred := range aliasCreds {
		_, err := e.ChorusClient.SetUserCredentials(tstCtx, &pb.SetUserCredentialsRequest{
			Storage: storage,
			User:    user,
			S3Cred: &pb.S3Credential{
				AccessKey: cred.AccessKeyID,
				SecretKey: cred.SecretAccessKey,
			},
			Alias: &alias,
		})
		r.NoError(err)
	}

	// alias must not be combined with swift credentials
	_, err := e.ChorusClient.SetUserCredentials(tstCtx, &pb.SetUserCredentialsRequest{
		Storage: "main",
		User:    user,
		SwiftCred: &pb.SwiftCredential{
			Username: "u",
			Password: "p",
		},
		Alias: &alias,
	})
	r.Error(err)

	// wait past the poll interval until the proxy accepts the new key
	aliasClient, aliasCore := newAliasProxyClient(t, proxyConf.Address, aliasCreds["main"])
	bucket := "proxy-alias-dynamic"
	r.Eventually(func() bool {
		return aliasClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{}) == nil
	}, e.WaitShort, e.RetryLong)

	// bucket replicates to followers (replication is keyed by user, not alias)
	r.Eventually(func() bool {
		for _, c := range []*mclient.Client{e.MainClient, e.F1Client, e.F2Client} {
			ok, err := c.BucketExists(tstCtx, bucket)
			if err != nil || !ok {
				return false
			}
		}
		return true
	}, e.WaitShort, e.RetryShort)

	// object written with the alias key replicates to followers
	objName := "obj"
	payload := bytes.Repeat([]byte("d"), 1024)
	_, err = aliasClient.PutObject(tstCtx, bucket, objName, bytes.NewReader(payload), int64(len(payload)), mclient.PutObjectOptions{})
	r.NoError(err)
	r.Eventually(func() bool {
		for _, c := range []*mclient.Client{e.MainClient, e.F1Client, e.F2Client} {
			if _, err := c.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{}); err != nil {
				return false
			}
		}
		return true
	}, e.WaitShort, e.RetryShort)

	// multipart upload through the proxy under the alias:
	// the multipart namespace is keyed by user and unaffected by alias
	mpObjName := "obj-mp"
	uploadID, err := aliasCore.NewMultipartUpload(tstCtx, bucket, mpObjName, mclient.PutObjectOptions{DisableContentSha256: true})
	r.NoError(err)
	partData := bytes.Repeat([]byte("e"), 1024*1024)
	part, err := aliasCore.PutObjectPart(tstCtx, bucket, mpObjName, uploadID, 1,
		bytes.NewReader(partData), int64(len(partData)), mclient.PutObjectPartOptions{})
	r.NoError(err)
	_, err = aliasCore.CompleteMultipartUpload(tstCtx, bucket, mpObjName, uploadID,
		[]mclient.CompletePart{{PartNumber: 1, ETag: part.ETag}}, mclient.PutObjectOptions{})
	r.NoError(err)
	r.Eventually(func() bool {
		for _, c := range []*mclient.Client{e.MainClient, e.F1Client, e.F2Client} {
			if _, err := c.StatObject(tstCtx, bucket, mpObjName, mclient.StatObjectOptions{}); err != nil {
				return false
			}
		}
		return true
	}, e.WaitShort, e.RetryShort)
}

// TestProxyAlias_CrossStorageForwarding verifies the cross-storage alias join:
// a request authenticated with the main-storage alias key and routed to
// another storage is re-signed with that storage's credential of the same
// alias.
func TestProxyAlias_CrossStorageForwarding(t *testing.T) {
	prevWorkerDC := workerConf.Storage.DynamicCredentials
	prevProxyDC := proxyConf.Storage.DynamicCredentials
	dc := objstore.DynamicCredentialsConfig{
		Enabled:           true,
		DisableEncryption: true,
		PollInterval:      300 * time.Millisecond,
	}
	workerConf.Storage.DynamicCredentials = dc
	proxyConf.Storage.DynamicCredentials = dc
	t.Cleanup(func() {
		workerConf.Storage.DynamicCredentials = prevWorkerDC
		proxyConf.Storage.DynamicCredentials = prevProxyDC
	})

	e := app.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	r := require.New(t)
	user := "test"
	alias := "laptop"

	aliasCreds := map[string]s3.CredentialsV4{
		"main": {AccessKeyID: "LAPTOPMAINACCESSKEY0", SecretAccessKey: "laptopMainSecretKey00000000000000000000"},
		"f1":   {AccessKeyID: "LAPTOPF1ACCESSKEY000", SecretAccessKey: "laptopF1SecretKey0000000000000000000000"},
	}
	for storage, cred := range aliasCreds {
		_, err := e.ChorusClient.SetUserCredentials(tstCtx, &pb.SetUserCredentialsRequest{
			Storage: storage,
			User:    user,
			S3Cred: &pb.S3Credential{
				AccessKey: cred.AccessKeyID,
				SecretKey: cred.SecretAccessKey,
			},
			Alias: &alias,
		})
		r.NoError(err)
	}

	// route the bucket to storage f1
	bucket := "proxy-alias-routed"
	_, err := e.PolicyClient.AddRouting(tstCtx, &pb.AddRoutingRequest{
		User:      user,
		Bucket:    &bucket,
		ToStorage: "f1",
	})
	r.NoError(err)

	// create the bucket signed with the main-storage alias key: the proxy
	// authenticates against main and forwards to f1 with the f1 alias cred
	aliasClient, _ := newAliasProxyClient(t, proxyConf.Address, aliasCreds["main"])
	r.Eventually(func() bool {
		return aliasClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{}) == nil
	}, e.WaitShort, e.RetryLong)

	// bucket exists on f1 and not on main
	ok, err := e.F1Client.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(ok)
	ok, err = e.MainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)
}
