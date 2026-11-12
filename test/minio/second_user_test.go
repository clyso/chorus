/*
 * Copyright © 2026 Clyso GmbH
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

package minio

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/minio/madmin-go/v4"
	mclient "github.com/minio/minio-go/v7"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/s3"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/test/app"
)

// Test_e2e_proxy_second_user_bucket_migration: a bucket-level migration is
// started for the bucket owner, and a second storage user with access to the
// same bucket writes through the proxy. Its writes must be captured as
// replication events - during and after the initial migration - when its
// credential is configured as an alias of the replication user.
func Test_e2e_proxy_second_user_bucket_migration(t *testing.T) {
	r := require.New(t)
	ctx := t.Context()
	bucket := bucketName("second-user")

	// second MinIO user with access to the bucket only
	secondCred := s3.CredentialsV4{
		AccessKeyID:     "second-" + xid.New().String(),
		SecretAccessKey: "second-secret-" + xid.New().String(),
	}
	adm, err := madmin.New(minioEndpoint, storageCreds.AccessKeyID, storageCreds.SecretAccessKey, false)
	r.NoError(err)
	r.NoError(adm.AddUser(ctx, secondCred.AccessKeyID, secondCred.SecretAccessKey))
	policyName := "bucket-" + bucket
	r.NoError(adm.AddCannedPolicy(ctx, policyName, fmt.Appendf(nil, `{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:*"],
    "Resource": ["arn:aws:s3:::%[1]s", "arn:aws:s3:::%[1]s/*"]
  }]
}`, bucket)))
	_, err = adm.AttachPolicy(ctx, madmin.PolicyAssociationReq{Policies: []string{policyName}, User: secondCred.AccessKeyID})
	r.NoError(err)

	// proxy knows the second user as an alias of the replication user
	pc, err := app.DeepCopyStruct(proxyConf)
	r.NoError(err)
	pc.Storage.Storages[minioKey].S3.Credentials[user]["second"] = secondCred
	e := app.SetupChorus(t, workerConf, pc)

	// existing bucket with data, owned by the replication user
	r.NoError(minioClient.MakeBucket(ctx, bucket, mclient.MakeBucketOptions{}))
	put := func(c *mclient.Client, name string) {
		t.Helper()
		data := bytes.Repeat([]byte(name), 128)
		// unsigned payload: the proxy cannot re-sign aws-chunked streaming uploads to MinIO
		_, err := c.PutObject(ctx, bucket, name, bytes.NewReader(data), int64(len(data)), mclient.PutObjectOptions{DisableContentSha256: true})
		r.NoError(err)
	}
	for i := range 20 {
		put(minioClient, fmt.Sprintf("existing-%d", i))
	}

	replID := &pb.ReplicationID{
		User:        user,
		FromStorage: minioKey,
		FromBucket:  &bucket,
		ToStorage:   followerKey,
		ToBucket:    &bucket,
	}
	_, err = e.PolicyClient.AddReplication(ctx, &pb.AddReplicationRequest{Id: replID})
	r.NoError(err)

	secondClient, err := newS3Client(e.ProxyAddr, secondCred)
	r.NoError(err)

	// write during initial migration
	put(secondClient, "second-during-init")

	r.Eventually(func() bool {
		repl, err := e.PolicyClient.GetReplication(ctx, replID)
		return err == nil && repl.IsInitDone
	}, e.WaitLong, e.RetryLong, "initial migration did not finish")

	// write after initial migration: only an event can replicate it
	put(secondClient, "second-after-init")

	r.Eventually(func() bool {
		repl, err := e.PolicyClient.GetReplication(ctx, replID)
		return err == nil && repl.Events == 2 && repl.EventsDone == repl.Events
	}, e.WaitLong, e.RetryLong, "writes of the second user were not captured as replication events")

	for _, obj := range []string{"second-during-init", "second-after-init", "existing-0"} {
		requireSameObject(t, minioClient, followerClient, bucket, obj)
	}
}
