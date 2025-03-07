/*
 * Copyright © 2025 Clyso GmbH
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

package migration

import (
	"bytes"
	"context"
	"math/rand"
	"testing"
	"time"

	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
)

const (
	ConsistencyWait    = 20 * time.Second
	ConsistencyRetryIn = 500 * time.Millisecond

	ConsistencyCheckStorage1 = "main"
	ConsistencyCheckStorage2 = "f1"

	ConsistencyCheckBucket1 = "consistency-bucket1"
	ConsistencyCheckBucket2 = "consistency-bucket2"

	ConsistencyCheckObject1 = "path/to/object1"
	ConsistencyCheckObject2 = "path/to/object2"
	ConsistencyCheckObject3 = "path/object3"
	ConsistencyCheckObject4 = "object4"
	ConsistencyCheckDir1    = "empty/dir"
)

func consistencyCheckFillUpStorages(ctx context.Context, r *require.Assertions, client *minio.Client, bucketName string, objectSeed []byte) {
	err := client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
	r.NoError(err)

	objectNames := []string{ConsistencyCheckObject1, ConsistencyCheckObject2, ConsistencyCheckObject3, ConsistencyCheckObject4}
	for _, objectName := range objectNames {
		uploadInfo, err := client.PutObject(ctx, bucketName, objectName, bytes.NewReader(objectSeed), int64(len(objectSeed)), minio.PutObjectOptions{
			ContentType: "binary/octet-stream", DisableContentSha256: true,
		})

		r.NoError(err)
		r.EqualValues(objectName, uploadInfo.Key)
		r.EqualValues(bucketName, uploadInfo.Bucket)
	}

	uploadInfo, err := client.PutObject(ctx, bucketName, ConsistencyCheckDir1, bytes.NewReader([]byte{}), 0, minio.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})

	r.NoError(err)
	r.EqualValues(ConsistencyCheckDir1, uploadInfo.Key)
	r.EqualValues(bucketName, uploadInfo.Bucket)
}

func consistencyCheckSetup2Storages(ctx context.Context, r *require.Assertions) {
	objectSeed := bytes.Repeat([]byte("3"), rand.Intn(1<<20)+32*1024)
	consistencyCheckFillUpStorages(ctx, r, mainClient, ConsistencyCheckBucket1, objectSeed)
	consistencyCheckFillUpStorages(ctx, r, f1Client, ConsistencyCheckBucket2, objectSeed)
}

func consistencyCheckTeardown2Storages(ctx context.Context, checkIDs ...string) {
	cleanup(ConsistencyCheckBucket1, ConsistencyCheckBucket2)

	for _, checkID := range checkIDs {
		_, _ = apiClient.DeleteConsistencyCheckReport(ctx, &pb.DeleteConsistencyCheckReportRequest{Id: checkID})
	}
}

func TestConsistency_2Storages_Success(t *testing.T) {
	r := require.New(t)
	ctx := context.WithoutCancel(tstCtx)
	consistencyCheckSetup2Storages(ctx, r)
	defer consistencyCheckTeardown2Storages(ctx)

	checkRequest := &pb.StartConsistencyCheckRequest{
		Locations: []*pb.MigrateLocation{
			{
				Storage: ConsistencyCheckStorage1,
				Bucket:  ConsistencyCheckBucket1,
				User:    user,
			},
			{
				Storage: ConsistencyCheckStorage2,
				Bucket:  ConsistencyCheckBucket2,
				User:    user,
			},
		},
	}
	startCheckResponse, err := apiClient.StartConsistencyCheck(ctx, checkRequest)
	r.NoError(err)
	r.NotNil(startCheckResponse)
	r.NotEmpty(startCheckResponse.Id)

	var getCheckResponse *pb.GetConsistencyCheckReportResponse
	r.Eventually(func() bool {
		getCheckResponse, err = apiClient.GetConsistencyCheckReport(ctx, &pb.GetConsistencyCheckReportRequest{Id: startCheckResponse.Id})
		if err != nil {
			return false
		}
		if getCheckResponse == nil {
			return false
		}
		if getCheckResponse.Check == nil {
			return false
		}
		if !getCheckResponse.Check.Ready {
			return false
		}
		return true
	}, ConsistencyWait, ConsistencyRetryIn)

	r.Equal(len(getCheckResponse.Entries), 0)
}

func TestConsistency_2Storages_NoObject_Failure(t *testing.T) {
	r := require.New(t)
	ctx := context.WithoutCancel(tstCtx)
	consistencyCheckSetup2Storages(ctx, r)
	defer consistencyCheckTeardown2Storages(ctx)

	err := mainClient.RemoveObject(ctx, ConsistencyCheckBucket1, ConsistencyCheckObject3, minio.RemoveObjectOptions{})
	r.NoError(err)

	checkRequest := &pb.StartConsistencyCheckRequest{
		Locations: []*pb.MigrateLocation{
			{
				Storage: ConsistencyCheckStorage1,
				Bucket:  ConsistencyCheckBucket1,
				User:    user,
			},
			{
				Storage: ConsistencyCheckStorage2,
				Bucket:  ConsistencyCheckBucket2,
				User:    user,
			},
		},
	}
	startCheckResponse, err := apiClient.StartConsistencyCheck(ctx, checkRequest)
	r.NoError(err)
	r.NotNil(startCheckResponse)
	r.NotEmpty(startCheckResponse.Id)

	var getCheckResponse *pb.GetConsistencyCheckReportResponse
	r.Eventually(func() bool {
		getCheckResponse, err = apiClient.GetConsistencyCheckReport(ctx, &pb.GetConsistencyCheckReportRequest{Id: startCheckResponse.Id})
		if err != nil {
			return false
		}
		if getCheckResponse == nil {
			return false
		}
		if getCheckResponse.Check == nil {
			return false
		}
		if !getCheckResponse.Check.Ready {
			return false
		}
		return true
	}, ConsistencyWait, ConsistencyRetryIn)

	r.Equal(len(getCheckResponse.Entries), 1)
	r.NotContains(getCheckResponse.Entries[0].Storages, ConsistencyCheckStorage1)
	r.Contains(getCheckResponse.Entries[0].Storages, ConsistencyCheckStorage2)
	r.Equal(getCheckResponse.Entries[0].Object, ConsistencyCheckObject3)
}

func TestConsistency_2Storages_NoDir_Failure(t *testing.T) {
	r := require.New(t)
	ctx := context.WithoutCancel(tstCtx)
	consistencyCheckSetup2Storages(ctx, r)
	defer consistencyCheckTeardown2Storages(ctx)

	err := mainClient.RemoveObject(ctx, ConsistencyCheckBucket1, "/path/to", minio.RemoveObjectOptions{ForceDelete: true})
	r.NoError(err)

	checkRequest := &pb.StartConsistencyCheckRequest{
		Locations: []*pb.MigrateLocation{
			{
				Storage: ConsistencyCheckStorage1,
				Bucket:  ConsistencyCheckBucket1,
				User:    user,
			},
			{
				Storage: ConsistencyCheckStorage2,
				Bucket:  ConsistencyCheckBucket2,
				User:    user,
			},
		},
	}
	startCheckResponse, err := apiClient.StartConsistencyCheck(ctx, checkRequest)
	r.NoError(err)
	r.NotNil(startCheckResponse)
	r.NotEmpty(startCheckResponse.Id)

	var getCheckResponse *pb.GetConsistencyCheckReportResponse
	r.Eventually(func() bool {
		getCheckResponse, err = apiClient.GetConsistencyCheckReport(ctx, &pb.GetConsistencyCheckReportRequest{Id: startCheckResponse.Id})
		if err != nil {
			return false
		}
		if getCheckResponse == nil {
			return false
		}
		if getCheckResponse.Check == nil {
			return false
		}
		if !getCheckResponse.Check.Ready {
			return false
		}
		return true
	}, ConsistencyWait, ConsistencyRetryIn)

	r.Equal(len(getCheckResponse.Entries), 2)
	r.NotContains(getCheckResponse.Entries[0].Storages, ConsistencyCheckStorage1)
	r.NotContains(getCheckResponse.Entries[1].Storages, ConsistencyCheckStorage1)
	r.Contains(getCheckResponse.Entries[0].Storages, ConsistencyCheckStorage2)
	r.Contains(getCheckResponse.Entries[1].Storages, ConsistencyCheckStorage2)
	r.Contains([]string{ConsistencyCheckObject1, ConsistencyCheckObject2}, getCheckResponse.Entries[0].Object)
	r.Contains([]string{ConsistencyCheckObject1, ConsistencyCheckObject2}, getCheckResponse.Entries[1].Object)
}

func TestConsistency_2Storages_NoEmptyDir_Failure(t *testing.T) {
	r := require.New(t)
	ctx := context.WithoutCancel(tstCtx)
	consistencyCheckSetup2Storages(ctx, r)
	defer consistencyCheckTeardown2Storages(ctx)

	err := mainClient.RemoveObject(ctx, ConsistencyCheckBucket1, ConsistencyCheckDir1, minio.RemoveObjectOptions{})
	r.NoError(err)

	checkRequest := &pb.StartConsistencyCheckRequest{
		Locations: []*pb.MigrateLocation{
			{
				Storage: ConsistencyCheckStorage1,
				Bucket:  ConsistencyCheckBucket1,
				User:    user,
			},
			{
				Storage: ConsistencyCheckStorage2,
				Bucket:  ConsistencyCheckBucket2,
				User:    user,
			},
		},
	}
	startCheckResponse, err := apiClient.StartConsistencyCheck(ctx, checkRequest)
	r.NoError(err)
	r.NotNil(startCheckResponse)
	r.NotEmpty(startCheckResponse.Id)

	var getCheckResponse *pb.GetConsistencyCheckReportResponse
	r.Eventually(func() bool {
		getCheckResponse, err = apiClient.GetConsistencyCheckReport(ctx, &pb.GetConsistencyCheckReportRequest{Id: startCheckResponse.Id})
		if err != nil {
			return false
		}
		if getCheckResponse == nil {
			return false
		}
		if getCheckResponse.Check == nil {
			return false
		}
		if !getCheckResponse.Check.Ready {
			return false
		}
		return true
	}, ConsistencyWait, ConsistencyRetryIn)

	r.Equal(len(getCheckResponse.Entries), 1)
	r.NotContains(getCheckResponse.Entries[0].Storages, ConsistencyCheckStorage1)
	r.Contains(getCheckResponse.Entries[0].Storages, ConsistencyCheckStorage2)
}

func TestConsistency_2Storages_WrongEtag_Failure(t *testing.T) {
	r := require.New(t)
	ctx := context.WithoutCancel(tstCtx)
	consistencyCheckSetup2Storages(ctx, r)
	defer consistencyCheckTeardown2Storages(ctx)

	objectSeed := bytes.Repeat([]byte("3"), rand.Intn(1<<20)+32*1024)
	uploadInfo, err := mainClient.PutObject(ctx, ConsistencyCheckBucket1, ConsistencyCheckObject1, bytes.NewReader(objectSeed), int64(len(objectSeed)), minio.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)
	r.EqualValues(ConsistencyCheckObject1, uploadInfo.Key)
	r.EqualValues(ConsistencyCheckBucket1, uploadInfo.Bucket)

	checkRequest := &pb.StartConsistencyCheckRequest{
		Locations: []*pb.MigrateLocation{
			{
				Storage: ConsistencyCheckStorage1,
				Bucket:  ConsistencyCheckBucket1,
				User:    user,
			},
			{
				Storage: ConsistencyCheckStorage2,
				Bucket:  ConsistencyCheckBucket2,
				User:    user,
			},
		},
	}
	startCheckResponse, err := apiClient.StartConsistencyCheck(ctx, checkRequest)
	r.NoError(err)
	r.NotNil(startCheckResponse)
	r.NotEmpty(startCheckResponse.Id)

	var getCheckResponse *pb.GetConsistencyCheckReportResponse
	r.Eventually(func() bool {
		getCheckResponse, err = apiClient.GetConsistencyCheckReport(ctx, &pb.GetConsistencyCheckReportRequest{Id: startCheckResponse.Id})
		if err != nil {
			return false
		}
		if getCheckResponse == nil {
			return false
		}
		if getCheckResponse.Check == nil {
			return false
		}
		if !getCheckResponse.Check.Ready {
			return false
		}
		return true
	}, ConsistencyWait, ConsistencyRetryIn)

	r.Equal(len(getCheckResponse.Entries), 1)
	r.Contains(getCheckResponse.Entries[0].Storages, ConsistencyCheckStorage1)
}
