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
	"errors"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/clyso/chorus/pkg/api"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/minio/minio-go/v7"
	"github.com/redis/go-redis/v9"
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

func consistencyCheckTeardown2Storages(ctx context.Context, r *require.Assertions, locationsForChecks ...[]*pb.MigrateLocation) {
	cleanup(ConsistencyCheckBucket1, ConsistencyCheckBucket2)

	for _, locations := range locationsForChecks {
		_, _ = apiClient.DeleteConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
		consistencyCheckID := api.MakeConsistencyCheckID(locations)
		r.Eventually(func() bool {
			entries, err := storageSvc.FindConsistencyCheckSets(ctx, consistencyCheckID)
			if !errors.Is(err, redis.Nil) && err != nil {
				return false
			}
			if len(entries) != 0 {
				return false
			}
			counter, err := storageSvc.GetConsistencyCheckCompletedCounter(ctx, consistencyCheckID)
			if !errors.Is(err, redis.Nil) && err != nil {
				return false
			}
			if counter != 0 {
				return false
			}
			counter, err = storageSvc.GetConsistencyCheckScheduledCounter(ctx, consistencyCheckID)
			if !errors.Is(err, redis.Nil) && err != nil {
				return false
			}
			if counter != 0 {
				return false
			}
			ids, err := storageSvc.ListConsistencyCheckIDs(ctx)
			if !errors.Is(err, redis.Nil) && err != nil {
				return false
			}
			if slices.Contains(ids, consistencyCheckID) {
				return false
			}
			storages, err := storageSvc.GetConsistencyCheckStorages(ctx, consistencyCheckID)
			if !errors.Is(err, redis.Nil) && err != nil {
				return false
			}
			if len(storages) != 0 {
				return false
			}
			readiness, err := storageSvc.GetConsistencyCheckReadiness(ctx, consistencyCheckID)
			if !errors.Is(err, redis.Nil) && err != nil {
				return false
			}
			if readiness != false {
				return false
			}
			return true
		}, ConsistencyWait, ConsistencyRetryIn)
	}
}

func TestConsistency_2Storages_Success(t *testing.T) {
	locations := []*pb.MigrateLocation{
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
	}

	r := require.New(t)
	ctx := context.WithoutCancel(tstCtx)
	consistencyCheckSetup2Storages(ctx, r)
	defer consistencyCheckTeardown2Storages(ctx, r, locations)

	checkRequest := &pb.ConsistencyCheckRequest{
		Locations: locations,
	}
	_, err := apiClient.StartConsistencyCheck(ctx, checkRequest)
	r.NoError(err)

	var getCheckResponse *pb.GetConsistencyCheckReportResponse
	r.Eventually(func() bool {
		getCheckResponse, err = apiClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
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

	r.True(getCheckResponse.Check.Consistent)

	checkEntries, err := apiClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
		Locations: locations,
		PageSize:  10,
	})
	r.NoError(err)
	r.Len(checkEntries.Entries, 0)
}

func TestConsistency_2Storages_NoObject_Failure(t *testing.T) {
	locations := []*pb.MigrateLocation{
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
	}

	r := require.New(t)
	ctx := context.WithoutCancel(tstCtx)
	consistencyCheckSetup2Storages(ctx, r)
	defer consistencyCheckTeardown2Storages(ctx, r, locations)

	err := mainClient.RemoveObject(ctx, ConsistencyCheckBucket1, ConsistencyCheckObject3, minio.RemoveObjectOptions{})
	r.NoError(err)

	checkRequest := &pb.ConsistencyCheckRequest{
		Locations: locations,
	}
	_, err = apiClient.StartConsistencyCheck(ctx, checkRequest)
	r.NoError(err)

	var getCheckResponse *pb.GetConsistencyCheckReportResponse
	r.Eventually(func() bool {
		getCheckResponse, err = apiClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
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

	r.False(getCheckResponse.Check.Consistent)

	checkEntries, err := apiClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
		Locations: locations,
		PageSize:  10,
	})
	r.NoError(err)
	r.Len(checkEntries.Entries, 1)
	r.NotContains(checkEntries.Entries[0].Storages, ConsistencyCheckStorage1)
	r.Contains(checkEntries.Entries[0].Storages, ConsistencyCheckStorage2)
	r.Equal(checkEntries.Entries[0].Object, ConsistencyCheckObject3)
}

func TestConsistency_2Storages_NoDir_Failure(t *testing.T) {
	locations := []*pb.MigrateLocation{
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
	}

	r := require.New(t)
	ctx := context.WithoutCancel(tstCtx)
	consistencyCheckSetup2Storages(ctx, r)
	defer consistencyCheckTeardown2Storages(ctx, r, locations)

	err := mainClient.RemoveObject(ctx, ConsistencyCheckBucket1, ConsistencyCheckObject1, minio.RemoveObjectOptions{ForceDelete: true})
	r.NoError(err)
	err = mainClient.RemoveObject(ctx, ConsistencyCheckBucket1, ConsistencyCheckObject2, minio.RemoveObjectOptions{ForceDelete: true})
	r.NoError(err)

	checkRequest := &pb.ConsistencyCheckRequest{
		Locations: locations,
	}
	_, err = apiClient.StartConsistencyCheck(ctx, checkRequest)
	r.NoError(err)

	var getCheckResponse *pb.GetConsistencyCheckReportResponse
	r.Eventually(func() bool {
		getCheckResponse, err = apiClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
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

	r.False(getCheckResponse.Check.Consistent)

	checkEntries, err := apiClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
		Locations: locations,
		PageSize:  10,
	})
	r.NoError(err)
	r.Len(checkEntries.Entries, 3)
	r.NotContains(checkEntries.Entries[0].Storages, ConsistencyCheckStorage1)
	r.NotContains(checkEntries.Entries[1].Storages, ConsistencyCheckStorage1)
	r.NotContains(checkEntries.Entries[2].Storages, ConsistencyCheckStorage1)
	r.Contains(checkEntries.Entries[0].Storages, ConsistencyCheckStorage2)
	r.Contains(checkEntries.Entries[1].Storages, ConsistencyCheckStorage2)
	r.Contains(checkEntries.Entries[2].Storages, ConsistencyCheckStorage2)
	r.Contains([]string{ConsistencyCheckObject1, ConsistencyCheckObject2, "path/to/"}, checkEntries.Entries[0].Object)
	r.Contains([]string{ConsistencyCheckObject1, ConsistencyCheckObject2, "path/to/"}, checkEntries.Entries[1].Object)
	r.Contains([]string{ConsistencyCheckObject1, ConsistencyCheckObject2, "path/to/"}, checkEntries.Entries[2].Object)
}

func TestConsistency_2Storages_NoEmptyDir_Failure(t *testing.T) {
	locations := []*pb.MigrateLocation{
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
	}

	r := require.New(t)
	ctx := context.WithoutCancel(tstCtx)
	consistencyCheckSetup2Storages(ctx, r)
	defer consistencyCheckTeardown2Storages(ctx, r, locations)

	err := mainClient.RemoveObject(ctx, ConsistencyCheckBucket1, ConsistencyCheckDir1, minio.RemoveObjectOptions{})
	r.NoError(err)

	checkRequest := &pb.ConsistencyCheckRequest{
		Locations: locations,
	}
	_, err = apiClient.StartConsistencyCheck(ctx, checkRequest)
	r.NoError(err)

	var getCheckResponse *pb.GetConsistencyCheckReportResponse
	r.Eventually(func() bool {
		getCheckResponse, err = apiClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
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

	r.False(getCheckResponse.Check.Consistent)

	checkEntries, err := apiClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
		Locations: locations,
		PageSize:  10,
	})
	r.NoError(err)
	r.Len(checkEntries.Entries, 2)
	r.NotContains(checkEntries.Entries[0].Storages, ConsistencyCheckStorage1)
	r.Contains(checkEntries.Entries[0].Storages, ConsistencyCheckStorage2)
	r.NotContains(checkEntries.Entries[1].Storages, ConsistencyCheckStorage1)
	r.Contains(checkEntries.Entries[1].Storages, ConsistencyCheckStorage2)
}

func TestConsistency_2Storages_WrongEtag_Failure(t *testing.T) {
	locations := []*pb.MigrateLocation{
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
	}

	r := require.New(t)
	ctx := context.WithoutCancel(tstCtx)
	consistencyCheckSetup2Storages(ctx, r)
	defer consistencyCheckTeardown2Storages(ctx, r, locations)

	objectSeed := bytes.Repeat([]byte("3"), rand.Intn(1<<20)+32*1024)
	uploadInfo, err := mainClient.PutObject(ctx, ConsistencyCheckBucket1, ConsistencyCheckObject1, bytes.NewReader(objectSeed), int64(len(objectSeed)), minio.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)
	r.EqualValues(ConsistencyCheckObject1, uploadInfo.Key)
	r.EqualValues(ConsistencyCheckBucket1, uploadInfo.Bucket)

	checkRequest := &pb.ConsistencyCheckRequest{
		Locations: locations,
	}
	_, err = apiClient.StartConsistencyCheck(ctx, checkRequest)
	r.NoError(err)

	var getCheckResponse *pb.GetConsistencyCheckReportResponse
	r.Eventually(func() bool {
		getCheckResponse, err = apiClient.GetConsistencyCheckReport(ctx, &pb.ConsistencyCheckRequest{Locations: locations})
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

	r.False(getCheckResponse.Check.Consistent)

	checkEntries, err := apiClient.GetConsistencyCheckReportEntries(ctx, &pb.GetConsistencyCheckReportEntriesRequest{
		Locations: locations,
		PageSize:  10,
	})
	r.NoError(err)
	r.Equal(len(checkEntries.Entries), 2)
	r.Equal(checkEntries.Entries[0].Object, checkEntries.Entries[1].Object)
	r.NotEqual(checkEntries.Entries[0].Etag, checkEntries.Entries[1].Etag)
	r.Len(checkEntries.Entries[0].Storages, 1)
	r.Len(checkEntries.Entries[1].Storages, 1)
	r.NotEqual(checkEntries.Entries[0].Storages[0], checkEntries.Entries[1].Storages[0])
}
