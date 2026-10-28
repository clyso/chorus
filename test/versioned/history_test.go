// Copyright 2026 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package versioned

import (
	"bytes"
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
	. "github.com/onsi/gomega"
)

const historyPrefix = "history/"

// p - put a version, d - delete creating a delete marker.
// Object names are prefixes of each other on purpose.
var histories = map[string]string{
	historyPrefix + "a":              "ppp",
	historyPrefix + "a-b":            "ppp",
	historyPrefix + "ab":             "ppp",
	historyPrefix + "delete-markers": "pdpdp",
}

func fillHistories(ctx context.Context, client *minio.Client, bucket string) {
	for name, history := range histories {
		for i, op := range history {
			if op == 'd' {
				Expect(client.RemoveObject(ctx, bucket, name, minio.RemoveObjectOptions{})).To(Succeed())
				continue
			}
			data := fmt.Appendf(nil, "%s version %d", name, i)
			_, err := client.PutObject(ctx, bucket, name, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())
		}
	}
}

// Delete marker version ids are not compared, as migration does not preserve them.
func expectSameHistories(ctx context.Context, src *minio.Client, srcBucket string, dest *minio.Client, destBucket string) {
	srcVersions := listHistories(ctx, src, srcBucket)
	Expect(srcVersions).To(HaveLen(len(histories)))
	for name, history := range histories {
		Expect(srcVersions[name]).To(HaveLen(len(history)))
	}
	Expect(listHistories(ctx, dest, destBucket)).To(Equal(srcVersions))
}

func listHistories(ctx context.Context, client *minio.Client, bucket string) map[string][]string {
	versions := map[string][]string{}
	for object := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: historyPrefix, WithVersions: true, Recursive: true}) {
		Expect(object.Err).NotTo(HaveOccurred())
		version := fmt.Sprintf("%s %s %d", object.VersionID, object.ETag, object.Size)
		if object.IsDeleteMarker {
			version = "delete-marker"
		}
		versions[object.Key] = append(versions[object.Key], version)
	}
	return versions
}
