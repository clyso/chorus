// Copyright 2025 Clyso GmbH
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

package entity

type ObjectLockID struct {
	Storage string
	Bucket  string
	Name    string
	Version string
}

func NewObjectLockID(storage string, bucket string, name string, version string) ObjectLockID {
	return ObjectLockID{
		Storage: storage,
		Bucket:  bucket,
		Name:    name,
		Version: version,
	}
}

type BucketLockID struct {
	Storage string
	Bucket  string
}

func NewBucketLockID(storage string, bucket string) BucketLockID {
	return BucketLockID{
		Storage: storage,
		Bucket:  bucket,
	}
}
