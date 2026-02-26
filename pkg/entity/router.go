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

type UserUploadObjectID struct {
	User   string
	Bucket string
}

func NewUserUploadObjectID(user string, bucket string) UserUploadObjectID {
	return UserUploadObjectID{
		User:   user,
		Bucket: bucket,
	}
}

type UserUploadObject struct {
	Object   string
	UploadID string
}

func NewUserUploadObject(object string, uploadID string) UserUploadObject {
	return UserUploadObject{
		Object:   object,
		UploadID: uploadID,
	}
}
