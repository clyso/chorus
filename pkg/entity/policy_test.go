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

import (
	"reflect"
	"testing"
)

func TestNewUserReplicationPolicy(t *testing.T) {
	type args struct {
		user        string
		fromStorage string
		toStorage   string
	}
	tests := []struct {
		name string
		args args
		want UserReplicationPolicy
	}{
		{
			name: "basic",
			args: args{
				user:        "user1",
				fromStorage: "storageA",
				toStorage:   "storageB",
			},
			want: UserReplicationPolicy{
				User:        "user1",
				FromStorage: "storageA",
				ToStorage:   "storageB",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewUserReplicationPolicy(tt.args.user, tt.args.fromStorage, tt.args.toStorage); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewUserReplicationPolicy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUserReplicationPolicy_Validate(t *testing.T) {
	type fields struct {
		User        string
		FromStorage string
		ToStorage   string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "valid policy",
			fields: fields{
				User:        "user1",
				FromStorage: "storageA",
				ToStorage:   "storageB",
			},
			wantErr: false,
		},
		{
			name: "missing user",
			fields: fields{
				User:        "",
				FromStorage: "storageA",
				ToStorage:   "storageB",
			},
			wantErr: true,
		},
		{
			name: "missing from storage",
			fields: fields{
				User:        "user1",
				FromStorage: "",
				ToStorage:   "storageB",
			},
			wantErr: true,
		},
		{
			name: "missing to storage",
			fields: fields{
				User:        "user1",
				FromStorage: "storageA",
				ToStorage:   "",
			},
			wantErr: true,
		},
		{
			name: "same from and to storage",
			fields: fields{
				User:        "user1",
				FromStorage: "storageA",
				ToStorage:   "storageA",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := UserReplicationPolicy{
				User:        tt.fields.User,
				FromStorage: tt.fields.FromStorage,
				ToStorage:   tt.fields.ToStorage,
			}
			if err := p.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("UserReplicationPolicy.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBucketReplicationPolicy_Validate(t *testing.T) {
	type fields struct {
		User        string
		FromStorage string
		FromBucket  string
		ToStorage   string
		ToBucket    string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "valid policy",
			fields: fields{
				User:        "user1",
				FromStorage: "storageA",
				FromBucket:  "bucket1",
				ToStorage:   "storageB",
				ToBucket:    "bucket2",
			},
			wantErr: false,
		},
		{
			name: "missing user",
			fields: fields{
				User:        "",
				FromStorage: "storageA",
				FromBucket:  "bucket1",
				ToStorage:   "storageB",
				ToBucket:    "bucket2",
			},
			wantErr: true,
		},
		{
			name: "missing from storage",
			fields: fields{
				User:        "user1",
				FromStorage: "",
				FromBucket:  "bucket1",
				ToStorage:   "storageB",
				ToBucket:    "bucket2",
			},
			wantErr: true,
		},
		{
			name: "missing from bucket",
			fields: fields{
				User:        "user1",
				FromStorage: "storageA",
				FromBucket:  "",
				ToStorage:   "storageB",
				ToBucket:    "bucket2",
			},
			wantErr: true,
		},
		{
			name: "missing to storage",
			fields: fields{
				User:        "user1",
				FromStorage: "storageA",
				FromBucket:  "bucket1",
				ToStorage:   "",
				ToBucket:    "bucket2",
			},
			wantErr: true,
		},
		{
			name: "missing to bucket",
			fields: fields{
				User:        "user1",
				FromStorage: "storageA",
				FromBucket:  "bucket1",
				ToStorage:   "storageB",
				ToBucket:    "",
			},
			wantErr: true,
		},
		{
			name: "same from and to storage and bucket",
			fields: fields{
				User:        "user1",
				FromStorage: "storageA",
				FromBucket:  "bucket1",
				ToStorage:   "storageA",
				ToBucket:    "bucket1",
			},
			wantErr: true,
		},
		{
			name: "same from and to storage but different bucket",
			fields: fields{
				User:        "user1",
				FromStorage: "storageA",
				FromBucket:  "bucket1",
				ToStorage:   "storageA",
				ToBucket:    "bucket2",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := BucketReplicationPolicy{
				User:        tt.fields.User,
				FromStorage: tt.fields.FromStorage,
				FromBucket:  tt.fields.FromBucket,
				ToStorage:   tt.fields.ToStorage,
				ToBucket:    tt.fields.ToBucket,
			}
			if err := p.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("BucketReplicationPolicy.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewBucketRepliationPolicy(t *testing.T) {
	type args struct {
		user        string
		fromStorage string
		fromBucket  string
		toStorage   string
		toBucket    string
	}
	tests := []struct {
		name string
		args args
		want BucketReplicationPolicy
	}{
		{
			name: "basic",
			args: args{
				user:        "user1",
				fromStorage: "storageA",
				fromBucket:  "bucket1",
				toStorage:   "storageB",
				toBucket:    "bucket2",
			},
			want: BucketReplicationPolicy{
				User:        "user1",
				FromStorage: "storageA",
				FromBucket:  "bucket1",
				ToStorage:   "storageB",
				ToBucket:    "bucket2",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewBucketRepliationPolicy(tt.args.user, tt.args.fromStorage, tt.args.fromBucket, tt.args.toStorage, tt.args.toBucket); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewBucketRepliationPolicy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBucketReplicationPolicy_ToID(t *testing.T) {
	type fields struct {
		User        string
		FromStorage string
		FromBucket  string
		ToStorage   string
		ToBucket    string
	}
	tests := []struct {
		name   string
		fields fields
		want   BucketReplicationPolicyID
	}{
		{
			name: "basic",
			fields: fields{
				User:        "user1",
				FromStorage: "storageA",
				FromBucket:  "bucket1",
				ToStorage:   "storageB",
				ToBucket:    "bucket2",
			},
			want: BucketReplicationPolicyID{
				User:       "user1",
				FromBucket: "bucket1",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := BucketReplicationPolicy{
				User:        tt.fields.User,
				FromStorage: tt.fields.FromStorage,
				FromBucket:  tt.fields.FromBucket,
				ToStorage:   tt.fields.ToStorage,
				ToBucket:    tt.fields.ToBucket,
			}
			if got := p.LookupID(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BucketReplicationPolicy.ToID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUserReplicationPolicy_ToID(t *testing.T) {
	type fields struct {
		User        string
		FromStorage string
		ToStorage   string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "basic",
			fields: fields{
				User:        "user1",
				FromStorage: "storageA",
				ToStorage:   "storageB",
			},
			want: "user1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := UserReplicationPolicy{
				User:        tt.fields.User,
				FromStorage: tt.fields.FromStorage,
				ToStorage:   tt.fields.ToStorage,
			}
			if got := p.LookupID(); got != tt.want {
				t.Errorf("UserReplicationPolicy.ToID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBucketReplicationPolicy_Destination(t *testing.T) {
	type fields struct {
		User        string
		FromStorage string
		FromBucket  string
		ToStorage   string
		ToBucket    string
	}
	tests := []struct {
		name   string
		fields fields
		want   BucketReplicationPolicyDestination
	}{
		{
			name: "basic",
			fields: fields{
				User:        "user1",
				FromStorage: "storageA",
				FromBucket:  "bucket1",
				ToStorage:   "storageB",
				ToBucket:    "bucket2",
			},
			want: BucketReplicationPolicyDestination{
				ToStorage: "storageB",
				ToBucket:  "bucket2",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := BucketReplicationPolicy{
				User:        tt.fields.User,
				FromStorage: tt.fields.FromStorage,
				FromBucket:  tt.fields.FromBucket,
				ToStorage:   tt.fields.ToStorage,
				ToBucket:    tt.fields.ToBucket,
			}
			if got := p.Destination(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BucketReplicationPolicy.Destination() = %v, want %v", got, tt.want)
			}
		})
	}
}
