package meta

import (
	"reflect"
	"testing"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
)

func Test_redisReplicationMATCH(t *testing.T) {
	type args struct {
		replID entity.UniversalReplicationID
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "bucket replication",
			args: args{
				replID: entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
					User:        "user1",
					FromStorage: "storage1",
					FromBucket:  "bucket1",
					ToStorage:   "storage2",
					ToBucket:    "bucket2",
				}),
			},
			want: "v:user1:storage1:storage2:bucket1:bucket2",
		},
		{
			name: "user replication",
			args: args{
				replID: entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
					User:        "user1",
					FromStorage: "storage1",
					ToStorage:   "storage1",
				}),
			},
			want: "v:user1:storage1:storage1:*",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := redisReplicationMATCH(tt.args.replID); got != tt.want {
				t.Errorf("redisReplicationMATCH() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_toRedisKeys(t *testing.T) {
	type args struct {
		replID   entity.UniversalReplicationID
		bucket   string
		objectID string
		kind     versionKind
	}
	tests := []struct {
		name string
		args args
		want redisKeys
	}{
		{
			name: "bucket payload for bucket replication",
			args: args{
				replID: entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
					User:        "user1",
					FromStorage: "storage1",
					FromBucket:  "bucket1",
					ToStorage:   "storage2",
					ToBucket:    "bucket2",
				}),
				bucket:   "bucket1",
				objectID: "",
				kind:     payload,
			},
			want: redisKeys{
				hashKey:   "v:user1:storage1:storage2:bucket1:bucket2",
				fromField: "f::p",
				toField:   "t::p",
			},
		},
		{
			name: "bucket payload for user replication",
			args: args{
				replID: entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
					User:        "user1",
					FromStorage: "storage1",
					ToStorage:   "storage2",
				}),
				bucket:   "bucket1",
				objectID: "",
				kind:     payload,
			},
			want: redisKeys{
				hashKey:   "v:user1:storage1:storage2:bucket1:bucket1",
				fromField: "f::p",
				toField:   "t::p",
			},
		},
		{
			name: "object tags for bucket replication",
			args: args{
				replID: entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
					User:        "user1",
					FromStorage: "storage1",
					FromBucket:  "bucket1",
					ToStorage:   "storage2",
					ToBucket:    "bucket2",
				}),
				bucket:   "bucket1",
				objectID: "obj1",
				kind:     tags,
			},
			want: redisKeys{
				hashKey:   "v:user1:storage1:storage2:bucket1:bucket2",
				fromField: "f:obj1:t",
				toField:   "t:obj1:t",
			},
		},
		{
			name: "object acl for user replication",
			args: args{
				replID: entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
					User:        "user1",
					FromStorage: "storage1",
					ToStorage:   "storage2",
				}),
				bucket:   "bucket1",
				objectID: "obj1",
				kind:     acl,
			},
			want: redisKeys{
				hashKey:   "v:user1:storage1:storage2:bucket1:bucket1",
				fromField: "f:obj1:a",
				toField:   "t:obj1:a",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toRedisKeys(tt.args.replID, tt.args.bucket, tt.args.objectID, tt.args.kind); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("toRedisKeys() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_toRedisBucketKeys(t *testing.T) {
	type args struct {
		replID entity.UniversalReplicationID
		bucket string
		kind   versionKind
	}
	tests := []struct {
		name    string
		args    args
		want    redisKeys
		wantErr bool
	}{
		{
			name: "bucket tags for bucket replication",
			args: args{
				replID: entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
					User:        "user1",
					FromStorage: "storage1",
					FromBucket:  "bucket1",
					ToStorage:   "storage2",
					ToBucket:    "bucket2",
				}),
				bucket: "bucket1",
				kind:   tags,
			},
			want: redisKeys{
				hashKey:   "v:user1:storage1:storage2:bucket1:bucket2",
				fromField: "f::t",
				toField:   "t::t",
			},
			wantErr: false,
		},
		{
			name: "bucket acl for user replication",
			args: args{
				replID: entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
					User:        "user1",
					FromStorage: "storage1",
					ToStorage:   "storage2",
				}),
				bucket: "bucket1",
				kind:   acl,
			},
			want: redisKeys{
				hashKey:   "v:user1:storage1:storage2:bucket1:bucket1",
				fromField: "f::a",
				toField:   "t::a",
			},
			wantErr: false,
		},
		{
			name: "missing bucket name",
			args: args{
				replID: entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
					User:        "user1",
					FromStorage: "storage1",
					ToStorage:   "storage2",
				}),
				bucket: "",
				kind:   payload,
			},
			want:    redisKeys{},
			wantErr: true,
		},
		{
			name: "invalid version kind",
			args: args{
				replID: entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
					User: "user1",

					FromStorage: "storage1",
					ToStorage:   "storage2",
				}),
				bucket: "bucket1",
				kind:   "invalid_kind",
			},
			want:    redisKeys{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toRedisBucketKeys(tt.args.replID, tt.args.bucket, tt.args.kind)
			if (err != nil) != tt.wantErr {
				t.Fatalf("toRedisBucketKeys() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("toRedisBucketKeys() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_toRedisObjKeys(t *testing.T) {
	type args struct {
		replID entity.UniversalReplicationID
		object dom.Object
		kind   versionKind
	}
	tests := []struct {
		name    string
		args    args
		want    redisKeys
		wantErr bool
	}{
		{
			name: "object payload for bucket replication",
			args: args{
				replID: entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
					User:        "user1",
					FromStorage: "storage1",
					FromBucket:  "bucket1",
					ToStorage:   "storage2",
					ToBucket:    "bucket2",
				}),
				object: dom.Object{
					Bucket:  "bucket1",
					Name:    "obj1",
					Version: "",
				},
				kind: payload,
			},
			want: redisKeys{
				hashKey:   "v:user1:storage1:storage2:bucket1:bucket2",
				fromField: "f:obj1:p",
				toField:   "t:obj1:p",
			},
			wantErr: false,
		},
		{
			name: "object tags for user replication",
			args: args{
				replID: entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
					User:        "user1",
					FromStorage: "storage1",
					ToStorage:   "storage2",
				}),
				object: dom.Object{
					Bucket:  "bucket1",
					Name:    "obj1",
					Version: "",
				},
				kind: tags,
			},
			want: redisKeys{
				hashKey:   "v:user1:storage1:storage2:bucket1:bucket1",
				fromField: "f:obj1:t",
				toField:   "t:obj1:t",
			},
			wantErr: false,
		},
		{
			name: "missing bucket name",
			args: args{
				replID: entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
					User:        "user1",
					FromStorage: "storage1",
					ToStorage:   "storage2",
				}),
				object: dom.Object{
					Bucket:  "",
					Name:    "obj1",
					Version: "",
				},
				kind: payload,
			},
			want:    redisKeys{},
			wantErr: true,
		},
		{
			name: "missing object ID",
			args: args{
				replID: entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
					User:        "user1",
					FromStorage: "storage1",
					ToStorage:   "storage2",
				}),
				object: dom.Object{
					Bucket:  "bucket1",
					Name:    "",
					Version: "",
				},
				kind: acl,
			},
			want:    redisKeys{},
			wantErr: true,
		},
		{
			name: "invalid version kind",
			args: args{
				replID: entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
					User:        "user1",
					FromStorage: "storage1",
					ToStorage:   "storage2",
				}),
				object: dom.Object{
					Bucket:  "bucket1",
					Name:    "obj1",
					Version: "",
				},
				kind: "invalid_kind",
			},
			want:    redisKeys{},
			wantErr: true,
		},
		{
			name: "object with version specified",
			args: args{
				replID: entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
					User:        "user1",
					FromStorage: "storage1",
					ToStorage:   "storage2",
				}),
				object: dom.Object{
					Bucket:  "bucket1",
					Name:    "obj1",
					Version: "v1",
				},
				kind: payload,
			},
			want: redisKeys{
				hashKey:   "v:user1:storage1:storage2:bucket1:bucket1",
				fromField: "f:obj1v1:p",
				toField:   "t:obj1v1:p",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toRedisObjKeys(tt.args.replID, tt.args.object, tt.args.kind)
			if (err != nil) != tt.wantErr {
				t.Fatalf("toRedisObjKeys() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("toRedisObjKeys() = %v, want %v", got, tt.want)
			}
		})
	}
}
