package tasks

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/entity"
)

func Test_toTaskID(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "all set",
			args: []string{"a", "b", "c"},
			want: "a:b:c",
		},
		{
			name: "with empty",
			args: []string{"a", "b", ""},
			want: "a:b:",
		},
		{
			name: "all empty",
			args: []string{"", "", ""},
			want: "::",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toTaskID(tt.args...); got != tt.want {
				t.Errorf("toTaskID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_encode_BucketCreate(t *testing.T) {
	in := BucketCreatePayload{
		ReplicationID: ReplicationID{
			Replication: entity.ReplicationStatusID{
				User:        "u",
				FromStorage: "fs",
				FromBucket:  "fb",
				ToStorage:   "ts",
				ToBucket:    "tb",
			},
		},
		Bucket:   "b",
		Location: "l",
	}
	r := require.New(t)
	task, err := bucketCreate.Encode(context.Background(), in)
	r.NoError(err)
	r.Equal(TypeBucketCreate, task.Type())
	payload := task.Payload()
	got := BucketCreatePayload{}
	err = json.Unmarshal(payload, &got)
	r.NoError(err)
	r.EqualValues(in, got)
}

func Test_encode_BucketDelete(t *testing.T) {
	in := BucketDeletePayload{
		ReplicationID: ReplicationID{
			Replication: entity.ReplicationStatusID{
				User:        "u",
				FromStorage: "fs",
				FromBucket:  "fb",
				ToStorage:   "ts",
				ToBucket:    "tb",
			},
		},
		Bucket: "b",
	}
	r := require.New(t)
	task, err := bucketDelete.Encode(context.Background(), in)
	r.NoError(err)
	r.Equal(TypeBucketDelete, task.Type())
	payload := task.Payload()
	got := BucketDeletePayload{}
	err = json.Unmarshal(payload, &got)
	r.NoError(err)
	r.EqualValues(in, got)
}
