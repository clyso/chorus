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
	r := require.New(t)
	expect := BucketCreatePayload{
		Bucket:   "fb",
		Location: "l",
	}
	_, err := bucketCreate.Encode(context.Background(), expect)
	t.Log(err)
	r.Error(err, " should fail without replication ID")

	expect.SetReplicationID(entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User:        "u",
		FromStorage: "fs",
		FromBucket:  "fb",
		ToStorage:   "ts",
		ToBucket:    "tb",
	}))

	// encode
	task, err := bucketCreate.Encode(context.Background(), expect)
	r.NoError(err)
	r.Equal(TypeBucketCreate, task.Type())
	payload := task.Payload()
	// decode back
	got := BucketCreatePayload{}
	err = json.Unmarshal(payload, &got)
	r.NoError(err)

	// check that all fields preserved
	r.EqualValues(expect.ID.AsString(), got.ID.AsString())
	r.EqualValues(expect.Bucket, got.Bucket)
	r.EqualValues(expect.Location, got.Location)
	r.EqualValues(expect.ID.FromStorage(), got.ID.FromStorage())
	r.EqualValues("fs", got.ID.FromStorage())
	r.EqualValues(expect.ID.ToStorage(), got.ID.ToStorage())
	r.EqualValues("ts", got.ID.ToStorage())
	r.EqualValues(expect.ID.User(), got.ID.User())
	r.EqualValues("u", got.ID.User())
	expectFromBucket, expectToBucket := expect.ID.FromToBuckets(expect.Bucket)
	gotFromBucket, gotToBucket := got.ID.FromToBuckets(got.Bucket)
	r.EqualValues(expectFromBucket, gotFromBucket)
	r.EqualValues(expectToBucket, gotToBucket)
	r.EqualValues("fb", gotFromBucket)
	r.EqualValues("tb", gotToBucket)
}
