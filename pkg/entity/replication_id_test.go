package entity

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIDFromBucketReplication(t *testing.T) {
	type args struct {
		id     ReplicationStatusID
		bucket string
	}

	tests := []struct {
		name      string
		args      args
		wantPanic bool
	}{
		{
			name: "valid",
			args: args{
				id: ReplicationStatusID{
					User:        "u",
					FromStorage: "fs",
					FromBucket:  "fb",
					ToStorage:   "ts",
					ToBucket:    "tb",
				},
				bucket: "fb",
			},
			wantPanic: false,
		},
		{
			name: "invalid bucket is not equal to from bucket",
			args: args{
				id: ReplicationStatusID{
					User:        "u",
					FromStorage: "fs",
					FromBucket:  "fb",
					ToStorage:   "ts",
					ToBucket:    "tb",
				},
				bucket: "tb",
			},
			wantPanic: true,
		},
		{
			name: "invalid bucket is not equal to from bucket",
			args: args{
				id: ReplicationStatusID{
					User:        "u",
					FromStorage: "fs",
					FromBucket:  "fb",
					ToStorage:   "ts",
					ToBucket:    "tb",
				},
				bucket: "",
			},
			wantPanic: true,
		},
		{
			name: "invalid bucket is not equal to from bucket",
			args: args{
				id: ReplicationStatusID{
					User:        "u",
					FromStorage: "fs",
					FromBucket:  "fb",
					ToStorage:   "ts",
					ToBucket:    "tb",
				},
				bucket: "xyz",
			},
			wantPanic: true,
		},
	}
	for _, tt := range tests {
		r := require.New(t)
		got := IDFromBucketReplication(tt.args.id)
		r.Equal(tt.args.id.User, got.User())
		r.Equal(tt.args.id.FromStorage, got.FromStorage())
		r.Equal(tt.args.id.ToStorage, got.ToStorage())
		if tt.wantPanic {
			r.Panics(func() { got.FromToBuckets(tt.args.bucket) })
			return
		} else {
			fromBucket, toBucket := got.FromToBuckets(tt.args.bucket)
			r.Equal(tt.args.id.FromBucket, fromBucket)
			r.Equal(tt.args.id.ToBucket, toBucket)
		}
		r.False(got.IsEmpty())
		r.Equal(strings.Join([]string{tt.args.id.User, tt.args.id.FromStorage, tt.args.id.ToStorage, tt.args.id.FromBucket, tt.args.id.ToBucket}, ":"), got.AsString())
		bid, ok := got.AsBucketID()
		r.True(ok)
		r.EqualValues(tt.args.id, bid)
	}
}

func Test_UniversalReplicationID_json(t *testing.T) {
	r := require.New(t)
	in := UniversalReplicationID{
		user:        "u",
		fromStorage: "fs",
		toStorage:   "ts",
		fromBucket:  "fb",
		toBucket:    "tb",
	}
	data, err := json.Marshal(&in)
	r.NoError(err)
	var out UniversalReplicationID
	err = json.Unmarshal(data, &out)
	r.NoError(err)
	r.Equal(in, out)
	r.Equal(in.FromStorage(), out.FromStorage())
	r.Equal("fs", out.FromStorage())
	r.Equal(in.ToStorage(), out.ToStorage())
	r.Equal("ts", out.ToStorage())
	r.Equal(in.User(), out.User())
	r.Equal("u", out.User())

	fromBucket, toBucket := out.FromToBuckets("fb")
	r.Equal("fb", fromBucket)
	r.Equal("tb", toBucket)
	inFromBucket, inToBucket := in.FromToBuckets("fb")
	r.Equal(inFromBucket, fromBucket)
	r.Equal(inToBucket, toBucket)
}

func Test_UniversalReplicationID_json_field(t *testing.T) {
	r := require.New(t)
	type wrapper struct {
		ID       UniversalReplicationID  `json:"id"`
		IDPtr    *UniversalReplicationID `json:"id_ptr"`
		IDNilPtr *UniversalReplicationID `json:"id_nil_ptr"`
		Other    string                  `json:"other"`
	}
	inID := UniversalReplicationID{
		user:        "u",
		fromStorage: "fs",
		toStorage:   "ts",
		fromBucket:  "fb",
		toBucket:    "tb",
	}
	in := wrapper{
		ID:       inID,
		IDPtr:    &inID,
		IDNilPtr: nil,
		Other:    "other",
	}

	data, err := json.Marshal(&in)
	r.NoError(err)
	var out wrapper
	err = json.Unmarshal(data, &out)
	r.NoError(err)
	r.EqualValues(in, out)
	r.Equal(in.ID, out.ID)
	r.Equal(in.Other, out.Other)
	r.Equal(in.ID.FromStorage(), out.ID.FromStorage())
	r.Equal("fs", out.ID.FromStorage())
	r.False(out.ID.IsEmpty())

	r.NotNil(out.IDPtr)
	r.Equal(*in.IDPtr, *out.IDPtr)
	r.Equal(in.IDPtr.FromStorage(), out.IDPtr.FromStorage())
	r.Equal("fs", out.IDPtr.FromStorage())
	r.False(out.IDPtr.IsEmpty())

	r.Nil(in.IDNilPtr)
	r.Nil(out.IDNilPtr)
	r.True(out.IDNilPtr.IsEmpty())
}
