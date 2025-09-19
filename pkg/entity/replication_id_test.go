package entity

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIDFromBucketReplication(t *testing.T) {
	type args struct {
		id     BucketReplicationPolicy
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
				id: BucketReplicationPolicy{
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
				id: BucketReplicationPolicy{
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
				id: BucketReplicationPolicy{
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
				id: BucketReplicationPolicy{
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
		got := UniversalFromBucketReplication(tt.args.id)
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

func TestUniversalFromBucketReplication(t *testing.T) {
	r := require.New(t)
	bp := BucketReplicationPolicy{
		User:        "u",
		FromStorage: "fs",
		FromBucket:  "fb",
		ToStorage:   "ts",
		ToBucket:    "tb",
	}
	got := UniversalFromBucketReplication(bp)
	r.False(got.IsEmpty())
	r.Equal(bp.User, got.User())
	r.Equal(bp.FromStorage, got.FromStorage())
	r.Equal(bp.ToStorage, got.ToStorage())
	fromBucket, toBucket := got.FromToBuckets("fb")
	r.Equal(bp.FromBucket, fromBucket)
	r.Equal(bp.ToBucket, toBucket)

	str := got.AsString()
	fromStr, err := UniversalIDFromString(str)
	r.NoError(err)
	r.Equal(got, fromStr)

	_, ok := got.AsUserID()
	r.False(ok)
	asBucket, ok := got.AsBucketID()
	r.True(ok)
	r.EqualValues(bp, asBucket)

	// same bucket replication
	bp2 := BucketReplicationPolicy{
		User:        "u",
		FromStorage: "fs",
		FromBucket:  "b",
		ToStorage:   "ts",
		ToBucket:    "b",
	}

	got = UniversalFromBucketReplication(bp2)
	r.False(got.IsEmpty())
	r.Equal(bp2.User, got.User())
	r.Equal(bp2.FromStorage, got.FromStorage())
	r.Equal(bp2.ToStorage, got.ToStorage())
	fromBucket, toBucket = got.FromToBuckets("b")
	r.Equal(bp2.FromBucket, fromBucket)
	r.Equal(bp2.ToBucket, toBucket)

	str = got.AsString()
	fromStr, err = UniversalIDFromString(str)
	r.NoError(err)
	r.Equal(got, fromStr)

	_, ok = got.AsUserID()
	r.False(ok)
	asBucket, ok = got.AsBucketID()
	r.True(ok)
	r.EqualValues(bp2, asBucket)
}

func TestUniversalFromUserReplication(t *testing.T) {
	r := require.New(t)
	ur := UserReplicationPolicy{
		User:        "u",
		FromStorage: "fs",
		ToStorage:   "ts",
	}
	got := UniversalFromUserReplication(ur)
	r.False(got.IsEmpty())
	r.Equal(ur.User, got.User())
	r.Equal(ur.FromStorage, got.FromStorage())
	r.Equal(ur.ToStorage, got.ToStorage())
	fromBucket, toBucket := got.FromToBuckets("anybucket")
	r.Equal("anybucket", fromBucket)
	r.Equal("anybucket", toBucket)

	str := got.AsString()
	fromStr, err := UniversalIDFromString(str)
	r.NoError(err)
	r.Equal(got, fromStr)

	_, ok := got.AsBucketID()
	r.False(ok)
	asUser, ok := got.AsUserID()
	r.True(ok)
	r.EqualValues(ur, asUser)
}

func TestUniversalReplicationID_Validate(t *testing.T) {
	r := require.New(t)
	brValid := BucketReplicationPolicy{
		User:        "u",
		FromStorage: "fs",
		ToStorage:   "ts",
		FromBucket:  "fb",
		ToBucket:    "tb",
	}
	got := UniversalFromBucketReplication(brValid)
	r.NoError(got.Validate())
	brInvalid := BucketReplicationPolicy{
		User:        "u",
		FromStorage: "fs",
		ToStorage:   "fs",
		FromBucket:  "fb",
		ToBucket:    "fb",
	}
	got = UniversalFromBucketReplication(brInvalid)
	r.Error(got.Validate())
	brEmpty := BucketReplicationPolicy{}
	got = UniversalFromBucketReplication(brEmpty)
	r.Error(got.Validate())

	urValid := UserReplicationPolicy{
		User:        "u",
		FromStorage: "fs",
		ToStorage:   "ts",
	}
	got = UniversalFromUserReplication(urValid)
	r.NoError(got.Validate())
	urInvalid := UserReplicationPolicy{
		User:        "u",
		FromStorage: "fs",
		ToStorage:   "fs",
	}
	got = UniversalFromUserReplication(urInvalid)
	r.Error(got.Validate())
	urEmpty := UserReplicationPolicy{}
	got = UniversalFromUserReplication(urEmpty)
	r.Error(got.Validate())
}

func TestUniversalReplicationID_IsEmpty(t *testing.T) {
	type fields struct {
		user        string
		fromStorage string
		toStorage   string
		fromBucket  string
		toBucket    string
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name:   "empty",
			fields: fields{},
			want:   true,
		},
		{
			name: "non-empty",
			fields: fields{
				user:        "u",
				fromStorage: "fs",
				toStorage:   "ts",
				fromBucket:  "",
				toBucket:    "",
			},
			want: false,
		},
		{
			name: "no user - empty",
			fields: fields{
				user:        "",
				fromStorage: "fs",
				toStorage:   "ts",
				fromBucket:  "fb",
				toBucket:    "tb",
			},
			want: true,
		},
		{
			name: "no fromStorage - empty",
			fields: fields{
				user:        "u",
				fromStorage: "",
				toStorage:   "ts",
				fromBucket:  "fb",
				toBucket:    "tb",
			},
			want: true,
		},
		{
			name: "no toStorage - empty",
			fields: fields{
				user:        "u",
				fromStorage: "fs",
				toStorage:   "",
				fromBucket:  "fb",
				toBucket:    "tb",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &UniversalReplicationID{
				user:        tt.fields.user,
				fromStorage: tt.fields.fromStorage,
				toStorage:   tt.fields.toStorage,
				fromBucket:  tt.fields.fromBucket,
				toBucket:    tt.fields.toBucket,
			}
			if got := r.IsEmpty(); got != tt.want {
				t.Errorf("UniversalReplicationID.IsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUniversalReplicationID_Swap(t *testing.T) {
	type fields struct {
		user        string
		fromStorage string
		toStorage   string
		fromBucket  string
		toBucket    string
	}
	tests := []struct {
		name   string
		fields fields
		want   UniversalReplicationID
	}{
		{
			name: "bucket replication",
			fields: fields{
				user:        "u",
				fromStorage: "fs",
				toStorage:   "ts",
				fromBucket:  "fb",
				toBucket:    "tb",
			},
			want: UniversalReplicationID{
				user:        "u",
				fromStorage: "ts",
				toStorage:   "fs",
				fromBucket:  "tb",
				toBucket:    "fb",
			},
		},
		{
			name: "user replication",
			fields: fields{
				user:        "u",
				fromStorage: "fs",
				toStorage:   "ts",
				fromBucket:  "",
				toBucket:    "",
			},
			want: UniversalReplicationID{
				user:        "u",
				fromStorage: "ts",
				toStorage:   "fs",
				fromBucket:  "",
				toBucket:    "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			repl := &UniversalReplicationID{
				user:        tt.fields.user,
				fromStorage: tt.fields.fromStorage,
				toStorage:   tt.fields.toStorage,
				fromBucket:  tt.fields.fromBucket,
				toBucket:    tt.fields.toBucket,
			}
			got := repl.Swap()
			r.EqualValues(tt.want, got)
			r.Equal(tt.want.User(), got.User())
			r.Equal(tt.want.user, got.user)
			r.Equal(tt.want.FromStorage(), got.FromStorage())
			r.Equal(tt.want.fromStorage, got.fromStorage)
			r.Equal(tt.want.ToStorage(), got.ToStorage())
			r.Equal(tt.want.toStorage, got.toStorage)
			r.Equal(tt.want.fromBucket, got.fromBucket)
			r.Equal(tt.want.toBucket, got.toBucket)
			r.NoError(repl.Validate())
			r.NoError(got.Validate())
			// repl not modified
			r.Equal(tt.fields.user, repl.user)
			r.Equal(tt.fields.fromStorage, repl.fromStorage)
			r.Equal(tt.fields.toStorage, repl.toStorage)
			r.Equal(tt.fields.fromBucket, repl.fromBucket)
			r.Equal(tt.fields.toBucket, repl.toBucket)
		})
	}
}
