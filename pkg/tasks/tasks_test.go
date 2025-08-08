package tasks

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_custom_bucket_compatibility(t *testing.T) {
	r := require.New(t)
	//setup:
	type OldSync struct {
		FromStorage string
		ToStorage   string
		CreatedAt   time.Time
	}
	type OldBucketCreatePayload struct {
		OldSync
		Bucket   string
		Location string
	}

	oldTask := OldBucketCreatePayload{
		OldSync: OldSync{
			FromStorage: "stor1",
			ToStorage:   "stor2",
			CreatedAt:   time.Now(),
		},
		Bucket:   "buck1",
		Location: "loc",
	}
	oldJson, err := json.Marshal(&oldTask)
	r.NoError(err)
	t.Run("new handler works with old", func(t *testing.T) {
		r := require.New(t)
		var p BucketCreatePayload
		r.NoError(json.Unmarshal(oldJson, &p))
		r.EqualValues(oldTask.FromStorage, p.FromStorage)
		r.EqualValues(oldTask.ToStorage, p.ToStorage)
		r.EqualValues(oldTask.CreatedAt.Unix(), p.CreatedAt.Unix())
		r.EqualValues(oldTask.Bucket, p.Bucket)
		r.EqualValues(oldTask.Location, p.Location)
		r.Empty(p.ToBucket)
	})

	t.Run("custom bucket name preserved", func(t *testing.T) {
		r := require.New(t)
		bucket := "bucket"
		src := BucketCreatePayload{
			Sync: Sync{
				ToBucket: bucket,
			},
		}
		srcJson, err := json.Marshal(&src)
		r.NoError(err)
		var dst BucketCreatePayload
		r.NoError(json.Unmarshal(srcJson, &dst))
		r.NotNil(dst.ToBucket)
		r.EqualValues(src.ToBucket, dst.ToBucket, "custom bucket preserved")

		src = BucketCreatePayload{}
		srcJson, err = json.Marshal(&src)
		r.NoError(err)
		r.NoError(json.Unmarshal(srcJson, &dst))
		r.Empty(dst.ToBucket, "no custom bucket unmarshal")
	})
}
