package test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/clyso/chorus/test/env"
	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
)

func TestApi_Versioning_Bucket(t *testing.T) {
	t.Skip()
	e := env.SetupEmbedded(t, workerConf, proxyConf)
	e.CreateMainFollowerUserReplications(t)
	tstCtx := t.Context()
	bucket := "bucket-versioning"
	r := require.New(t)

	ok, err := e.MainClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.False(ok)

	err = e.MainClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)

	err = e.MainClient.EnableVersioning(tstCtx, bucket)
	r.NoError(err)

	obj1 := getTestObj("obj1", bucket)
	obj1Info, err := e.MainClient.PutObject(tstCtx, obj1.bucket, obj1.name, bytes.NewReader(obj1.data), int64(len(obj1.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)
	t.Log(obj1Info.VersionID)
	obj2 := getTestObj("photo/sept/obj2", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj2.bucket, obj2.name, bytes.NewReader(obj2.data), int64(len(obj2.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)
	obj3 := getTestObj("photo/obj3", bucket)
	_, err = e.MainClient.PutObject(tstCtx, obj3.bucket, obj3.name, bytes.NewReader(obj3.data), int64(len(obj3.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)

	obj1upd := getTestObj(obj1.name, obj1.bucket)
	r.False(bytes.Equal(obj1.data, obj1upd.data))
	obj1updInfo, err := e.MainClient.PutObject(tstCtx, obj1upd.bucket, obj1upd.name, bytes.NewReader(obj1upd.data), int64(len(obj1upd.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)
	t.Log(obj1updInfo.VersionID)

	obj2upd := getTestObj(obj1.name, obj1.bucket)
	r.False(bytes.Equal(obj1.data, obj1upd.data))
	obj2updInfo, err := e.MainClient.PutObject(tstCtx, obj2upd.bucket, obj2upd.name, bytes.NewReader(obj2upd.data), int64(len(obj2upd.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)
	t.Log(obj2updInfo.VersionID)

	err = e.MainClient.RemoveObject(tstCtx, bucket, obj1.name, mclient.RemoveObjectOptions{})
	r.NoError(err)

	obj3upd := getTestObj(obj1.name, obj1.bucket)
	obj3updInfo, err := e.MainClient.PutObject(tstCtx, obj3upd.bucket, obj3upd.name, bytes.NewReader(obj3upd.data), int64(len(obj3upd.data)), mclient.PutObjectOptions{ContentType: "binary/octet-stream", DisableContentSha256: true})
	r.NoError(err)
	t.Log(obj3updInfo.VersionID)

	objs, err := listObjectsVer(e.MainClient, bucket, "")
	r.NoError(err)
	t.Log("\n" + strings.Join(objs, "\n"))

}

func listObjectsVer(c *mclient.Client, bucket string, prefix string) ([]string, error) {
	var res []string
	objCh := c.ListObjects(context.Background(), bucket, mclient.ListObjectsOptions{Prefix: prefix, WithVersions: true})
	for obj := range objCh {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if obj.Size == 0 && !obj.IsDeleteMarker {
			subRes, err := listObjectsVer(c, bucket, obj.Key)
			if err != nil {
				return nil, err
			}
			res = append(res, subRes...)
		} else {
			key := obj.Key
			if obj.VersionID != "" {
				ver := obj.VersionID
				if len(ver) > 10 {
					ver = ver[:4] + ".." + ver[len(ver)-4:]
				}
				key += fmt.Sprintf(": %s : l-%v : d-%v", ver, obj.IsLatest, obj.IsDeleteMarker)
			}
			res = append(res, key)
		}
	}
	return res, nil
}
