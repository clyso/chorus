//go:build agent

package agent

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math/rand"
	"strings"

	mclient "github.com/minio/minio-go/v7"

	"github.com/clyso/chorus/pkg/s3"
)

type testObj struct {
	name   string
	data   []byte
	bucket string
}

func getTestObj(name, bucket string) testObj {
	return testObj{
		name:   name,
		data:   bytes.Repeat([]byte(name[len(name)-1:]), rand.Intn(1<<20)+32*1024),
		bucket: bucket,
	}
}

func listObjects(c *mclient.Client, bucket string, prefix string) ([]string, error) {
	var res []string
	objCh := c.ListObjects(tstCtx, bucket, mclient.ListObjectsOptions{Prefix: prefix})
	for obj := range objCh {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if obj.Size == 0 {
			subRes, err := listObjects(c, bucket, obj.Key)
			if err != nil {
				return nil, err
			}
			res = append(res, subRes...)
		} else {
			res = append(res, obj.Key)
		}
	}
	return res, nil
}

func generateCredentials() s3.CredentialsV4 {
	res := s3.CredentialsV4{}
	const (
		// Minimum length for MinIO access key.
		accessKeyMinLen = 3

		// Maximum length for MinIO access key.
		// There is no max length enforcement for access keys
		accessKeyMaxLen = 20

		// Minimum length for MinIO secret key for both server
		secretKeyMinLen = 8

		// Maximum secret key length for MinIO, this
		// is used when autogenerating new credentials.
		// There is no max length enforcement for secret keys
		secretKeyMaxLen = 40

		// Alpha numeric table used for generating access keys.
		alphaNumericTable = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"

		// Total length of the alpha numeric table.
		alphaNumericTableLen = byte(len(alphaNumericTable))
	)
	readBytes := func(size int) (data []byte, err error) {
		data = make([]byte, size)
		var n int
		if n, err = rand.Read(data); err != nil {
			return nil, err
		} else if n != size {
			panic(fmt.Errorf("Not enough data. Expected to read: %v bytes, got: %v bytes", size, n))
		}
		return data, nil
	}

	// Generate access key.
	keyBytes, err := readBytes(accessKeyMaxLen)
	if err != nil {
		panic(err)
	}
	for i := 0; i < accessKeyMaxLen; i++ {
		keyBytes[i] = alphaNumericTable[keyBytes[i]%alphaNumericTableLen]
	}
	res.AccessKeyID = string(keyBytes)

	// Generate secret key.
	keyBytes, err = readBytes(secretKeyMaxLen)
	if err != nil {
		panic(err)
	}

	res.SecretAccessKey = strings.ReplaceAll(string([]byte(base64.StdEncoding.EncodeToString(keyBytes))[:secretKeyMaxLen]),
		"/", "+")

	return res
}
