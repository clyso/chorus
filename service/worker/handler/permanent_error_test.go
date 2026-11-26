package handler

import (
	"errors"
	"fmt"
	"testing"

	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
)

func TestIsPermanentS3Error(t *testing.T) {
	s3Err := func(status int, code string) error {
		return fmt.Errorf("unable to upload object: %w", mclient.ErrorResponse{StatusCode: status, Code: code})
	}
	cases := []struct {
		err  error
		name string
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "not an S3 error", err: errors.New("boom"), want: false},
		{name: "invalid argument", err: s3Err(400, "InvalidArgument"), want: true},
		{name: "entity too large", err: s3Err(400, "EntityTooLarge"), want: true},
		{name: "method not allowed", err: s3Err(405, "MethodNotAllowed"), want: true},
		{name: "slow down", err: s3Err(400, "SlowDown"), want: false},
		{name: "request timeout", err: s3Err(408, "RequestTimeout"), want: false},
		{name: "too many requests", err: s3Err(429, "TooManyRequests"), want: false},
		{name: "access denied", err: s3Err(403, "AccessDenied"), want: false},
		{name: "no such bucket", err: s3Err(404, "NoSuchBucket"), want: false},
		{name: "internal error", err: s3Err(500, "InternalError"), want: false},
		{name: "service unavailable", err: s3Err(503, "ServiceUnavailable"), want: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, isPermanentS3Error(c.err))
		})
	}
}
