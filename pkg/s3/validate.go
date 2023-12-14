package s3

import (
	"fmt"
	"github.com/clyso/chorus/pkg/dom"
	"regexp"
	"strings"
)

var (
	validBucketName       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9\.\-\_\:]{1,61}[A-Za-z0-9]$`)
	validBucketNameStrict = regexp.MustCompile(`^[a-z0-9][a-z0-9\.\-]{1,61}[a-z0-9]$`)
	ipAddress             = regexp.MustCompile(`^(\d+\.){3}\d+$`)
)

func ValidateBucketName(bucketName string) error {
	if strings.TrimSpace(bucketName) == "" {
		return fmt.Errorf("%w: bucket name cannot be empty", dom.ErrInvalidArg)
	}
	if len(bucketName) < 3 {
		return fmt.Errorf("%w: bucket name cannot be shorter than 3 characters", dom.ErrInvalidArg)
	}
	if len(bucketName) > 63 {
		return fmt.Errorf("%w: bucket name cannot be longer than 63 characters", dom.ErrInvalidArg)
	}
	if ipAddress.MatchString(bucketName) {
		return fmt.Errorf("%w: bucket name cannot be an ip address", dom.ErrInvalidArg)
	}
	if strings.Contains(bucketName, "..") || strings.Contains(bucketName, ".-") || strings.Contains(bucketName, "-.") {
		return fmt.Errorf("%w: bucket name contains invalid characters", dom.ErrInvalidArg)
	}

	if !validBucketNameStrict.MatchString(bucketName) {
		return fmt.Errorf("%w: bucket name contains invalid characters", dom.ErrInvalidArg)
	}

	if !validBucketName.MatchString(bucketName) {
		return fmt.Errorf("%w: bucket name contains invalid characters", dom.ErrInvalidArg)
	}
	return nil
}
