package router

import (
	"encoding/xml"
	"github.com/clyso/chorus/pkg/dom"
)

// createBucketConfiguration container for bucket configuration.
type createBucketConfiguration struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CreateBucketConfiguration" json:"-"`
	Location string   `xml:"LocationConstraint"`
}

type objectID struct {
	Key string `xml:"Key"`

	// Versions not supported in GoFakeS3 yet.
	VersionID string `xml:"VersionId,omitempty" json:"VersionId,omitempty"`
}

func (o objectID) toDom(bucket string) dom.Object {
	return dom.Object{
		Bucket:  bucket,
		Name:    o.Key,
		Version: o.VersionID,
	}
}

type multiDeleteResult struct {
	XMLName xml.Name      `xml:"DeleteResult"`
	Deleted []objectID    `xml:"Deleted"`
	Error   []errorResult `xml:",omitempty"`
}

type errorResult struct {
	XMLName   xml.Name `xml:"Error"`
	Key       string   `xml:"Key,omitempty"`
	Code      string   `xml:"Code,omitempty"`
	Message   string   `xml:"Message,omitempty"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId,omitempty"`
}

type deleteObjectsRequest struct {
	Objects []objectID `xml:"Object"`

	// Element to enable quiet mode for the request. When you add this element,
	// you must set its value to true.
	//
	// By default, the operation uses verbose mode in which the response
	// includes the result of deletion of each key in your request. In quiet
	// mode the response includes only keys where the delete operation
	// encountered an error. For a successful deletion, the operation does not
	// return any information about the delete in the response body.
	Quiet bool `xml:"Quiet"`
}

type initiateMultipartUploadResult struct {
	Bucket   string
	Key      string
	UploadID string `xml:"UploadId"`
}

type completeMultipartUploadResult struct {
	Location string
	Bucket   string
	Key      string
	ETag     string

	// Checksum values, hash of hashes of parts.
	ChecksumCRC32  string
	ChecksumCRC32C string
	ChecksumSHA1   string
	ChecksumSHA256 string
}
