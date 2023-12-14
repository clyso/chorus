package s3

//go:generate stringer -type Method

type Method uint8

const (
	UndefinedMethod Method = iota
	CreateBucket
	DeleteBucket
	HeadBucket
	ListBuckets
	GetBucketLocation

	GetBucketTagging
	PutBucketTagging
	DeleteBucketTagging

	GetBucketLifecycle
	DeleteBucketLifecycle
	PutBucketLifecycle

	GetBucketPolicy
	PutBucketPolicy
	DeleteBucketPolicy
	GetBucketPolicyStatus

	GetBucketAcl
	PutBucketAcl

	GetBucketVersioning
	PutBucketVersioning

	GetBucketWebsite
	PutBucketWebsite
	DeleteBucketWebsite

	GetBucketReplication
	PutBucketReplication
	DeleteBucketReplication

	GetBucketNotification
	// DeleteBucketNotification is Ceph extension
	DeleteBucketNotification
	PutBucketNotification

	GetBucketEncryption
	PutBucketEncryption
	DeleteBucketEncryption

	GetBucketRequestPayment
	PutBucketRequestPayment

	GetBucketMetricsConfiguration
	ListBucketMetricsConfiguration
	PutBucketMetricsConfiguration
	DeleteBucketMetricsConfiguration

	GetBucketAnalyticsConfiguration
	ListBucketAnalyticsConfiguration
	PutBucketAnalyticsConfiguration
	DeleteBucketAnalyticsConfiguration

	GetBucketIntelligentTieringConfiguration
	ListBucketIntelligentTieringConfiguration
	PutBucketIntelligentTieringConfiguration
	DeleteBucketIntelligentTieringConfiguration

	GetBucketInventoryConfiguration
	ListBucketInventoryConfiguration
	PutBucketInventoryConfiguration
	DeleteBucketInventoryConfiguration

	GetBucketAccelerateConfiguration
	PutBucketAccelerateConfiguration

	GetBucketLogging
	PutBucketLogging

	GetBucketOwnershipControls
	PutBucketOwnershipControls
	DeleteBucketOwnershipControls

	GetBucketCors
	PutBucketCors
	DeleteBucketCors

	GetObject
	HeadObject
	PutObject
	DeleteObject
	DeleteObjects
	ListObjects
	ListObjectsV2

	GetObjectAttributes
	CopyObject
	ListObjectVersions
	RestoreObject
	SelectObjectContent
	WriteGetObjectResponse

	CreateMultipartUpload
	UploadPart
	CompleteMultipartUpload
	AbortMultipartUpload
	ListMultipartUploads
	ListParts
	UploadPartCopy

	GetObjectTagging
	PutObjectTagging
	DeleteObjectTagging

	GetObjectAcl
	PutObjectAcl

	GetPublicAccessBlock
	PutPublicAccessBlock
	DeletePublicAccessBlock

	GetObjectRetention
	PutObjectRetention

	GetObjectLegalHold
	PutObjectLegalHold

	GetObjectLockConfiguration
	PutObjectLockConfiguration

	GetObjectTorrent
)
