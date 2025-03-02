/*
 * Copyright © 2023 Clyso GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package s3

//go:generate go tool stringer -type Method

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
