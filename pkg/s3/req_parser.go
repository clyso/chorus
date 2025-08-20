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

import (
	"net/http"
	"strings"
)

// ParseBucketAndObject extracts bucket and object from the request based on hostname and path
// Returns bucket, object, and whether bucket was found in hostname (virtual host style)
func ParseBucketAndObject(r *http.Request) (bucket string, object string) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)
	bucket = parts[0]
	if bucket == "" {
		bucket = r.Header.Get("x-amz-bucket")
	}

	if len(parts) == 2 {
		object = parts[1]
	}

	return
}

func ParseReq(r *http.Request) (bucket string, object string, method Method) {
	query := r.URL.Query()
	bucket, object = ParseBucketAndObject(r)

	switch {
	case query.Has("lifecycle") && bucket != "":
		method = routeBucketLC(r)
	case query.Has("policyStatus") && bucket != "":
		method = GetBucketPolicyStatus
	case query.Has("policy") && bucket != "":
		method = routeBucketPolicy(r)
	case query.Has("acl") && bucket != "":
		method = routeACL(r, object)
	case query.Has("versioning") && bucket != "":
		method = routeBucketVersioning(r)
	case query.Has("website") && bucket != "":
		method = routeBucketWebsite(r)
	case query.Has("replication") && bucket != "":
		method = routeBucketReplication(r)
	case query.Has("notification") && bucket != "":
		method = routeBucketNotification(r)
	case query.Has("encryption") && bucket != "":
		method = routeBucketEncryption(r)
	case query.Has("requestPayment") && bucket != "":
		method = routeBucketRequestPayment(r)
	case query.Has("metrics") && bucket != "":
		method = routeBucketMetrics(r)
	case query.Has("analytics") && bucket != "":
		method = routeBucketAnalytics(r)
	case query.Has("intelligent-tiering") && bucket != "":
		method = routeBucketTiering(r)
	case query.Has("cors") && bucket != "":
		method = routeBucketCors(r)
	case query.Has("accelerate") && bucket != "":
		method = routeBucketAccelerate(r)
	case query.Has("logging") && bucket != "":
		method = routeBucketLogging(r)
	case query.Has("ownershipControls") && bucket != "":
		method = routeBucketOwnershipControls(r)
	case query.Has("inventory") && bucket != "":
		method = routeBucketInventory(r)
	case query.Has("publicAccessBlock") && bucket != "":
		method = routePublicAccessBlock(r)
	case query.Has("versions") && bucket != "" && r.Method == "GET":
		method = ListObjectVersions
		//object:
	case query.Has("attributes") && r.Method == "GET" && object != "":
		method = GetObjectAttributes
	case query.Has("restore") && r.Method == "POST" && object != "":
		method = RestoreObject
	case query.Has("select") && query.Get("select-type") == "2" && r.Method == "POST" && object != "":
		method = SelectObjectContent
	case query.Has("delete") && r.Method == "POST":
		method = DeleteObjects
	case query.Has("retention") && object != "":
		method = routeObjectRetention(r)
	case query.Has("legal-hold") && object != "":
		method = routeObjectLegalHold(r)
	case query.Has("object-lock") && object != "":
		method = routeObjectLock(r)
	case query.Get("uploadId") != "":
		method = routeMultipartUpload(r)
	case query.Has("uploads"):
		method = routeMultipartUploadBase(r)
	case query.Has("tagging"):
		method = routeTagging(r, object)
	case bucket != "" && object != "":
		method = routeObject(r)
	case bucket != "" || r.Method == "GET":
		method = routeBucket(r, bucket)
	default:
		method = UndefinedMethod
	}
	return
}

func routeObject(r *http.Request) Method {
	switch r.Method {
	case "GET":
		if r.URL.Query().Has("torrent") {
			return GetObjectTorrent
		}
		return GetObject
	case "HEAD":
		return HeadObject
	case "PUT":
		if r.Header.Get(AmzCopySource) != "" {
			return CopyObject
		}
		return PutObject
	case "DELETE":
		return DeleteObject
	default:
		return UndefinedMethod
	}
}

func routeBucketLC(r *http.Request) Method {
	switch r.Method {
	case "GET":
		return GetBucketLifecycle
	case "PUT":
		return PutBucketLifecycle
	case "DELETE":
		return DeleteBucketLifecycle
	default:
		return UndefinedMethod
	}
}

func routeBucketPolicy(r *http.Request) Method {
	switch r.Method {
	case "GET":
		return GetBucketPolicy
	case "PUT":
		return PutBucketPolicy
	case "DELETE":
		return DeleteBucketPolicy
	default:
		return UndefinedMethod
	}
}

func routeBucketWebsite(r *http.Request) Method {
	switch r.Method {
	case "GET":
		return GetBucketWebsite
	case "PUT":
		return PutBucketWebsite
	case "DELETE":
		return DeleteBucketWebsite
	default:
		return UndefinedMethod
	}
}

func routeBucketReplication(r *http.Request) Method {
	switch r.Method {
	case "GET":
		return GetBucketReplication
	case "PUT":
		return PutBucketReplication
	case "DELETE":
		return DeleteBucketReplication
	default:
		return UndefinedMethod
	}
}

func routeBucketEncryption(r *http.Request) Method {
	switch r.Method {
	case "GET":
		return GetBucketEncryption
	case "PUT":
		return PutBucketEncryption
	case "DELETE":
		return DeleteBucketEncryption
	default:
		return UndefinedMethod
	}
}

func routeBucketCors(r *http.Request) Method {
	switch r.Method {
	case "GET":
		return GetBucketCors
	case "PUT":
		return PutBucketCors
	case "DELETE":
		return DeleteBucketCors
	default:
		return UndefinedMethod
	}
}

func routePublicAccessBlock(r *http.Request) Method {
	switch r.Method {
	case "GET":
		return GetPublicAccessBlock
	case "PUT":
		return PutPublicAccessBlock
	case "DELETE":
		return DeletePublicAccessBlock
	default:
		return UndefinedMethod
	}
}

func routeObjectRetention(r *http.Request) Method {
	switch r.Method {
	case "GET":
		return GetObjectRetention
	case "PUT":
		return PutObjectRetention
	default:
		return UndefinedMethod
	}
}

func routeObjectLegalHold(r *http.Request) Method {
	switch r.Method {
	case "GET":
		return GetObjectLegalHold
	case "PUT":
		return PutObjectLegalHold
	default:
		return UndefinedMethod
	}
}

func routeObjectLock(r *http.Request) Method {
	switch r.Method {
	case "GET":
		return GetObjectLockConfiguration
	case "PUT":
		return PutObjectLockConfiguration
	default:
		return UndefinedMethod
	}
}

func routeBucketOwnershipControls(r *http.Request) Method {
	switch r.Method {
	case "GET":
		return GetBucketOwnershipControls
	case "PUT":
		return PutBucketOwnershipControls
	case "DELETE":
		return DeleteBucketOwnershipControls
	default:
		return UndefinedMethod
	}
}

func routeBucketMetrics(r *http.Request) Method {
	switch r.Method {
	case "GET":
		if r.URL.Query().Get("id") != "" {
			return GetBucketMetricsConfiguration
		}
		return ListBucketMetricsConfiguration
	case "PUT":
		return PutBucketMetricsConfiguration
	case "DELETE":
		return DeleteBucketMetricsConfiguration
	default:
		return UndefinedMethod
	}
}

func routeBucketAnalytics(r *http.Request) Method {
	switch r.Method {
	case "GET":
		if r.URL.Query().Get("id") != "" {
			return GetBucketAnalyticsConfiguration
		}
		return ListBucketAnalyticsConfiguration
	case "PUT":
		return PutBucketAnalyticsConfiguration
	case "DELETE":
		return DeleteBucketAnalyticsConfiguration
	default:
		return UndefinedMethod
	}
}

func routeBucketTiering(r *http.Request) Method {
	switch r.Method {
	case "GET":
		if r.URL.Query().Get("id") != "" {
			return GetBucketIntelligentTieringConfiguration
		}
		return ListBucketIntelligentTieringConfiguration
	case "PUT":
		return PutBucketIntelligentTieringConfiguration
	case "DELETE":
		return DeleteBucketIntelligentTieringConfiguration
	default:
		return UndefinedMethod
	}
}

func routeBucketInventory(r *http.Request) Method {
	switch r.Method {
	case "GET":
		if r.URL.Query().Get("id") != "" {
			return GetBucketInventoryConfiguration
		}
		return ListBucketInventoryConfiguration
	case "PUT":
		return PutBucketInventoryConfiguration
	case "DELETE":
		return DeleteBucketInventoryConfiguration
	default:
		return UndefinedMethod
	}
}

func routeBucketRequestPayment(r *http.Request) Method {
	switch r.Method {
	case "GET":
		return GetBucketRequestPayment
	case "PUT":
		return PutBucketRequestPayment
	default:
		return UndefinedMethod
	}
}

func routeACL(r *http.Request, object string) Method {

	switch r.Method {
	case "GET":
		if object != "" {
			return GetObjectAcl
		}
		return GetBucketAcl
	case "PUT":
		if object != "" {
			return PutObjectAcl
		}
		return PutBucketAcl
	default:
		return UndefinedMethod
	}
}

func routeBucketNotification(r *http.Request) Method {
	switch r.Method {
	case "GET":
		return GetBucketNotification
	case "PUT":
		return PutBucketNotification
	case "DELETE":
		return DeleteBucketNotification
	default:
		return UndefinedMethod
	}
}

func routeBucketVersioning(r *http.Request) Method {
	switch r.Method {
	case "GET":
		return GetBucketVersioning
	case "PUT":
		return PutBucketVersioning
	default:
		return UndefinedMethod
	}
}

func routeBucketAccelerate(r *http.Request) Method {
	switch r.Method {
	case "GET":
		return GetBucketAccelerateConfiguration
	case "PUT":
		return PutBucketAccelerateConfiguration
	default:
		return UndefinedMethod
	}
}

func routeBucketLogging(r *http.Request) Method {
	switch r.Method {
	case "GET":
		return GetBucketLogging
	case "PUT":
		return PutBucketLogging
	default:
		return UndefinedMethod
	}
}

func routeBucket(r *http.Request, bucket string) Method {
	switch r.Method {
	case "GET":
		if bucket == "" {
			return ListBuckets
		}
		if r.URL.Query().Has("location") {
			return GetBucketLocation
		}
		if r.URL.Query().Get("list-type") == "2" {
			return ListObjectsV2
		}
		return ListObjects
	case "PUT":
		return CreateBucket
	case "DELETE":
		return DeleteBucket
	case "HEAD":
		return HeadBucket
	case "POST":
		if r.URL.Query().Has("delete") {
			return DeleteObjects
		}
		return UndefinedMethod
	default:
		return UndefinedMethod
	}
}

func routeMultipartUploadBase(r *http.Request) Method {
	switch r.Method {
	case "GET":
		return ListMultipartUploads
	case "POST":
		return CreateMultipartUpload
	default:
		return UndefinedMethod
	}
}

func routeMultipartUpload(r *http.Request) Method {
	switch r.Method {
	case "GET":
		return ListParts
	case "PUT":
		if r.Header.Get(AmzCopySource) != "" {
			return UploadPartCopy
		}
		return UploadPart
	case "DELETE":
		return AbortMultipartUpload
	case "POST":
		return CompleteMultipartUpload
	default:
		return UndefinedMethod
	}
}

func routeTagging(r *http.Request, object string) Method {
	if object != "" {
		switch r.Method {
		case "GET":
			return GetObjectTagging
		case "PUT":
			return PutObjectTagging
		case "DELETE":
			return DeleteObjectTagging
		default:
			return UndefinedMethod
		}
	}

	switch r.Method {
	case "GET":
		return GetBucketTagging
	case "PUT":
		return PutBucketTagging
	case "DELETE":
		return DeleteBucketTagging
	default:
		return UndefinedMethod
	}
}
