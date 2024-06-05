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

package s3client

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/clyso/chorus/pkg/s3"
	mclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/signer"
)

// List of success status.
var successStatus = map[int]bool{
	http.StatusOK:             true,
	http.StatusNoContent:      true,
	http.StatusPartialContent: true,
}

var s3ErrorResponseMap = map[string]string{
	"AccessDenied":                      "Access Denied.",
	"BadDigest":                         "The Content-Md5 you specified did not match what we received.",
	"EntityTooSmall":                    "Your proposed upload is smaller than the minimum allowed object size.",
	"EntityTooLarge":                    "Your proposed upload exceeds the maximum allowed object size.",
	"IncompleteBody":                    "You did not provide the number of bytes specified by the Content-Length HTTP header.",
	"InternalError":                     "We encountered an internal error, please try again.",
	"InvalidAccessKeyId":                "The access key ID you provided does not exist in our records.",
	"InvalidBucketName":                 "The specified bucket is not valid.",
	"InvalidDigest":                     "The Content-Md5 you specified is not valid.",
	"InvalidRange":                      "The requested range is not satisfiable",
	"MalformedXML":                      "The XML you provided was not well-formed or did not validate against our published schema.",
	"MissingContentLength":              "You must provide the Content-Length HTTP header.",
	"MissingContentMD5":                 "Missing required header for this request: Content-Md5.",
	"MissingRequestBodyError":           "Request body is empty.",
	"NoSuchBucket":                      "The specified bucket does not exist.",
	"NoSuchBucketPolicy":                "The bucket policy does not exist",
	"NoSuchKey":                         "The specified key does not exist.",
	"NoSuchUpload":                      "The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.",
	"NotImplemented":                    "A header you provided implies functionality that is not implemented",
	"PreconditionFailed":                "At least one of the pre-conditions you specified did not hold",
	"RequestTimeTooSkewed":              "The difference between the request time and the server's time is too large.",
	"SignatureDoesNotMatch":             "The request signature we calculated does not match the signature you provided. Check your key and signing method.",
	"MethodNotAllowed":                  "The specified method is not allowed against this resource.",
	"InvalidPart":                       "One or more of the specified parts could not be found.",
	"InvalidPartOrder":                  "The list of parts was not in ascending order. The parts list must be specified in order by part number.",
	"InvalidObjectState":                "The operation is not valid for the current state of the object.",
	"AuthorizationHeaderMalformed":      "The authorization header is malformed; the region is wrong.",
	"MalformedPOSTRequest":              "The body of your POST request is not well-formed multipart/form-data.",
	"BucketNotEmpty":                    "The bucket you tried to delete is not empty",
	"AllAccessDisabled":                 "All access to this bucket has been disabled.",
	"MalformedPolicy":                   "Policy has invalid resource.",
	"MissingFields":                     "Missing fields in request.",
	"AuthorizationQueryParametersError": "Error parsing the X-Amz-Credential parameter; the Credential is mal-formed; expecting \"<YOUR-AKID>/YYYYMMDD/REGION/SERVICE/aws4_request\".",
	"MalformedDate":                     "Invalid date format header, expected to be in ISO8601, RFC1123 or RFC1123Z time format.",
	"BucketAlreadyOwnedByYou":           "Your previous request to create the named bucket succeeded and you already own it.",
	"InvalidDuration":                   "Duration provided in the request is invalid.",
	"XAmzContentSHA256Mismatch":         "The provided 'x-amz-content-sha256' header does not match what was computed.",
	// Add new API errors here.
}

// httpRespToErrorResponse returns a new encoded ErrorResponse
// structure as error.
func httpRespToErrorResponse(resp *http.Response, bucketName, objectName string) error {
	if resp == nil {
		return fmt.Errorf("empty http response")
	}

	errResp := mclient.ErrorResponse{
		StatusCode: resp.StatusCode,
		Server:     resp.Header.Get("Server"),
	}

	errBody, err := xmlDecodeAndBody(resp.Body, &errResp)
	// Xml decoding failed with no body, fall back to HTTP headers.
	if err != nil {
		switch resp.StatusCode {
		case http.StatusNotFound:
			if objectName == "" {
				errResp = mclient.ErrorResponse{
					StatusCode: resp.StatusCode,
					Code:       "NoSuchBucket",
					Message:    "The specified bucket does not exist.",
					BucketName: bucketName,
				}
			} else {
				errResp = mclient.ErrorResponse{
					StatusCode: resp.StatusCode,
					Code:       "NoSuchKey",
					Message:    "The specified key does not exist.",
					BucketName: bucketName,
					Key:        objectName,
				}
			}
		case http.StatusForbidden:
			errResp = mclient.ErrorResponse{
				StatusCode: resp.StatusCode,
				Code:       "AccessDenied",
				Message:    "Access Denied.",
				BucketName: bucketName,
				Key:        objectName,
			}
		case http.StatusConflict:
			errResp = mclient.ErrorResponse{
				StatusCode: resp.StatusCode,
				Code:       "Conflict",
				Message:    "Bucket not empty.",
				BucketName: bucketName,
			}
		case http.StatusPreconditionFailed:
			errResp = mclient.ErrorResponse{
				StatusCode: resp.StatusCode,
				Code:       "PreconditionFailed",
				Message:    s3ErrorResponseMap["PreconditionFailed"],
				BucketName: bucketName,
				Key:        objectName,
			}
		default:
			msg := resp.Status
			if len(errBody) > 0 {
				msg = string(errBody)
				if len(msg) > 1024 {
					msg = msg[:1024] + "..."
				}
			}
			errResp = mclient.ErrorResponse{
				StatusCode: resp.StatusCode,
				Code:       resp.Status,
				Message:    msg,
				BucketName: bucketName,
			}
		}
	}

	// Save hostID, requestID and region information
	// from headers if not available through error XML.
	if errResp.RequestID == "" {
		errResp.RequestID = resp.Header.Get("x-amz-request-id")
	}
	if errResp.HostID == "" {
		errResp.HostID = resp.Header.Get("x-amz-id-2")
	}
	if errResp.Region == "" {
		errResp.Region = resp.Header.Get("x-amz-bucket-region")
	}
	if errResp.Code == "InvalidRegion" && errResp.Region != "" {
		errResp.Message = fmt.Sprintf("Region does not match, expecting region ‘%s’.", errResp.Region)
	}

	return errResp
}

// xmlDecodeAndBody reads the whole body up to 1MB and
// tries to XML decode it into v.
// The body that was read and any error from reading or decoding is returned.
func xmlDecodeAndBody(bodyReader io.Reader, v interface{}) ([]byte, error) {
	// read the whole body (up to 1MB)
	const maxBodyLength = 1 << 20
	body, err := io.ReadAll(io.LimitReader(bodyReader, maxBodyLength))
	if err != nil {
		return nil, err
	}
	return bytes.TrimSpace(body), xmlDecoder(bytes.NewReader(body), v)
}

// xmlDecoder provide decoded value in xml.
func xmlDecoder(body io.Reader, v interface{}) error {
	d := xml.NewDecoder(body)
	return d.Decode(v)
}

// closeResponse close non nil response with any response Body.
// convenient wrapper to drain any remaining data on response body.
//
// Subsequently this allows golang http RoundTripper
// to re-use the same connection for future requests.
func closeResponse(resp *http.Response) {
	// Callers should close resp.Body when done reading from it.
	// If resp.Body is not closed, the Client's underlying RoundTripper
	// (typically Transport) may not be able to re-use a persistent TCP
	// connection to the server for a subsequent "keep-alive" request.
	if resp != nil && resp.Body != nil {
		// Drain any remaining Body and then close the connection.
		// Without this closing connection would disallow re-using
		// the same connection for future uses.
		//  - http://stackoverflow.com/a/17961593/4465767
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

func ExtractReqBody(r *http.Request, body any) error {
	var b bytes.Buffer
	_, err := b.ReadFrom(r.Body)
	if err != nil {
		return err
	}
	r.Body = io.NopCloser(&b)

	return xml.Unmarshal(b.Bytes(), body)
}

func ExtractRespBody(r *http.Response, body any) error {
	var b bytes.Buffer
	_, err := b.ReadFrom(r.Body)
	if err != nil {
		return err
	}
	r.Body = io.NopCloser(&b)

	return xml.Unmarshal(b.Bytes(), body)
}

// Internal function called for different service types.
func signV4(req http.Request, accessKeyID, secretAccessKey, sessionToken, location string) (*http.Request, error) {
	var res *http.Request
	prevCL := req.Header.Get("Content-Length")
	if prevCL == "0" {
		req.Header.Del("Content-Length")
		defer func() {
			res.Header.Set("Content-Length", prevCL)
		}()
	}
	res = signer.SignV4(req, accessKeyID, secretAccessKey, sessionToken, location)
	return res, nil
}

var ignoreInSignature = map[string]struct{}{
	"x-forwarded-for":    {},
	"x-forwarded-host":   {},
	"x-forwarded-port":   {},
	"x-forwarded-proto":  {},
	"x-forwarded-server": {},
	"connection":         {},
	"x-real-ip":          {},
}

// processHeaders decides which orginal headers should be copied to forwarded request.
func processHeaders(origin http.Header) (toSign http.Header, notToSign http.Header) {
	// get a set of headers from origin signature if it was V4
	var origSigned map[string]struct{}
	if len(origin[s3.Authorization]) == 1 {
		res, err := s3.ParseSignV4(origin[s3.Authorization][0])
		if err == nil {
			origSigned = make(map[string]struct{}, len(res.SignedHeaders))
			for _, v := range res.SignedHeaders {
				origSigned[strings.ToLower(v)] = struct{}{}
			}
		}
	}

	// process headers
	toSign, notToSign = http.Header{}, http.Header{}
	for name, vals := range origin {
		if name == "Authorization" || name == "X-Amz-Date" {
			// remove from request
			continue
		}
		if _, ok := ignoreInSignature[strings.ToLower(name)]; ok {
			notToSign[name] = vals
			continue
		}

		if origSigned == nil || len(origSigned) == 0 {
			// sign all if there no signedHeaders in origin
			toSign[name] = vals
			continue
		}

		// sign only from origin signature
		if _, ok := origSigned[strings.ToLower(name)]; ok {
			toSign[name] = vals
		} else {
			notToSign[name] = vals
		}
	}
	return
}
