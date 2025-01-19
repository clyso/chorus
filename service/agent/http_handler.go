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

package agent

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/notifications"
	"github.com/clyso/chorus/pkg/s3"
)

func HTTPHandler(handler *notifications.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		bytes, err := io.ReadAll(req.Body)
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Msg("unable to read event body")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var body s3EventBody
		err = json.Unmarshal(bytes, &body)
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Msg("unable to unmarshal event body")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, record := range body.Records {
			user, err := notifications.UserIDFromNotificationID(record.S3.ConfigurationId)
			if err != nil {
				zerolog.Ctx(ctx).Err(err).Msg("skip notification with invalid id")
				continue
			}
			reqCtx := xctx.SetUser(ctx, user)
			reqCtx = xctx.SetBucket(reqCtx, record.S3.Bucket.Name)
			reqCtx = xctx.SetObject(reqCtx, record.S3.Object.Key)
			methodArr := strings.Split(record.EventName, ":")
			switch methodArr[len(methodArr)-1] {
			case "Put", "Post":
				reqCtx = xctx.SetMethod(reqCtx, s3.PutObject)
			case "Copy":
				reqCtx = xctx.SetMethod(reqCtx, s3.CopyObject)
			case "CompleteMultipartUpload":
				reqCtx = xctx.SetMethod(reqCtx, s3.CompleteMultipartUpload)
			case "Delete":
				reqCtx = xctx.SetMethod(reqCtx, s3.DeleteObject)
			}

			switch {
			case strings.Contains(record.EventName, "ObjectCreated"):
				err = handler.PutObject(reqCtx, notifications.ObjCreated{
					Bucket:  record.S3.Bucket.Name,
					ObjKey:  record.S3.Object.Key,
					ObjETag: record.S3.Object.ETag,
					ObjSize: int64(record.S3.Object.Size),
				})
				if err != nil {
					zerolog.Ctx(reqCtx).Err(err).Msg("unable to replicate ObjectCreated")
				}
			case strings.Contains(record.EventName, "ObjectRemoved"):
				err = handler.DeleteObject(reqCtx, notifications.ObjDeleted{
					Bucket: record.S3.Bucket.Name,
					ObjKey: record.S3.Object.Key,
				})
				if err != nil {
					zerolog.Ctx(reqCtx).Err(err).Msg("unable to replicate ObjectRemoved")
				}
			default:
				zerolog.Ctx(reqCtx).Warn().Msgf("unknown s3 notification event %s", record.EventName)
			}
		}
	}
}

type s3EventBody struct {
	Records []struct {
		EventVersion string    `json:"eventVersion"`
		EventSource  string    `json:"eventSource"`
		AwsRegion    string    `json:"awsRegion"`
		EventTime    time.Time `json:"eventTime"`
		EventName    string    `json:"eventName"`
		UserIdentity struct {
			PrincipalId string `json:"principalId"`
		} `json:"userIdentity"`
		RequestParameters struct {
			SourceIPAddress string `json:"sourceIPAddress"`
		} `json:"requestParameters"`
		ResponseElements struct {
			XAmzRequestId string `json:"x-amz-request-id"`
			XAmzId2       string `json:"x-amz-id-2"`
		} `json:"responseElements"`
		S3 struct {
			S3SchemaVersion string `json:"s3SchemaVersion"`
			ConfigurationId string `json:"configurationId"`
			Bucket          struct {
				Name          string `json:"name"`
				OwnerIdentity struct {
					PrincipalId string `json:"principalId"`
				} `json:"ownerIdentity"`
				Arn string `json:"arn"`
			} `json:"bucket"`
			Object struct {
				Key       string `json:"key"`
				Size      int    `json:"size"`
				ETag      string `json:"eTag"`
				Sequencer string `json:"sequencer"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}
