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

package router

import (
	"errors"
	"net/http"

	"github.com/rs/zerolog"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/util"
)

func Middleware(policySvc policy.Service, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket, object, method := s3.ParseReq(r)
		ctx := log.WithBucket(r.Context(), bucket)
		ctx = log.WithObjName(ctx, object)
		ctx = log.WithMethod(ctx, method)
		ctx = log.WithFlow(ctx, xctx.Event)
		if method == s3.UndefinedMethod {
			zerolog.Ctx(ctx).Warn().Str("request_url", r.Method+": "+r.URL.Path+"?"+r.URL.RawQuery).Msg("unable to define s3 method")
		}
		user := xctx.GetUser(ctx)
		// build policy and add to context
		routeTo, err := policySvc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(user, bucket))
		if err != nil {
			util.WriteError(ctx, w, err)
			return
		}
		ctx = xctx.SetRoutingPolicy(ctx, routeTo)
		replicationPolicies, err := policySvc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(user, bucket))
		if err != nil && !errors.Is(err, dom.ErrNotFound) {
			util.WriteError(ctx, w, err)
			return
		}
		if replicationPolicies != nil {
			var replications []entity.ReplicationStatusID
			for _, replTo := range replicationPolicies.Destinations {
				replications = append(replications, entity.ReplicationStatusID{
					User:        user,
					FromStorage: replicationPolicies.FromStorage,
					FromBucket:  bucket,
					ToStorage:   replTo.Storage,
					ToBucket:    replTo.Bucket,
				})
			}
			if len(replications) != 0 {
				ctx = xctx.SetReplications(ctx, replications)
			}
		}
		switchInfo, err := policySvc.GetInProgressZeroDowntimeSwitchInfo(ctx, entity.NewReplicationSwitchInfoID(xctx.GetUser(ctx), bucket))
		if err != nil && !errors.Is(err, dom.ErrNotFound) {
			util.WriteError(ctx, w, err)
			return
		}
		// set in progress switch to context only if exists
		if switchInfo.MultipartTTL != 0 {
			ctx = xctx.SetInProgressZeroDowntime(ctx, switchInfo)
		}

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
