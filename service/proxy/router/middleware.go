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
	"context"
	"errors"
	"fmt"
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

		policyCtx, err := initPolicyContext(ctx, policySvc)
		if err != nil {
			util.WriteError(ctx, w, err)
			return
		}
		ctx = policyCtx

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// TODO: move this logic to a single method in policy service in the next PR
func initPolicyContext(ctx context.Context, policySvc policy.Service) (context.Context, error) {
	user := xctx.GetUser(ctx)
	bucket := xctx.GetBucket(ctx)

	if bucket == "" {
		// handle ListBuckets request here. It is the only request without a bucket.
		if xctx.GetMethod(ctx) != s3.ListBuckets {
			// should never happen
			return nil, fmt.Errorf("%w: bucket is not defined in context for s3 method %s", dom.ErrInternal, xctx.GetMethod(ctx).String())
		}
		routeTo, err := policySvc.GetUserRoutingPolicy(ctx, user)
		if err != nil {
			return nil, err
		}
		ctx = xctx.SetRoutingPolicy(ctx, routeTo)
		return ctx, nil
	}

	// Set bucket routing policy
	routeTo, err := policySvc.GetRoutingPolicy(ctx, entity.NewBucketRoutingPolicyID(user, bucket))
	if err != nil {
		return nil, err
	}
	ctx = xctx.SetRoutingPolicy(ctx, routeTo)

	// Set bucket replication policies
	replicationPolicies, err := policySvc.GetBucketReplicationPolicies(ctx, entity.NewBucketReplicationPolicyID(user, bucket))
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return nil, err
	}
	if replicationPolicies != nil {
		var replications []entity.UniversalReplicationID
		for _, replTo := range replicationPolicies.Destinations {
			replications = append(replications, entity.IDFromBucketReplication(entity.ReplicationStatusID{
				User:        user,
				FromStorage: replicationPolicies.FromStorage,
				FromBucket:  bucket,
				ToStorage:   replTo.Storage,
				ToBucket:    replTo.Bucket,
			}))
		}
		if len(replications) != 0 {
			ctx = xctx.SetReplications(ctx, replications)
		}
	}

	// Set in-progress zero-downtime switch info
	switchInfo, err := policySvc.GetInProgressZeroDowntimeSwitchInfo(ctx, entity.NewReplicationSwitchInfoID(xctx.GetUser(ctx), bucket))
	if err != nil && !errors.Is(err, dom.ErrNotFound) {
		return nil, err
	}
	if switchInfo.MultipartTTL != 0 {
		ctx = xctx.SetInProgressZeroDowntime(ctx, switchInfo)
	}
	return ctx, nil
}
