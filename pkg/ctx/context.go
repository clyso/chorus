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

package ctx

import (
	"context"

	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/s3"
)

type methodKey struct{}
type objectKey struct{}
type bucketKey struct{}
type storageKey struct{}
type flowKey struct{}
type traceKey struct{}
type userKey struct{}

type Flow string

const (
	Event     Flow = "event"
	Migration Flow = "migration"
	Api       Flow = "api"
)

func SetMethod(ctx context.Context, in s3.Method) context.Context {
	return context.WithValue(ctx, methodKey{}, in)
}

func GetMethod(ctx context.Context) s3.Method {
	res, _ := ctx.Value(methodKey{}).(s3.Method)
	return res
}

func SetObject(ctx context.Context, in string) context.Context {
	if in == "" {
		zerolog.Ctx(ctx).Warn().Msg("ignore: trying to set empty object to ctx")
		return ctx
	}
	if prev := GetObject(ctx); prev != "" {
		zerolog.Ctx(ctx).Warn().Msgf("cannot set object %s, ctx already contains object %s", in, prev)
		return ctx
	}
	return context.WithValue(ctx, objectKey{}, in)
}

func GetObject(ctx context.Context) string {
	res, _ := ctx.Value(objectKey{}).(string)
	return res
}

func SetBucket(ctx context.Context, in string) context.Context {
	if in == "" {
		zerolog.Ctx(ctx).Warn().Msg("ignore: trying to set empty bucket to ctx")
		return ctx
	}
	if prev := GetBucket(ctx); prev != "" {
		zerolog.Ctx(ctx).Warn().Msgf("cannot set bucket %s, ctx already contains bucket %s", in, prev)
		return ctx
	}
	return context.WithValue(ctx, bucketKey{}, in)
}

func GetBucket(ctx context.Context) string {
	res, _ := ctx.Value(bucketKey{}).(string)
	return res
}

func SetStorage(ctx context.Context, in string) context.Context {
	if in == "" {
		zerolog.Ctx(ctx).Warn().Msg("ignore: trying to set empty storage to ctx")
		return ctx
	}
	if prev := GetStorage(ctx); prev != "" {
		zerolog.Ctx(ctx).Warn().Msgf("cannot set storage %s, ctx already contains storage %s", in, prev)
		return ctx
	}
	return context.WithValue(ctx, storageKey{}, in)
}

func GetStorage(ctx context.Context) string {
	res, _ := ctx.Value(storageKey{}).(string)
	return res
}

func SetFlow(ctx context.Context, in Flow) context.Context {
	return context.WithValue(ctx, flowKey{}, in)
}

func GetFlow(ctx context.Context) Flow {
	res, _ := ctx.Value(flowKey{}).(Flow)
	return res
}

func SetTraceID(ctx context.Context, in string) context.Context {
	return context.WithValue(ctx, traceKey{}, in)
}

func GetTraceID(ctx context.Context) string {
	res, _ := ctx.Value(traceKey{}).(string)
	return res
}

func SetUser(ctx context.Context, u string) context.Context {
	if u == "" {
		zerolog.Ctx(ctx).Warn().Msg("ignore: trying to set empty user to ctx")
		return ctx
	}
	if prev := GetUser(ctx); prev != "" {
		zerolog.Ctx(ctx).Warn().Msgf("cannot set user %s, ctx already contains user %s", u, prev)
		return ctx
	}
	return context.WithValue(ctx, userKey{}, u)
}

func GetUser(ctx context.Context) string {
	k, _ := ctx.Value(userKey{}).(string)
	return k
}
