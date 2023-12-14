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

package log

import (
	"context"
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/rs/zerolog"
)

func WithMethod(ctx context.Context, method s3.Method) context.Context {
	zerolog.Ctx(ctx).UpdateContext(func(c zerolog.Context) zerolog.Context {
		return c.Str(Method, method.String())
	})
	return xctx.SetMethod(ctx, method)
}

func WithObjName(ctx context.Context, objName string) context.Context {
	if objName == "" {
		return ctx
	}
	zerolog.Ctx(ctx).UpdateContext(func(c zerolog.Context) zerolog.Context {
		return c.Str(Object, objName)
	})
	return xctx.SetObject(ctx, objName)
}

func WithBucket(ctx context.Context, bucket string) context.Context {
	if bucket == "" {
		return ctx
	}
	zerolog.Ctx(ctx).UpdateContext(func(c zerolog.Context) zerolog.Context {
		return c.Str(Bucket, bucket)
	})
	return xctx.SetBucket(ctx, bucket)
}

func WithStorage(ctx context.Context, storage string) context.Context {
	if storage == "" {
		return ctx
	}
	zerolog.Ctx(ctx).UpdateContext(func(c zerolog.Context) zerolog.Context {
		return c.Str(Storage, storage)
	})
	return xctx.SetStorage(ctx, storage)
}

func WithFlow(ctx context.Context, f xctx.Flow) context.Context {
	if f == "" {
		return ctx
	}
	zerolog.Ctx(ctx).UpdateContext(func(c zerolog.Context) zerolog.Context {
		return c.Str(flow, string(f))
	})
	return xctx.SetFlow(ctx, f)
}

func WithTraceID(ctx context.Context, t string) context.Context {
	if t == "" {
		return ctx
	}
	zerolog.Ctx(ctx).UpdateContext(func(c zerolog.Context) zerolog.Context {
		return c.Str(TraceID, t)
	})
	return xctx.SetTraceID(ctx, t)
}

func WithUser(ctx context.Context, u string) context.Context {
	if u == "" {
		return ctx
	}
	zerolog.Ctx(ctx).UpdateContext(func(c zerolog.Context) zerolog.Context {
		return c.Str(user, u)
	})
	return xctx.SetUser(ctx, u)
}

func StartNew(from context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ctx = zerolog.Ctx(from).WithContext(ctx)

	if u := xctx.GetUser(from); u != "" {
		ctx = xctx.SetUser(ctx, u)
	}
	if s := xctx.GetStorage(from); s != "" {
		ctx = xctx.SetStorage(ctx, s)
	}
	if b := xctx.GetBucket(from); b != "" {
		ctx = xctx.SetBucket(ctx, b)
	}
	if f := xctx.GetFlow(from); f != "" {
		ctx = xctx.SetFlow(ctx, f)
	}
	if m := xctx.GetMethod(from); m != s3.UndefinedMethod {
		ctx = xctx.SetMethod(ctx, m)
	}
	if o := xctx.GetObject(from); o != "" {
		ctx = xctx.SetObject(ctx, o)
	}
	if t := xctx.GetTraceID(from); t != "" {
		ctx = xctx.SetTraceID(ctx, t)
	}
	return ctx, cancel
}
