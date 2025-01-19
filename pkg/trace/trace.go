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

package trace

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/clyso/chorus/pkg/dom"
)

type Config struct {
	Enabled  bool   `yaml:"enabled"`
	Endpoint string `yaml:"endpoint"`
}

func NewTracerProvider(conf *Config, version dom.AppInfo) (func(ctx context.Context) error, trace.TracerProvider, error) {
	var tp *sdktrace.TracerProvider
	if !conf.Enabled {
		tp = sdktrace.NewTracerProvider(
			sdktrace.WithResource(sdkresource.NewSchemaless(
				semconv.ServiceNameKey.String(version.App),
				semconv.ServiceVersionKey.String(version.Version),
				semconv.ServiceInstanceIDKey.String(version.AppID),
			)),
		)
		//return func(ctx context.Context) error { return nil }, trace.NewNoopTracerProvider(), nil
	} else {
		exp, err := jaeger.New(
			jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(conf.Endpoint)),
		)
		if err != nil {
			return nil, nil, err
		}

		tp = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exp),
			sdktrace.WithResource(sdkresource.NewSchemaless(
				semconv.ServiceNameKey.String(version.App),
				semconv.ServiceVersionKey.String(version.Version),
				semconv.ServiceInstanceIDKey.String(version.AppID),
			)),
		)
	}

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, tp, nil
}
