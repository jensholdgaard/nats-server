// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package telemetry wires the OpenTelemetry SDK and the OTLP exporter for
// the nats-server binary, based on the `opentelemetry` configuration block
// and the standard `OTEL_*` environment variables.
//
// The server package itself only depends on the OpenTelemetry API.
// Applications embedding the server can use this package, or configure
// their own SDK and pass a MeterProvider to Server.SetMeterProvider.
package telemetry

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/nats-io/nats-server/v2/server"
)

// ShutdownFunc flushes and releases the telemetry providers.
type ShutdownFunc func(context.Context) error

// Start configures OpenTelemetry for the server according to its options.
// If no signal is enabled, or OTEL_SDK_DISABLED is set to true, it does
// nothing. The returned function must be called once the server has shut
// down; it is never nil.
func Start(ctx context.Context, s *server.Server, opts *server.Options) (ShutdownFunc, error) {
	noop := func(context.Context) error { return nil }
	if !opts.OpenTelemetry.Metrics.Enabled || sdkDisabled() {
		return noop, nil
	}

	// Route errors from the SDK, such as failed exports, to the server log.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		s.Warnf("OpenTelemetry: %v", err)
	}))

	res, err := newResource(ctx, s)
	if err != nil {
		return noop, err
	}
	mp, err := newMeterProvider(ctx, res, &opts.OpenTelemetry)
	if err != nil {
		return noop, err
	}
	if err := runtime.Start(runtime.WithMeterProvider(mp)); err != nil {
		return noop, errors.Join(err, mp.Shutdown(ctx))
	}
	if err := s.SetMeterProvider(mp); err != nil {
		return noop, errors.Join(err, mp.Shutdown(ctx))
	}
	s.Noticef("OpenTelemetry metrics export enabled")
	return mp.Shutdown, nil
}

func sdkDisabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("OTEL_SDK_DISABLED")), "true")
}

// newResource describes this server instance. Values from OTEL_SERVICE_NAME
// and OTEL_RESOURCE_ATTRIBUTES take precedence over the defaults.
func newResource(ctx context.Context, s *server.Server) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{
		semconv.ServiceName("nats-server"),
		semconv.ServiceVersion(server.VERSION),
		semconv.ServiceInstanceID(s.ID()),
		attribute.String("nats.server.name", s.Name()),
	}
	if cn := s.ClusterName(); cn != "" {
		attrs = append(attrs, attribute.String("nats.cluster.name", cn))
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(attrs...),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithFromEnv(),
	)
	// A partial resource is still usable, it only lacks some detected attributes.
	if err != nil && !errors.Is(err, resource.ErrPartialResource) {
		return nil, err
	}
	return res, nil
}

func newMeterProvider(ctx context.Context, res *resource.Resource, o *server.OpenTelemetryOpts) (*sdkmetric.MeterProvider, error) {
	var eopts []otlpmetrichttp.Option
	if o.Endpoint != "" {
		u, err := url.Parse(o.Endpoint)
		if err != nil {
			return nil, err
		}
		// Like OTEL_EXPORTER_OTLP_ENDPOINT, the endpoint is a base URL to
		// which the signal specific path is appended.
		u.Path = strings.TrimSuffix(u.Path, "/") + "/v1/metrics"
		eopts = append(eopts, otlpmetrichttp.WithEndpointURL(u.String()))
	}
	if len(o.Headers) > 0 {
		eopts = append(eopts, otlpmetrichttp.WithHeaders(o.Headers))
	}
	exp, err := otlpmetrichttp.New(ctx, eopts...)
	if err != nil {
		return nil, err
	}
	var ropts []sdkmetric.PeriodicReaderOption
	if o.Metrics.Interval > 0 {
		ropts = append(ropts, sdkmetric.WithInterval(o.Metrics.Interval))
	}
	return sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp, ropts...)),
	), nil
}
