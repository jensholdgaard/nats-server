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

package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestOpenTelemetryConfig(t *testing.T) {
	for _, test := range []struct {
		name     string
		content  string
		expected OpenTelemetryOpts
		err      string
	}{
		{
			name: "full block",
			content: `
				opentelemetry {
					endpoint: "https://collector:4318"
					headers: { authorization: "Bearer secret" }
					metrics { interval: "30s" }
				}`,
			expected: OpenTelemetryOpts{
				Endpoint: "https://collector:4318",
				Headers:  map[string]string{"authorization": "Bearer secret"},
				Metrics:  OpenTelemetryMetricsOpts{Enabled: true, Interval: 30 * time.Second},
			},
		},
		{
			name:     "alias and boolean metrics",
			content:  `otel { metrics: true }`,
			expected: OpenTelemetryOpts{Metrics: OpenTelemetryMetricsOpts{Enabled: true}},
		},
		{
			name:     "metrics block disabled",
			content:  `otel { metrics { enabled: false, interval: "5s" } }`,
			expected: OpenTelemetryOpts{Metrics: OpenTelemetryMetricsOpts{Interval: 5 * time.Second}},
		},
		{
			name:    "invalid endpoint",
			content: `otel { endpoint: "collector:4318", metrics: true }`,
			err:     "invalid opentelemetry endpoint",
		},
		{
			name:    "unknown field",
			content: `otel { metrics: true, protocol: "grpc" }`,
			err:     `unknown field "protocol"`,
		},
		{
			name:    "unknown metrics field",
			content: `otel { metrics { cardinality: 10 } }`,
			err:     `unknown field "cardinality"`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(test.content))
			opts, err := ProcessConfigFile(conf)
			if test.err != _EMPTY_ {
				if err == nil || !strings.Contains(err.Error(), test.err) {
					t.Fatalf("Expected error containing %q, got %v", test.err, err)
				}
				return
			}
			require_NoError(t, err)
			require_Equal(t, opts.OpenTelemetry.Endpoint, test.expected.Endpoint)
			require_Equal(t, len(opts.OpenTelemetry.Headers), len(test.expected.Headers))
			for k, v := range test.expected.Headers {
				require_Equal(t, opts.OpenTelemetry.Headers[k], v)
			}
			require_Equal(t, opts.OpenTelemetry.Metrics, test.expected.Metrics)
		})
	}
}

func TestOpenTelemetryConfigReload(t *testing.T) {
	s, _, conf := newServerWithContent(t, []byte(`
		listen: 127.0.0.1:-1
		otel { metrics: true, headers: { authorization: "Bearer old" } }
	`))
	defer s.Shutdown()

	// Unchanged block must not prevent a reload.
	changeCurrentConfigContentWithNewContent(t, conf, []byte(`
		listen: 127.0.0.1:-1
		debug: true
		otel { metrics: true, headers: { authorization: "Bearer old" } }
	`))
	require_NoError(t, s.Reload())

	changeCurrentConfigContentWithNewContent(t, conf, []byte(`
		listen: 127.0.0.1:-1
		debug: true
		otel { metrics: true, headers: { authorization: "Bearer new" } }
	`))
	err := s.Reload()
	if err == nil || !strings.Contains(err.Error(), "config reload not supported for OpenTelemetry") {
		t.Fatalf("Expected reload error, got %v", err)
	}
	// Header values may be credentials and must not leak in the error.
	if strings.Contains(err.Error(), "Bearer") {
		t.Fatalf("Reload error leaks header values: %v", err)
	}
}

// otelFindMetric returns the metric with the given name, or nil.
func otelFindMetric(rm *metricdata.ResourceMetrics, name string) *metricdata.Metrics {
	for _, sm := range rm.ScopeMetrics {
		for i := range sm.Metrics {
			if sm.Metrics[i].Name == name {
				return &sm.Metrics[i]
			}
		}
	}
	return nil
}

// otelInt64Value returns the value of the data point of an int64 sum metric
// matching the given attribute (or the only data point if kv is nil).
func otelInt64Value(t *testing.T, rm *metricdata.ResourceMetrics, name string, kv *attribute.KeyValue) int64 {
	t.Helper()
	m := otelFindMetric(rm, name)
	if m == nil {
		t.Fatalf("Metric %q not found", name)
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("Metric %q is a %T, expected an int64 sum", name, m.Data)
	}
	for _, dp := range sum.DataPoints {
		if kv == nil {
			return dp.Value
		}
		if v, ok := dp.Attributes.Value(kv.Key); ok && v == kv.Value {
			return dp.Value
		}
	}
	t.Fatalf("No data point for metric %q with attribute %v", name, kv)
	return 0
}

func TestOpenTelemetryMetrics(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())
	require_NoError(t, s.SetMeterProvider(mp))

	collect := func() *metricdata.ResourceMetrics {
		t.Helper()
		var rm metricdata.ResourceMetrics
		require_NoError(t, reader.Collect(context.Background(), &rm))
		return &rm
	}

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()
	sub := natsSubSync(t, nc, "foo")
	const n = 10
	for range n {
		natsPub(t, nc, "foo", []byte("hello"))
	}
	for range n {
		natsNexMsg(t, sub, time.Second)
	}
	js, err := nc.JetStream()
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"bar"}})
	require_NoError(t, err)
	_, err = js.Publish("bar", []byte("hello"))
	require_NoError(t, err)

	rm := collect()
	if otelFindMetric(rm, "nats.server.messages") == nil {
		t.Fatalf("Server metrics are not reported")
	}

	receive, transmit := otelAttrIODirection.String("receive"), otelAttrIODirection.String("transmit")
	if v := otelInt64Value(t, rm, "nats.server.client.messages", &receive); v < n {
		t.Fatalf("Expected at least %d messages received from clients, got %d", n, v)
	}
	if v := otelInt64Value(t, rm, "nats.server.client.messages", &transmit); v < n {
		t.Fatalf("Expected at least %d messages sent to clients, got %d", n, v)
	}
	if v := otelInt64Value(t, rm, "nats.server.client.io", &receive); v < 5*n {
		t.Fatalf("Expected at least %d bytes received from clients, got %d", 5*n, v)
	}
	client := otelAttrConnKind.String("client")
	require_Equal(t, otelInt64Value(t, rm, "nats.server.connections", &client), int64(s.NumClients()))
	if v := otelInt64Value(t, rm, "nats.server.subscriptions", nil); v < 1 {
		t.Fatalf("Expected at least one subscription, got %d", v)
	}

	file := otelAttrJSStorageType.String("file")
	if v := otelInt64Value(t, rm, "nats.jetstream.storage.usage", &file); v <= 0 {
		t.Fatalf("Expected JetStream file storage to be in use, got %d", v)
	}
	if v := otelInt64Value(t, rm, "nats.jetstream.api.requests", nil); v < 1 {
		t.Fatalf("Expected JetStream API requests, got %d", v)
	}
	require_Equal(t, otelInt64Value(t, rm, "nats.jetstream.accounts", nil), 1)

	// Counters are cumulative and must not go backwards between collections.
	before := otelInt64Value(t, rm, "nats.server.messages", &receive)
	natsPub(t, nc, "foo", []byte("hello"))
	natsFlush(t, nc)
	if after := otelInt64Value(t, collect(), "nats.server.messages", &receive); after <= before {
		t.Fatalf("Expected messages received to increase from %d, got %d", before, after)
	}

	// After shutdown the instruments are unregistered.
	nc.Close()
	s.Shutdown()
	if otelFindMetric(collect(), "nats.server.messages") != nil {
		t.Fatalf("Server metrics are still reported after shutdown")
	}
}

func TestOpenTelemetrySetMeterProvider(t *testing.T) {
	s := RunServer(DefaultOptions())
	defer s.Shutdown()

	reader1, reader2 := sdkmetric.NewManualReader(), sdkmetric.NewManualReader()
	mp1 := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader1))
	mp2 := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader2))
	defer mp1.Shutdown(context.Background())
	defer mp2.Shutdown(context.Background())

	reported := func(r sdkmetric.Reader) bool {
		var rm metricdata.ResourceMetrics
		require_NoError(t, r.Collect(context.Background(), &rm))
		return otelFindMetric(&rm, "nats.server.connections") != nil
	}

	require_NoError(t, s.SetMeterProvider(mp1))
	require_True(t, reported(reader1))

	// Switching providers moves the instruments.
	require_NoError(t, s.SetMeterProvider(mp2))
	require_False(t, reported(reader1))
	require_True(t, reported(reader2))

	// A nil provider removes them.
	require_NoError(t, s.SetMeterProvider(nil))
	require_False(t, reported(reader2))
}
