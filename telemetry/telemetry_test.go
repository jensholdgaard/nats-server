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

package telemetry

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"

	"github.com/nats-io/nats-server/v2/server"
)

// otlpReceiver is a minimal OTLP/HTTP metrics receiver.
type otlpReceiver struct {
	mu       sync.Mutex
	requests []*colmetricspb.ExportMetricsServiceRequest
	headers  []http.Header
}

func (r *otlpReceiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != "/v1/metrics" {
		http.NotFound(w, req)
		return
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var emr colmetricspb.ExportMetricsServiceRequest
	if err := proto.Unmarshal(body, &emr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.mu.Lock()
	r.requests = append(r.requests, &emr)
	r.headers = append(r.headers, req.Header.Clone())
	r.mu.Unlock()
	w.Header().Set("Content-Type", "application/x-protobuf")
	b, _ := proto.Marshal(&colmetricspb.ExportMetricsServiceResponse{})
	w.Write(b)
}

func TestTelemetryDisabled(t *testing.T) {
	opts := &server.Options{Port: -1, NoLog: true, NoSigs: true}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	shutdown, err := Start(context.Background(), s, opts)
	if err != nil {
		t.Fatalf("Error starting telemetry: %v", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("Error shutting down telemetry: %v", err)
	}
}

func TestTelemetryExportsOTLPMetrics(t *testing.T) {
	recv := &otlpReceiver{}
	collector := httptest.NewServer(recv)
	defer collector.Close()

	opts := &server.Options{
		Host:       "127.0.0.1",
		Port:       -1,
		ServerName: "otel-test",
		NoLog:      true,
		NoSigs:     true,
		OpenTelemetry: server.OpenTelemetryOpts{
			Endpoint: collector.URL,
			Headers:  map[string]string{"Authorization": "Bearer token"},
			// Long interval: the export must be triggered by the server shutdown.
			Metrics: server.OpenTelemetryMetricsOpts{Enabled: true, Interval: time.Hour},
		},
	}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	shutdown, err := Start(context.Background(), s, opts)
	if err != nil {
		t.Fatalf("Error starting telemetry: %v", err)
	}
	defer shutdown(context.Background())

	s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatalf("Server not ready")
	}
	s.Shutdown()
	s.WaitForShutdown()

	recv.mu.Lock()
	defer recv.mu.Unlock()
	if len(recv.requests) == 0 {
		t.Fatalf("No metrics were exported on shutdown")
	}
	if h := recv.headers[0].Get("Authorization"); h != "Bearer token" {
		t.Fatalf("Expected configured header to be sent, got %q", h)
	}

	rms := recv.requests[0].GetResourceMetrics()
	if len(rms) != 1 {
		t.Fatalf("Expected one resource, got %d", len(rms))
	}
	attrs := map[string]string{}
	for _, kv := range rms[0].GetResource().GetAttributes() {
		attrs[kv.GetKey()] = kv.GetValue().GetStringValue()
	}
	for k, v := range map[string]string{
		"service.name":        "nats-server",
		"service.version":     server.VERSION,
		"service.instance.id": s.ID(),
		"nats.server.name":    "otel-test",
	} {
		if attrs[k] != v {
			t.Fatalf("Expected resource attribute %s=%q, got %q", k, v, attrs[k])
		}
	}

	names := map[string]bool{}
	for _, sm := range rms[0].GetScopeMetrics() {
		for _, m := range sm.GetMetrics() {
			names[m.GetName()] = true
		}
	}
	for _, name := range []string{
		"nats.server.messages",
		"nats.server.connections",
		"nats.server.subscriptions",
		"go.memory.used", // runtime instrumentation
	} {
		if !names[name] {
			t.Fatalf("Expected metric %q to be exported, got %v", name, names)
		}
	}
}
