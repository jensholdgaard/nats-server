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
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/nats-io/nats-server/v2/server/pse"
)

// This file only depends on the OpenTelemetry metrics API, never the SDK.
// Wiring an SDK and an exporter is left to the binary (see the `telemetry`
// package) or to the application embedding the server, which hands us a
// metric.MeterProvider via SetMeterProvider.
//
// All instruments are asynchronous (observable): values are read from the
// counters the server already maintains for /varz and /jsz, once per
// collection cycle of the provider's reader. Nothing is added to the
// message processing path.

const (
	otelMeterName = "github.com/nats-io/nats-server/v2/server"

	// How long Shutdown will wait for the meter provider to flush.
	otelShutdownFlushTimeout = 5 * time.Second
)

// Attribute keys. Where an OpenTelemetry semantic convention exists we use
// it, everything NATS specific lives under the `nats.` namespace.
var (
	otelAttrIODirection    = attribute.Key("network.io.direction")
	otelAttrConnKind       = attribute.Key("nats.connection.kind")
	otelAttrJSStorageType  = attribute.Key("nats.jetstream.storage.type")
	otelSetDirectionIn     = attribute.NewSet(otelAttrIODirection.String("receive"))
	otelSetDirectionOut    = attribute.NewSet(otelAttrIODirection.String("transmit"))
	otelSetKindClient      = attribute.NewSet(otelAttrConnKind.String("client"))
	otelSetKindRoute       = attribute.NewSet(otelAttrConnKind.String("route"))
	otelSetKindGateway     = attribute.NewSet(otelAttrConnKind.String("gateway"))
	otelSetKindLeaf        = attribute.NewSet(otelAttrConnKind.String("leaf"))
	otelSetJSStorageMemory = attribute.NewSet(otelAttrJSStorageType.String("memory"))
	otelSetJSStorageFile   = attribute.NewSet(otelAttrJSStorageType.String("file"))
)

// otelMetrics holds the state of the OpenTelemetry metrics instrumentation.
type otelMetrics struct {
	mu  sync.Mutex
	mp  metric.MeterProvider
	reg metric.Registration
}

// otelInstruments are the instruments observed by the server callback.
type otelInstruments struct {
	messages       metric.Int64ObservableCounter
	bytes          metric.Int64ObservableCounter
	clientMessages metric.Int64ObservableCounter
	clientBytes    metric.Int64ObservableCounter
	connections    metric.Int64ObservableUpDownCounter
	connTotal      metric.Int64ObservableCounter
	subscriptions  metric.Int64ObservableUpDownCounter
	slowConsumers  metric.Int64ObservableCounter
	staleConns     metric.Int64ObservableCounter
	stalls         metric.Int64ObservableCounter

	procMemory metric.Int64ObservableUpDownCounter
	procCPU    metric.Float64ObservableGauge

	jsStorageUsage    metric.Int64ObservableUpDownCounter
	jsStorageReserved metric.Int64ObservableUpDownCounter
	jsStorageLimit    metric.Int64ObservableUpDownCounter
	jsAccounts        metric.Int64ObservableUpDownCounter
	jsHAAssets        metric.Int64ObservableUpDownCounter
	jsAPIRequests     metric.Int64ObservableCounter
	jsAPIErrors       metric.Int64ObservableCounter
	jsAPIInflight     metric.Int64ObservableUpDownCounter
}

// SetMeterProvider registers the server's metric instruments with the given
// OpenTelemetry MeterProvider. Values are reported every time the provider's
// readers collect. Passing nil removes a previously registered provider.
//
// If the provider implements `ForceFlush(context.Context) error` (as the
// OpenTelemetry SDK MeterProvider does), it is flushed when the server shuts
// down so that the final values are exported. The server does not shut the
// provider down, its lifecycle is owned by the caller.
func (s *Server) SetMeterProvider(mp metric.MeterProvider) error {
	om := &s.otelMetrics
	om.mu.Lock()
	defer om.mu.Unlock()

	if om.reg != nil {
		if err := om.reg.Unregister(); err != nil {
			return err
		}
		om.reg, om.mp = nil, nil
	}
	if mp == nil {
		return nil
	}
	reg, err := s.registerOTelMetrics(mp.Meter(otelMeterName, metric.WithInstrumentationVersion(VERSION)))
	if err != nil {
		return err
	}
	om.reg, om.mp = reg, mp
	return nil
}

// shutdownOTelMetrics flushes the meter provider, if possible, and unregisters
// the server's instruments. Invoked from Shutdown.
func (s *Server) shutdownOTelMetrics() {
	om := &s.otelMetrics
	om.mu.Lock()
	defer om.mu.Unlock()

	if om.reg == nil {
		return
	}
	if f, ok := om.mp.(interface{ ForceFlush(context.Context) error }); ok {
		ctx, cancel := context.WithTimeout(context.Background(), otelShutdownFlushTimeout)
		if err := f.ForceFlush(ctx); err != nil {
			s.Warnf("Error flushing OpenTelemetry metrics: %v", err)
		}
		cancel()
	}
	if err := om.reg.Unregister(); err != nil {
		s.Warnf("Error unregistering OpenTelemetry metrics: %v", err)
	}
	om.reg, om.mp = nil, nil
}

func (s *Server) registerOTelMetrics(m metric.Meter) (metric.Registration, error) {
	var (
		ins  otelInstruments
		err  error
		errs []error
	)
	i64c := func(name, unit, desc string) metric.Int64ObservableCounter {
		var c metric.Int64ObservableCounter
		c, err = m.Int64ObservableCounter(name, metric.WithUnit(unit), metric.WithDescription(desc))
		errs = append(errs, err)
		return c
	}
	i64u := func(name, unit, desc string) metric.Int64ObservableUpDownCounter {
		var c metric.Int64ObservableUpDownCounter
		c, err = m.Int64ObservableUpDownCounter(name, metric.WithUnit(unit), metric.WithDescription(desc))
		errs = append(errs, err)
		return c
	}

	ins.messages = i64c("nats.server.messages", "{message}", "Messages received and sent by the server, across all connection kinds.")
	ins.bytes = i64c("nats.server.io", "By", "Payload bytes received and sent by the server, across all connection kinds.")
	ins.clientMessages = i64c("nats.server.client.messages", "{message}", "Messages received from and sent to client connections.")
	ins.clientBytes = i64c("nats.server.client.io", "By", "Payload bytes received from and sent to client connections.")
	ins.connections = i64u("nats.server.connections", "{connection}", "Currently open connections.")
	ins.connTotal = i64c("nats.server.client.connections.opened", "{connection}", "Client connections accepted since the server started.")
	ins.subscriptions = i64u("nats.server.subscriptions", "{subscription}", "Currently active subscriptions across all accounts.")
	ins.slowConsumers = i64c("nats.server.slow_consumers", "{event}", "Number of times a connection was detected as a slow consumer.")
	ins.staleConns = i64c("nats.server.stale_connections", "{event}", "Number of connections closed for being stale.")
	ins.stalls = i64c("nats.server.client.stalls", "{event}", "Number of times producers were stalled because of slow consumers.")

	ins.procMemory = i64u("process.memory.usage", "By", "The amount of physical memory in use.")
	ins.procCPU, err = m.Float64ObservableGauge("process.cpu.utilization", metric.WithUnit("1"),
		metric.WithDescription("CPU utilization of the process, normalized by the number of available CPUs."))
	errs = append(errs, err)

	ins.jsStorageUsage = i64u("nats.jetstream.storage.usage", "By", "JetStream storage in use.")
	ins.jsStorageReserved = i64u("nats.jetstream.storage.reserved", "By", "JetStream storage reserved by account limits.")
	ins.jsStorageLimit = i64u("nats.jetstream.storage.limit", "By", "JetStream storage the server is configured to provide.")
	ins.jsAccounts = i64u("nats.jetstream.accounts", "{account}", "Accounts with JetStream enabled on this server.")
	ins.jsHAAssets = i64u("nats.jetstream.ha_assets", "{asset}", "Replicated assets (raft groups) on this server.")
	ins.jsAPIRequests = i64c("nats.jetstream.api.requests", "{request}", "JetStream API requests received.")
	ins.jsAPIErrors = i64c("nats.jetstream.api.errors", "{request}", "JetStream API requests that resulted in an error response.")
	ins.jsAPIInflight = i64u("nats.jetstream.api.inflight", "{request}", "JetStream API requests currently being processed.")

	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s.observeOTelMetrics(o, &ins)
		return nil
	},
		ins.messages, ins.bytes, ins.clientMessages, ins.clientBytes,
		ins.connections, ins.connTotal, ins.subscriptions,
		ins.slowConsumers, ins.staleConns, ins.stalls,
		ins.procMemory, ins.procCPU,
		ins.jsStorageUsage, ins.jsStorageReserved, ins.jsStorageLimit,
		ins.jsAccounts, ins.jsHAAssets,
		ins.jsAPIRequests, ins.jsAPIErrors, ins.jsAPIInflight,
	)
}

// observeOTelMetrics records the current values of all instruments.
// Must be called without holding any server or JetStream lock.
func (s *Server) observeOTelMetrics(o metric.Observer, ins *otelInstruments) {
	in, out := metric.WithAttributeSet(otelSetDirectionIn), metric.WithAttributeSet(otelSetDirectionOut)
	o.ObserveInt64(ins.messages, atomic.LoadInt64(&s.inMsgs), in)
	o.ObserveInt64(ins.messages, atomic.LoadInt64(&s.outMsgs), out)
	o.ObserveInt64(ins.bytes, atomic.LoadInt64(&s.inBytes), in)
	o.ObserveInt64(ins.bytes, atomic.LoadInt64(&s.outBytes), out)
	o.ObserveInt64(ins.clientMessages, atomic.LoadInt64(&s.inClientMsgs), in)
	o.ObserveInt64(ins.clientMessages, atomic.LoadInt64(&s.outClientMsgs), out)
	o.ObserveInt64(ins.clientBytes, atomic.LoadInt64(&s.inClientBytes), in)
	o.ObserveInt64(ins.clientBytes, atomic.LoadInt64(&s.outClientBytes), out)
	o.ObserveInt64(ins.stalls, atomic.LoadInt64(&s.stalls))

	client, route := metric.WithAttributeSet(otelSetKindClient), metric.WithAttributeSet(otelSetKindRoute)
	gateway, leaf := metric.WithAttributeSet(otelSetKindGateway), metric.WithAttributeSet(otelSetKindLeaf)
	o.ObserveInt64(ins.slowConsumers, int64(s.scStats.clients.Load()), client)
	o.ObserveInt64(ins.slowConsumers, int64(s.scStats.routes.Load()), route)
	o.ObserveInt64(ins.slowConsumers, int64(s.scStats.gateways.Load()), gateway)
	o.ObserveInt64(ins.slowConsumers, int64(s.scStats.leafs.Load()), leaf)
	o.ObserveInt64(ins.staleConns, int64(s.staleStats.clients.Load()), client)
	o.ObserveInt64(ins.staleConns, int64(s.staleStats.routes.Load()), route)
	o.ObserveInt64(ins.staleConns, int64(s.staleStats.gateways.Load()), gateway)
	o.ObserveInt64(ins.staleConns, int64(s.staleStats.leafs.Load()), leaf)

	s.mu.RLock()
	nClients, nRoutes, nLeafs := len(s.clients), s.numRoutes(), len(s.leafs)
	totalClients := s.totalClients
	nSubs := s.numSubscriptions()
	s.mu.RUnlock()
	// Gateway lock is independent of the server lock.
	nGateways := s.numInboundGateways() + s.numOutboundGateways()

	o.ObserveInt64(ins.connections, int64(nClients), client)
	o.ObserveInt64(ins.connections, int64(nRoutes), route)
	o.ObserveInt64(ins.connections, int64(nGateways), gateway)
	o.ObserveInt64(ins.connections, int64(nLeafs), leaf)
	o.ObserveInt64(ins.connTotal, int64(totalClients))
	o.ObserveInt64(ins.subscriptions, int64(nSubs))

	var pcpu float64
	var rss, vss int64
	if err := pse.ProcUsage(&pcpu, &rss, &vss); err == nil {
		o.ObserveInt64(ins.procMemory, rss)
		// pse reports a percentage of a single CPU.
		o.ObserveFloat64(ins.procCPU, pcpu/100/float64(runtime.NumCPU()))
	}

	// JetStream lock must not be acquired while holding the server lock.
	js := s.getJetStream()
	if js == nil {
		return
	}
	js.mu.RLock()
	maxMem, maxStore := js.config.MaxMemory, js.config.MaxStore
	js.mu.RUnlock()
	st := js.usageStats()

	mem, file := metric.WithAttributeSet(otelSetJSStorageMemory), metric.WithAttributeSet(otelSetJSStorageFile)
	o.ObserveInt64(ins.jsStorageUsage, int64(st.Memory), mem)
	o.ObserveInt64(ins.jsStorageUsage, int64(st.Store), file)
	o.ObserveInt64(ins.jsStorageReserved, int64(st.ReservedMemory), mem)
	o.ObserveInt64(ins.jsStorageReserved, int64(st.ReservedStore), file)
	o.ObserveInt64(ins.jsStorageLimit, maxMem, mem)
	o.ObserveInt64(ins.jsStorageLimit, maxStore, file)
	o.ObserveInt64(ins.jsAccounts, int64(st.Accounts))
	o.ObserveInt64(ins.jsHAAssets, int64(st.HAAssets))
	o.ObserveInt64(ins.jsAPIRequests, int64(st.API.Total))
	o.ObserveInt64(ins.jsAPIErrors, int64(st.API.Errors))
	o.ObserveInt64(ins.jsAPIInflight, int64(st.API.Inflight))
}
