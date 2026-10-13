# OpenTelemetry (OTLP) metrics — proof of concept

> Status: proof of concept on a fork. Not proposed upstream.

The server pushes its metrics over OTLP/HTTP to an OpenTelemetry Collector
(or any OTLP-capable backend), replacing the need to run
prometheus-nats-exporter or Surveyor to scrape the HTTP monitoring endpoints.

## Configuration

```
opentelemetry {           # or `otel`
  # OTLP/HTTP base URL; `/v1/metrics` is appended (same semantics as
  # OTEL_EXPORTER_OTLP_ENDPOINT). Scheme decides plain HTTP vs TLS.
  endpoint: "http://otel-collector:4318"

  # Optional headers sent with each export, e.g. for authentication.
  headers { Authorization: "Bearer ..." }

  # `metrics: true` or a block. The presence of the block enables metrics.
  metrics {
    interval: "15s"       # default: OTEL_METRIC_EXPORT_INTERVAL or 60s
  }
}
```

Everything not set in the config falls back to the standard OpenTelemetry
environment variables (`OTEL_EXPORTER_OTLP_*`, `OTEL_METRIC_EXPORT_INTERVAL`,
`OTEL_RESOURCE_ATTRIBUTES`, `OTEL_SERVICE_NAME`, ...). `OTEL_SDK_DISABLED=true`
turns export off. The block can not be changed with a config reload.

## Design

* `server/otel_metrics.go` depends on the OpenTelemetry **API only**
  (`go.opentelemetry.io/otel/metric`). The server exposes
  `(*Server).SetMeterProvider(metric.MeterProvider)`, mirroring `SetLogger`.
  Applications embedding the server can pass their own provider.
* All instruments are **asynchronous** and observed in a single callback that
  reads the counters already kept for `/varz` and `/jsz`. Nothing is added to
  the message path; the cost is paid once per export interval.
* `telemetry/` builds the SDK `MeterProvider` (OTLP/HTTP exporter, periodic
  reader, resource, Go runtime metrics) from the options. Only `main.go`
  imports it.
* `Server.Shutdown` calls `ForceFlush` on the provider (if it has one) before
  unregistering the instruments. This is needed because the SIGINT/SIGTERM
  handlers `os.Exit` right after shutdown.

### Resource

| Attribute | Value |
|---|---|
| `service.name` | `nats-server` (overridable via `OTEL_SERVICE_NAME`) |
| `service.version` | server version |
| `service.instance.id` | server ID |
| `nats.server.name` | server name |
| `nats.cluster.name` | cluster name, if clustered |
| `host.name`, `telemetry.sdk.*` | detected |

## Metrics

Names follow OpenTelemetry conventions (dot separated, unit in metadata, no
`_total` suffix). There is no semantic convention for broker-side metrics, so
NATS specific metrics use the `nats.` namespace. Prometheus-compatible
backends will translate e.g. `nats.server.messages` to
`nats_server_messages_total`.

| Metric | Type | Unit | Attributes |
|---|---|---|---|
| `nats.server.messages` | Counter | `{message}` | `network.io.direction` = `receive`/`transmit` |
| `nats.server.io` | Counter | `By` | `network.io.direction` |
| `nats.server.client.messages` | Counter | `{message}` | `network.io.direction` |
| `nats.server.client.io` | Counter | `By` | `network.io.direction` |
| `nats.server.connections` | UpDownCounter | `{connection}` | `nats.connection.kind` = `client`/`route`/`gateway`/`leaf` |
| `nats.server.client.connections.opened` | Counter | `{connection}` | |
| `nats.server.subscriptions` | UpDownCounter | `{subscription}` | |
| `nats.server.slow_consumers` | Counter | `{event}` | `nats.connection.kind` |
| `nats.server.stale_connections` | Counter | `{event}` | `nats.connection.kind` |
| `nats.server.client.stalls` | Counter | `{event}` | |
| `process.memory.usage` | UpDownCounter | `By` | |
| `process.cpu.utilization` | Gauge | `1` | |
| `nats.jetstream.storage.usage` | UpDownCounter | `By` | `nats.jetstream.storage.type` = `memory`/`file` |
| `nats.jetstream.storage.reserved` | UpDownCounter | `By` | `nats.jetstream.storage.type` |
| `nats.jetstream.storage.limit` | UpDownCounter | `By` | `nats.jetstream.storage.type` |
| `nats.jetstream.accounts` | UpDownCounter | `{account}` | |
| `nats.jetstream.ha_assets` | UpDownCounter | `{asset}` | |
| `nats.jetstream.api.requests` | Counter | `{request}` | |
| `nats.jetstream.api.errors` | Counter | `{request}` | |
| `nats.jetstream.api.inflight` | UpDownCounter | `{request}` | |
| `go.*` | various | | from `contrib/instrumentation/runtime` |

## Measurements

Linux/amd64 binary, `go build .`, Go 1.26.9:

| Build | Size | Δ vs main |
|---|---|---|
| main | 23.4 MB | |
| + OTel API in `server/` | 23.6 MB | +0.2 MB |
| + SDK and runtime metrics | 25.4 MB | +2.0 MB |
| + `otlpmetrichttp` exporter (this PoC) | 34.9 MB | +11.5 MB |

The exporter accounts for most of the growth: `go.opentelemetry.io/proto/otlp`
bundles the gRPC service stubs, which pulls in `google.golang.org/grpc`,
genproto and protobuf, even for the HTTP-only exporter.

`go.mod` gains 8 direct and 13 indirect requirements.
`go.opentelemetry.io/proto/otlp` and `google.golang.org/protobuf` are direct
only because the `telemetry` test decodes the exported payload.

## Open items

* Binary size: options are an in-tree minimal OTLP/HTTP exporter (OTLP
  supports a JSON encoding, which needs no protobuf/gRPC), or a build tag so
  the default binary is unchanged.
* Opt-in per account / stream / consumer metrics, with a cardinality cap.
* Route, gateway and leafnode level metrics, Raft metrics.
* Name mapping from prometheus-nats-exporter series for dashboard migration,
  and an example Collector pipeline.
* Allow changing the export interval and endpoint on reload.
