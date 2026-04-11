# Observability Platform

A production-grade unified observability platform for distributed systems, providing distributed tracing, log aggregation, metrics collection, and intelligent alerting.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Observability Platform                     │
│                                                               │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐   │
│  │   Collector  │    │ Query Engine │    │ Alert Manager│   │
│  │  (OTel GRPC) │    │  (HTTP API)  │    │ (Eval Loop)  │   │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘   │
│         │                   │                   │            │
│  ┌──────▼───────────────────▼───────────────────▼───────┐   │
│  │                   ClickHouse                          │   │
│  │          (Traces | Logs | Metrics Storage)            │   │
│  └───────────────────────────────────────────────────────┘   │
│                                                               │
│  ┌──────────────┐    ┌──────────────┐                        │
│  │    Kafka     │    │    Redis     │                        │
│  │  (Log Queue) │    │   (Cache)    │                        │
│  └──────────────┘    └──────────────┘                        │
└─────────────────────────────────────────────────────────────┘
```

## Components

### Collector
Receives telemetry data via OpenTelemetry Protocol (OTLP) over gRPC and HTTP. Processes spans, logs, and metrics through configurable pipelines before exporting to ClickHouse and Kafka.

### Query Engine
HTTP API server providing trace search, log aggregation, metric queries (PromQL subset), and service dependency map generation.

### Alert Manager
Evaluates alert rules against metrics and logs, manages alert state transitions, and dispatches notifications to PagerDuty, Slack, and email.

## Data Flow

```
Services → OTel SDK → Collector → ClickHouse
                               └→ Kafka → Log Processor → ClickHouse
                                        └→ Metric Aggregator → ClickHouse

ClickHouse → Query Engine → Grafana / Dashboard
           → Alert Manager → PagerDuty / Slack / Email
```

## Storage Schema

### Traces
- `traces.spans` - Span storage with MergeTree, partitioned by date
- `traces.trace_summaries` - Materialized view for trace-level aggregations

### Logs
- `logs.records` - Log records with full-text search, partitioned by date
- `logs.aggregations` - Pre-aggregated counts by service/severity

### Metrics
- `metrics.data_points` - Raw metric data points
- `metrics.1min`, `metrics.5min`, `metrics.1hr`, `metrics.1day` - Downsampled views

## API Reference

### Traces
- `GET /api/v1/traces` - Search traces by service, operation, duration, status, tags
- `GET /api/v1/traces/{traceId}` - Get full trace with all spans
- `GET /api/v1/traces/{traceId}/spans` - Get individual spans for a trace

### Logs
- `GET /api/v1/logs` - Search logs with filters (service, severity, time range, keyword)
- `GET /api/v1/logs/context/{traceId}` - Get logs correlated with a trace

### Metrics
- `GET /api/v1/metrics/query` - Execute PromQL-compatible query
- `GET /api/v1/metrics/series` - List metric series
- `GET /api/v1/metrics/labels` - Get label names and values

### Service Map
- `GET /api/v1/service-map` - Auto-generated service dependency graph

### Grafana Integration
- `POST /api/v1/grafana/query` - Grafana datasource query endpoint
- `GET /api/v1/grafana/annotations` - Grafana annotations
- `POST /api/v1/grafana/search` - Grafana metric search

## Alert Configuration

Alert rules are defined in `config/alert-rules.yaml`:

```yaml
rules:
  - name: high_error_rate
    expression: rate(http_requests_total{status=~"5.."}[5m]) / rate(http_requests_total[5m]) > 0.05
    duration: 5m
    severity: critical
    annotations:
      summary: "High error rate detected"
    notification_channels:
      - pagerduty
      - slack
```

## Grafana Integration

1. Add a new datasource of type "SimpleJSON" or custom plugin
2. Point to `http://query-service:8080/api/v1/grafana`
3. Import the provided dashboard JSON from `config/grafana/`

## Deployment

### Docker Compose (Development)
```bash
docker-compose up -d
```

### Production
```bash
make build
./bin/collector --config config/collector.yaml
./bin/query --config config/query.yaml
./bin/alertmanager --config config/alertmanager.yaml
```

## Capacity Planning

- **Spans**: ~10KB per span average. 1M spans/day ≈ 10GB/day raw, ~3GB compressed
- **Logs**: ~500B per log entry. 100M logs/day ≈ 50GB/day raw, ~15GB compressed
- **Metrics**: ~100B per data point. 10M points/day ≈ 1GB/day raw, ~300MB compressed
- **Retention**: 7 days hot (SSD), 90 days warm (HDD), 1 year cold (object storage)
- **ClickHouse**: Single node handles ~500K inserts/sec, cluster for higher throughput
