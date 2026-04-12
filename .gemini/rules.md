# Observability Platform - Gemini Rules

## Project Overview
Go-based unified observability platform: distributed tracing, log aggregation, metrics collection, alerting.
Three main services: Collector, Query API, Alert Manager.

## Tech Stack
- Go 1.22
- ClickHouse for trace/log/metric storage
- Kafka for async log processing
- Prometheus for internal metrics
- OpenTelemetry SDK for instrumentation
- chi router for HTTP, gRPC for OTLP receiver

## Three Services
1. **Collector** (`cmd/collector/`) - Receives OTLP data, processes, exports to ClickHouse/Kafka
2. **Query** (`cmd/query/`) - HTTP API for querying traces, logs, metrics, service map
3. **Alert Manager** (`cmd/alertmanager/`) - Evaluates alert rules, sends notifications

## Commands
- Build all: `make build`
- Test: `make test`
- Lint: `make lint`
- Run collector: `go run cmd/collector/main.go`
- Run query: `go run cmd/query/main.go`
- Run alert manager: `go run cmd/alertmanager/main.go`

## Key Packages
- `internal/collector/` - OTLP receiver, processors, exporters
- `internal/storage/clickhouse/` - ClickHouse client and queries
- `internal/query/` - Query engines for traces, logs, metrics
- `internal/api/` - HTTP API handlers (includes Grafana-compatible endpoints)
- `internal/alerting/` - Alert rule evaluation, notification dispatch
- `internal/pipeline/` - Async log/metric processing pipeline

## ClickHouse Schema
Tables use MergeTree engine with date partitioning and TTL retention.
Do NOT modify `schema.sql` or `migrations.go` without DBA review.

## Do NOT
- Do not modify OTLP receiver wire format
- Do not change ClickHouse table schemas without migration
- Do not add new alert notification channels without ops review
- Do not bypass batch processor for direct ClickHouse writes
