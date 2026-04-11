package clickhouse

import (
	"context"
	"fmt"

	"go.uber.org/zap"
)

// RunMigrations creates all required ClickHouse databases and tables if they do not yet exist.
func RunMigrations(ctx context.Context, client *Client, retentionDays int, logger *zap.Logger) error {
	migrations := []struct {
		name string
		ddl  string
	}{
		{"create traces database", `CREATE DATABASE IF NOT EXISTS traces`},
		{"create logs database", `CREATE DATABASE IF NOT EXISTS logs`},
		{"create metrics database", `CREATE DATABASE IF NOT EXISTS metrics`},
		{"create traces.spans table", tracesSpansDDL(retentionDays)},
		{"create logs.records table", logsRecordsDDL(retentionDays)},
		{"create metrics.data_points table", metricsDataPointsDDL(retentionDays)},
		{"create metrics.agg_5min table", metricsAgg5MinDDL()},
		{"create metrics.agg_1hour table", metricsAgg1HourDDL()},
		{"create metrics.agg_1day table", metricsAgg1DayDDL()},
		{"create traces.trace_summaries view", traceSummariesViewDDL()},
		{"create logs.severity_counts view", logSeverityCountsViewDDL()},
	}

	for _, m := range migrations {
		logger.Info("running migration", zap.String("name", m.name))
		if err := client.ExecWithRetry(ctx, m.ddl); err != nil {
			return fmt.Errorf("migration %q: %w", m.name, err)
		}
	}

	logger.Info("all migrations completed successfully")
	return nil
}

func tracesSpansDDL(retentionDays int) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS traces.spans (
    trace_id            String,
    span_id             String,
    parent_span_id      String,
    trace_state         String,
    service_name        LowCardinality(String),
    operation_name      LowCardinality(String),
    start_time          DateTime64(9, 'UTC'),
    end_time            DateTime64(9, 'UTC'),
    duration_ms         Int64,
    status              Int32,
    status_message      String,
    kind                Int32,
    attributes          Map(String, String),
    resource_attributes Map(String, String),
    events              String,
    links               String,
    _date               Date MATERIALIZED toDate(start_time)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(start_time)
ORDER BY (service_name, start_time, trace_id, span_id)
TTL toDateTime(start_time) + INTERVAL %d DAY
SETTINGS index_granularity = 8192`, retentionDays)
}

func logsRecordsDDL(retentionDays int) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS logs.records (
    timestamp           DateTime64(9, 'UTC'),
    observed_timestamp  DateTime64(9, 'UTC'),
    severity            Int32,
    severity_text       LowCardinality(String),
    body                String,
    service_name        LowCardinality(String),
    trace_id            String,
    span_id             String,
    trace_flags         UInt32,
    resource_attributes Map(String, String),
    log_attributes      Map(String, String),
    _date               Date MATERIALIZED toDate(timestamp)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (service_name, timestamp, severity)
TTL toDateTime(timestamp) + INTERVAL %d DAY
SETTINGS index_granularity = 8192`, retentionDays)
}

func metricsDataPointsDDL(retentionDays int) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS metrics.data_points (
    name         LowCardinality(String),
    description  String,
    unit         LowCardinality(String),
    type         Int32,
    labels       Map(String, String),
    value        Float64,
    timestamp    DateTime64(3, 'UTC'),
    service_name LowCardinality(String),
    _date        Date MATERIALIZED toDate(timestamp)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (name, service_name, timestamp)
TTL toDateTime(timestamp) + INTERVAL %d DAY
SETTINGS index_granularity = 8192`, retentionDays)
}

func metricsAgg5MinDDL() string {
	return `
CREATE TABLE IF NOT EXISTS metrics.agg_5min (
    name         LowCardinality(String),
    description  String,
    unit         LowCardinality(String),
    type         Int32,
    labels       Map(String, String),
    value        Float64,
    timestamp    DateTime('UTC'),
    service_name LowCardinality(String)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (name, service_name, labels, timestamp)
SETTINGS index_granularity = 8192`
}

func metricsAgg1HourDDL() string {
	return `
CREATE TABLE IF NOT EXISTS metrics.agg_1hour (
    name         LowCardinality(String),
    description  String,
    unit         LowCardinality(String),
    type         Int32,
    labels       Map(String, String),
    value        Float64,
    timestamp    DateTime('UTC'),
    service_name LowCardinality(String)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (name, service_name, labels, timestamp)
SETTINGS index_granularity = 8192`
}

func metricsAgg1DayDDL() string {
	return `
CREATE TABLE IF NOT EXISTS metrics.agg_1day (
    name         LowCardinality(String),
    description  String,
    unit         LowCardinality(String),
    type         Int32,
    labels       Map(String, String),
    value        Float64,
    timestamp    Date,
    service_name LowCardinality(String)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (name, service_name, labels, timestamp)
SETTINGS index_granularity = 8192`
}

func traceSummariesViewDDL() string {
	return `
CREATE MATERIALIZED VIEW IF NOT EXISTS traces.trace_summaries
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMMDD(trace_start)
ORDER BY (trace_id)
POPULATE
AS SELECT
    trace_id,
    argMinState(service_name, start_time)   AS root_service,
    argMinState(operation_name, start_time) AS root_operation,
    minState(start_time)                    AS trace_start,
    maxState(end_time)                      AS trace_end,
    countState()                            AS span_count,
    countIfState(status = 2)                AS error_count,
    uniqExactState(service_name)            AS service_count
FROM traces.spans
GROUP BY trace_id`
}

func logSeverityCountsViewDDL() string {
	return `
CREATE MATERIALIZED VIEW IF NOT EXISTS logs.severity_counts
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(bucket)
ORDER BY (service_name, severity, bucket)
POPULATE
AS SELECT
    service_name,
    severity,
    toStartOfMinute(timestamp) AS bucket,
    count()                    AS cnt
FROM logs.records
GROUP BY service_name, severity, bucket`
}
