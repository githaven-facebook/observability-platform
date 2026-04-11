-- Observability Platform ClickHouse Schema
-- Applied automatically via migrations.go on startup.

-- ============================================================
-- Databases
-- ============================================================
CREATE DATABASE IF NOT EXISTS traces;
CREATE DATABASE IF NOT EXISTS logs;
CREATE DATABASE IF NOT EXISTS metrics;

-- ============================================================
-- Traces
-- ============================================================
CREATE TABLE IF NOT EXISTS traces.spans
(
    trace_id            String,
    span_id             String,
    parent_span_id      String,
    trace_state         String,
    service_name        LowCardinality(String),
    operation_name      LowCardinality(String),
    start_time          DateTime64(9, 'UTC'),
    end_time            DateTime64(9, 'UTC'),
    duration_ms         Int64,
    status              Int32,       -- 0=unset, 1=ok, 2=error
    status_message      String,
    kind                Int32,       -- 0=unspecified … 5=consumer
    attributes          Map(String, String),
    resource_attributes Map(String, String),
    events              String,      -- JSON array
    links               String,      -- JSON array
    _date               Date MATERIALIZED toDate(start_time)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(start_time)
ORDER BY (service_name, start_time, trace_id, span_id)
TTL toDateTime(start_time) + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

-- Materialized view: pre-aggregate trace summaries.
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
GROUP BY trace_id;

-- ============================================================
-- Logs
-- ============================================================
CREATE TABLE IF NOT EXISTS logs.records
(
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
TTL toDateTime(timestamp) + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

-- Materialized view: per-minute severity counts for dashboards.
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
GROUP BY service_name, severity, bucket;

-- ============================================================
-- Metrics
-- ============================================================
CREATE TABLE IF NOT EXISTS metrics.data_points
(
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
TTL toDateTime(timestamp) + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

-- 5-minute downsampled aggregates (avg value per window).
CREATE TABLE IF NOT EXISTS metrics.agg_5min
(
    name         LowCardinality(String),
    description  String,
    unit         LowCardinality(String),
    type         Int32,
    labels       Map(String, String),
    value        AggregateFunction(avg, Float64),
    timestamp    DateTime('UTC'),
    service_name LowCardinality(String)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (name, service_name, labels, timestamp);

CREATE MATERIALIZED VIEW IF NOT EXISTS metrics.mv_5min
TO metrics.agg_5min
AS SELECT
    name, description, unit, type, labels,
    avgState(value)              AS value,
    toStartOfFiveMinutes(timestamp) AS timestamp,
    service_name
FROM metrics.data_points
GROUP BY name, description, unit, type, labels, timestamp, service_name;

-- 1-hour downsampled aggregates.
CREATE TABLE IF NOT EXISTS metrics.agg_1hour
(
    name         LowCardinality(String),
    description  String,
    unit         LowCardinality(String),
    type         Int32,
    labels       Map(String, String),
    value        AggregateFunction(avg, Float64),
    timestamp    DateTime('UTC'),
    service_name LowCardinality(String)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (name, service_name, labels, timestamp);

CREATE MATERIALIZED VIEW IF NOT EXISTS metrics.mv_1hour
TO metrics.agg_1hour
AS SELECT
    name, description, unit, type, labels,
    avgState(value)          AS value,
    toStartOfHour(timestamp) AS timestamp,
    service_name
FROM metrics.data_points
GROUP BY name, description, unit, type, labels, timestamp, service_name;

-- 1-day downsampled aggregates.
CREATE TABLE IF NOT EXISTS metrics.agg_1day
(
    name         LowCardinality(String),
    description  String,
    unit         LowCardinality(String),
    type         Int32,
    labels       Map(String, String),
    value        AggregateFunction(avg, Float64),
    timestamp    Date,
    service_name LowCardinality(String)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (name, service_name, labels, timestamp);

CREATE MATERIALIZED VIEW IF NOT EXISTS metrics.mv_1day
TO metrics.agg_1day
AS SELECT
    name, description, unit, type, labels,
    avgState(value)         AS value,
    toDate(timestamp)       AS timestamp,
    service_name
FROM metrics.data_points
GROUP BY name, description, unit, type, labels, timestamp, service_name;
