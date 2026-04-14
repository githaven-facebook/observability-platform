package query

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/model"
	"github.com/nicedavid98/observability-platform/internal/storage/clickhouse"
)

// LogQueryEngine executes log queries against ClickHouse.
type LogQueryEngine struct {
	store  *clickhouse.LogStore
	logger *zap.Logger
}

// NewLogQueryEngine creates a LogQueryEngine backed by the given store.
func NewLogQueryEngine(store *clickhouse.LogStore, logger *zap.Logger) *LogQueryEngine {
	return &LogQueryEngine{store: store, logger: logger}
}

// LogSearchParams defines criteria for searching log records.
type LogSearchParams struct {
	ServiceName string
	MinSeverity model.Severity
	Keyword     string
	TraceID     string
	SpanID      string
	StartTime   time.Time
	EndTime     time.Time
	Limit       int
}

// SearchLogs finds log records matching the given filter parameters.
func (e *LogQueryEngine) SearchLogs(ctx context.Context, params LogSearchParams) ([]*model.LogRecord, error) { //nolint:gocritic // params is a query value type copied intentionally
	if params.Limit <= 0 {
		params.Limit = 200
	}
	if params.EndTime.IsZero() {
		params.EndTime = time.Now()
	}
	if params.StartTime.IsZero() {
		params.StartTime = params.EndTime.Add(-15 * time.Minute)
	}

	q := clickhouse.LogQuery{
		StartTime:   params.StartTime,
		EndTime:     params.EndTime,
		ServiceName: params.ServiceName,
		MinSeverity: params.MinSeverity,
		Keyword:     params.Keyword,
		TraceID:     params.TraceID,
		SpanID:      params.SpanID,
		Limit:       params.Limit,
	}

	records, err := e.store.QueryLogs(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("search logs: %w", err)
	}
	return records, nil
}

// GetLogContext returns all logs correlated with the given trace ID, ordered by time.
// This is the primary entry point for the "surrounding context" feature in the UI.
func (e *LogQueryEngine) GetLogContext(ctx context.Context, traceID string) ([]*model.LogRecord, error) {
	if traceID == "" {
		return nil, fmt.Errorf("trace_id is required")
	}
	records, err := e.store.GetLogContext(ctx, traceID, 5*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("get log context for trace %s: %w", traceID, err)
	}
	e.logger.Debug("retrieved log context", zap.String("trace_id", traceID), zap.Int("count", len(records)))
	return records, nil
}

// FullTextSearch performs keyword search across the log body field.
func (e *LogQueryEngine) FullTextSearch(ctx context.Context, keyword string, startTime, endTime time.Time, limit int) ([]*model.LogRecord, error) {
	if keyword == "" {
		return nil, fmt.Errorf("keyword is required for full-text search")
	}
	params := LogSearchParams{
		Keyword:   keyword,
		StartTime: startTime,
		EndTime:   endTime,
		Limit:     limit,
	}
	return e.SearchLogs(ctx, params)
}

// FilterBySeverity returns log records at or above the given severity threshold.
func (e *LogQueryEngine) FilterBySeverity(ctx context.Context, serviceName string, minSeverity model.Severity, startTime, endTime time.Time, limit int) ([]*model.LogRecord, error) {
	params := LogSearchParams{
		ServiceName: serviceName,
		MinSeverity: minSeverity,
		StartTime:   startTime,
		EndTime:     endTime,
		Limit:       limit,
	}
	return e.SearchLogs(ctx, params)
}
