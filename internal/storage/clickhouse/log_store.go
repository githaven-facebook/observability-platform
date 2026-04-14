package clickhouse

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/model"
)

// LogStore handles log record persistence and querying in ClickHouse.
type LogStore struct {
	client *Client
	logger *zap.Logger
}

// NewLogStore creates a LogStore using the given client.
func NewLogStore(client *Client, logger *zap.Logger) *LogStore {
	return &LogStore{client: client, logger: logger}
}

// InsertLogs batch-inserts log records using ClickHouse native batch protocol.
func (s *LogStore) InsertLogs(ctx context.Context, logs []*model.LogRecord) error {
	if len(logs) == 0 {
		return nil
	}

	batch, err := s.client.Conn().PrepareBatch(ctx, `
		INSERT INTO logs.records (
			timestamp, observed_timestamp, severity, severity_text,
			body, service_name, trace_id, span_id, trace_flags,
			resource_attributes, log_attributes
		)`)
	if err != nil {
		return fmt.Errorf("prepare log batch: %w", err)
	}

	for _, l := range logs {
		if err := batch.Append(
			l.Timestamp,
			l.ObservedTimestamp,
			int32(l.Severity),
			l.SeverityText,
			l.Body,
			l.ServiceName,
			l.TraceID,
			l.SpanID,
			l.TraceFlags,
			l.ResourceAttributes,
			l.LogAttributes,
		); err != nil {
			return fmt.Errorf("append log record: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send log batch: %w", err)
	}

	s.logger.Debug("inserted log records", zap.Int("count", len(logs)))
	return nil
}

// QueryLogs searches for log records matching the given filter criteria.
func (s *LogStore) QueryLogs(ctx context.Context, q LogQuery) ([]*model.LogRecord, error) { //nolint:gocritic // query struct passed by value for immutable use
	query := `
		SELECT timestamp, observed_timestamp, severity, severity_text,
		       body, service_name, trace_id, span_id, trace_flags,
		       resource_attributes, log_attributes
		FROM logs.records
		WHERE timestamp >= ? AND timestamp <= ?
	`
	args := []interface{}{q.StartTime, q.EndTime}

	if q.ServiceName != "" {
		query += " AND service_name = ?"
		args = append(args, q.ServiceName)
	}
	if q.MinSeverity > 0 {
		query += " AND severity >= ?"
		args = append(args, int32(q.MinSeverity))
	}
	if q.Keyword != "" {
		query += " AND positionCaseInsensitive(body, ?) > 0"
		args = append(args, q.Keyword)
	}
	if q.TraceID != "" {
		query += " AND trace_id = ?"
		args = append(args, q.TraceID)
	}

	query += " ORDER BY timestamp DESC LIMIT ?"
	args = append(args, q.Limit)

	rows, err := s.client.Conn().Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query logs: %w", err)
	}
	defer rows.Close()

	return scanLogRecords(rows)
}

// GetLogContext returns logs surrounding a trace, ordered by timestamp.
func (s *LogStore) GetLogContext(ctx context.Context, traceID string, window time.Duration) ([]*model.LogRecord, error) {
	rows, err := s.client.Conn().Query(ctx, `
		SELECT timestamp, observed_timestamp, severity, severity_text,
		       body, service_name, trace_id, span_id, trace_flags,
		       resource_attributes, log_attributes
		FROM logs.records
		WHERE trace_id = ?
		ORDER BY timestamp ASC
	`, traceID)
	if err != nil {
		return nil, fmt.Errorf("query log context for trace %s: %w", traceID, err)
	}
	defer rows.Close()

	return scanLogRecords(rows)
}

// LogQuery holds filter parameters for log searches.
type LogQuery struct {
	StartTime   time.Time
	EndTime     time.Time
	ServiceName string
	MinSeverity model.Severity
	Keyword     string
	TraceID     string
	SpanID      string
	Limit       int
}

func scanLogRecords(rows interface {
	Next() bool
	Scan(...interface{}) error
	Err() error
}) ([]*model.LogRecord, error) {
	var records []*model.LogRecord
	for rows.Next() {
		var rec model.LogRecord
		var severity int32
		var resourceAttrs, logAttrs map[string]string

		if err := rows.Scan(
			&rec.Timestamp, &rec.ObservedTimestamp,
			&severity, &rec.SeverityText,
			&rec.Body, &rec.ServiceName,
			&rec.TraceID, &rec.SpanID, &rec.TraceFlags,
			&resourceAttrs, &logAttrs,
		); err != nil {
			return nil, fmt.Errorf("scan log record: %w", err)
		}
		rec.Severity = model.Severity(severity)
		rec.ResourceAttributes = resourceAttrs
		rec.LogAttributes = logAttrs
		records = append(records, &rec)
	}
	return records, rows.Err()
}
