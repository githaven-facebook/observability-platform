package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/model"
)

// TraceStore handles span persistence and querying in ClickHouse.
type TraceStore struct {
	client *Client
	logger *zap.Logger
}

// NewTraceStore creates a TraceStore using the given client.
func NewTraceStore(client *Client, logger *zap.Logger) *TraceStore {
	return &TraceStore{client: client, logger: logger}
}

// InsertSpans batch-inserts spans using ClickHouse native batch protocol.
func (s *TraceStore) InsertSpans(ctx context.Context, spans []*model.Span) error {
	if len(spans) == 0 {
		return nil
	}

	batch, err := s.client.Conn().PrepareBatch(ctx,
		"INSERT INTO traces.spans (trace_id, span_id, parent_span_id, trace_state, service_name, operation_name, start_time, end_time, duration_ms, status, status_message, kind, attributes, resource_attributes, events, links)")
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, span := range spans {
		eventsJSON, _ := json.Marshal(span.Events)
		linksJSON, _ := json.Marshal(span.Links)

		if err := batch.Append(
			span.TraceID,
			span.SpanID,
			span.ParentSpanID,
			span.TraceState,
			span.ServiceName,
			span.OperationName,
			span.StartTime,
			span.EndTime,
			span.DurationMs,
			int32(span.Status),
			span.StatusMessage,
			int32(span.Kind),
			span.Attributes,
			span.ResourceAttrs,
			string(eventsJSON),
			string(linksJSON),
		); err != nil {
			return fmt.Errorf("append span %s: %w", span.SpanID, err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}

	s.logger.Debug("inserted spans", zap.Int("count", len(spans)))
	return nil
}

// GetTrace retrieves all spans for a given trace ID and assembles them into a Trace.
func (s *TraceStore) GetTrace(ctx context.Context, traceID string) (*model.Trace, error) {
	rows, err := s.client.Conn().Query(ctx, `
		SELECT trace_id, span_id, parent_span_id, trace_state, service_name, operation_name,
		       start_time, end_time, duration_ms, status, status_message, kind,
		       attributes, resource_attributes
		FROM traces.spans
		WHERE trace_id = ?
		ORDER BY start_time ASC
	`, traceID)
	if err != nil {
		return nil, fmt.Errorf("query trace %s: %w", traceID, err)
	}
	defer rows.Close()

	spans, err := scanSpans(rows)
	if err != nil {
		return nil, err
	}
	if len(spans) == 0 {
		return nil, fmt.Errorf("trace %s not found", traceID)
	}

	return assembleTrace(traceID, spans), nil
}

// QuerySpans searches for spans matching the given filter criteria.
func (s *TraceStore) QuerySpans(ctx context.Context, q SpanQuery) ([]*model.Span, error) {
	query := `
		SELECT trace_id, span_id, parent_span_id, trace_state, service_name, operation_name,
		       start_time, end_time, duration_ms, status, status_message, kind,
		       attributes, resource_attributes
		FROM traces.spans
		WHERE start_time >= ? AND start_time <= ?
	`
	args := []interface{}{q.StartTime, q.EndTime}

	if q.ServiceName != "" {
		query += " AND service_name = ?"
		args = append(args, q.ServiceName)
	}
	if q.OperationName != "" {
		query += " AND operation_name = ?"
		args = append(args, q.OperationName)
	}
	if q.MinDurationMs > 0 {
		query += " AND duration_ms >= ?"
		args = append(args, q.MinDurationMs)
	}
	if q.MaxDurationMs > 0 {
		query += " AND duration_ms <= ?"
		args = append(args, q.MaxDurationMs)
	}
	if q.StatusFilter != nil {
		query += " AND status = ?"
		args = append(args, int32(*q.StatusFilter))
	}

	query += " ORDER BY start_time DESC LIMIT ?"
	args = append(args, q.Limit)

	rows, err := s.client.Conn().Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query spans: %w", err)
	}
	defer rows.Close()

	return scanSpans(rows)
}

// GetTraceSummaries returns lightweight trace summaries for list views.
func (s *TraceStore) GetTraceSummaries(ctx context.Context, q SpanQuery) ([]model.TraceSummary, error) {
	rows, err := s.client.Conn().Query(ctx, `
		SELECT
			trace_id,
			argMin(service_name, start_time)  AS root_service,
			argMin(operation_name, start_time) AS root_operation,
			min(start_time)                    AS trace_start,
			max(end_time) - min(start_time)    AS duration_ms,
			count()                            AS span_count,
			countIf(status = 2)                AS error_count,
			uniqExact(service_name)            AS service_count
		FROM traces.spans
		WHERE start_time >= ? AND start_time <= ?
		GROUP BY trace_id
		ORDER BY trace_start DESC
		LIMIT ?
	`, q.StartTime, q.EndTime, q.Limit)
	if err != nil {
		return nil, fmt.Errorf("query trace summaries: %w", err)
	}
	defer rows.Close()

	var summaries []model.TraceSummary
	for rows.Next() {
		var ts model.TraceSummary
		var durationNs int64
		if err := rows.Scan(
			&ts.TraceID, &ts.RootService, &ts.RootOperation, &ts.StartTime,
			&durationNs, &ts.SpanCount, &ts.ErrorCount, &ts.ServiceCount,
		); err != nil {
			return nil, fmt.Errorf("scan trace summary: %w", err)
		}
		ts.DurationMs = durationNs / int64(time.Millisecond)
		ts.HasError = ts.ErrorCount > 0
		summaries = append(summaries, ts)
	}
	return summaries, rows.Err()
}

// SpanQuery holds filter parameters for span searches.
type SpanQuery struct {
	StartTime     time.Time
	EndTime       time.Time
	ServiceName   string
	OperationName string
	MinDurationMs int64
	MaxDurationMs int64
	StatusFilter  *model.SpanStatus
	Tags          map[string]string
	Limit         int
}

func scanSpans(rows interface {
	Next() bool
	Scan(...interface{}) error
	Err() error
}) ([]*model.Span, error) {
	var spans []*model.Span
	for rows.Next() {
		var span model.Span
		var status, kind int32
		var attrs, resourceAttrs map[string]string

		if err := rows.Scan(
			&span.TraceID, &span.SpanID, &span.ParentSpanID, &span.TraceState,
			&span.ServiceName, &span.OperationName,
			&span.StartTime, &span.EndTime, &span.DurationMs,
			&status, &span.StatusMessage, &kind,
			&attrs, &resourceAttrs,
		); err != nil {
			return nil, fmt.Errorf("scan span: %w", err)
		}
		span.Status = model.SpanStatus(status)
		span.Kind = model.SpanKind(kind)
		span.Attributes = attrs
		span.ResourceAttrs = resourceAttrs
		spans = append(spans, &span)
	}
	return spans, rows.Err()
}

func assembleTrace(traceID string, spans []*model.Span) *model.Trace {
	trace := &model.Trace{
		TraceID:   traceID,
		Spans:     spans,
		SpanCount: len(spans),
	}

	serviceSet := make(map[string]struct{})
	for _, s := range spans {
		serviceSet[s.ServiceName] = struct{}{}
		if s.HasError() {
			trace.HasError = true
		}
		if s.IsRoot() {
			trace.RootSpan = s
		}
		if trace.StartTime.IsZero() || s.StartTime.Before(trace.StartTime) {
			trace.StartTime = s.StartTime
		}
		if s.EndTime.After(trace.EndTime) {
			trace.EndTime = s.EndTime
		}
	}

	trace.DurationMs = trace.EndTime.Sub(trace.StartTime).Milliseconds()
	for svc := range serviceSet {
		trace.Services = append(trace.Services, svc)
	}
	return trace
}
