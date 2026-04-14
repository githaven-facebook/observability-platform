// Package query implements the query engines for traces, logs, metrics, and service maps.
package query

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/model"
	"github.com/nicedavid98/observability-platform/internal/storage/clickhouse"
)

// TraceQueryEngine executes trace and span queries against ClickHouse.
type TraceQueryEngine struct {
	store  *clickhouse.TraceStore
	logger *zap.Logger
}

// NewTraceQueryEngine creates a TraceQueryEngine backed by the given store.
func NewTraceQueryEngine(store *clickhouse.TraceStore, logger *zap.Logger) *TraceQueryEngine {
	return &TraceQueryEngine{store: store, logger: logger}
}

// TraceSearchParams defines criteria for searching traces.
type TraceSearchParams struct {
	ServiceName   string
	OperationName string
	MinDurationMs int64
	MaxDurationMs int64
	Status        *model.SpanStatus
	Tags          map[string]string
	StartTime     time.Time
	EndTime       time.Time
	Limit         int
}

// SearchTraces finds traces matching the given criteria, returning summaries.
func (e *TraceQueryEngine) SearchTraces(ctx context.Context, params TraceSearchParams) ([]model.TraceSummary, error) { //nolint:gocritic // params is a query value type copied intentionally
	if params.Limit <= 0 {
		params.Limit = 100
	}
	if params.EndTime.IsZero() {
		params.EndTime = time.Now()
	}
	if params.StartTime.IsZero() {
		params.StartTime = params.EndTime.Add(-1 * time.Hour)
	}

	q := clickhouse.SpanQuery{
		StartTime:     params.StartTime,
		EndTime:       params.EndTime,
		ServiceName:   params.ServiceName,
		OperationName: params.OperationName,
		MinDurationMs: params.MinDurationMs,
		MaxDurationMs: params.MaxDurationMs,
		StatusFilter:  params.Status,
		Tags:          params.Tags,
		Limit:         params.Limit,
	}

	summaries, err := e.store.GetTraceSummaries(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("search traces: %w", err)
	}
	return summaries, nil
}

// GetTrace retrieves the full trace with all spans for the given trace ID.
func (e *TraceQueryEngine) GetTrace(ctx context.Context, traceID string) (*model.Trace, error) {
	if traceID == "" {
		return nil, fmt.Errorf("trace_id is required")
	}
	trace, err := e.store.GetTrace(ctx, traceID)
	if err != nil {
		return nil, fmt.Errorf("get trace %s: %w", traceID, err)
	}
	reconstructTimeline(trace)
	return trace, nil
}

// GetSpans retrieves spans for a trace, optionally filtered by service or operation.
func (e *TraceQueryEngine) GetSpans(ctx context.Context, traceID string) ([]*model.Span, error) {
	trace, err := e.store.GetTrace(ctx, traceID)
	if err != nil {
		return nil, fmt.Errorf("get spans for trace %s: %w", traceID, err)
	}
	return trace.Spans, nil
}

// reconstructTimeline sorts spans chronologically and sets parent-child references
// so callers can render a proper Gantt-style timeline.
func reconstructTimeline(trace *model.Trace) {
	sort.Slice(trace.Spans, func(i, j int) bool {
		return trace.Spans[i].StartTime.Before(trace.Spans[j].StartTime)
	})
}

// ScatterGatherQuery executes a trace query across multiple time partitions in parallel
// and merges results sorted by start time. This is useful for very large time ranges.
func (e *TraceQueryEngine) ScatterGatherQuery(ctx context.Context, params TraceSearchParams) ([]model.TraceSummary, error) { //nolint:gocritic // params is a query value type copied intentionally
	const partitionWindow = 6 * time.Hour
	if params.EndTime.Sub(params.StartTime) <= partitionWindow {
		return e.SearchTraces(ctx, params)
	}

	type partitionResult struct {
		summaries []model.TraceSummary
		err       error
	}

	var partitions []TraceSearchParams
	cur := params.StartTime
	for cur.Before(params.EndTime) {
		end := cur.Add(partitionWindow)
		if end.After(params.EndTime) {
			end = params.EndTime
		}
		p := params
		p.StartTime = cur
		p.EndTime = end
		partitions = append(partitions, p)
		cur = end
	}

	results := make([]partitionResult, len(partitions))
	done := make(chan int, len(partitions))

	for i, p := range partitions {
		go func(idx int, pp TraceSearchParams) {
			summaries, err := e.SearchTraces(ctx, pp)
			results[idx] = partitionResult{summaries: summaries, err: err}
			done <- idx
		}(i, p)
	}

	for range partitions {
		<-done
	}

	var merged []model.TraceSummary
	for _, r := range results {
		if r.err != nil {
			e.logger.Warn("scatter-gather partition error", zap.Error(r.err))
			continue
		}
		merged = append(merged, r.summaries...)
	}

	sort.Slice(merged, func(i, j int) bool {
		return merged[i].StartTime.After(merged[j].StartTime)
	})

	if params.Limit > 0 && len(merged) > params.Limit {
		merged = merged[:params.Limit]
	}
	return merged, nil
}
