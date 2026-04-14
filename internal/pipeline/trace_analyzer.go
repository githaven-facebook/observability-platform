package pipeline

import (
	"context"
	"math"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/model"
	"github.com/nicedavid98/observability-platform/internal/storage/clickhouse"
)

// SlowSpanThresholdMs is the default threshold for flagging slow spans.
const SlowSpanThresholdMs = 1000

// TraceAnalyzer performs offline analysis on trace data: detecting slow spans,
// error patterns, service dependency changes, and latency anomalies.
type TraceAnalyzer struct {
	traceStore *clickhouse.TraceStore
	logger     *zap.Logger
}

// NewTraceAnalyzer creates a TraceAnalyzer backed by the given trace store.
func NewTraceAnalyzer(traceStore *clickhouse.TraceStore, logger *zap.Logger) *TraceAnalyzer {
	return &TraceAnalyzer{traceStore: traceStore, logger: logger}
}

// AnalysisReport summarises findings from a trace analysis run.
type AnalysisReport struct {
	WindowStart       time.Time
	WindowEnd         time.Time
	SlowSpans         []SlowSpanInfo
	ErrorPatterns     []ErrorPattern
	LatencyAnomalies  []LatencyAnomaly
	ServiceChanges    []ServiceDependencyChange
}

// SlowSpanInfo describes a single span that exceeded the latency threshold.
type SlowSpanInfo struct {
	TraceID       string
	SpanID        string
	ServiceName   string
	OperationName string
	DurationMs    int64
}

// ErrorPattern groups repeated errors from the same service and operation.
type ErrorPattern struct {
	ServiceName   string
	OperationName string
	Count         int64
	Sample        string // sample status message
}

// LatencyAnomaly flags a service/operation pair whose p99 latency deviates significantly
// from the baseline.
type LatencyAnomaly struct {
	ServiceName   string
	OperationName string
	BaselineP99Ms float64
	CurrentP99Ms  float64
	DeviationPct  float64
}

// ServiceDependencyChange signals that a new or removed service edge was detected.
type ServiceDependencyChange struct {
	Source  string
	Target  string
	ChangeType string // "added" or "removed"
}

// Analyze runs all analysis passes over traces in the given time window.
func (a *TraceAnalyzer) Analyze(ctx context.Context, windowStart, windowEnd time.Time) (*AnalysisReport, error) {
	q := clickhouse.SpanQuery{
		StartTime: windowStart,
		EndTime:   windowEnd,
		Limit:     50000,
	}

	spans, err := a.traceStore.QuerySpans(ctx, q)
	if err != nil {
		return nil, err
	}

	report := &AnalysisReport{
		WindowStart:   windowStart,
		WindowEnd:     windowEnd,
		SlowSpans:     detectSlowSpans(spans),
		ErrorPatterns: detectErrorPatterns(spans),
	}

	a.logger.Info("trace analysis complete",
		zap.Int("spans_analyzed", len(spans)),
		zap.Int("slow_spans", len(report.SlowSpans)),
		zap.Int("error_patterns", len(report.ErrorPatterns)),
	)
	return report, nil
}

// DetectLatencyAnomalies compares the current window's latency distribution against
// a baseline window and returns services/operations with significant deviation.
func (a *TraceAnalyzer) DetectLatencyAnomalies(
	ctx context.Context,
	baselineStart, baselineEnd,
	currentStart, currentEnd time.Time,
	deviationThreshold float64,
) ([]LatencyAnomaly, error) {
	baselineSpans, err := a.traceStore.QuerySpans(ctx, clickhouse.SpanQuery{
		StartTime: baselineStart, EndTime: baselineEnd, Limit: 100000,
	})
	if err != nil {
		return nil, err
	}
	currentSpans, err := a.traceStore.QuerySpans(ctx, clickhouse.SpanQuery{
		StartTime: currentStart, EndTime: currentEnd, Limit: 100000,
	})
	if err != nil {
		return nil, err
	}

	baselineP99 := computeP99ByOperation(baselineSpans)
	currentP99 := computeP99ByOperation(currentSpans)

	var anomalies []LatencyAnomaly
	for k, current := range currentP99 {
		baseline, ok := baselineP99[k]
		if !ok || baseline == 0 {
			continue
		}
		deviation := (current - baseline) / baseline * 100
		if deviation > deviationThreshold {
			anomalies = append(anomalies, LatencyAnomaly{
				ServiceName:   k.svc,
				OperationName: k.op,
				BaselineP99Ms: baseline,
				CurrentP99Ms:  current,
				DeviationPct:  deviation,
			})
		}
	}

	sort.Slice(anomalies, func(i, j int) bool {
		return anomalies[i].DeviationPct > anomalies[j].DeviationPct
	})
	return anomalies, nil
}

func detectSlowSpans(spans []*model.Span) []SlowSpanInfo {
	var slow []SlowSpanInfo
	for _, s := range spans {
		if s.DurationMs >= SlowSpanThresholdMs {
			slow = append(slow, SlowSpanInfo{
				TraceID:       s.TraceID,
				SpanID:        s.SpanID,
				ServiceName:   s.ServiceName,
				OperationName: s.OperationName,
				DurationMs:    s.DurationMs,
			})
		}
	}
	sort.Slice(slow, func(i, j int) bool { return slow[i].DurationMs > slow[j].DurationMs })
	return slow
}

func detectErrorPatterns(spans []*model.Span) []ErrorPattern {
	type key struct{ svc, op string }
	counts := make(map[key]*ErrorPattern)

	for _, s := range spans {
		if !s.HasError() {
			continue
		}
		k := key{svc: s.ServiceName, op: s.OperationName}
		p, ok := counts[k]
		if !ok {
			p = &ErrorPattern{
				ServiceName:   s.ServiceName,
				OperationName: s.OperationName,
				Sample:        s.StatusMessage,
			}
			counts[k] = p
		}
		p.Count++
	}

	patterns := make([]ErrorPattern, 0, len(counts))
	for _, p := range counts {
		patterns = append(patterns, *p)
	}
	sort.Slice(patterns, func(i, j int) bool { return patterns[i].Count > patterns[j].Count })
	return patterns
}

func computeP99ByOperation(spans []*model.Span) map[struct{ svc, op string }]float64 {
	type opKey = struct{ svc, op string }
	durations := make(map[opKey][]float64)

	for _, s := range spans {
		k := opKey{svc: s.ServiceName, op: s.OperationName}
		durations[k] = append(durations[k], float64(s.DurationMs))
	}

	result := make(map[opKey]float64, len(durations))
	for k, d := range durations {
		result[k] = p99(d)
	}
	return result
}

func p99(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	sorted := make([]float64, len(data))
	copy(sorted, data)
	sort.Float64s(sorted)
	idx := 0.99 * float64(len(sorted)-1)
	lo := int(math.Floor(idx))
	hi := int(math.Ceil(idx))
	if lo == hi {
		return sorted[lo]
	}
	frac := idx - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}
