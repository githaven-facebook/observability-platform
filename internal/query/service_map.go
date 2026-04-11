package query

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/model"
	"github.com/nicedavid98/observability-platform/internal/storage/clickhouse"
)

// ServiceEdge represents a directed call relationship between two services.
type ServiceEdge struct {
	Source       string  `json:"source"`
	Target       string  `json:"target"`
	CallRate     float64 `json:"call_rate"`      // calls per second
	ErrorRate    float64 `json:"error_rate"`     // fraction [0, 1]
	P50LatencyMs float64 `json:"p50_latency_ms"`
	P95LatencyMs float64 `json:"p95_latency_ms"`
	P99LatencyMs float64 `json:"p99_latency_ms"`
	TotalCalls   int64   `json:"total_calls"`
	ErrorCalls   int64   `json:"error_calls"`
}

// ServiceNode represents a single service in the dependency graph.
type ServiceNode struct {
	Name         string  `json:"name"`
	CallRate     float64 `json:"call_rate"`
	ErrorRate    float64 `json:"error_rate"`
	P99LatencyMs float64 `json:"p99_latency_ms"`
	SpanCount    int64   `json:"span_count"`
}

// ServiceMap is the auto-generated dependency graph for a time window.
type ServiceMap struct {
	Nodes     []ServiceNode `json:"nodes"`
	Edges     []ServiceEdge `json:"edges"`
	StartTime time.Time     `json:"start_time"`
	EndTime   time.Time     `json:"end_time"`
}

// ServiceMapEngine derives the service dependency graph from trace data.
type ServiceMapEngine struct {
	store  *clickhouse.TraceStore
	logger *zap.Logger
}

// NewServiceMapEngine creates a ServiceMapEngine backed by the given trace store.
func NewServiceMapEngine(store *clickhouse.TraceStore, logger *zap.Logger) *ServiceMapEngine {
	return &ServiceMapEngine{store: store, logger: logger}
}

// BuildServiceMap queries spans for the given window and constructs the dependency graph.
func (e *ServiceMapEngine) BuildServiceMap(ctx context.Context, startTime, endTime time.Time) (*ServiceMap, error) {
	if endTime.IsZero() {
		endTime = time.Now()
	}
	if startTime.IsZero() {
		startTime = endTime.Add(-1 * time.Hour)
	}

	q := clickhouse.SpanQuery{
		StartTime: startTime,
		EndTime:   endTime,
		Limit:     100000,
	}

	spans, err := e.store.QuerySpans(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query spans for service map: %w", err)
	}

	smap := buildGraphFromSpans(spans, startTime, endTime)
	e.logger.Info("built service map",
		zap.Int("nodes", len(smap.Nodes)),
		zap.Int("edges", len(smap.Edges)),
	)
	return smap, nil
}

// BuildServiceMapFromSpans is exported for testing. It computes the service dependency
// graph directly from a slice of spans without a ClickHouse query.
func BuildServiceMapFromSpans(spans []*model.Span, startTime, endTime time.Time) *ServiceMap {
	return buildGraphFromSpans(spans, startTime, endTime)
}

// buildGraphFromSpans computes edges by matching child spans to their parent's service.
func buildGraphFromSpans(spans []*model.Span, startTime, endTime time.Time) *ServiceMap {
	// Index spans by spanID for parent lookup.
	spanByID := make(map[string]*model.Span, len(spans))
	for _, s := range spans {
		spanByID[s.SpanID] = s
	}

	type edgeKey struct{ src, dst string }
	type edgeStats struct {
		durations  []float64
		total      int64
		errors     int64
	}

	edgeMap := make(map[edgeKey]*edgeStats)
	nodeMap := make(map[string]*nodeStats)

	type nodeStats struct {
		durations []float64
		total     int64
		errors    int64
	}

	for _, s := range spans {
		ns, ok := nodeMap[s.ServiceName]
		if !ok {
			ns = &nodeStats{}
			nodeMap[s.ServiceName] = ns
		}
		ns.total++
		ns.durations = append(ns.durations, float64(s.DurationMs))
		if s.HasError() {
			ns.errors++
		}

		if s.ParentSpanID == "" {
			continue
		}
		parent, ok := spanByID[s.ParentSpanID]
		if !ok || parent.ServiceName == s.ServiceName {
			continue
		}

		key := edgeKey{src: parent.ServiceName, dst: s.ServiceName}
		es, ok := edgeMap[key]
		if !ok {
			es = &edgeStats{}
			edgeMap[key] = es
		}
		es.total++
		es.durations = append(es.durations, float64(s.DurationMs))
		if s.HasError() {
			es.errors++
		}
	}

	windowSec := endTime.Sub(startTime).Seconds()
	if windowSec <= 0 {
		windowSec = 1
	}

	// Build nodes.
	nodes := make([]ServiceNode, 0, len(nodeMap))
	for name, ns := range nodeMap {
		nodes = append(nodes, ServiceNode{
			Name:         name,
			CallRate:     float64(ns.total) / windowSec,
			ErrorRate:    safeDiv(float64(ns.errors), float64(ns.total)),
			P99LatencyMs: percentile(ns.durations, 0.99),
			SpanCount:    ns.total,
		})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })

	// Build edges.
	edges := make([]ServiceEdge, 0, len(edgeMap))
	for key, es := range edgeMap {
		edges = append(edges, ServiceEdge{
			Source:       key.src,
			Target:       key.dst,
			CallRate:     float64(es.total) / windowSec,
			ErrorRate:    safeDiv(float64(es.errors), float64(es.total)),
			P50LatencyMs: percentile(es.durations, 0.50),
			P95LatencyMs: percentile(es.durations, 0.95),
			P99LatencyMs: percentile(es.durations, 0.99),
			TotalCalls:   es.total,
			ErrorCalls:   es.errors,
		})
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].Source != edges[j].Source {
			return edges[i].Source < edges[j].Source
		}
		return edges[i].Target < edges[j].Target
	})

	return &ServiceMap{
		Nodes:     nodes,
		Edges:     edges,
		StartTime: startTime,
		EndTime:   endTime,
	}
}

func percentile(data []float64, p float64) float64 {
	if len(data) == 0 {
		return 0
	}
	sorted := make([]float64, len(data))
	copy(sorted, data)
	sort.Float64s(sorted)
	idx := p * float64(len(sorted)-1)
	lo := int(math.Floor(idx))
	hi := int(math.Ceil(idx))
	if lo == hi {
		return sorted[lo]
	}
	frac := idx - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

func safeDiv(num, denom float64) float64 {
	if denom == 0 {
		return 0
	}
	return num / denom
}
