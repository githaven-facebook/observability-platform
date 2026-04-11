package unit_test

import (
	"testing"
	"time"

	"github.com/nicedavid98/observability-platform/internal/model"
	"github.com/nicedavid98/observability-platform/internal/query"
)

// TestServiceMap_EmptySpans verifies that no edges are generated from an empty span set.
func TestServiceMap_EmptySpans(t *testing.T) {
	smap := buildTestServiceMap(nil)
	if len(smap.Nodes) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(smap.Nodes))
	}
	if len(smap.Edges) != 0 {
		t.Errorf("expected 0 edges, got %d", len(smap.Edges))
	}
}

// TestServiceMap_SingleService verifies a trace with one service produces one node and no edges.
func TestServiceMap_SingleService(t *testing.T) {
	spans := []*model.Span{
		{
			TraceID:       "trace-1",
			SpanID:        "span-1",
			ServiceName:   "frontend",
			OperationName: "GET /",
			StartTime:     time.Now(),
			EndTime:       time.Now().Add(10 * time.Millisecond),
			DurationMs:    10,
		},
	}
	smap := buildTestServiceMap(spans)
	if len(smap.Nodes) != 1 {
		t.Errorf("expected 1 node, got %d", len(smap.Nodes))
	}
	if len(smap.Edges) != 0 {
		t.Errorf("expected 0 edges for single service, got %d", len(smap.Edges))
	}
}

// TestServiceMap_TwoServices verifies that a parent-child span pair across
// different services generates exactly one edge.
func TestServiceMap_TwoServices(t *testing.T) {
	spans := []*model.Span{
		{
			TraceID:       "trace-1",
			SpanID:        "span-1",
			ParentSpanID:  "",
			ServiceName:   "frontend",
			OperationName: "GET /product",
			StartTime:     time.Now(),
			EndTime:       time.Now().Add(20 * time.Millisecond),
			DurationMs:    20,
		},
		{
			TraceID:       "trace-1",
			SpanID:        "span-2",
			ParentSpanID:  "span-1",
			ServiceName:   "product-service",
			OperationName: "GetProduct",
			StartTime:     time.Now().Add(1 * time.Millisecond),
			EndTime:       time.Now().Add(15 * time.Millisecond),
			DurationMs:    14,
		},
	}
	smap := buildTestServiceMap(spans)

	if len(smap.Nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(smap.Nodes))
	}
	if len(smap.Edges) != 1 {
		t.Errorf("expected 1 edge, got %d", len(smap.Edges))
	}

	edge := smap.Edges[0]
	if edge.Source != "frontend" || edge.Target != "product-service" {
		t.Errorf("expected edge frontend→product-service, got %s→%s", edge.Source, edge.Target)
	}
}

// TestServiceMap_ErrorRate verifies that error rate is computed correctly on edges.
func TestServiceMap_ErrorRate(t *testing.T) {
	now := time.Now()
	spans := []*model.Span{
		{TraceID: "t1", SpanID: "s1", ServiceName: "frontend", StartTime: now, EndTime: now.Add(10 * time.Millisecond), DurationMs: 10},
		{TraceID: "t1", SpanID: "s2", ParentSpanID: "s1", ServiceName: "backend", Status: model.SpanStatusOK, StartTime: now, EndTime: now.Add(5 * time.Millisecond), DurationMs: 5},
		{TraceID: "t2", SpanID: "s3", ServiceName: "frontend", StartTime: now, EndTime: now.Add(10 * time.Millisecond), DurationMs: 10},
		{TraceID: "t2", SpanID: "s4", ParentSpanID: "s3", ServiceName: "backend", Status: model.SpanStatusError, StartTime: now, EndTime: now.Add(5 * time.Millisecond), DurationMs: 5},
	}

	smap := buildTestServiceMap(spans)

	var edge *query.ServiceEdge
	for i := range smap.Edges {
		if smap.Edges[i].Source == "frontend" && smap.Edges[i].Target == "backend" {
			edge = &smap.Edges[i]
			break
		}
	}
	if edge == nil {
		t.Fatal("expected edge frontend→backend")
	}

	// 1 error out of 2 calls = 50% error rate.
	const want = 0.5
	if edge.ErrorRate != want {
		t.Errorf("expected error rate %.2f, got %.2f", want, edge.ErrorRate)
	}
}

// TestServiceMap_SameServiceParentChild verifies that spans within the same service
// do not generate self-edges.
func TestServiceMap_SameServiceParentChild(t *testing.T) {
	now := time.Now()
	spans := []*model.Span{
		{TraceID: "t1", SpanID: "s1", ServiceName: "backend", StartTime: now, EndTime: now.Add(10 * time.Millisecond), DurationMs: 10},
		{TraceID: "t1", SpanID: "s2", ParentSpanID: "s1", ServiceName: "backend", StartTime: now, EndTime: now.Add(5 * time.Millisecond), DurationMs: 5},
	}

	smap := buildTestServiceMap(spans)
	if len(smap.Edges) != 0 {
		t.Errorf("expected no self-edges within same service, got %d edges", len(smap.Edges))
	}
}

// buildTestServiceMap is a test helper that calls the internal graph builder directly
// without needing a real ClickHouse connection.
func buildTestServiceMap(spans []*model.Span) *query.ServiceMap {
	now := time.Now()
	return query.BuildServiceMapFromSpans(spans, now.Add(-time.Hour), now)
}
