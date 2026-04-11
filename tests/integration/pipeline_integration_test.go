// Package integration contains end-to-end tests that require live infrastructure.
// Tests are skipped automatically when the INTEGRATION_TESTS environment variable is not set.
package integration_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/config"
	"github.com/nicedavid98/observability-platform/internal/model"
	"github.com/nicedavid98/observability-platform/internal/query"
	"github.com/nicedavid98/observability-platform/internal/storage/clickhouse"
)

func requireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test: set INTEGRATION_TESTS=1 to run")
	}
}

func testClickHouseDSN() string {
	if dsn := os.Getenv("CLICKHOUSE_DSN"); dsn != "" {
		return dsn
	}
	return "clickhouse://default:@localhost:9000/default"
}

// TestPipeline_IngestAndQueryTrace verifies the full path:
// insert spans → query by trace ID → verify all spans returned.
func TestPipeline_IngestAndQueryTrace(t *testing.T) {
	requireIntegration(t)

	logger := zap.NewNop()
	cfg := &config.StorageConfig{
		ClickHouseDSN:   testClickHouseDSN(),
		RetentionDays:   7,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: 5 * time.Minute,
		DialTimeout:     10 * time.Second,
	}

	client, err := clickhouse.NewClient(cfg, logger)
	if err != nil {
		t.Fatalf("connect to ClickHouse: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	if err := clickhouse.RunMigrations(ctx, client, 7, logger); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	traceStore := clickhouse.NewTraceStore(client, logger)
	traceEngine := query.NewTraceQueryEngine(traceStore, logger)

	// Generate a unique trace ID for this test run.
	traceID := fmt.Sprintf("integration-trace-%d", time.Now().UnixNano())
	now := time.Now()

	spans := []*model.Span{
		{
			TraceID:       traceID,
			SpanID:        "span-root",
			ServiceName:   "integration-frontend",
			OperationName: "GET /test",
			StartTime:     now,
			EndTime:       now.Add(50 * time.Millisecond),
			DurationMs:    50,
			Status:        model.SpanStatusOK,
		},
		{
			TraceID:       traceID,
			SpanID:        "span-db",
			ParentSpanID:  "span-root",
			ServiceName:   "integration-db",
			OperationName: "SELECT",
			StartTime:     now.Add(5 * time.Millisecond),
			EndTime:       now.Add(30 * time.Millisecond),
			DurationMs:    25,
			Status:        model.SpanStatusOK,
		},
	}

	if err := traceStore.InsertSpans(ctx, spans); err != nil {
		t.Fatalf("insert spans: %v", err)
	}

	// Allow ClickHouse to make the data visible.
	time.Sleep(500 * time.Millisecond)

	trace, err := traceEngine.GetTrace(ctx, traceID)
	if err != nil {
		t.Fatalf("get trace: %v", err)
	}

	if trace.SpanCount != 2 {
		t.Errorf("expected 2 spans, got %d", trace.SpanCount)
	}
	if trace.HasError {
		t.Error("expected no errors in trace")
	}
}

// TestPipeline_IngestAndQueryLogs verifies log ingestion and search.
func TestPipeline_IngestAndQueryLogs(t *testing.T) {
	requireIntegration(t)

	logger := zap.NewNop()
	cfg := &config.StorageConfig{
		ClickHouseDSN:   testClickHouseDSN(),
		RetentionDays:   7,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: 5 * time.Minute,
		DialTimeout:     10 * time.Second,
	}

	client, err := clickhouse.NewClient(cfg, logger)
	if err != nil {
		t.Fatalf("connect to ClickHouse: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	logStore := clickhouse.NewLogStore(client, logger)
	logEngine := query.NewLogQueryEngine(logStore, logger)

	serviceName := fmt.Sprintf("integration-svc-%d", time.Now().UnixNano())
	now := time.Now()

	logs := []*model.LogRecord{
		{
			Timestamp:   now,
			Severity:    model.SeverityInfo,
			Body:        "user login successful",
			ServiceName: serviceName,
		},
		{
			Timestamp:   now.Add(time.Millisecond),
			Severity:    model.SeverityError,
			Body:        "database connection failed",
			ServiceName: serviceName,
		},
	}

	if err := logStore.InsertLogs(ctx, logs); err != nil {
		t.Fatalf("insert logs: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	records, err := logEngine.SearchLogs(ctx, query.LogSearchParams{
		ServiceName: serviceName,
		StartTime:   now.Add(-time.Minute),
		EndTime:     now.Add(time.Minute),
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search logs: %v", err)
	}

	if len(records) != 2 {
		t.Errorf("expected 2 log records, got %d", len(records))
	}
}

// TestPipeline_ServiceMapGeneration verifies service map derivation from traces.
func TestPipeline_ServiceMapGeneration(t *testing.T) {
	requireIntegration(t)

	logger := zap.NewNop()
	cfg := &config.StorageConfig{
		ClickHouseDSN:   testClickHouseDSN(),
		RetentionDays:   7,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: 5 * time.Minute,
		DialTimeout:     10 * time.Second,
	}

	client, err := clickhouse.NewClient(cfg, logger)
	if err != nil {
		t.Fatalf("connect to ClickHouse: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	traceStore := clickhouse.NewTraceStore(client, logger)
	smEngine := query.NewServiceMapEngine(traceStore, logger)

	traceID := fmt.Sprintf("sm-trace-%d", time.Now().UnixNano())
	now := time.Now()

	spans := []*model.Span{
		{TraceID: traceID, SpanID: "s1", ServiceName: "gateway", OperationName: "route", StartTime: now, EndTime: now.Add(20 * time.Millisecond), DurationMs: 20},
		{TraceID: traceID, SpanID: "s2", ParentSpanID: "s1", ServiceName: "auth", OperationName: "verify", StartTime: now, EndTime: now.Add(10 * time.Millisecond), DurationMs: 10},
	}

	if err := traceStore.InsertSpans(ctx, spans); err != nil {
		t.Fatalf("insert spans: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	smap, err := smEngine.BuildServiceMap(ctx, now.Add(-time.Minute), now.Add(time.Minute))
	if err != nil {
		t.Fatalf("build service map: %v", err)
	}

	if len(smap.Nodes) < 2 {
		t.Errorf("expected at least 2 nodes, got %d", len(smap.Nodes))
	}

	found := false
	for _, e := range smap.Edges {
		if e.Source == "gateway" && e.Target == "auth" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected gateway→auth edge in service map")
	}
}
