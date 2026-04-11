package unit_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nicedavid98/observability-platform/internal/collector"
	"github.com/nicedavid98/observability-platform/internal/config"
	"github.com/nicedavid98/observability-platform/internal/model"
)

// TestBatchProcessor_FlushOnSize verifies that the batch processor flushes
// when the batch size threshold is reached.
func TestBatchProcessor_FlushOnSize(t *testing.T) {
	flushed := make(chan struct{}, 1)
	var flushedSpans []*model.Span

	cfg := &config.CollectorConfig{
		BatchSize:     3,
		ExportTimeout: 10 * time.Second,
	}

	bp := collector.NewBatchProcessor(cfg, noopLogger(), func(spans []*model.Span, _ []*model.LogRecord, _ []*model.MetricDataPoint) {
		flushedSpans = spans
		flushed <- struct{}{}
	})
	defer bp.Stop()

	spans := makeSpans(3)
	_, err := bp.ProcessSpans(context.Background(), spans)
	if err != nil {
		t.Fatalf("ProcessSpans: %v", err)
	}

	select {
	case <-flushed:
	case <-time.After(2 * time.Second):
		t.Fatal("batch processor did not flush within timeout")
	}

	if len(flushedSpans) != 3 {
		t.Errorf("expected 3 flushed spans, got %d", len(flushedSpans))
	}
}

// TestBatchProcessor_FlushOnTimeout verifies that the batch processor flushes
// remaining items when the export timeout fires.
func TestBatchProcessor_FlushOnTimeout(t *testing.T) {
	flushed := make(chan struct{}, 1)

	cfg := &config.CollectorConfig{
		BatchSize:     100,
		ExportTimeout: 100 * time.Millisecond,
	}

	bp := collector.NewBatchProcessor(cfg, noopLogger(), func(_ []*model.Span, _ []*model.LogRecord, _ []*model.MetricDataPoint) {
		flushed <- struct{}{}
	})
	defer bp.Stop()

	_, _ = bp.ProcessSpans(context.Background(), makeSpans(1))

	select {
	case <-flushed:
	case <-time.After(2 * time.Second):
		t.Fatal("batch processor did not flush on timeout")
	}
}

// TestSamplingProcessor_AlwaysSampleErrors verifies that error spans bypass
// the sampling rate and are always included.
func TestSamplingProcessor_AlwaysSampleErrors(t *testing.T) {
	cfg := &config.SamplingConfig{
		HeadSampleRate: 0.0, // drop everything
	}
	proc := collector.NewSamplingProcessor(cfg, noopLogger())

	spans := []*model.Span{
		{SpanID: "1", Status: model.SpanStatusOK},
		{SpanID: "2", Status: model.SpanStatusError},
		{SpanID: "3", Status: model.SpanStatusOK},
	}

	result, err := proc.ProcessSpans(context.Background(), spans)
	if err != nil {
		t.Fatalf("ProcessSpans: %v", err)
	}

	// Only the error span should survive at 0% sample rate.
	if len(result) != 1 || result[0].SpanID != "2" {
		t.Errorf("expected only error span, got %d spans", len(result))
	}
}

// TestSamplingProcessor_ServiceRate verifies per-service sampling rates override
// the global rate.
func TestSamplingProcessor_ServiceRate(t *testing.T) {
	cfg := &config.SamplingConfig{
		HeadSampleRate: 0.0,
		ServiceRates:   map[string]float64{"critical-service": 1.0},
	}
	proc := collector.NewSamplingProcessor(cfg, noopLogger())

	spans := []*model.Span{
		{SpanID: "1", ServiceName: "critical-service", Status: model.SpanStatusOK},
		{SpanID: "2", ServiceName: "other-service", Status: model.SpanStatusOK},
	}

	result, err := proc.ProcessSpans(context.Background(), spans)
	if err != nil {
		t.Fatalf("ProcessSpans: %v", err)
	}

	if len(result) != 1 || result[0].SpanID != "1" {
		t.Errorf("expected only critical-service span, got %d spans", len(result))
	}
}

// TestAttributeProcessor_AddAction verifies that the add action inserts
// a new attribute only when the key is absent.
func TestAttributeProcessor_AddAction(t *testing.T) {
	rules := []collector.AttributeProcessorRule{
		{Action: "add", Key: "env", Value: "production"},
	}
	proc := collector.NewAttributeProcessor(rules, noopLogger())

	spans := []*model.Span{
		{SpanID: "1", Attributes: map[string]string{}},
		{SpanID: "2", Attributes: map[string]string{"env": "staging"}}, // should not overwrite
	}

	result, err := proc.ProcessSpans(context.Background(), spans)
	if err != nil {
		t.Fatalf("ProcessSpans: %v", err)
	}

	if result[0].Attributes["env"] != "production" {
		t.Errorf("expected env=production on span 1, got %q", result[0].Attributes["env"])
	}
	if result[1].Attributes["env"] != "staging" {
		t.Errorf("expected env=staging preserved on span 2, got %q", result[1].Attributes["env"])
	}
}

// TestAttributeProcessor_RemoveAction verifies that the remove action deletes a key.
func TestAttributeProcessor_RemoveAction(t *testing.T) {
	rules := []collector.AttributeProcessorRule{
		{Action: "remove", Key: "sensitive"},
	}
	proc := collector.NewAttributeProcessor(rules, noopLogger())

	spans := []*model.Span{
		{SpanID: "1", Attributes: map[string]string{"sensitive": "secret", "safe": "ok"}},
	}

	result, err := proc.ProcessSpans(context.Background(), spans)
	if err != nil {
		t.Fatalf("ProcessSpans: %v", err)
	}

	if _, exists := result[0].Attributes["sensitive"]; exists {
		t.Error("expected sensitive attribute to be removed")
	}
	if result[0].Attributes["safe"] != "ok" {
		t.Error("expected safe attribute to be preserved")
	}
}

// TestAttributeProcessor_RenameAction verifies that the rename action moves a key.
func TestAttributeProcessor_RenameAction(t *testing.T) {
	rules := []collector.AttributeProcessorRule{
		{Action: "rename", Key: "old_key", NewKey: "new_key"},
	}
	proc := collector.NewAttributeProcessor(rules, noopLogger())

	spans := []*model.Span{
		{SpanID: "1", Attributes: map[string]string{"old_key": "value"}},
	}

	result, err := proc.ProcessSpans(context.Background(), spans)
	if err != nil {
		t.Fatalf("ProcessSpans: %v", err)
	}

	if _, exists := result[0].Attributes["old_key"]; exists {
		t.Error("expected old_key to be removed after rename")
	}
	if result[0].Attributes["new_key"] != "value" {
		t.Errorf("expected new_key=value, got %q", result[0].Attributes["new_key"])
	}
}

// TestResourceProcessor_NormalizesServiceName verifies that service names are
// lowercased and have separators normalised to dashes.
func TestResourceProcessor_NormalizesServiceName(t *testing.T) {
	proc := collector.NewResourceProcessor(noopLogger())

	tests := []struct {
		input    string
		expected string
	}{
		{"My_Service", "my-service"},
		{"MyService/v2", "myservice-v2"},
		{"backend service", "backend-service"},
		{"already-normalized", "already-normalized"},
	}

	for _, tt := range tests {
		spans := []*model.Span{{ServiceName: tt.input}}
		result, err := proc.ProcessSpans(context.Background(), spans)
		if err != nil {
			t.Fatalf("ProcessSpans: %v", err)
		}
		if result[0].ServiceName != tt.expected {
			t.Errorf("input %q: expected %q, got %q", tt.input, tt.expected, result[0].ServiceName)
		}
	}
}

// helpers

func makeSpans(n int) []*model.Span {
	spans := make([]*model.Span, n)
	for i := range spans {
		spans[i] = &model.Span{
			SpanID:      fmt.Sprintf("span-%d", i),
			TraceID:     "trace-1",
			ServiceName: "test-service",
			StartTime:   time.Now(),
			EndTime:     time.Now().Add(time.Millisecond),
			DurationMs:  1,
		}
	}
	return spans
}
