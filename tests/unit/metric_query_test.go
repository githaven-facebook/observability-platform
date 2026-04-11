package unit_test

import (
	"testing"

	"github.com/nicedavid98/observability-platform/internal/model"
)

// TestSelectResolution_RangeMapping verifies that the correct downsampling resolution
// is chosen for various time ranges.
func TestSelectResolution_RangeMapping(t *testing.T) {
	tests := []struct {
		name        string
		rangeSeconds int64
		expected    model.DownsampleResolution
	}{
		{"one minute range", 60, model.Resolution1Min},
		{"30 minute range", 1800, model.Resolution1Min},
		{"two hour range", 7200, model.Resolution5Min},
		{"12 hour range", 43200, model.Resolution1Hour},
		{"two day range", 172800, model.Resolution1Day},
		{"seven day range", 7 * 24 * 3600, model.Resolution1Day},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := model.SelectResolution(tt.rangeSeconds)
			if result != tt.expected {
				t.Errorf("SelectResolution(%d) = %q, want %q",
					tt.rangeSeconds, result, tt.expected)
			}
		})
	}
}

// TestMetricType_String verifies human-readable type names.
func TestMetricType_String(t *testing.T) {
	tests := []struct {
		metricType model.MetricType
		expected   string
	}{
		{model.MetricTypeGauge, "gauge"},
		{model.MetricTypeCounter, "counter"},
		{model.MetricTypeHistogram, "histogram"},
		{model.MetricTypeSummary, "summary"},
		{model.MetricTypeUnspecified, "unspecified"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if tt.metricType.String() != tt.expected {
				t.Errorf("MetricType.String() = %q, want %q",
					tt.metricType.String(), tt.expected)
			}
		})
	}
}

// TestMetricDataPoint_Labels verifies that label maps are handled correctly.
func TestMetricDataPoint_Labels(t *testing.T) {
	point := &model.MetricDataPoint{
		Name:        "http_requests_total",
		Type:        model.MetricTypeCounter,
		Value:       42.0,
		Labels:      map[string]string{"method": "GET", "status": "200"},
		ServiceName: "frontend",
	}

	if point.Name != "http_requests_total" {
		t.Errorf("unexpected name: %s", point.Name)
	}
	if point.Labels["method"] != "GET" {
		t.Errorf("expected label method=GET, got %q", point.Labels["method"])
	}
	if point.Labels["status"] != "200" {
		t.Errorf("expected label status=200, got %q", point.Labels["status"])
	}
}

// TestLogRecord_SeverityParsing verifies ParseSeverity round-trips correctly.
func TestLogRecord_SeverityParsing(t *testing.T) {
	tests := []struct {
		input    string
		expected model.Severity
	}{
		{"INFO", model.SeverityInfo},
		{"info", model.SeverityInfo},
		{"ERROR", model.SeverityError},
		{"WARN", model.SeverityWarn},
		{"WARNING", model.SeverityWarn},
		{"DEBUG", model.SeverityDebug},
		{"FATAL", model.SeverityFatal},
		{"TRACE", model.SeverityTrace},
		{"unknown", model.SeverityUnspecified},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := model.ParseSeverity(tt.input)
			if result != tt.expected {
				t.Errorf("ParseSeverity(%q) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

// TestLogRecord_IsError verifies the IsError helper.
func TestLogRecord_IsError(t *testing.T) {
	tests := []struct {
		severity model.Severity
		expected bool
	}{
		{model.SeverityDebug, false},
		{model.SeverityInfo, false},
		{model.SeverityWarn, false},
		{model.SeverityError, true},
		{model.SeverityFatal, true},
	}

	for _, tt := range tests {
		rec := &model.LogRecord{Severity: tt.severity}
		if rec.IsError() != tt.expected {
			t.Errorf("IsError() with severity %d = %v, want %v",
				tt.severity, rec.IsError(), tt.expected)
		}
	}
}

// TestSpan_IsRoot checks that a span without a parent is identified as root.
func TestSpan_IsRoot(t *testing.T) {
	root := &model.Span{SpanID: "root", ParentSpanID: ""}
	child := &model.Span{SpanID: "child", ParentSpanID: "root"}

	if !root.IsRoot() {
		t.Error("expected root span to be identified as root")
	}
	if child.IsRoot() {
		t.Error("expected child span to not be identified as root")
	}
}

// TestSpan_HasError checks error status detection.
func TestSpan_HasError(t *testing.T) {
	ok := &model.Span{Status: model.SpanStatusOK}
	errSpan := &model.Span{Status: model.SpanStatusError}

	if ok.HasError() {
		t.Error("OK span should not HasError")
	}
	if !errSpan.HasError() {
		t.Error("error span should HasError")
	}
}
