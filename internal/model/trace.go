// Package model defines shared domain types for the observability platform.
package model

import "time"

// SpanStatus represents the completion status of a span.
type SpanStatus int32

const (
	// SpanStatusUnset indicates no status was set.
	SpanStatusUnset SpanStatus = 0
	// SpanStatusOK indicates the span completed successfully.
	SpanStatusOK SpanStatus = 1
	// SpanStatusError indicates the span completed with an error.
	SpanStatusError SpanStatus = 2
)

// String returns a human-readable name for the status.
func (s SpanStatus) String() string {
	switch s {
	case SpanStatusOK:
		return "ok"
	case SpanStatusError:
		return "error"
	default:
		return "unset"
	}
}

// SpanKind represents the role of a span in a distributed trace.
type SpanKind int32

const (
	SpanKindUnspecified SpanKind = 0
	SpanKindInternal    SpanKind = 1
	SpanKindServer      SpanKind = 2
	SpanKindClient      SpanKind = 3
	SpanKindProducer    SpanKind = 4
	SpanKindConsumer    SpanKind = 5
)

// SpanEvent represents a timestamped annotation within a span.
type SpanEvent struct {
	Timestamp  time.Time         `json:"timestamp"`
	Name       string            `json:"name"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// SpanLink represents a causal link from one span to another, potentially in a different trace.
type SpanLink struct {
	TraceID    string            `json:"trace_id"`
	SpanID     string            `json:"span_id"`
	TraceState string            `json:"trace_state,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// Span represents a single unit of work within a distributed trace.
type Span struct {
	TraceID        string            `json:"trace_id"`
	SpanID         string            `json:"span_id"`
	ParentSpanID   string            `json:"parent_span_id,omitempty"`
	TraceState     string            `json:"trace_state,omitempty"`
	ServiceName    string            `json:"service_name"`
	OperationName  string            `json:"operation_name"`
	StartTime      time.Time         `json:"start_time"`
	EndTime        time.Time         `json:"end_time"`
	DurationMs     int64             `json:"duration_ms"`
	Status         SpanStatus        `json:"status"`
	StatusMessage  string            `json:"status_message,omitempty"`
	Kind           SpanKind          `json:"kind"`
	Attributes     map[string]string `json:"attributes,omitempty"`
	ResourceAttrs  map[string]string `json:"resource_attributes,omitempty"`
	Events         []SpanEvent       `json:"events,omitempty"`
	Links          []SpanLink        `json:"links,omitempty"`
}

// IsRoot reports whether this span has no parent, making it the root of a trace.
func (s *Span) IsRoot() bool {
	return s.ParentSpanID == ""
}

// HasError reports whether the span status indicates an error.
func (s *Span) HasError() bool {
	return s.Status == SpanStatusError
}

// Trace is a collection of spans that share the same trace ID.
type Trace struct {
	TraceID   string  `json:"trace_id"`
	Spans     []*Span `json:"spans"`
	RootSpan  *Span   `json:"root_span,omitempty"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	DurationMs int64   `json:"duration_ms"`
	HasError   bool    `json:"has_error"`
	SpanCount  int     `json:"span_count"`
	Services   []string `json:"services"`
}

// TraceSummary provides a lightweight overview of a trace for list views.
type TraceSummary struct {
	TraceID       string    `json:"trace_id"`
	RootService   string    `json:"root_service"`
	RootOperation string    `json:"root_operation"`
	StartTime     time.Time `json:"start_time"`
	DurationMs    int64     `json:"duration_ms"`
	SpanCount     int       `json:"span_count"`
	ErrorCount    int       `json:"error_count"`
	ServiceCount  int       `json:"service_count"`
	HasError      bool      `json:"has_error"`
}
