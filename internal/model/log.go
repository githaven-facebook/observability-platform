package model

import "time"

// Severity represents the log level of a log record.
type Severity int32

const (
	SeverityUnspecified Severity = 0
	SeverityTrace       Severity = 1
	SeverityTrace2      Severity = 2
	SeverityTrace3      Severity = 3
	SeverityTrace4      Severity = 4
	SeverityDebug       Severity = 5
	SeverityDebug2      Severity = 6
	SeverityDebug3      Severity = 7
	SeverityDebug4      Severity = 8
	SeverityInfo        Severity = 9
	SeverityInfo2       Severity = 10
	SeverityInfo3       Severity = 11
	SeverityInfo4       Severity = 12
	SeverityWarn        Severity = 13
	SeverityWarn2       Severity = 14
	SeverityWarn3       Severity = 15
	SeverityWarn4       Severity = 16
	SeverityError       Severity = 17
	SeverityError2      Severity = 18
	SeverityError3      Severity = 19
	SeverityError4      Severity = 20
	SeverityFatal       Severity = 21
	SeverityFatal2      Severity = 22
	SeverityFatal3      Severity = 23
	SeverityFatal4      Severity = 24
)

// String returns the canonical name for a severity level.
func (s Severity) String() string {
	switch {
	case s >= SeverityFatal:
		return "FATAL"
	case s >= SeverityError:
		return "ERROR"
	case s >= SeverityWarn:
		return "WARN"
	case s >= SeverityInfo:
		return "INFO"
	case s >= SeverityDebug:
		return "DEBUG"
	case s >= SeverityTrace:
		return "TRACE"
	default:
		return "UNSPECIFIED"
	}
}

// ParseSeverity converts a string level name to the base Severity constant.
func ParseSeverity(level string) Severity {
	switch level {
	case "FATAL", "fatal":
		return SeverityFatal
	case "ERROR", "error":
		return SeverityError
	case "WARN", "warn", "WARNING", "warning":
		return SeverityWarn
	case "INFO", "info":
		return SeverityInfo
	case "DEBUG", "debug":
		return SeverityDebug
	case "TRACE", "trace":
		return SeverityTrace
	default:
		return SeverityUnspecified
	}
}

// LogRecord represents a single structured log entry.
type LogRecord struct {
	Timestamp          time.Time         `json:"timestamp"`
	ObservedTimestamp  time.Time         `json:"observed_timestamp"`
	Severity           Severity          `json:"severity"`
	SeverityText       string            `json:"severity_text,omitempty"`
	Body               string            `json:"body"`
	ServiceName        string            `json:"service_name"`
	TraceID            string            `json:"trace_id,omitempty"`
	SpanID             string            `json:"span_id,omitempty"`
	TraceFlags         uint32            `json:"trace_flags,omitempty"`
	ResourceAttributes map[string]string `json:"resource_attributes,omitempty"`
	LogAttributes      map[string]string `json:"log_attributes,omitempty"`
	// Extracted structured fields from unstructured log body.
	StructuredFields map[string]string `json:"structured_fields,omitempty"`
}

// IsError reports whether this log record represents an error or worse.
func (l *LogRecord) IsError() bool {
	return l.Severity >= SeverityError
}

// HasTrace reports whether this log record is correlated with a trace.
func (l *LogRecord) HasTrace() bool {
	return l.TraceID != ""
}
