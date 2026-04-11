package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/model"
	"github.com/nicedavid98/observability-platform/internal/query"
)

// TraceHandler serves trace and span API endpoints.
type TraceHandler struct {
	engine *query.TraceQueryEngine
	logger *zap.Logger
}

// NewTraceHandler creates a TraceHandler backed by the given query engine.
func NewTraceHandler(engine *query.TraceQueryEngine, logger *zap.Logger) *TraceHandler {
	return &TraceHandler{engine: engine, logger: logger}
}

// SearchTraces handles GET /api/v1/traces.
func (h *TraceHandler) SearchTraces(w http.ResponseWriter, r *http.Request) {
	params, err := parseTraceSearchParams(r)
	if err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	summaries, err := h.engine.SearchTraces(r.Context(), params)
	if err != nil {
		h.logger.Error("search traces", zap.Error(err))
		jsonError(w, "failed to search traces", http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]interface{}{
		"traces": summaries,
		"total":  len(summaries),
	})
}

// GetTrace handles GET /api/v1/traces/{traceId}.
func (h *TraceHandler) GetTrace(w http.ResponseWriter, r *http.Request) {
	traceID := chi.URLParam(r, "traceId")
	if traceID == "" {
		jsonError(w, "traceId is required", http.StatusBadRequest)
		return
	}

	trace, err := h.engine.GetTrace(r.Context(), traceID)
	if err != nil {
		h.logger.Error("get trace", zap.String("trace_id", traceID), zap.Error(err))
		jsonError(w, "trace not found", http.StatusNotFound)
		return
	}

	writeJSON(w, trace)
}

// GetSpans handles GET /api/v1/traces/{traceId}/spans.
func (h *TraceHandler) GetSpans(w http.ResponseWriter, r *http.Request) {
	traceID := chi.URLParam(r, "traceId")
	if traceID == "" {
		jsonError(w, "traceId is required", http.StatusBadRequest)
		return
	}

	spans, err := h.engine.GetSpans(r.Context(), traceID)
	if err != nil {
		h.logger.Error("get spans", zap.String("trace_id", traceID), zap.Error(err))
		jsonError(w, "spans not found", http.StatusNotFound)
		return
	}

	writeJSON(w, map[string]interface{}{
		"spans": spans,
		"total": len(spans),
	})
}

func parseTraceSearchParams(r *http.Request) (query.TraceSearchParams, error) {
	q := r.URL.Query()
	params := query.TraceSearchParams{
		ServiceName:   q.Get("service"),
		OperationName: q.Get("operation"),
	}

	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return params, errorf("invalid limit: %s", v)
		}
		params.Limit = n
	}

	if v := q.Get("min_duration_ms"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return params, errorf("invalid min_duration_ms: %s", v)
		}
		params.MinDurationMs = n
	}

	if v := q.Get("max_duration_ms"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return params, errorf("invalid max_duration_ms: %s", v)
		}
		params.MaxDurationMs = n
	}

	if v := q.Get("status"); v != "" {
		switch v {
		case "ok":
			s := model.SpanStatusOK
			params.Status = &s
		case "error":
			s := model.SpanStatusError
			params.Status = &s
		default:
			return params, errorf("invalid status %q: must be ok or error", v)
		}
	}

	var err error
	params.StartTime, err = parseTimeParam(q.Get("start"), time.Time{})
	if err != nil {
		return params, errorf("invalid start: %s", err)
	}
	params.EndTime, err = parseTimeParam(q.Get("end"), time.Time{})
	if err != nil {
		return params, errorf("invalid end: %s", err)
	}

	return params, nil
}

func parseTimeParam(s string, def time.Time) (time.Time, error) {
	if s == "" {
		return def, nil
	}
	// Try Unix timestamp first.
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(n, 0), nil
	}
	return time.Parse(time.RFC3339, s)
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Headers already sent; nothing useful to do.
		return
	}
}

func errorf(format string, args ...interface{}) error {
	return fmt.Errorf(format, args...) //nolint:goerr113
}
