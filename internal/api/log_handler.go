package api

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/model"
	"github.com/nicedavid98/observability-platform/internal/query"
)

// LogHandler serves log API endpoints.
type LogHandler struct {
	engine *query.LogQueryEngine
	logger *zap.Logger
}

// NewLogHandler creates a LogHandler backed by the given query engine.
func NewLogHandler(engine *query.LogQueryEngine, logger *zap.Logger) *LogHandler {
	return &LogHandler{engine: engine, logger: logger}
}

// SearchLogs handles GET /api/v1/logs.
func (h *LogHandler) SearchLogs(w http.ResponseWriter, r *http.Request) {
	params, err := parseLogSearchParams(r)
	if err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	records, err := h.engine.SearchLogs(r.Context(), params)
	if err != nil {
		h.logger.Error("search logs", zap.Error(err))
		jsonError(w, "failed to search logs", http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]interface{}{
		"logs":  records,
		"total": len(records),
	})
}

// GetLogContext handles GET /api/v1/logs/context/{traceId}.
func (h *LogHandler) GetLogContext(w http.ResponseWriter, r *http.Request) {
	traceID := chi.URLParam(r, "traceId")
	if traceID == "" {
		jsonError(w, "traceId is required", http.StatusBadRequest)
		return
	}

	records, err := h.engine.GetLogContext(r.Context(), traceID)
	if err != nil {
		h.logger.Error("get log context", zap.String("trace_id", traceID), zap.Error(err))
		jsonError(w, "failed to get log context", http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]interface{}{
		"logs":     records,
		"total":    len(records),
		"trace_id": traceID,
	})
}

func parseLogSearchParams(r *http.Request) (query.LogSearchParams, error) {
	q := r.URL.Query()
	params := query.LogSearchParams{
		ServiceName: q.Get("service"),
		Keyword:     q.Get("q"),
		TraceID:     q.Get("trace_id"),
		SpanID:      q.Get("span_id"),
	}

	if v := q.Get("severity"); v != "" {
		params.MinSeverity = model.ParseSeverity(v)
	}

	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return params, errorf("invalid limit: %s", v)
		}
		params.Limit = n
	}

	var err error
	params.StartTime, err = parseTimeParam(q.Get("start"), params.StartTime)
	if err != nil {
		return params, errorf("invalid start: %s", err)
	}
	params.EndTime, err = parseTimeParam(q.Get("end"), params.EndTime)
	if err != nil {
		return params, errorf("invalid end: %s", err)
	}

	return params, nil
}
