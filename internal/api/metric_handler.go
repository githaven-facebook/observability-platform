package api

import (
	"net/http"
	"strconv"

	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/query"
)

// MetricHandler serves metric API endpoints.
type MetricHandler struct {
	engine *query.MetricQueryEngine
	logger *zap.Logger
}

// NewMetricHandler creates a MetricHandler backed by the given query engine.
func NewMetricHandler(engine *query.MetricQueryEngine, logger *zap.Logger) *MetricHandler {
	return &MetricHandler{engine: engine, logger: logger}
}

// Query handles GET /api/v1/metrics/query.
func (h *MetricHandler) Query(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	expr := q.Get("query")
	if expr == "" {
		jsonError(w, "query parameter is required", http.StatusBadRequest)
		return
	}

	params := query.QueryParams{Query: expr}
	var err error
	params.StartTime, err = parseTimeParam(q.Get("start"), params.StartTime)
	if err != nil {
		jsonError(w, "invalid start: "+err.Error(), http.StatusBadRequest)
		return
	}
	params.EndTime, err = parseTimeParam(q.Get("end"), params.EndTime)
	if err != nil {
		jsonError(w, "invalid end: "+err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.engine.Query(r.Context(), params)
	if err != nil {
		h.logger.Error("metric query", zap.String("query", expr), zap.Error(err))
		jsonError(w, "query failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	writeJSON(w, map[string]interface{}{
		"status": "success",
		"data":   result,
	})
}

// ListSeries handles GET /api/v1/metrics/series.
func (h *MetricHandler) ListSeries(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix := q.Get("match")
	limit := 1000
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}

	series, err := h.engine.ListSeries(r.Context(), prefix, limit)
	if err != nil {
		h.logger.Error("list metric series", zap.Error(err))
		jsonError(w, "failed to list series", http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]interface{}{
		"series": series,
		"total":  len(series),
	})
}

// ListLabels handles GET /api/v1/metrics/labels.
func (h *MetricHandler) ListLabels(w http.ResponseWriter, r *http.Request) {
	metricName := r.URL.Query().Get("metric")
	if metricName == "" {
		jsonError(w, "metric parameter is required", http.StatusBadRequest)
		return
	}

	labels, err := h.engine.ListLabels(r.Context(), metricName)
	if err != nil {
		h.logger.Error("list metric labels", zap.Error(err))
		jsonError(w, "failed to list labels", http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]interface{}{"labels": labels})
}

// ListLabelValues handles GET /api/v1/metrics/labels/{labelName}/values.
func (h *MetricHandler) ListLabelValues(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	metricName := q.Get("metric")
	labelName := q.Get("label")
	if metricName == "" || labelName == "" {
		jsonError(w, "metric and label parameters are required", http.StatusBadRequest)
		return
	}

	values, err := h.engine.ListLabelValues(r.Context(), metricName, labelName)
	if err != nil {
		h.logger.Error("list label values", zap.Error(err))
		jsonError(w, "failed to list label values", http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]interface{}{"values": values})
}
