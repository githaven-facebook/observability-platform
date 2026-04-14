package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/query"
)

// DashboardHandler serves Grafana-compatible datasource endpoints.
type DashboardHandler struct {
	metricEngine *query.MetricQueryEngine
	traceEngine  *query.TraceQueryEngine
	logger       *zap.Logger
}

// NewDashboardHandler creates a DashboardHandler.
func NewDashboardHandler(
	metricEngine *query.MetricQueryEngine,
	traceEngine *query.TraceQueryEngine,
	logger *zap.Logger,
) *DashboardHandler {
	return &DashboardHandler{
		metricEngine: metricEngine,
		traceEngine:  traceEngine,
		logger:       logger,
	}
}

// grafanaQueryRequest is the payload sent by Grafana SimpleJSON datasource.
type grafanaQueryRequest struct {
	Range struct {
		From string `json:"from"`
		To   string `json:"to"`
	} `json:"range"`
	Targets []struct {
		Target string `json:"target"`
		Type   string `json:"type"` // timeserie or table
	} `json:"targets"`
	MaxDataPoints int `json:"maxDataPoints"`
}

// grafanaTimeSeriesResponse is the response format for a Grafana time series panel.
type grafanaTimeSeriesResponse struct {
	Target     string      `json:"target"`
	Datapoints [][]float64 `json:"datapoints"` // [[value, timestamp_ms], ...]
}

// GrafanaTest handles GET /api/v1/grafana/ — Grafana uses this to verify connectivity.
func (h *DashboardHandler) GrafanaTest(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// GrafanaQuery handles POST /api/v1/grafana/query.
func (h *DashboardHandler) GrafanaQuery(w http.ResponseWriter, r *http.Request) { //nolint:gocognit // query handler requires multiple parse steps
	var req grafanaQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	startTime, err := parseGrafanaTime(req.Range.From)
	if err != nil {
		jsonError(w, "invalid range.from", http.StatusBadRequest)
		return
	}
	endTime, err := parseGrafanaTime(req.Range.To)
	if err != nil {
		jsonError(w, "invalid range.to", http.StatusBadRequest)
		return
	}

	var responses []grafanaTimeSeriesResponse
	for _, target := range req.Targets {
		result, err := h.metricEngine.Query(r.Context(), query.QueryParams{
			Query:     target.Target,
			StartTime: startTime,
			EndTime:   endTime,
		})
		if err != nil {
			h.logger.Warn("grafana query failed", zap.String("target", target.Target), zap.Error(err))
			continue
		}

		for _, item := range result.Result {
			ts := grafanaTimeSeriesResponse{Target: target.Target}
			for _, v := range item.Values {
				if len(v) < 2 {
					continue
				}
				tStr, _ := v[0].(string)
				valStr, _ := v[1].(string)
				var tSec int64
				var val float64
				if _, err := fmt.Sscanf(tStr, "%d", &tSec); err == nil {
					if _, err := fmt.Sscanf(valStr, "%f", &val); err == nil {
						ts.Datapoints = append(ts.Datapoints, []float64{val, float64(tSec * 1000)})
					}
				}
			}
			responses = append(responses, ts)
		}
	}

	writeJSON(w, responses)
}

// GrafanaAnnotations handles GET /api/v1/grafana/annotations.
func (h *DashboardHandler) GrafanaAnnotations(w http.ResponseWriter, r *http.Request) {
	// Return empty annotations list; extend later with alert-firing events.
	writeJSON(w, []interface{}{})
}

// GrafanaSearch handles POST /api/v1/grafana/search — returns available metrics.
func (h *DashboardHandler) GrafanaSearch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Target string `json:"target"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	series, err := h.metricEngine.ListSeries(r.Context(), req.Target, 500)
	if err != nil {
		h.logger.Error("grafana search", zap.Error(err))
		jsonError(w, "failed to list metrics", http.StatusInternalServerError)
		return
	}

	names := make([]string, 0, len(series))
	seen := make(map[string]struct{})
	for _, s := range series {
		if _, ok := seen[s.Name]; !ok {
			names = append(names, s.Name)
			seen[s.Name] = struct{}{}
		}
	}

	writeJSON(w, names)
}

func parseGrafanaTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02T15:04:05.999Z", s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("unrecognized time format: %s", s)
}
