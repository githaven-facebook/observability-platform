package api

import (
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/query"
)

// ServiceMapHandler serves the service dependency map endpoint.
type ServiceMapHandler struct {
	engine *query.ServiceMapEngine
	logger *zap.Logger
}

// NewServiceMapHandler creates a ServiceMapHandler backed by the given engine.
func NewServiceMapHandler(engine *query.ServiceMapEngine, logger *zap.Logger) *ServiceMapHandler {
	return &ServiceMapHandler{engine: engine, logger: logger}
}

// GetServiceMap handles GET /api/v1/service-map.
func (h *ServiceMapHandler) GetServiceMap(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	endTime := time.Now()
	startTime := endTime.Add(-1 * time.Hour)

	var err error
	if v := q.Get("start"); v != "" {
		startTime, err = parseTimeParam(v, startTime)
		if err != nil {
			jsonError(w, "invalid start: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	if v := q.Get("end"); v != "" {
		endTime, err = parseTimeParam(v, endTime)
		if err != nil {
			jsonError(w, "invalid end: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	smap, err := h.engine.BuildServiceMap(r.Context(), startTime, endTime)
	if err != nil {
		h.logger.Error("build service map", zap.Error(err))
		jsonError(w, "failed to build service map", http.StatusInternalServerError)
		return
	}

	writeJSON(w, smap)
}
