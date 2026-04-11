// Package api implements the HTTP query API server.
package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/config"
)

// Server is the HTTP API server for the query service.
type Server struct {
	cfg        *config.QueryConfig
	router     chi.Router
	httpServer *http.Server
	logger     *zap.Logger
}

// Handlers bundles all route handlers for dependency injection.
type Handlers struct {
	Trace      *TraceHandler
	Log        *LogHandler
	Metric     *MetricHandler
	ServiceMap *ServiceMapHandler
	Dashboard  *DashboardHandler
}

// NewServer constructs the HTTP server with all routes registered.
func NewServer(cfg *config.QueryConfig, h *Handlers, logger *zap.Logger) *Server {
	s := &Server{cfg: cfg, logger: logger}
	s.router = s.buildRouter(h)
	s.httpServer = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.BindAddr, cfg.Port),
		Handler:      s.router,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}
	return s
}

func (s *Server) buildRouter(h *Handlers) chi.Router {
	r := chi.NewRouter()

	// Global middleware.
	r.Use(chimiddleware.RequestID)
	r.Use(chimiddleware.RealIP)
	r.Use(requestLogger(s.logger))
	r.Use(chimiddleware.Recoverer)
	r.Use(chimiddleware.Timeout(s.cfg.MaxQueryDuration))
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-Request-ID"},
		ExposedHeaders:   []string{"X-Request-ID"},
		AllowCredentials: false,
		MaxAge:           300,
	}))

	// Health / readiness.
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// API v1.
	r.Route("/api/v1", func(r chi.Router) {
		r.Use(authMiddleware(s.logger))

		// Traces.
		r.Get("/traces", h.Trace.SearchTraces)
		r.Get("/traces/{traceId}", h.Trace.GetTrace)
		r.Get("/traces/{traceId}/spans", h.Trace.GetSpans)

		// Logs.
		r.Get("/logs", h.Log.SearchLogs)
		r.Get("/logs/context/{traceId}", h.Log.GetLogContext)

		// Metrics.
		r.Get("/metrics/query", h.Metric.Query)
		r.Get("/metrics/series", h.Metric.ListSeries)
		r.Get("/metrics/labels", h.Metric.ListLabels)
		r.Get("/metrics/labels/{labelName}/values", h.Metric.ListLabelValues)

		// Service map.
		r.Get("/service-map", h.ServiceMap.GetServiceMap)

		// Grafana datasource.
		r.Route("/grafana", func(r chi.Router) {
			r.Get("/", h.Dashboard.GrafanaTest)
			r.Post("/query", h.Dashboard.GrafanaQuery)
			r.Get("/annotations", h.Dashboard.GrafanaAnnotations)
			r.Post("/search", h.Dashboard.GrafanaSearch)
		})
	})

	return r
}

// Start begins accepting requests.
func (s *Server) Start() error {
	s.logger.Info("query API server starting", zap.String("addr", s.httpServer.Addr))
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("listen and serve: %w", err)
	}
	return nil
}

// Shutdown gracefully drains in-flight requests.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("query API server shutting down")
	return s.httpServer.Shutdown(ctx)
}

// jsonError writes a JSON error response with the given HTTP status code.
func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	fmt.Fprintf(w, `{"error":%q}`, msg)
}

// requestLogger returns a chi-compatible middleware that logs each request with zap.
func requestLogger(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ww := chimiddleware.NewWrapResponseWriter(w, r.ProtoMajor)
			start := time.Now()
			next.ServeHTTP(ww, r)
			logger.Info("http request",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Int("status", ww.Status()),
				zap.Duration("duration", time.Since(start)),
				zap.String("request_id", chimiddleware.GetReqID(r.Context())),
			)
		})
	}
}
