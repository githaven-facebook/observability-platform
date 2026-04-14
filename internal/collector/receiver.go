// Package collector implements the OpenTelemetry collector pipeline.
package collector

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nicedavid98/observability-platform/internal/config"
	"github.com/nicedavid98/observability-platform/internal/model"
)

// SignalReceiver defines the interface for processing incoming telemetry signals.
type SignalReceiver interface {
	ReceiveTraces(ctx context.Context, traces []*model.Span) error
	ReceiveLogs(ctx context.Context, logs []*model.LogRecord) error
	ReceiveMetrics(ctx context.Context, metrics []*model.MetricDataPoint) error
}

// Receiver accepts OTLP telemetry over gRPC and HTTP.
type Receiver struct {
	cfg        *config.CollectorConfig
	logger     *zap.Logger
	pipeline   SignalReceiver
	grpcServer *grpc.Server
	httpServer *http.Server
}

// NewReceiver creates a new OTLP receiver wired to the given pipeline.
func NewReceiver(cfg *config.CollectorConfig, pipeline SignalReceiver, logger *zap.Logger) *Receiver {
	return &Receiver{
		cfg:      cfg,
		logger:   logger,
		pipeline: pipeline,
	}
}

// Start launches both the gRPC and HTTP listeners.
func (r *Receiver) Start(ctx context.Context) error {
	if err := r.startGRPC(ctx); err != nil {
		return fmt.Errorf("start gRPC receiver: %w", err)
	}
	if err := r.startHTTP(ctx); err != nil {
		return fmt.Errorf("start HTTP receiver: %w", err)
	}
	return nil
}

func (r *Receiver) startGRPC(ctx context.Context) error {
	addr := fmt.Sprintf("%s:%d", r.cfg.OTLP.BindAddr, r.cfg.OTLP.GRPCPort)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	r.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(64<<20),
		grpc.ChainUnaryInterceptor(grpcLoggingInterceptor(r.logger)),
	)

	go func() {
		r.logger.Info("gRPC OTLP receiver listening", zap.String("addr", addr))
		if err := r.grpcServer.Serve(lis); err != nil {
			r.logger.Error("gRPC server error", zap.Error(err))
		}
	}()

	go func() {
		<-ctx.Done()
		r.grpcServer.GracefulStop()
	}()

	return nil
}

func (r *Receiver) startHTTP(_ context.Context) error {
	addr := fmt.Sprintf("%s:%d", r.cfg.OTLP.BindAddr, r.cfg.OTLP.HTTPPort)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/traces", r.handleHTTPTraces)
	mux.HandleFunc("/v1/logs", r.handleHTTPLogs)
	mux.HandleFunc("/v1/metrics", r.handleHTTPMetrics)

	r.httpServer = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		r.logger.Info("HTTP OTLP receiver listening", zap.String("addr", addr))
		if err := r.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			r.logger.Error("HTTP server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop gracefully shuts down both listeners.
func (r *Receiver) Stop(ctx context.Context) error {
	if r.httpServer != nil {
		if err := r.httpServer.Shutdown(ctx); err != nil {
			r.logger.Warn("HTTP server shutdown error", zap.Error(err))
		}
	}
	if r.grpcServer != nil {
		r.grpcServer.GracefulStop()
	}
	return nil
}

func (r *Receiver) handleHTTPTraces(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	unmarshal := &ptrace.ProtoUnmarshaler{}
	data := make([]byte, req.ContentLength)
	if _, err := req.Body.Read(data); err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	td, err := unmarshal.UnmarshalTraces(data)
	if err != nil {
		http.Error(w, "unmarshal traces", http.StatusBadRequest)
		return
	}
	spans := convertPTraces(td)
	if err := r.pipeline.ReceiveTraces(req.Context(), spans); err != nil {
		r.logger.Error("pipeline receive traces", zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (r *Receiver) handleHTTPLogs(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	unmarshal := &plog.ProtoUnmarshaler{}
	data := make([]byte, req.ContentLength)
	if _, err := req.Body.Read(data); err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	ld, err := unmarshal.UnmarshalLogs(data)
	if err != nil {
		http.Error(w, "unmarshal logs", http.StatusBadRequest)
		return
	}
	logs := convertPLogs(ld)
	if err := r.pipeline.ReceiveLogs(req.Context(), logs); err != nil {
		r.logger.Error("pipeline receive logs", zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (r *Receiver) handleHTTPMetrics(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	unmarshal := &pmetric.ProtoUnmarshaler{}
	data := make([]byte, req.ContentLength)
	if _, err := req.Body.Read(data); err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	md, err := unmarshal.UnmarshalMetrics(data)
	if err != nil {
		http.Error(w, "unmarshal metrics", http.StatusBadRequest)
		return
	}
	metrics := convertPMetrics(md)
	if err := r.pipeline.ReceiveMetrics(req.Context(), metrics); err != nil {
		r.logger.Error("pipeline receive metrics", zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// convertPTraces converts OTel pdata traces to internal model spans.
func convertPTraces(td ptrace.Traces) []*model.Span {
	var spans []*model.Span
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		resourceAttrs := extractAttributes(rs.Resource().Attributes())
		serviceName, _ := rs.Resource().Attributes().Get("service.name")

		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				ps := ss.Spans().At(k)
				span := &model.Span{
					TraceID:       ps.TraceID().String(),
					SpanID:        ps.SpanID().String(),
					ParentSpanID:  ps.ParentSpanID().String(),
					ServiceName:   serviceName.Str(),
					OperationName: ps.Name(),
					StartTime:     ps.StartTimestamp().AsTime(),
					EndTime:       ps.EndTimestamp().AsTime(),
					DurationMs:    ps.EndTimestamp().AsTime().Sub(ps.StartTimestamp().AsTime()).Milliseconds(),
					Status:        model.SpanStatus(ps.Status().Code()),
					StatusMessage: ps.Status().Message(),
					Kind:          model.SpanKind(ps.Kind()),
					Attributes:    extractAttributes(ps.Attributes()),
					ResourceAttrs: resourceAttrs,
				}
				spans = append(spans, span)
			}
		}
	}
	return spans
}

// convertPLogs converts OTel pdata logs to internal model log records.
func convertPLogs(ld plog.Logs) []*model.LogRecord {
	var records []*model.LogRecord
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		resourceAttrs := extractAttributes(rl.Resource().Attributes())
		serviceName, _ := rl.Resource().Attributes().Get("service.name")

		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				pl := sl.LogRecords().At(k)
				rec := &model.LogRecord{
					Timestamp:          pl.Timestamp().AsTime(),
					ObservedTimestamp:  pl.ObservedTimestamp().AsTime(),
					Severity:           model.Severity(pl.SeverityNumber()),
					SeverityText:       pl.SeverityText(),
					Body:               pl.Body().AsString(),
					ServiceName:        serviceName.Str(),
					TraceID:            pl.TraceID().String(),
					SpanID:             pl.SpanID().String(),
					TraceFlags:         uint32(pl.Flags()),
					ResourceAttributes: resourceAttrs,
					LogAttributes:      extractAttributes(pl.Attributes()),
				}
				records = append(records, rec)
			}
		}
	}
	return records
}

// convertPMetrics converts OTel pdata metrics to internal model data points.
func convertPMetrics(md pmetric.Metrics) []*model.MetricDataPoint {
	var points []*model.MetricDataPoint
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		serviceName, _ := rm.Resource().Attributes().Get("service.name")

		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			for k := 0; k < sm.Metrics().Len(); k++ {
				pm := sm.Metrics().At(k)
				pts := extractMetricDataPoints(pm, serviceName.Str())
				points = append(points, pts...)
			}
		}
	}
	return points
}

func extractMetricDataPoints(m pmetric.Metric, serviceName string) []*model.MetricDataPoint {
	var points []*model.MetricDataPoint
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		for i := 0; i < m.Gauge().DataPoints().Len(); i++ {
			dp := m.Gauge().DataPoints().At(i)
			points = append(points, &model.MetricDataPoint{
				Name:        m.Name(),
				Description: m.Description(),
				Unit:        m.Unit(),
				Type:        model.MetricTypeGauge,
				Labels:      extractAttributes(dp.Attributes()),
				Value:       dp.DoubleValue(),
				Timestamp:   dp.Timestamp().AsTime(),
				ServiceName: serviceName,
			})
		}
	case pmetric.MetricTypeSum:
		for i := 0; i < m.Sum().DataPoints().Len(); i++ {
			dp := m.Sum().DataPoints().At(i)
			points = append(points, &model.MetricDataPoint{
				Name:        m.Name(),
				Description: m.Description(),
				Unit:        m.Unit(),
				Type:        model.MetricTypeCounter,
				Labels:      extractAttributes(dp.Attributes()),
				Value:       dp.DoubleValue(),
				Timestamp:   dp.Timestamp().AsTime(),
				ServiceName: serviceName,
			})
		}
	}
	return points
}

func grpcLoggingInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start)
		if err != nil {
			logger.Warn("gRPC request failed",
				zap.String("method", info.FullMethod),
				zap.Duration("duration", duration),
				zap.Error(err),
			)
			return nil, status.Errorf(codes.Internal, "internal error")
		}
		logger.Debug("gRPC request handled",
			zap.String("method", info.FullMethod),
			zap.Duration("duration", duration),
		)
		return resp, nil
	}
}
