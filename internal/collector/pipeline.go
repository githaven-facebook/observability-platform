package collector

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/model"
)

// Pipeline wires together receivers, processors, and exporters for a single signal type.
type Pipeline struct {
	name       string
	processors []Processor
	exporter   Exporter
	logger     *zap.Logger
}

// NewPipeline creates a named pipeline with the given processor chain and exporter.
func NewPipeline(name string, processors []Processor, exporter Exporter, logger *zap.Logger) *Pipeline {
	return &Pipeline{
		name:       name,
		processors: processors,
		exporter:   exporter,
		logger:     logger,
	}
}

// ProcessAndExportSpans runs spans through the processor chain then exports them.
func (p *Pipeline) ProcessAndExportSpans(ctx context.Context, spans []*model.Span) error {
	var err error
	for _, proc := range p.processors {
		spans, err = proc.ProcessSpans(ctx, spans)
		if err != nil {
			return fmt.Errorf("pipeline %q processor: %w", p.name, err)
		}
	}
	if len(spans) == 0 {
		return nil
	}
	if err := p.exporter.ExportSpans(ctx, spans); err != nil {
		return fmt.Errorf("pipeline %q exporter: %w", p.name, err)
	}
	p.logger.Debug("pipeline processed spans", zap.String("pipeline", p.name), zap.Int("count", len(spans)))
	return nil
}

// ProcessAndExportLogs runs logs through the processor chain then exports them.
func (p *Pipeline) ProcessAndExportLogs(ctx context.Context, logs []*model.LogRecord) error {
	var err error
	for _, proc := range p.processors {
		logs, err = proc.ProcessLogs(ctx, logs)
		if err != nil {
			return fmt.Errorf("pipeline %q processor: %w", p.name, err)
		}
	}
	if len(logs) == 0 {
		return nil
	}
	if err := p.exporter.ExportLogs(ctx, logs); err != nil {
		return fmt.Errorf("pipeline %q exporter: %w", p.name, err)
	}
	p.logger.Debug("pipeline processed logs", zap.String("pipeline", p.name), zap.Int("count", len(logs)))
	return nil
}

// ProcessAndExportMetrics runs metrics through the processor chain then exports them.
func (p *Pipeline) ProcessAndExportMetrics(ctx context.Context, metrics []*model.MetricDataPoint) error {
	var err error
	for _, proc := range p.processors {
		metrics, err = proc.ProcessMetrics(ctx, metrics)
		if err != nil {
			return fmt.Errorf("pipeline %q processor: %w", p.name, err)
		}
	}
	if len(metrics) == 0 {
		return nil
	}
	if err := p.exporter.ExportMetrics(ctx, metrics); err != nil {
		return fmt.Errorf("pipeline %q exporter: %w", p.name, err)
	}
	p.logger.Debug("pipeline processed metrics", zap.String("pipeline", p.name), zap.Int("count", len(metrics)))
	return nil
}

// CollectorPipeline is the top-level container that implements SignalReceiver
// and routes each signal type through its configured Pipeline.
type CollectorPipeline struct {
	traces  *Pipeline
	logs    *Pipeline
	metrics *Pipeline
	logger  *zap.Logger
}

// NewCollectorPipeline constructs the top-level pipeline from three signal-specific sub-pipelines.
func NewCollectorPipeline(traces, logs, metrics *Pipeline, logger *zap.Logger) *CollectorPipeline {
	return &CollectorPipeline{
		traces:  traces,
		logs:    logs,
		metrics: metrics,
		logger:  logger,
	}
}

// ReceiveTraces implements SignalReceiver for spans/traces.
func (cp *CollectorPipeline) ReceiveTraces(ctx context.Context, spans []*model.Span) error {
	if err := cp.traces.ProcessAndExportSpans(ctx, spans); err != nil {
		cp.logger.Error("traces pipeline error", zap.Error(err))
		return err
	}
	return nil
}

// ReceiveLogs implements SignalReceiver for log records.
func (cp *CollectorPipeline) ReceiveLogs(ctx context.Context, logs []*model.LogRecord) error {
	if err := cp.logs.ProcessAndExportLogs(ctx, logs); err != nil {
		cp.logger.Error("logs pipeline error", zap.Error(err))
		return err
	}
	return nil
}

// ReceiveMetrics implements SignalReceiver for metric data points.
func (cp *CollectorPipeline) ReceiveMetrics(ctx context.Context, metrics []*model.MetricDataPoint) error {
	if err := cp.metrics.ProcessAndExportMetrics(ctx, metrics); err != nil {
		cp.logger.Error("metrics pipeline error", zap.Error(err))
		return err
	}
	return nil
}
