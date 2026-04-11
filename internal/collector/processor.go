package collector

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/config"
	"github.com/nicedavid98/observability-platform/internal/model"
)

// Processor defines the interface for a single stage in the telemetry pipeline.
type Processor interface {
	ProcessSpans(ctx context.Context, spans []*model.Span) ([]*model.Span, error)
	ProcessLogs(ctx context.Context, logs []*model.LogRecord) ([]*model.LogRecord, error)
	ProcessMetrics(ctx context.Context, metrics []*model.MetricDataPoint) ([]*model.MetricDataPoint, error)
}

// BatchProcessor accumulates telemetry until a size or time threshold is met.
type BatchProcessor struct {
	cfg    *config.CollectorConfig
	logger *zap.Logger
	mu     sync.Mutex

	spanBatch   []*model.Span
	logBatch    []*model.LogRecord
	metricBatch []*model.MetricDataPoint

	flushFn func(spans []*model.Span, logs []*model.LogRecord, metrics []*model.MetricDataPoint)
	ticker  *time.Ticker
	done    chan struct{}
}

// NewBatchProcessor creates a BatchProcessor that flushes when the batch is full or a timeout fires.
func NewBatchProcessor(cfg *config.CollectorConfig, logger *zap.Logger,
	flushFn func([]*model.Span, []*model.LogRecord, []*model.MetricDataPoint)) *BatchProcessor {
	bp := &BatchProcessor{
		cfg:     cfg,
		logger:  logger,
		flushFn: flushFn,
		ticker:  time.NewTicker(cfg.ExportTimeout),
		done:    make(chan struct{}),
	}
	go bp.runFlushLoop()
	return bp
}

func (bp *BatchProcessor) runFlushLoop() {
	for {
		select {
		case <-bp.ticker.C:
			bp.flush()
		case <-bp.done:
			bp.ticker.Stop()
			return
		}
	}
}

func (bp *BatchProcessor) flush() {
	bp.mu.Lock()
	spans := bp.spanBatch
	logs := bp.logBatch
	metrics := bp.metricBatch
	bp.spanBatch = nil
	bp.logBatch = nil
	bp.metricBatch = nil
	bp.mu.Unlock()

	if len(spans) > 0 || len(logs) > 0 || len(metrics) > 0 {
		bp.flushFn(spans, logs, metrics)
	}
}

// Stop shuts down the background flush loop.
func (bp *BatchProcessor) Stop() {
	close(bp.done)
}

// ProcessSpans appends spans to the batch and flushes if the batch is full.
func (bp *BatchProcessor) ProcessSpans(_ context.Context, spans []*model.Span) ([]*model.Span, error) {
	bp.mu.Lock()
	bp.spanBatch = append(bp.spanBatch, spans...)
	shouldFlush := len(bp.spanBatch) >= bp.cfg.BatchSize
	bp.mu.Unlock()

	if shouldFlush {
		bp.flush()
	}
	return spans, nil
}

// ProcessLogs appends log records to the batch and flushes if the batch is full.
func (bp *BatchProcessor) ProcessLogs(_ context.Context, logs []*model.LogRecord) ([]*model.LogRecord, error) {
	bp.mu.Lock()
	bp.logBatch = append(bp.logBatch, logs...)
	shouldFlush := len(bp.logBatch) >= bp.cfg.BatchSize
	bp.mu.Unlock()

	if shouldFlush {
		bp.flush()
	}
	return logs, nil
}

// ProcessMetrics appends metric data points to the batch and flushes if the batch is full.
func (bp *BatchProcessor) ProcessMetrics(_ context.Context, metrics []*model.MetricDataPoint) ([]*model.MetricDataPoint, error) {
	bp.mu.Lock()
	bp.metricBatch = append(bp.metricBatch, metrics...)
	shouldFlush := len(bp.metricBatch) >= bp.cfg.BatchSize
	bp.mu.Unlock()

	if shouldFlush {
		bp.flush()
	}
	return metrics, nil
}

// AttributeProcessorRule defines a single attribute transformation.
type AttributeProcessorRule struct {
	Action  string // add, remove, rename, hash
	Key     string
	NewKey  string // for rename
	Value   string // for add
	Pattern string // for regex match
	compiled *regexp.Regexp
}

// AttributeProcessor adds, removes, or renames attributes on spans and logs.
type AttributeProcessor struct {
	rules  []AttributeProcessorRule
	logger *zap.Logger
}

// NewAttributeProcessor creates an AttributeProcessor from the given rules.
func NewAttributeProcessor(rules []AttributeProcessorRule, logger *zap.Logger) *AttributeProcessor {
	for i, r := range rules {
		if r.Pattern != "" {
			rules[i].compiled = regexp.MustCompile(r.Pattern)
		}
	}
	return &AttributeProcessor{rules: rules, logger: logger}
}

// ProcessSpans applies attribute rules to each span.
func (ap *AttributeProcessor) ProcessSpans(_ context.Context, spans []*model.Span) ([]*model.Span, error) {
	for _, span := range spans {
		ap.applyRules(span.Attributes)
	}
	return spans, nil
}

// ProcessLogs applies attribute rules to each log record.
func (ap *AttributeProcessor) ProcessLogs(_ context.Context, logs []*model.LogRecord) ([]*model.LogRecord, error) {
	for _, log := range logs {
		ap.applyRules(log.LogAttributes)
	}
	return logs, nil
}

// ProcessMetrics passes metrics through unchanged.
func (ap *AttributeProcessor) ProcessMetrics(_ context.Context, metrics []*model.MetricDataPoint) ([]*model.MetricDataPoint, error) {
	return metrics, nil
}

func (ap *AttributeProcessor) applyRules(attrs map[string]string) {
	if attrs == nil {
		return
	}
	for _, rule := range ap.rules {
		switch rule.Action {
		case "add":
			if _, exists := attrs[rule.Key]; !exists {
				attrs[rule.Key] = rule.Value
			}
		case "remove":
			delete(attrs, rule.Key)
		case "rename":
			if v, ok := attrs[rule.Key]; ok {
				attrs[rule.NewKey] = v
				delete(attrs, rule.Key)
			}
		case "hash":
			if v, ok := attrs[rule.Key]; ok {
				h := fnv.New64a()
				_, _ = h.Write([]byte(v))
				attrs[rule.Key] = fmt.Sprintf("%x", h.Sum64())
			}
		}
	}
}

// SamplingProcessor applies head-based and tail-based sampling to spans.
type SamplingProcessor struct {
	cfg    *config.SamplingConfig
	logger *zap.Logger
}

// NewSamplingProcessor creates a SamplingProcessor using the given sampling config.
func NewSamplingProcessor(cfg *config.SamplingConfig, logger *zap.Logger) *SamplingProcessor {
	return &SamplingProcessor{cfg: cfg, logger: logger}
}

// ProcessSpans applies sampling rules and returns only sampled spans.
func (sp *SamplingProcessor) ProcessSpans(_ context.Context, spans []*model.Span) ([]*model.Span, error) {
	sampled := spans[:0]
	for _, s := range spans {
		if sp.shouldSample(s) {
			sampled = append(sampled, s)
		}
	}
	return sampled, nil
}

func (sp *SamplingProcessor) shouldSample(s *model.Span) bool {
	// Always sample errors.
	if s.HasError() {
		return true
	}
	// Use per-service rate if configured.
	if rate, ok := sp.cfg.ServiceRates[s.ServiceName]; ok {
		return rand.Float64() < rate
	}
	// Fall back to global head sample rate.
	return rand.Float64() < sp.cfg.HeadSampleRate
}

// ProcessLogs passes logs through unchanged (sampling is span-level only).
func (sp *SamplingProcessor) ProcessLogs(_ context.Context, logs []*model.LogRecord) ([]*model.LogRecord, error) {
	return logs, nil
}

// ProcessMetrics passes metrics through unchanged.
func (sp *SamplingProcessor) ProcessMetrics(_ context.Context, metrics []*model.MetricDataPoint) ([]*model.MetricDataPoint, error) {
	return metrics, nil
}

// ResourceProcessor normalizes service names and enriches resource attributes.
type ResourceProcessor struct {
	logger *zap.Logger
}

// NewResourceProcessor creates a ResourceProcessor.
func NewResourceProcessor(logger *zap.Logger) *ResourceProcessor {
	return &ResourceProcessor{logger: logger}
}

// ProcessSpans normalizes the service name on each span.
func (rp *ResourceProcessor) ProcessSpans(_ context.Context, spans []*model.Span) ([]*model.Span, error) {
	for _, s := range spans {
		s.ServiceName = normalizeServiceName(s.ServiceName)
	}
	return spans, nil
}

// ProcessLogs normalizes the service name on each log record.
func (rp *ResourceProcessor) ProcessLogs(_ context.Context, logs []*model.LogRecord) ([]*model.LogRecord, error) {
	for _, l := range logs {
		l.ServiceName = normalizeServiceName(l.ServiceName)
	}
	return logs, nil
}

// ProcessMetrics normalizes the service name on each metric data point.
func (rp *ResourceProcessor) ProcessMetrics(_ context.Context, metrics []*model.MetricDataPoint) ([]*model.MetricDataPoint, error) {
	for _, m := range metrics {
		m.ServiceName = normalizeServiceName(m.ServiceName)
	}
	return metrics, nil
}

func normalizeServiceName(name string) string {
	name = strings.ToLower(name)
	name = strings.ReplaceAll(name, "/", "-")
	name = strings.ReplaceAll(name, " ", "-")
	name = strings.ReplaceAll(name, "_", "-")
	return name
}
