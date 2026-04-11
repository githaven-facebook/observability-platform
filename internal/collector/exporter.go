package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/model"
	"github.com/nicedavid98/observability-platform/internal/storage/clickhouse"
)

// Exporter defines the interface for writing telemetry to a backend.
type Exporter interface {
	ExportSpans(ctx context.Context, spans []*model.Span) error
	ExportLogs(ctx context.Context, logs []*model.LogRecord) error
	ExportMetrics(ctx context.Context, metrics []*model.MetricDataPoint) error
}

// ClickHouseExporter writes telemetry to ClickHouse via the storage layer.
type ClickHouseExporter struct {
	traceStore  *clickhouse.TraceStore
	logStore    *clickhouse.LogStore
	metricStore *clickhouse.MetricStore
	logger      *zap.Logger
}

// NewClickHouseExporter creates an exporter backed by ClickHouse storage.
func NewClickHouseExporter(
	traceStore *clickhouse.TraceStore,
	logStore *clickhouse.LogStore,
	metricStore *clickhouse.MetricStore,
	logger *zap.Logger,
) *ClickHouseExporter {
	return &ClickHouseExporter{
		traceStore:  traceStore,
		logStore:    logStore,
		metricStore: metricStore,
		logger:      logger,
	}
}

// ExportSpans writes a batch of spans to ClickHouse.
func (e *ClickHouseExporter) ExportSpans(ctx context.Context, spans []*model.Span) error {
	if len(spans) == 0 {
		return nil
	}
	if err := e.traceStore.InsertSpans(ctx, spans); err != nil {
		return fmt.Errorf("insert spans: %w", err)
	}
	e.logger.Debug("exported spans to ClickHouse", zap.Int("count", len(spans)))
	return nil
}

// ExportLogs writes a batch of log records to ClickHouse.
func (e *ClickHouseExporter) ExportLogs(ctx context.Context, logs []*model.LogRecord) error {
	if len(logs) == 0 {
		return nil
	}
	if err := e.logStore.InsertLogs(ctx, logs); err != nil {
		return fmt.Errorf("insert logs: %w", err)
	}
	e.logger.Debug("exported logs to ClickHouse", zap.Int("count", len(logs)))
	return nil
}

// ExportMetrics writes a batch of metric data points to ClickHouse.
func (e *ClickHouseExporter) ExportMetrics(ctx context.Context, metrics []*model.MetricDataPoint) error {
	if len(metrics) == 0 {
		return nil
	}
	if err := e.metricStore.InsertDataPoints(ctx, metrics); err != nil {
		return fmt.Errorf("insert metrics: %w", err)
	}
	e.logger.Debug("exported metrics to ClickHouse", zap.Int("count", len(metrics)))
	return nil
}

// KafkaExporter publishes telemetry as JSON to Kafka topics for async downstream processing.
type KafkaExporter struct {
	producer kafkaProducer
	logger   *zap.Logger

	tracesTopic  string
	logsTopic    string
	metricsTopic string
}

// kafkaProducer abstracts Kafka message production for testability.
type kafkaProducer interface {
	WriteMessages(ctx context.Context, topic string, key, value []byte) error
	Close() error
}

// NewKafkaExporter creates a KafkaExporter using the given producer.
func NewKafkaExporter(producer kafkaProducer, tracesTopic, logsTopic, metricsTopic string, logger *zap.Logger) *KafkaExporter {
	return &KafkaExporter{
		producer:     producer,
		logger:       logger,
		tracesTopic:  tracesTopic,
		logsTopic:    logsTopic,
		metricsTopic: metricsTopic,
	}
}

// ExportSpans publishes spans to the traces Kafka topic.
func (e *KafkaExporter) ExportSpans(ctx context.Context, spans []*model.Span) error {
	for _, s := range spans {
		data, err := json.Marshal(s)
		if err != nil {
			return fmt.Errorf("marshal span: %w", err)
		}
		if err := e.producer.WriteMessages(ctx, e.tracesTopic, []byte(s.TraceID), data); err != nil {
			return fmt.Errorf("write span to kafka: %w", err)
		}
	}
	return nil
}

// ExportLogs publishes log records to the logs Kafka topic.
func (e *KafkaExporter) ExportLogs(ctx context.Context, logs []*model.LogRecord) error {
	for _, l := range logs {
		data, err := json.Marshal(l)
		if err != nil {
			return fmt.Errorf("marshal log: %w", err)
		}
		key := []byte(l.ServiceName)
		if err := e.producer.WriteMessages(ctx, e.logsTopic, key, data); err != nil {
			return fmt.Errorf("write log to kafka: %w", err)
		}
	}
	return nil
}

// ExportMetrics publishes metric data points to the metrics Kafka topic.
func (e *KafkaExporter) ExportMetrics(ctx context.Context, metrics []*model.MetricDataPoint) error {
	for _, m := range metrics {
		data, err := json.Marshal(m)
		if err != nil {
			return fmt.Errorf("marshal metric: %w", err)
		}
		key := []byte(m.Name)
		if err := e.producer.WriteMessages(ctx, e.metricsTopic, key, data); err != nil {
			return fmt.Errorf("write metric to kafka: %w", err)
		}
	}
	return nil
}

// prometheusRemoteWriteRequest is the payload format for Prometheus remote write.
type prometheusRemoteWriteRequest struct {
	Timeseries []prometheusTimeSeries `json:"timeseries"`
}

type prometheusTimeSeries struct {
	Labels  []prometheusLabel  `json:"labels"`
	Samples []prometheusSample `json:"samples"`
}

type prometheusLabel struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type prometheusSample struct {
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"` // Unix milliseconds.
}

// PrometheusRemoteWriteExporter sends metrics via Prometheus remote write protocol.
type PrometheusRemoteWriteExporter struct {
	endpoint   string
	httpClient *http.Client
	logger     *zap.Logger
}

// NewPrometheusRemoteWriteExporter creates an exporter targeting the given remote write endpoint.
func NewPrometheusRemoteWriteExporter(endpoint string, logger *zap.Logger) *PrometheusRemoteWriteExporter {
	return &PrometheusRemoteWriteExporter{
		endpoint: endpoint,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger: logger,
	}
}

// ExportSpans is a no-op for this exporter.
func (e *PrometheusRemoteWriteExporter) ExportSpans(_ context.Context, _ []*model.Span) error {
	return nil
}

// ExportLogs is a no-op for this exporter.
func (e *PrometheusRemoteWriteExporter) ExportLogs(_ context.Context, _ []*model.LogRecord) error {
	return nil
}

// ExportMetrics converts data points to Prometheus remote write format and POSTs them.
func (e *PrometheusRemoteWriteExporter) ExportMetrics(ctx context.Context, metrics []*model.MetricDataPoint) error {
	if len(metrics) == 0 {
		return nil
	}

	req := prometheusRemoteWriteRequest{}
	for _, m := range metrics {
		labels := []prometheusLabel{{Name: "__name__", Value: m.Name}}
		for k, v := range m.Labels {
			labels = append(labels, prometheusLabel{Name: k, Value: v})
		}
		req.Timeseries = append(req.Timeseries, prometheusTimeSeries{
			Labels: labels,
			Samples: []prometheusSample{{
				Value:     m.Value,
				Timestamp: m.Timestamp.UnixMilli(),
			}},
		})
	}

	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal remote write request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create remote write request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := e.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("send remote write request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("remote write returned status %d", resp.StatusCode)
	}
	return nil
}

// FanoutExporter writes to multiple exporters, collecting all errors.
type FanoutExporter struct {
	exporters []Exporter
	logger    *zap.Logger
}

// NewFanoutExporter creates an exporter that fans out to all provided exporters.
func NewFanoutExporter(exporters []Exporter, logger *zap.Logger) *FanoutExporter {
	return &FanoutExporter{exporters: exporters, logger: logger}
}

// ExportSpans writes spans to all exporters.
func (f *FanoutExporter) ExportSpans(ctx context.Context, spans []*model.Span) error {
	var errs []error
	for _, exp := range f.exporters {
		if err := exp.ExportSpans(ctx, spans); err != nil {
			f.logger.Error("fanout export spans error", zap.Error(err))
			errs = append(errs, err)
		}
	}
	return joinErrors(errs)
}

// ExportLogs writes logs to all exporters.
func (f *FanoutExporter) ExportLogs(ctx context.Context, logs []*model.LogRecord) error {
	var errs []error
	for _, exp := range f.exporters {
		if err := exp.ExportLogs(ctx, logs); err != nil {
			f.logger.Error("fanout export logs error", zap.Error(err))
			errs = append(errs, err)
		}
	}
	return joinErrors(errs)
}

// ExportMetrics writes metrics to all exporters.
func (f *FanoutExporter) ExportMetrics(ctx context.Context, metrics []*model.MetricDataPoint) error {
	var errs []error
	for _, exp := range f.exporters {
		if err := exp.ExportMetrics(ctx, metrics); err != nil {
			f.logger.Error("fanout export metrics error", zap.Error(err))
			errs = append(errs, err)
		}
	}
	return joinErrors(errs)
}

func joinErrors(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	msgs := make([]string, 0, len(errs))
	for _, e := range errs {
		msgs = append(msgs, e.Error())
	}
	return fmt.Errorf("multiple export errors: %s", strings.Join(msgs, "; "))
}
