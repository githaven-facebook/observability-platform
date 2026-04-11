// Package pipeline implements asynchronous telemetry processing via Kafka consumers.
package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/config"
	"github.com/nicedavid98/observability-platform/internal/model"
	"github.com/nicedavid98/observability-platform/internal/storage/clickhouse"
)

// LogProcessor consumes log records from Kafka, enriches and transforms them,
// and writes the processed records to ClickHouse.
type LogProcessor struct {
	cfg      *config.KafkaConfig
	reader   *kafka.Reader
	logStore *clickhouse.LogStore
	logger   *zap.Logger

	extractors []*fieldExtractor
}

// fieldExtractor maps a regex pattern to a structured field name.
type fieldExtractor struct {
	name    string
	pattern *regexp.Regexp
	group   string // named capture group
}

// NewLogProcessor creates a LogProcessor that consumes from the configured logs topic.
func NewLogProcessor(cfg *config.KafkaConfig, logStore *clickhouse.LogStore, logger *zap.Logger) *LogProcessor {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.LogsTopic,
		GroupID:        cfg.ConsumerGroup,
		MinBytes:       1,
		MaxBytes:       cfg.MaxBytes,
		CommitInterval: cfg.CommitInterval,
		StartOffset:    kafka.LastOffset,
	})

	extractors := defaultFieldExtractors()
	return &LogProcessor{
		cfg:        cfg,
		reader:     reader,
		logStore:   logStore,
		logger:     logger,
		extractors: extractors,
	}
}

// Run starts consuming log messages until the context is cancelled.
func (p *LogProcessor) Run(ctx context.Context) error {
	p.logger.Info("log processor started", zap.String("topic", p.cfg.LogsTopic))

	var batch []*model.LogRecord
	flushTicker := time.NewTicker(5 * time.Second)
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				p.flush(ctx, batch)
			}
			return p.reader.Close()
		case <-flushTicker.C:
			if len(batch) > 0 {
				p.flush(ctx, batch)
				batch = batch[:0]
			}
		default:
			msg, err := p.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				p.logger.Error("fetch log message", zap.Error(err))
				continue
			}

			rec, err := p.processMessage(msg.Value)
			if err != nil {
				p.logger.Warn("process log message", zap.Error(err))
				_ = p.reader.CommitMessages(ctx, msg)
				continue
			}

			if rec != nil {
				batch = append(batch, rec)
			}
			_ = p.reader.CommitMessages(ctx, msg)

			if len(batch) >= 500 {
				p.flush(ctx, batch)
				batch = batch[:0]
			}
		}
	}
}

func (p *LogProcessor) processMessage(data []byte) (*model.LogRecord, error) {
	var rec model.LogRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("unmarshal log record: %w", err)
	}

	// Filter: drop debug logs in production.
	if rec.Severity <= model.SeverityDebug {
		return nil, nil
	}

	// Enrich with service metadata from resource attributes.
	if rec.ServiceName == "" {
		if svc, ok := rec.ResourceAttributes["service.name"]; ok {
			rec.ServiceName = svc
		}
	}

	// Set observed timestamp if not already set.
	if rec.ObservedTimestamp.IsZero() {
		rec.ObservedTimestamp = time.Now()
	}

	// Extract structured fields from the log body.
	rec.StructuredFields = p.extractFields(rec.Body)

	return &rec, nil
}

func (p *LogProcessor) flush(ctx context.Context, batch []*model.LogRecord) {
	if err := p.logStore.InsertLogs(ctx, batch); err != nil {
		p.logger.Error("flush log batch to clickhouse",
			zap.Int("count", len(batch)),
			zap.Error(err),
		)
	} else {
		p.logger.Debug("flushed log batch", zap.Int("count", len(batch)))
	}
}

func (p *LogProcessor) extractFields(body string) map[string]string {
	fields := make(map[string]string)
	for _, ex := range p.extractors {
		match := ex.pattern.FindStringSubmatch(body)
		if match == nil {
			continue
		}
		idx := ex.pattern.SubexpIndex(ex.group)
		if idx > 0 && idx < len(match) {
			fields[ex.name] = strings.TrimSpace(match[idx])
		}
	}
	return fields
}

func defaultFieldExtractors() []*fieldExtractor {
	return []*fieldExtractor{
		{
			name:    "http_method",
			pattern: regexp.MustCompile(`(?i)(?P<method>GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\s+/`),
			group:   "method",
		},
		{
			name:    "http_status",
			pattern: regexp.MustCompile(`\bHTTP[/ ][\d.]+\s+(?P<status>\d{3})\b`),
			group:   "status",
		},
		{
			name:    "error_type",
			pattern: regexp.MustCompile(`(?P<error_type>[A-Z][a-zA-Z]+Error|[A-Z][a-zA-Z]+Exception):`),
			group:   "error_type",
		},
		{
			name:    "duration_ms",
			pattern: regexp.MustCompile(`duration[=:\s]+(?P<duration>[\d.]+)\s*ms`),
			group:   "duration",
		},
		{
			name:    "user_id",
			pattern: regexp.MustCompile(`user[_\s]?id[=:\s]+(?P<user_id>\d+)`),
			group:   "user_id",
		},
	}
}
