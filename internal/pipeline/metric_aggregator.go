package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/config"
	"github.com/nicedavid98/observability-platform/internal/model"
	"github.com/nicedavid98/observability-platform/internal/storage/clickhouse"
)

// MetricAggregator pre-aggregates raw metrics from Kafka into configurable time windows
// before writing downsampled data to ClickHouse.
type MetricAggregator struct {
	cfg         *config.KafkaConfig
	reader      *kafka.Reader
	metricStore *clickhouse.MetricStore
	logger      *zap.Logger

	windows []aggregationWindow
}

// aggregationWindow holds the state for a single time bucket size.
type aggregationWindow struct {
	size    time.Duration
	mu      sync.Mutex
	buckets map[bucketKey]*bucketValue
}

type bucketKey struct {
	name        string
	serviceName string
	labelKey    string // JSON-encoded labels for map key
	bucketTime  int64  // Unix seconds truncated to window size
}

type bucketValue struct {
	sum   float64
	count int64
	min   float64
	max   float64
}

// NewMetricAggregator creates a MetricAggregator consuming from the configured metrics topic.
func NewMetricAggregator(cfg *config.KafkaConfig, metricStore *clickhouse.MetricStore, logger *zap.Logger) *MetricAggregator {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.MetricsTopic,
		GroupID:        cfg.ConsumerGroup + "-metrics",
		MinBytes:       1,
		MaxBytes:       cfg.MaxBytes,
		CommitInterval: cfg.CommitInterval,
		StartOffset:    kafka.LastOffset,
	})

	windows := []aggregationWindow{
		{size: time.Minute, buckets: make(map[bucketKey]*bucketValue)},
		{size: 5 * time.Minute, buckets: make(map[bucketKey]*bucketValue)},
		{size: time.Hour, buckets: make(map[bucketKey]*bucketValue)},
		{size: 24 * time.Hour, buckets: make(map[bucketKey]*bucketValue)},
	}

	return &MetricAggregator{
		cfg:         cfg,
		reader:      reader,
		metricStore: metricStore,
		logger:      logger,
		windows:     windows,
	}
}

// Run processes metric messages and periodically flushes aggregated data.
func (a *MetricAggregator) Run(ctx context.Context) error {
	a.logger.Info("metric aggregator started", zap.String("topic", a.cfg.MetricsTopic))

	// Start per-window flush goroutines.
	for i := range a.windows {
		go a.flushLoop(ctx, &a.windows[i])
	}

	for {
		select {
		case <-ctx.Done():
			return a.reader.Close()
		default:
			msg, err := a.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				a.logger.Error("fetch metric message", zap.Error(err))
				continue
			}

			if err := a.aggregate(msg.Value); err != nil {
				a.logger.Warn("aggregate metric", zap.Error(err))
			}
			_ = a.reader.CommitMessages(ctx, msg)
		}
	}
}

func (a *MetricAggregator) aggregate(data []byte) error {
	var point model.MetricDataPoint
	if err := json.Unmarshal(data, &point); err != nil {
		return fmt.Errorf("unmarshal metric: %w", err)
	}

	labelKey, err := json.Marshal(point.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}

	for i := range a.windows {
		w := &a.windows[i]
		bucketTime := point.Timestamp.Truncate(w.size).Unix()
		key := bucketKey{
			name:        point.Name,
			serviceName: point.ServiceName,
			labelKey:    string(labelKey),
			bucketTime:  bucketTime,
		}

		w.mu.Lock()
		bv, ok := w.buckets[key]
		if !ok {
			bv = &bucketValue{min: point.Value, max: point.Value}
			w.buckets[key] = bv
		}
		bv.sum += point.Value
		bv.count++
		if point.Value < bv.min {
			bv.min = point.Value
		}
		if point.Value > bv.max {
			bv.max = point.Value
		}
		w.mu.Unlock()
	}
	return nil
}

func (a *MetricAggregator) flushLoop(ctx context.Context, w *aggregationWindow) {
	ticker := time.NewTicker(w.size)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			a.flushWindow(ctx, w)
		case <-ctx.Done():
			return
		}
	}
}

func (a *MetricAggregator) flushWindow(ctx context.Context, w *aggregationWindow) {
	w.mu.Lock()
	buckets := w.buckets
	w.buckets = make(map[bucketKey]*bucketValue)
	w.mu.Unlock()

	if len(buckets) == 0 {
		return
	}

	var points []*model.MetricDataPoint
	for k, v := range buckets {
		if v.count == 0 {
			continue
		}
		points = append(points, &model.MetricDataPoint{
			Name:        k.name,
			Type:        model.MetricTypeGauge,
			Value:       v.sum / float64(v.count), // store average
			Timestamp:   time.Unix(k.bucketTime, 0),
			ServiceName: k.serviceName,
		})
	}

	if err := a.metricStore.InsertDataPoints(ctx, points); err != nil {
		a.logger.Error("flush aggregated metrics",
			zap.Duration("window", w.size),
			zap.Int("count", len(points)),
			zap.Error(err),
		)
	} else {
		a.logger.Debug("flushed aggregated metrics",
			zap.Duration("window", w.size),
			zap.Int("count", len(points)),
		)
	}
}
