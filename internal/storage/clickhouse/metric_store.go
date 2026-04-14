package clickhouse

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/model"
)

// MetricStore handles metric data point persistence and querying in ClickHouse.
type MetricStore struct {
	client *Client
	logger *zap.Logger
}

// NewMetricStore creates a MetricStore using the given client.
func NewMetricStore(client *Client, logger *zap.Logger) *MetricStore {
	return &MetricStore{client: client, logger: logger}
}

// InsertDataPoints batch-inserts metric data points.
func (s *MetricStore) InsertDataPoints(ctx context.Context, points []*model.MetricDataPoint) error {
	if len(points) == 0 {
		return nil
	}

	batch, err := s.client.Conn().PrepareBatch(ctx, `
		INSERT INTO metrics.data_points (
			name, description, unit, type, labels, value, timestamp, service_name
		)`)
	if err != nil {
		return fmt.Errorf("prepare metric batch: %w", err)
	}

	for _, p := range points {
		if err := batch.Append(
			p.Name,
			p.Description,
			p.Unit,
			int32(p.Type),
			p.Labels,
			p.Value,
			p.Timestamp,
			p.ServiceName,
		); err != nil {
			return fmt.Errorf("append metric data point: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send metric batch: %w", err)
	}

	s.logger.Debug("inserted metric data points", zap.Int("count", len(points)))
	return nil
}

// QueryDataPoints retrieves raw data points for the given query parameters.
func (s *MetricStore) QueryDataPoints(ctx context.Context, q MetricQuery) ([]*model.MetricDataPoint, error) { //nolint:gocritic // query struct passed by value for immutable use
	table := resolutionTable(q.Resolution)

	query := fmt.Sprintf(`
		SELECT name, description, unit, type, labels, value, timestamp, service_name
		FROM metrics.%s
		WHERE name = ? AND timestamp >= ? AND timestamp <= ?
	`, table)
	args := []interface{}{q.Name, q.StartTime, q.EndTime}

	for k, v := range q.Labels {
		query += fmt.Sprintf(" AND labels['%s'] = ?", k)
		args = append(args, v)
	}

	if q.ServiceName != "" {
		query += " AND service_name = ?"
		args = append(args, q.ServiceName)
	}

	query += " ORDER BY timestamp ASC"

	rows, err := s.client.Conn().Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query metric data points: %w", err)
	}
	defer rows.Close()

	var points []*model.MetricDataPoint
	for rows.Next() {
		var p model.MetricDataPoint
		var metricType int32
		var labels map[string]string

		if err := rows.Scan(
			&p.Name, &p.Description, &p.Unit, &metricType,
			&labels, &p.Value, &p.Timestamp, &p.ServiceName,
		); err != nil {
			return nil, fmt.Errorf("scan metric data point: %w", err)
		}
		p.Type = model.MetricType(metricType)
		p.Labels = labels
		points = append(points, &p)
	}
	return points, rows.Err()
}

// QueryAggregated returns downsampled metric data for the given resolution.
func (s *MetricStore) QueryAggregated(ctx context.Context, q MetricQuery) ([]*model.MetricDataPoint, error) { //nolint:gocritic // query struct passed by value intentionally for mutation
	resolution := model.SelectResolution(int64(q.EndTime.Sub(q.StartTime).Seconds()))
	q.Resolution = resolution
	return s.QueryDataPoints(ctx, q)
}

// ListSeries returns distinct metric series matching the name prefix.
func (s *MetricStore) ListSeries(ctx context.Context, namePrefix string, limit int) ([]model.MetricSeries, error) {
	rows, err := s.client.Conn().Query(ctx, `
		SELECT DISTINCT name, labels
		FROM metrics.data_points
		WHERE name LIKE ?
		LIMIT ?
	`, namePrefix+"%", limit)
	if err != nil {
		return nil, fmt.Errorf("list metric series: %w", err)
	}
	defer rows.Close()

	var series []model.MetricSeries
	for rows.Next() {
		var s model.MetricSeries
		var labels map[string]string
		if err := rows.Scan(&s.Name, &labels); err != nil {
			return nil, fmt.Errorf("scan metric series: %w", err)
		}
		s.Labels = labels
		series = append(series, s)
	}
	return series, rows.Err()
}

// ListLabelValues returns distinct values for the given label key and metric name.
func (s *MetricStore) ListLabelValues(ctx context.Context, metricName, labelKey string) ([]string, error) {
	rows, err := s.client.Conn().Query(ctx, `
		SELECT DISTINCT labels[?] AS label_value
		FROM metrics.data_points
		WHERE name = ? AND label_value != ''
		ORDER BY label_value
		LIMIT 1000
	`, labelKey, metricName)
	if err != nil {
		return nil, fmt.Errorf("list label values: %w", err)
	}
	defer rows.Close()

	var values []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("scan label value: %w", err)
		}
		values = append(values, v)
	}
	return values, rows.Err()
}

// MetricQuery holds filter and resolution parameters for metric queries.
type MetricQuery struct {
	Name        string
	Labels      map[string]string
	ServiceName string
	StartTime   time.Time
	EndTime     time.Time
	Resolution  model.DownsampleResolution
	Limit       int
}

func resolutionTable(r model.DownsampleResolution) string {
	switch r {
	case model.Resolution1Day:
		return "agg_1day"
	case model.Resolution1Hour:
		return "agg_1hour"
	case model.Resolution5Min:
		return "agg_5min"
	default:
		return "data_points"
	}
}
