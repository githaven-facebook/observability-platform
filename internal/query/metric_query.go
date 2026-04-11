package query

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/model"
	"github.com/nicedavid98/observability-platform/internal/storage/clickhouse"
)

// MetricQueryEngine executes PromQL-compatible metric queries against ClickHouse.
type MetricQueryEngine struct {
	store  *clickhouse.MetricStore
	logger *zap.Logger
}

// NewMetricQueryEngine creates a MetricQueryEngine backed by the given store.
func NewMetricQueryEngine(store *clickhouse.MetricStore, logger *zap.Logger) *MetricQueryEngine {
	return &MetricQueryEngine{store: store, logger: logger}
}

// QueryParams defines parameters for an instant or range metric query.
type QueryParams struct {
	Query     string
	StartTime time.Time
	EndTime   time.Time
	Step      time.Duration
	Limit     int
}

// QueryResult wraps the result of a metric query.
type QueryResult struct {
	ResultType string              `json:"resultType"` // "matrix" or "vector"
	Result     []MetricResultItem  `json:"result"`
}

// MetricResultItem represents a single time series in a query result.
type MetricResultItem struct {
	Metric map[string]string `json:"metric"`
	Values [][]interface{}   `json:"values"` // [[timestamp, value], ...]
}

// Query parses a simple PromQL-subset query expression and returns results.
func (e *MetricQueryEngine) Query(ctx context.Context, params QueryParams) (*QueryResult, error) {
	if params.EndTime.IsZero() {
		params.EndTime = time.Now()
	}
	if params.StartTime.IsZero() {
		params.StartTime = params.EndTime.Add(-time.Hour)
	}
	if params.Limit <= 0 {
		params.Limit = 10000
	}

	expr, err := parsePromQLSubset(params.Query)
	if err != nil {
		return nil, fmt.Errorf("parse query %q: %w", params.Query, err)
	}

	resolution := model.SelectResolution(int64(params.EndTime.Sub(params.StartTime).Seconds()))
	q := clickhouse.MetricQuery{
		Name:      expr.metricName,
		Labels:    expr.labelMatchers,
		StartTime: params.StartTime,
		EndTime:   params.EndTime,
		Resolution: resolution,
		Limit:     params.Limit,
	}

	points, err := e.store.QueryDataPoints(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query metric data points: %w", err)
	}

	result := buildQueryResult(points, expr)
	e.logger.Debug("metric query completed",
		zap.String("query", params.Query),
		zap.Int("points", len(points)),
	)
	return result, nil
}

// ListSeries returns distinct metric series matching the given name prefix.
func (e *MetricQueryEngine) ListSeries(ctx context.Context, namePrefix string, limit int) ([]model.MetricSeries, error) {
	if limit <= 0 {
		limit = 1000
	}
	return e.store.ListSeries(ctx, namePrefix, limit)
}

// ListLabels returns distinct label names for the given metric.
func (e *MetricQueryEngine) ListLabels(ctx context.Context, metricName string) ([]string, error) {
	series, err := e.store.ListSeries(ctx, metricName, 100)
	if err != nil {
		return nil, err
	}
	labelSet := make(map[string]struct{})
	for _, s := range series {
		for k := range s.Labels {
			labelSet[k] = struct{}{}
		}
	}
	labels := make([]string, 0, len(labelSet))
	for k := range labelSet {
		labels = append(labels, k)
	}
	return labels, nil
}

// ListLabelValues returns distinct values for the given label key on a metric.
func (e *MetricQueryEngine) ListLabelValues(ctx context.Context, metricName, labelKey string) ([]string, error) {
	return e.store.ListLabelValues(ctx, metricName, labelKey)
}

// promQLExpr is the minimal parsed representation of a PromQL-subset expression.
type promQLExpr struct {
	metricName    string
	labelMatchers map[string]string
	function      string   // rate, sum, avg, histogram_quantile, etc.
	args          []string
}

// parsePromQLSubset parses a minimal subset of PromQL sufficient for dashboards.
// Supports: metric_name{label="value"}, rate(m[5m]), sum(m), avg(m).
func parsePromQLSubset(expr string) (*promQLExpr, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, fmt.Errorf("empty expression")
	}

	// Detect function wrapper: func(inner)
	result := &promQLExpr{}
	if idx := strings.Index(expr, "("); idx > 0 && strings.HasSuffix(expr, ")") {
		result.function = strings.TrimSpace(expr[:idx])
		inner := expr[idx+1 : len(expr)-1]
		// Strip range selector e.g. [5m]
		if ridx := strings.LastIndex(inner, "["); ridx > 0 {
			inner = inner[:ridx]
		}
		inner = strings.TrimSpace(inner)
		if err := parseMetricSelector(inner, result); err != nil {
			return nil, err
		}
		return result, nil
	}

	if err := parseMetricSelector(expr, result); err != nil {
		return nil, err
	}
	return result, nil
}

func parseMetricSelector(s string, result *promQLExpr) error {
	s = strings.TrimSpace(s)
	result.labelMatchers = make(map[string]string)

	lbrace := strings.Index(s, "{")
	if lbrace < 0 {
		result.metricName = s
		return nil
	}

	result.metricName = s[:lbrace]
	rbrace := strings.Index(s, "}")
	if rbrace < 0 {
		return fmt.Errorf("missing closing brace in %q", s)
	}

	labelStr := s[lbrace+1 : rbrace]
	for _, part := range strings.Split(labelStr, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		eqIdx := strings.Index(part, "=")
		if eqIdx < 0 {
			continue
		}
		key := strings.TrimSpace(part[:eqIdx])
		val := strings.Trim(strings.TrimSpace(part[eqIdx+1:]), `"'`)
		result.labelMatchers[key] = val
	}
	return nil
}

func buildQueryResult(points []*model.MetricDataPoint, expr *promQLExpr) *QueryResult {
	// Group points by unique label set fingerprint.
	type seriesKey struct{ labels string }
	seriesMap := make(map[string]*MetricResultItem)

	for _, p := range points {
		key := labelMapKey(p.Labels)
		item, ok := seriesMap[key]
		if !ok {
			metric := make(map[string]string, len(p.Labels)+1)
			metric["__name__"] = p.Name
			for k, v := range p.Labels {
				metric[k] = v
			}
			item = &MetricResultItem{Metric: metric}
			seriesMap[key] = item
		}
		item.Values = append(item.Values, []interface{}{
			strconv.FormatInt(p.Timestamp.Unix(), 10),
			strconv.FormatFloat(p.Value, 'f', -1, 64),
		})
	}

	result := &QueryResult{ResultType: "matrix"}
	for _, item := range seriesMap {
		result.Result = append(result.Result, *item)
	}
	return result
}

func labelMapKey(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	var parts []string
	for k, v := range labels {
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, ",")
}
