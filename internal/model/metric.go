package model

import "time"

// MetricType represents the type of a metric.
type MetricType int32

const (
	MetricTypeUnspecified MetricType = 0
	MetricTypeGauge       MetricType = 1
	MetricTypeCounter     MetricType = 2
	MetricTypeHistogram   MetricType = 3
	MetricTypeSummary     MetricType = 4
)

// String returns the metric type name.
func (m MetricType) String() string {
	switch m {
	case MetricTypeGauge:
		return "gauge"
	case MetricTypeCounter:
		return "counter"
	case MetricTypeHistogram:
		return "histogram"
	case MetricTypeSummary:
		return "summary"
	default:
		return "unspecified"
	}
}

// MetricDataPoint represents a single observation of a metric at a point in time.
type MetricDataPoint struct {
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Unit        string            `json:"unit,omitempty"`
	Type        MetricType        `json:"type"`
	Labels      map[string]string `json:"labels,omitempty"`
	Value       float64           `json:"value"`
	Timestamp   time.Time         `json:"timestamp"`
	ServiceName string            `json:"service_name"`
}

// HistogramDataPoint extends MetricDataPoint with histogram-specific fields.
type HistogramDataPoint struct {
	MetricDataPoint
	Count          uint64    `json:"count"`
	Sum            float64   `json:"sum"`
	BucketCounts   []uint64  `json:"bucket_counts"`
	ExplicitBounds []float64 `json:"explicit_bounds"`
	Min            float64   `json:"min"`
	Max            float64   `json:"max"`
}

// SummaryDataPoint extends MetricDataPoint with summary-specific quantile data.
type SummaryDataPoint struct {
	MetricDataPoint
	Count      uint64             `json:"count"`
	Sum        float64            `json:"sum"`
	Quantiles  []SummaryQuantile  `json:"quantiles"`
}

// SummaryQuantile holds a single quantile value for a summary metric.
type SummaryQuantile struct {
	Quantile float64 `json:"quantile"`
	Value    float64 `json:"value"`
}

// MetricSeries identifies a unique metric time series by name and label set.
type MetricSeries struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels"`
}

// MetricQueryResult holds the result of a metric query over a time range.
type MetricQueryResult struct {
	Series     []MetricSeries       `json:"series"`
	DataPoints []MetricDataPoint    `json:"data_points"`
	Resolution time.Duration        `json:"resolution"`
	StartTime  time.Time            `json:"start_time"`
	EndTime    time.Time            `json:"end_time"`
}

// DownsampleResolution represents a pre-aggregated time bucket.
type DownsampleResolution string

const (
	Resolution1Min  DownsampleResolution = "1m"
	Resolution5Min  DownsampleResolution = "5m"
	Resolution1Hour DownsampleResolution = "1h"
	Resolution1Day  DownsampleResolution = "1d"
)

// SelectResolution picks the best pre-aggregated resolution for the given time range.
func SelectResolution(rangeSeconds int64) DownsampleResolution {
	switch {
	case rangeSeconds > 7*24*3600: // > 7 days
		return Resolution1Day
	case rangeSeconds > 24*3600: // > 1 day
		return Resolution1Hour
	case rangeSeconds > 3600: // > 1 hour
		return Resolution5Min
	default:
		return Resolution1Min
	}
}
