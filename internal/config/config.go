// Package config provides configuration loading and validation for all platform services.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration structure for the observability platform.
type Config struct {
	Collector    CollectorConfig    `yaml:"collector"`
	Storage      StorageConfig      `yaml:"storage"`
	Kafka        KafkaConfig        `yaml:"kafka"`
	Alerting     AlertingConfig     `yaml:"alerting"`
	Query        QueryConfig        `yaml:"query"`
}

// CollectorConfig holds settings for the OTel collector pipeline.
type CollectorConfig struct {
	OTLP          OTLPConfig    `yaml:"otlp"`
	BatchSize     int           `yaml:"batch_size"`
	ExportTimeout time.Duration `yaml:"export_timeout"`
	Sampling      SamplingConfig `yaml:"sampling"`
}

// OTLPConfig defines gRPC and HTTP listener ports for OTLP ingestion.
type OTLPConfig struct {
	GRPCPort int    `yaml:"grpc_port"`
	HTTPPort int    `yaml:"http_port"`
	BindAddr string `yaml:"bind_addr"`
}

// SamplingConfig controls head-based and tail-based sampling rates.
type SamplingConfig struct {
	HeadSampleRate float64            `yaml:"head_sample_rate"`
	TailSampleRate float64            `yaml:"tail_sample_rate"`
	ServiceRates   map[string]float64 `yaml:"service_rates"`
	AlwaysSample   []string           `yaml:"always_sample_errors"`
}

// StorageConfig defines ClickHouse connection and retention parameters.
type StorageConfig struct {
	ClickHouseDSN   string        `yaml:"clickhouse_dsn"`
	RetentionDays   int           `yaml:"retention_days"`
	TablePrefix     string        `yaml:"table_prefix"`
	MaxOpenConns    int           `yaml:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
	DialTimeout     time.Duration `yaml:"dial_timeout"`
}

// KafkaConfig holds Kafka broker addresses and topic names.
type KafkaConfig struct {
	Brokers        []string      `yaml:"brokers"`
	TracesTopic    string        `yaml:"traces_topic"`
	LogsTopic      string        `yaml:"logs_topic"`
	MetricsTopic   string        `yaml:"metrics_topic"`
	ConsumerGroup  string        `yaml:"consumer_group"`
	CommitInterval time.Duration `yaml:"commit_interval"`
	MaxBytes       int           `yaml:"max_bytes"`
}

// AlertingConfig defines evaluation timing and notification channels.
type AlertingConfig struct {
	EvaluationInterval  time.Duration          `yaml:"evaluation_interval"`
	NotificationChannels []NotificationChannel `yaml:"notification_channels"`
	RulesFile           string                `yaml:"rules_file"`
	GroupWait           time.Duration          `yaml:"group_wait"`
	GroupInterval       time.Duration          `yaml:"group_interval"`
	RepeatInterval      time.Duration          `yaml:"repeat_interval"`
}

// NotificationChannel defines a notification destination with its type and settings.
type NotificationChannel struct {
	Name     string            `yaml:"name"`
	Type     string            `yaml:"type"` // pagerduty, slack, email
	Settings map[string]string `yaml:"settings"`
}

// QueryConfig controls the HTTP query API server.
type QueryConfig struct {
	Port             int           `yaml:"port"`
	BindAddr         string        `yaml:"bind_addr"`
	MaxQueryDuration time.Duration `yaml:"max_query_duration"`
	CacheTTL         time.Duration `yaml:"cache_ttl"`
	MaxResultRows    int           `yaml:"max_result_rows"`
	ReadTimeout      time.Duration `yaml:"read_timeout"`
	WriteTimeout     time.Duration `yaml:"write_timeout"`
	IdleTimeout      time.Duration `yaml:"idle_timeout"`
}

// Load reads a YAML config file from the given path and returns a parsed Config.
func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config file %q: %w", path, err)
	}
	defer f.Close()

	var cfg Config
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	cfg.applyDefaults()
	return &cfg, nil
}

// validate checks required fields and logical constraints.
func (c *Config) validate() error {
	if c.Storage.ClickHouseDSN == "" {
		return fmt.Errorf("storage.clickhouse_dsn is required")
	}
	if c.Storage.RetentionDays <= 0 {
		return fmt.Errorf("storage.retention_days must be positive")
	}
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers must not be empty")
	}
	return nil
}

// applyDefaults fills in zero values with sensible defaults.
func (c *Config) applyDefaults() {
	if c.Collector.OTLP.GRPCPort == 0 {
		c.Collector.OTLP.GRPCPort = 4317
	}
	if c.Collector.OTLP.HTTPPort == 0 {
		c.Collector.OTLP.HTTPPort = 4318
	}
	if c.Collector.BatchSize == 0 {
		c.Collector.BatchSize = 1000
	}
	if c.Collector.ExportTimeout == 0 {
		c.Collector.ExportTimeout = 30 * time.Second
	}
	if c.Collector.Sampling.HeadSampleRate == 0 {
		c.Collector.Sampling.HeadSampleRate = 1.0
	}
	if c.Storage.MaxOpenConns == 0 {
		c.Storage.MaxOpenConns = 10
	}
	if c.Storage.MaxIdleConns == 0 {
		c.Storage.MaxIdleConns = 5
	}
	if c.Storage.ConnMaxLifetime == 0 {
		c.Storage.ConnMaxLifetime = 5 * time.Minute
	}
	if c.Storage.DialTimeout == 0 {
		c.Storage.DialTimeout = 10 * time.Second
	}
	if c.Kafka.ConsumerGroup == "" {
		c.Kafka.ConsumerGroup = "observability-platform"
	}
	if c.Kafka.CommitInterval == 0 {
		c.Kafka.CommitInterval = time.Second
	}
	if c.Kafka.MaxBytes == 0 {
		c.Kafka.MaxBytes = 10 << 20 // 10 MB
	}
	if c.Alerting.EvaluationInterval == 0 {
		c.Alerting.EvaluationInterval = 30 * time.Second
	}
	if c.Alerting.GroupWait == 0 {
		c.Alerting.GroupWait = 30 * time.Second
	}
	if c.Alerting.GroupInterval == 0 {
		c.Alerting.GroupInterval = 5 * time.Minute
	}
	if c.Alerting.RepeatInterval == 0 {
		c.Alerting.RepeatInterval = 4 * time.Hour
	}
	if c.Query.Port == 0 {
		c.Query.Port = 8080
	}
	if c.Query.MaxQueryDuration == 0 {
		c.Query.MaxQueryDuration = 30 * time.Second
	}
	if c.Query.CacheTTL == 0 {
		c.Query.CacheTTL = 5 * time.Minute
	}
	if c.Query.MaxResultRows == 0 {
		c.Query.MaxResultRows = 10000
	}
	if c.Query.ReadTimeout == 0 {
		c.Query.ReadTimeout = 30 * time.Second
	}
	if c.Query.WriteTimeout == 0 {
		c.Query.WriteTimeout = 30 * time.Second
	}
	if c.Query.IdleTimeout == 0 {
		c.Query.IdleTimeout = 60 * time.Second
	}
}
