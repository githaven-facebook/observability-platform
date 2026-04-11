// Package clickhouse provides ClickHouse storage for traces, logs, and metrics.
package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/config"
)

// Client wraps a ClickHouse connection with retry logic and health checking.
type Client struct {
	conn   clickhouse.Conn
	cfg    *config.StorageConfig
	logger *zap.Logger
}

// NewClient creates and validates a ClickHouse connection using the given storage config.
func NewClient(cfg *config.StorageConfig, logger *zap.Logger) (*Client, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse options: %w", err)
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse connection: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}

	logger.Info("ClickHouse connection established", zap.String("dsn", sanitizeDSN(cfg.ClickHouseDSN)))
	return &Client{conn: conn, cfg: cfg, logger: logger}, nil
}

func parseOptions(cfg *config.StorageConfig) (*clickhouse.Options, error) {
	opts, err := clickhouse.ParseDSN(cfg.ClickHouseDSN)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	opts.MaxOpenConns = cfg.MaxOpenConns
	opts.MaxIdleConns = cfg.MaxIdleConns
	opts.ConnMaxLifetime = cfg.ConnMaxLifetime
	opts.DialTimeout = cfg.DialTimeout
	opts.ConnOpenStrategy = clickhouse.ConnOpenInOrder
	opts.Compression = &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	}

	// Enable TLS for production DSNs that specify secure transport.
	if opts.TLS != nil {
		opts.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	return opts, nil
}

// Ping checks the ClickHouse connection is alive.
func (c *Client) Ping(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

// Close releases the ClickHouse connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Conn exposes the underlying ClickHouse connection for store implementations.
func (c *Client) Conn() clickhouse.Conn {
	return c.conn
}

// ExecWithRetry runs a DDL or DML statement with up to maxRetries attempts.
func (c *Client) ExecWithRetry(ctx context.Context, query string, args ...interface{}) error {
	const maxRetries = 3
	const retryDelay = 500 * time.Millisecond

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := c.conn.Exec(ctx, query, args...); err != nil {
			lastErr = err
			c.logger.Warn("clickhouse exec failed, retrying",
				zap.Int("attempt", attempt),
				zap.Error(err),
			)
			time.Sleep(retryDelay * time.Duration(attempt))
			continue
		}
		return nil
	}
	return fmt.Errorf("exec after %d retries: %w", maxRetries, lastErr)
}

// sanitizeDSN removes passwords from a DSN string before logging it.
func sanitizeDSN(dsn string) string {
	// Simple heuristic: truncate after the host portion.
	for i, c := range dsn {
		if c == '@' {
			return "clickhouse://***@" + dsn[i+1:]
		}
	}
	return dsn
}
