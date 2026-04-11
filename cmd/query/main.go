// Command query starts the observability query API service.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/api"
	"github.com/nicedavid98/observability-platform/internal/config"
	"github.com/nicedavid98/observability-platform/internal/query"
	"github.com/nicedavid98/observability-platform/internal/storage/clickhouse"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "query: "+err.Error())
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "config/query.yaml", "path to query config file")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	flag.Parse()

	logger, err := buildLogger(*logLevel)
	if err != nil {
		return fmt.Errorf("build logger: %w", err)
	}
	defer logger.Sync() //nolint:errcheck

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger.Info("starting query service")

	chClient, err := clickhouse.NewClient(&cfg.Storage, logger)
	if err != nil {
		return fmt.Errorf("connect to clickhouse: %w", err)
	}
	defer chClient.Close()

	// Storage layer.
	traceStore := clickhouse.NewTraceStore(chClient, logger)
	logStore := clickhouse.NewLogStore(chClient, logger)
	metricStore := clickhouse.NewMetricStore(chClient, logger)

	// Query engines.
	traceEngine := query.NewTraceQueryEngine(traceStore, logger)
	logEngine := query.NewLogQueryEngine(logStore, logger)
	metricEngine := query.NewMetricQueryEngine(metricStore, logger)
	serviceMapEngine := query.NewServiceMapEngine(traceStore, logger)

	// HTTP handlers.
	handlers := &api.Handlers{
		Trace:      api.NewTraceHandler(traceEngine, logger),
		Log:        api.NewLogHandler(logEngine, logger),
		Metric:     api.NewMetricHandler(metricEngine, logger),
		ServiceMap: api.NewServiceMapHandler(serviceMapEngine, logger),
		Dashboard:  api.NewDashboardHandler(metricEngine, traceEngine, logger),
	}

	server := api.NewServer(&cfg.Query, handlers, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit
		logger.Info("query service shutting down")
		cancel()
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Query.WriteTimeout)
	defer shutdownCancel()
	return server.Shutdown(shutdownCtx)
}

func buildLogger(level string) (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	if err := cfg.Level.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid log level %q: %w", level, err)
	}
	return cfg.Build()
}
