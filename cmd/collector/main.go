// Command collector starts the OpenTelemetry collector pipeline service.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/nicedavid98/observability-platform/internal/collector"
	"github.com/nicedavid98/observability-platform/internal/config"
	"github.com/nicedavid98/observability-platform/internal/storage/clickhouse"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "collector: "+err.Error())
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "config/collector.yaml", "path to collector config file")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	flag.Parse()

	logger, err := buildLogger(*logLevel)
	if err != nil {
		return fmt.Errorf("build logger: %w", err)
	}
	defer logger.Sync() //nolint:errcheck // sync errors are non-critical on shutdown

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger.Info("starting collector service")

	chClient, err := clickhouse.NewClient(&cfg.Storage, logger)
	if err != nil {
		return fmt.Errorf("connect to clickhouse: %w", err)
	}
	defer chClient.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := clickhouse.RunMigrations(ctx, chClient, cfg.Storage.RetentionDays, logger); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}

	traceStore := clickhouse.NewTraceStore(chClient, logger)
	logStore := clickhouse.NewLogStore(chClient, logger)
	metricStore := clickhouse.NewMetricStore(chClient, logger)

	chExporter := collector.NewClickHouseExporter(traceStore, logStore, metricStore, logger)
	fanout := collector.NewFanoutExporter([]collector.Exporter{chExporter}, logger)

	resourceProc := collector.NewResourceProcessor(logger)
	samplingProc := collector.NewSamplingProcessor(&cfg.Collector.Sampling, logger)

	processors := []collector.Processor{resourceProc, samplingProc}

	tracePipeline := collector.NewPipeline("traces", processors, fanout, logger)
	logPipeline := collector.NewPipeline("logs", processors, fanout, logger)
	metricPipeline := collector.NewPipeline("metrics", processors, fanout, logger)

	pipeline := collector.NewCollectorPipeline(tracePipeline, logPipeline, metricPipeline, logger)

	recv := collector.NewReceiver(&cfg.Collector, pipeline, logger)
	if err := recv.Start(ctx); err != nil {
		return fmt.Errorf("start receiver: %w", err)
	}

	logger.Info("collector service ready",
		zap.Int("grpc_port", cfg.Collector.OTLP.GRPCPort),
		zap.Int("http_port", cfg.Collector.OTLP.HTTPPort),
	)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("collector service shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Collector.ExportTimeout)
	defer shutdownCancel()

	return recv.Stop(shutdownCtx)
}

func buildLogger(level string) (*zap.Logger, error) {
	var lvl zapcore.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid log level %q: %w", level, err)
	}
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(lvl)
	return cfg.Build()
}
