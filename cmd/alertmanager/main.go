// Command alertmanager starts the alert evaluation and notification service.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/alerting"
	"github.com/nicedavid98/observability-platform/internal/config"
	"github.com/nicedavid98/observability-platform/internal/query"
	"github.com/nicedavid98/observability-platform/internal/storage/clickhouse"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "alertmanager: "+err.Error())
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "config/alertmanager.yaml", "path to alertmanager config file")
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

	logger.Info("starting alertmanager service")

	chClient, err := clickhouse.NewClient(&cfg.Storage, logger)
	if err != nil {
		return fmt.Errorf("connect to clickhouse: %w", err)
	}
	defer chClient.Close()

	metricStore := clickhouse.NewMetricStore(chClient, logger)
	metricEngine := query.NewMetricQueryEngine(metricStore, logger)

	evaluator := alerting.NewEvaluator(metricEngine, logger)
	silences := alerting.NewSilenceManager(logger)

	notifiers := buildNotifiers(cfg.Alerting.NotificationChannels, logger)

	loader := alerting.NewRuleLoader(cfg.Alerting.RulesFile)

	engine := alerting.NewEngine(
		cfg.Alerting.EvaluationInterval,
		cfg.Alerting.GroupWait,
		cfg.Alerting.GroupInterval,
		cfg.Alerting.RepeatInterval,
		evaluator,
		notifiers,
		silences,
		loader,
		logger,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := engine.Start(ctx); err != nil {
		return fmt.Errorf("start alert engine: %w", err)
	}

	logger.Info("alertmanager service ready",
		zap.Duration("evaluation_interval", cfg.Alerting.EvaluationInterval),
	)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("alertmanager service shutting down")
	engine.Stop()

	// Allow in-flight notifications to drain.
	time.Sleep(2 * time.Second)
	return nil
}

func buildNotifiers(channels []config.NotificationChannel, logger *zap.Logger) map[string]alerting.NotificationSender {
	notifiers := make(map[string]alerting.NotificationSender)
	for _, ch := range channels {
		switch ch.Type {
		case "pagerduty":
			key := ch.Settings["integration_key"]
			if key == "" {
				logger.Warn("pagerduty notifier missing integration_key", zap.String("name", ch.Name))
				continue
			}
			n := alerting.NewPagerDutyNotifier(key, logger)
			notifiers[ch.Name] = alerting.NewGroupingNotifier(n, 4*time.Hour, logger)

		case "slack":
			webhook := ch.Settings["webhook_url"]
			channel := ch.Settings["channel"]
			if webhook == "" {
				logger.Warn("slack notifier missing webhook_url", zap.String("name", ch.Name))
				continue
			}
			n := alerting.NewSlackNotifier(webhook, channel, logger)
			notifiers[ch.Name] = alerting.NewGroupingNotifier(n, time.Hour, logger)

		case "email":
			n := alerting.NewEmailNotifier(
				ch.Settings["smtp_addr"],
				ch.Settings["from"],
				splitCSV(ch.Settings["to"]),
				ch.Settings["username"],
				ch.Settings["password"],
				logger,
			)
			notifiers[ch.Name] = alerting.NewGroupingNotifier(n, 4*time.Hour, logger)

		default:
			logger.Warn("unknown notification channel type",
				zap.String("type", ch.Type),
				zap.String("name", ch.Name),
			)
		}
	}
	return notifiers
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	var parts []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			parts = append(parts, p)
		}
	}
	return parts
}

func buildLogger(level string) (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	if err := cfg.Level.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid log level %q: %w", level, err)
	}
	return cfg.Build()
}
