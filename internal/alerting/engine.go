package alerting

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/model"
)

// Engine periodically evaluates alert rules, manages state transitions,
// and dispatches notifications through configured channels.
type Engine struct {
	cfg       engineConfig
	rules     []model.AlertRule
	evaluator *Evaluator
	notifiers map[string]NotificationSender
	silences  *SilenceManager
	loader    *RuleLoader

	mu     sync.RWMutex
	states map[string]*model.AlertInstance // keyed by fingerprint

	logger *zap.Logger
	done   chan struct{}
}

type engineConfig struct {
	evaluationInterval time.Duration
	groupWait          time.Duration
	groupInterval      time.Duration
	repeatInterval     time.Duration
}

// NewEngine creates an alert evaluation engine.
func NewEngine(
	evaluationInterval, groupWait, groupInterval, repeatInterval time.Duration,
	evaluator *Evaluator,
	notifiers map[string]NotificationSender,
	silences *SilenceManager,
	loader *RuleLoader,
	logger *zap.Logger,
) *Engine {
	return &Engine{
		cfg: engineConfig{
			evaluationInterval: evaluationInterval,
			groupWait:          groupWait,
			groupInterval:      groupInterval,
			repeatInterval:     repeatInterval,
		},
		evaluator: evaluator,
		notifiers: notifiers,
		silences:  silences,
		loader:    loader,
		states:    make(map[string]*model.AlertInstance),
		logger:    logger,
		done:      make(chan struct{}),
	}
}

// Start loads rules and begins the evaluation loop.
func (e *Engine) Start(ctx context.Context) error {
	rules, err := e.loader.Load()
	if err != nil {
		return fmt.Errorf("load alert rules: %w", err)
	}
	e.rules = rules
	e.logger.Info("loaded alert rules", zap.Int("count", len(rules)))

	go e.runLoop(ctx)
	return nil
}

// Stop signals the evaluation loop to exit.
func (e *Engine) Stop() {
	close(e.done)
}

func (e *Engine) runLoop(ctx context.Context) {
	ticker := time.NewTicker(e.cfg.evaluationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.evaluate(ctx)
		case <-e.done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (e *Engine) evaluate(ctx context.Context) {
	for _, rule := range e.rules {
		result, err := e.evaluator.Evaluate(ctx, rule)
		if err != nil {
			e.logger.Warn("rule evaluation error",
				zap.String("rule", rule.Name),
				zap.Error(err),
			)
			continue
		}
		e.processResult(ctx, result)
	}
}

func (e *Engine) processResult(ctx context.Context, result *EvaluationResult) {
	fp := fingerprint(result.Rule.Name, result.Rule.Labels)

	e.mu.Lock()
	existing, ok := e.states[fp]

	now := time.Now()
	if result.Firing {
		if !ok {
			// New alert — start in pending state.
			instance := &model.AlertInstance{
				RuleID:      fp,
				RuleName:    result.Rule.Name,
				State:       model.AlertStatePending,
				Severity:    result.Rule.Severity,
				Labels:      result.Rule.Labels,
				Annotations: result.Rule.Annotations,
				StartsAt:    now,
				UpdatedAt:   now,
				Value:       result.Value,
				Fingerprint: fp,
			}
			e.states[fp] = instance
			e.mu.Unlock()
			return
		}

		// Transition pending → firing after the duration has elapsed.
		if existing.State == model.AlertStatePending && now.Sub(existing.StartsAt) >= result.Rule.Duration {
			existing.State = model.AlertStateFiring
			existing.UpdatedAt = now
			existing.Value = result.Value
			e.mu.Unlock()

			if !e.silences.IsSilenced(*existing) {
				e.dispatch(ctx, *existing)
			}
			return
		}

		existing.Value = result.Value
		existing.UpdatedAt = now
		e.mu.Unlock()
		return
	}

	// Not firing — resolve if previously active.
	if ok && (existing.State == model.AlertStateFiring || existing.State == model.AlertStatePending) {
		existing.State = model.AlertStateResolved
		existing.EndsAt = now
		existing.UpdatedAt = now
		resolved := *existing
		delete(e.states, fp)
		e.mu.Unlock()

		if !e.silences.IsSilenced(resolved) {
			e.dispatch(ctx, resolved)
		}
		return
	}

	e.mu.Unlock()
}

func (e *Engine) dispatch(ctx context.Context, alert model.AlertInstance) {
	notification := model.AlertNotification{
		GroupKey:    alert.Fingerprint,
		Status:      alert.State,
		Alerts:      []model.AlertInstance{alert},
		GroupLabels: alert.Labels,
		SentAt:      time.Now(),
	}

	for _, channelName := range e.rules[0].NotificationChannels {
		notifier, ok := e.notifiers[channelName]
		if !ok {
			e.logger.Warn("notifier not found", zap.String("channel", channelName))
			continue
		}
		notification.Receiver = channelName
		if err := notifier.Send(ctx, notification); err != nil {
			e.logger.Error("notification send failed",
				zap.String("channel", channelName),
				zap.String("rule", alert.RuleName),
				zap.Error(err),
			)
		}
	}
}

// ActiveAlerts returns a snapshot of currently firing alerts.
func (e *Engine) ActiveAlerts() []model.AlertInstance {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var alerts []model.AlertInstance
	for _, a := range e.states {
		if a.State == model.AlertStateFiring {
			alerts = append(alerts, *a)
		}
	}
	return alerts
}

// fingerprint produces a stable hash from a rule name and its label set.
func fingerprint(ruleName string, labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	sb.WriteString(ruleName)
	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(labels[k])
		sb.WriteString(",")
	}

	h := sha256.Sum256([]byte(sb.String()))
	return fmt.Sprintf("%x", h[:8])
}
