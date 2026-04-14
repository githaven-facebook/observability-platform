package alerting

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/model"
	"github.com/nicedavid98/observability-platform/internal/query"
)

// EvaluationResult holds the outcome of evaluating a single alert rule.
type EvaluationResult struct {
	Rule    model.AlertRule
	Firing  bool
	Value   float64
	Message string
}

// Evaluator checks alert rule conditions against live metric and log data.
type Evaluator struct {
	metricEngine *query.MetricQueryEngine
	logger       *zap.Logger
}

// NewEvaluator creates an Evaluator backed by the given metric query engine.
func NewEvaluator(metricEngine *query.MetricQueryEngine, logger *zap.Logger) *Evaluator {
	return &Evaluator{metricEngine: metricEngine, logger: logger}
}

// Evaluate assesses a single alert rule and returns whether it is currently firing.
func (e *Evaluator) Evaluate(ctx context.Context, rule model.AlertRule) (*EvaluationResult, error) { //nolint:gocritic // rule is an interface-like value type
	now := time.Now()
	lookback := 5 * time.Minute
	if rule.Duration > 0 {
		lookback = rule.Duration
	}

	result, err := e.metricEngine.Query(ctx, query.QueryParams{
		Query:     rule.Expression,
		StartTime: now.Add(-lookback),
		EndTime:   now,
	})
	if err != nil {
		return nil, fmt.Errorf("evaluate rule %q: %w", rule.Name, err)
	}

	evalResult := &EvaluationResult{Rule: rule}

	if len(result.Result) == 0 {
		// No data — check for absence condition.
		for _, cond := range rule.Conditions {
			if cond.Type == model.ConditionTypeAbsence {
				evalResult.Firing = true
				evalResult.Message = fmt.Sprintf("no data for %s", lookback)
				return evalResult, nil
			}
		}
		return evalResult, nil
	}

	// Use the most recent value from the first series.
	latestValue := extractLatestValue(result.Result[0].Values)
	evalResult.Value = latestValue

	for _, cond := range rule.Conditions {
		switch cond.Type {
		case model.ConditionTypeThreshold:
			if evaluateThreshold(latestValue, cond.Operator, cond.Threshold) {
				evalResult.Firing = true
				evalResult.Message = fmt.Sprintf("value %.4f %s threshold %.4f",
					latestValue, cond.Operator, cond.Threshold)
			}
		case model.ConditionTypeRateOfChange:
			rate := computeRateOfChange(result.Result[0].Values)
			if evaluateThreshold(math.Abs(rate), cond.Operator, cond.Threshold) {
				evalResult.Firing = true
				evalResult.Message = fmt.Sprintf("rate of change %.4f %s threshold %.4f",
					rate, cond.Operator, cond.Threshold)
			}
		case model.ConditionTypeAbsence:
			// Handled above — data is present so absence is not triggered.
		}
		if evalResult.Firing {
			break
		}
	}

	// If no explicit conditions, fall back to evaluating the top-level expression threshold.
	if len(rule.Conditions) == 0 && latestValue > 0 {
		evalResult.Firing = true
		evalResult.Value = latestValue
	}

	e.logger.Debug("evaluated rule",
		zap.String("rule", rule.Name),
		zap.Bool("firing", evalResult.Firing),
		zap.Float64("value", evalResult.Value),
	)
	return evalResult, nil
}

func evaluateThreshold(value float64, operator string, threshold float64) bool {
	switch operator {
	case ">":
		return value > threshold
	case ">=":
		return value >= threshold
	case "<":
		return value < threshold
	case "<=":
		return value <= threshold
	case "==", "=":
		return value == threshold
	case "!=":
		return value != threshold
	default:
		return false
	}
}

func extractLatestValue(values [][]interface{}) float64 {
	if len(values) == 0 {
		return 0
	}
	last := values[len(values)-1]
	if len(last) < 2 {
		return 0
	}
	valStr, _ := last[1].(string)
	v, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return 0
	}
	return v
}

func computeRateOfChange(values [][]interface{}) float64 {
	if len(values) < 2 {
		return 0
	}
	first := extractLatestValueAt(values[0])
	last := extractLatestValueAt(values[len(values)-1])
	if first == 0 {
		return 0
	}
	return (last - first) / first
}

func extractLatestValueAt(v []interface{}) float64 {
	if len(v) < 2 {
		return 0
	}
	valStr, _ := v[1].(string)
	f, _ := strconv.ParseFloat(valStr, 64)
	return f
}
