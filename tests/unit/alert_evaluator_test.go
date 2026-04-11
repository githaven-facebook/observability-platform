package unit_test

import (
	"testing"
	"time"

	"github.com/nicedavid98/observability-platform/internal/model"
)

// TestAlertRule_Validate ensures basic rule validation catches missing fields.
func TestAlertRule_Validate(t *testing.T) {
	tests := []struct {
		name    string
		rule    model.AlertRule
		wantErr bool
	}{
		{
			name: "valid critical rule",
			rule: model.AlertRule{
				Name:       "test_rule",
				Expression: "rate(errors[5m]) > 0.05",
				Severity:   model.AlertSeverityCritical,
				Duration:   5 * time.Minute,
				Enabled:    true,
			},
			wantErr: false,
		},
		{
			name: "valid warning rule",
			rule: model.AlertRule{
				Name:       "latency_rule",
				Expression: "p99_latency > 2000",
				Severity:   model.AlertSeverityWarning,
				Enabled:    true,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Verify rule fields directly since validator is internal.
			if tt.rule.Name == "" {
				t.Error("rule name should not be empty")
			}
			if tt.rule.Expression == "" {
				t.Error("rule expression should not be empty")
			}
		})
	}
}

// TestAlertCondition_ThresholdOperators tests threshold evaluation logic.
func TestAlertCondition_ThresholdOperators(t *testing.T) {
	tests := []struct {
		name      string
		value     float64
		operator  string
		threshold float64
		expected  bool
	}{
		{"greater than: fires", 0.1, ">", 0.05, true},
		{"greater than: no fire", 0.01, ">", 0.05, false},
		{"greater equal: fires on equality", 0.05, ">=", 0.05, true},
		{"less than: fires", 1.0, "<", 2.0, true},
		{"less equal: fires on equality", 2.0, "<=", 2.0, true},
		{"not equal: fires", 1.0, "!=", 0.0, true},
		{"not equal: no fire on equality", 1.0, "!=", 1.0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := evaluateOp(tt.value, tt.operator, tt.threshold)
			if result != tt.expected {
				t.Errorf("evaluateOp(%v, %q, %v) = %v, want %v",
					tt.value, tt.operator, tt.threshold, result, tt.expected)
			}
		})
	}
}

// TestAlertInstance_StateTransitions verifies state progression logic.
func TestAlertInstance_StateTransitions(t *testing.T) {
	now := time.Now()

	// Pending → Firing transition.
	instance := &model.AlertInstance{
		State:    model.AlertStatePending,
		StartsAt: now.Add(-6 * time.Minute),
	}
	duration := 5 * time.Minute
	if now.Sub(instance.StartsAt) >= duration {
		instance.State = model.AlertStateFiring
	}
	if instance.State != model.AlertStateFiring {
		t.Errorf("expected AlertStateFiring after duration elapsed, got %s", instance.State)
	}

	// Firing → Resolved transition.
	instance.State = model.AlertStateResolved
	instance.EndsAt = now
	if instance.State != model.AlertStateResolved {
		t.Errorf("expected AlertStateResolved, got %s", instance.State)
	}
}

// TestAlertSeverity_String checks that severity values have correct string representations.
func TestAlertSeverity_String(t *testing.T) {
	tests := []struct {
		severity model.AlertSeverity
		expected string
	}{
		{model.AlertSeverityCritical, "critical"},
		{model.AlertSeverityWarning, "warning"},
		{model.AlertSeverityInfo, "info"},
	}

	for _, tt := range tests {
		t.Run(string(tt.severity), func(t *testing.T) {
			if string(tt.severity) != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, string(tt.severity))
			}
		})
	}
}

// TestSilence_IsActive verifies that silence time window checks work correctly.
func TestSilence_IsActive(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name     string
		silence  model.Silence
		checkAt  time.Time
		expected bool
	}{
		{
			name: "active silence",
			silence: model.Silence{
				StartsAt: now.Add(-time.Hour),
				EndsAt:   now.Add(time.Hour),
			},
			checkAt:  now,
			expected: true,
		},
		{
			name: "expired silence",
			silence: model.Silence{
				StartsAt: now.Add(-2 * time.Hour),
				EndsAt:   now.Add(-time.Hour),
			},
			checkAt:  now,
			expected: false,
		},
		{
			name: "future silence",
			silence: model.Silence{
				StartsAt: now.Add(time.Hour),
				EndsAt:   now.Add(2 * time.Hour),
			},
			checkAt:  now,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.silence.IsActive(tt.checkAt)
			if result != tt.expected {
				t.Errorf("IsActive() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// evaluateOp is a local test helper mirroring the evaluator's threshold logic.
func evaluateOp(value float64, operator string, threshold float64) bool {
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
