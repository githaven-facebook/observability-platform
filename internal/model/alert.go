package model

import "time"

// AlertSeverity classifies the urgency of an alert.
type AlertSeverity string

const (
	AlertSeverityCritical AlertSeverity = "critical"
	AlertSeverityWarning  AlertSeverity = "warning"
	AlertSeverityInfo     AlertSeverity = "info"
)

// AlertState tracks the current lifecycle state of an alert instance.
type AlertState string

const (
	AlertStatePending  AlertState = "pending"
	AlertStateFiring   AlertState = "firing"
	AlertStateResolved AlertState = "resolved"
	AlertStateSilenced AlertState = "silenced"
)

// ConditionType specifies what kind of check an alert condition performs.
type ConditionType string

const (
	ConditionTypeThreshold     ConditionType = "threshold"
	ConditionTypeRateOfChange  ConditionType = "rate_of_change"
	ConditionTypeAbsence       ConditionType = "absence"
)

// AlertCondition defines a single evaluatable condition.
type AlertCondition struct {
	Type       ConditionType `yaml:"type"       json:"type"`
	Expression string        `yaml:"expression" json:"expression"`
	Operator   string        `yaml:"operator"   json:"operator"` // >, <, >=, <=, ==, !=
	Threshold  float64       `yaml:"threshold"  json:"threshold"`
	Duration   time.Duration `yaml:"duration"   json:"duration"`
}

// AlertRule defines the full specification for an alert including conditions and routing.
type AlertRule struct {
	Name                 string            `yaml:"name"                  json:"name"`
	Expression           string            `yaml:"expression"            json:"expression"`
	Conditions           []AlertCondition  `yaml:"conditions"            json:"conditions,omitempty"`
	Duration             time.Duration     `yaml:"duration"              json:"duration"`
	Severity             AlertSeverity     `yaml:"severity"              json:"severity"`
	Labels               map[string]string `yaml:"labels"                json:"labels,omitempty"`
	Annotations          map[string]string `yaml:"annotations"           json:"annotations,omitempty"`
	NotificationChannels []string          `yaml:"notification_channels" json:"notification_channels,omitempty"`
	Enabled              bool              `yaml:"enabled"               json:"enabled"`
}

// AlertInstance represents a currently active or recently resolved alert.
type AlertInstance struct {
	RuleID      string            `json:"rule_id"`
	RuleName    string            `json:"rule_name"`
	State       AlertState        `json:"state"`
	Severity    AlertSeverity     `json:"severity"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	StartsAt    time.Time         `json:"starts_at"`
	EndsAt      time.Time         `json:"ends_at,omitempty"`
	UpdatedAt   time.Time         `json:"updated_at"`
	Value       float64           `json:"value"`
	Fingerprint string            `json:"fingerprint"`
}

// AlertNotification is the payload sent to a notification channel.
type AlertNotification struct {
	GroupKey    string           `json:"group_key"`
	Status      AlertState       `json:"status"`
	Receiver    string           `json:"receiver"`
	GroupLabels map[string]string `json:"group_labels"`
	Alerts      []AlertInstance  `json:"alerts"`
	SentAt      time.Time        `json:"sent_at"`
}

// Silence suppresses alerts matching specific label matchers for a time window.
type Silence struct {
	ID        string            `json:"id"`
	Matchers  []LabelMatcher    `json:"matchers"`
	StartsAt  time.Time         `json:"starts_at"`
	EndsAt    time.Time         `json:"ends_at"`
	CreatedBy string            `json:"created_by"`
	Comment   string            `json:"comment"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}

// LabelMatcher defines a label key/value equality or regex match condition.
type LabelMatcher struct {
	Name    string `json:"name"`
	Value   string `json:"value"`
	IsRegex bool   `json:"is_regex"`
	IsEqual bool   `json:"is_equal"` // false means not-equal / not-regex
}

// IsActive reports whether the silence is currently in effect.
func (s *Silence) IsActive(now time.Time) bool {
	return !now.Before(s.StartsAt) && now.Before(s.EndsAt)
}
