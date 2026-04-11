// Package alerting implements the alert evaluation engine and notification system.
package alerting

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/nicedavid98/observability-platform/internal/model"
)

// RuleFile is the top-level structure of an alert rules YAML file.
type RuleFile struct {
	Groups []RuleGroup `yaml:"groups"`
}

// RuleGroup bundles related alert rules with a shared evaluation interval.
type RuleGroup struct {
	Name     string            `yaml:"name"`
	Interval time.Duration     `yaml:"interval"`
	Rules    []model.AlertRule `yaml:"rules"`
}

// RuleLoader reads and validates alert rule files from disk.
type RuleLoader struct {
	rulesFile string
}

// NewRuleLoader creates a RuleLoader targeting the given file path.
func NewRuleLoader(rulesFile string) *RuleLoader {
	return &RuleLoader{rulesFile: rulesFile}
}

// Load reads the rules file and returns all enabled rules.
func (l *RuleLoader) Load() ([]model.AlertRule, error) {
	f, err := os.Open(l.rulesFile)
	if err != nil {
		return nil, fmt.Errorf("open rules file %q: %w", l.rulesFile, err)
	}
	defer f.Close()

	var rf RuleFile
	if err := yaml.NewDecoder(f).Decode(&rf); err != nil {
		return nil, fmt.Errorf("decode rules file: %w", err)
	}

	var rules []model.AlertRule
	for _, g := range rf.Groups {
		for _, r := range g.Rules {
			if err := validateRule(r); err != nil {
				return nil, fmt.Errorf("rule %q in group %q: %w", r.Name, g.Name, err)
			}
			if r.Enabled {
				rules = append(rules, r)
			}
		}
	}
	return rules, nil
}

func validateRule(r model.AlertRule) error {
	if r.Name == "" {
		return fmt.Errorf("name is required")
	}
	if r.Expression == "" {
		return fmt.Errorf("expression is required")
	}
	switch r.Severity {
	case model.AlertSeverityCritical, model.AlertSeverityWarning, model.AlertSeverityInfo:
	default:
		return fmt.Errorf("invalid severity %q", r.Severity)
	}
	return nil
}
