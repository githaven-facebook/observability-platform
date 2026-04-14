package alerting

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/smtp"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/model"
)

// NotificationSender is the interface for dispatching alert notifications.
type NotificationSender interface {
	Send(ctx context.Context, notification model.AlertNotification) error
	Name() string
}

// PagerDutyNotifier sends alert events to PagerDuty via the v2 Events API.
type PagerDutyNotifier struct {
	integrationKey string
	httpClient     *http.Client
	logger         *zap.Logger
}

const pagerDutyEventsURL = "https://events.pagerduty.com/v2/enqueue"

// NewPagerDutyNotifier creates a PagerDutyNotifier with the given integration key.
func NewPagerDutyNotifier(integrationKey string, logger *zap.Logger) *PagerDutyNotifier {
	return &PagerDutyNotifier{
		integrationKey: integrationKey,
		httpClient:     &http.Client{Timeout: 10 * time.Second},
		logger:         logger,
	}
}

// Name returns the notifier identifier.
func (n *PagerDutyNotifier) Name() string { return "pagerduty" }

// Send dispatches a PagerDuty v2 event for each firing alert in the notification.
func (n *PagerDutyNotifier) Send(ctx context.Context, notification model.AlertNotification) error { //nolint:gocritic // AlertNotification passed by value per NotificationSender interface
	for i := range notification.Alerts {
		if err := n.sendEvent(ctx, notification.Alerts[i], notification.Status); err != nil {
			n.logger.Error("pagerduty send failed",
				zap.String("rule", notification.Alerts[i].RuleName),
				zap.Error(err),
			)
			return err
		}
	}
	return nil
}

func (n *PagerDutyNotifier) sendEvent(ctx context.Context, alert model.AlertInstance, state model.AlertState) error { //nolint:gocritic // AlertInstance passed by value for immutable use
	action := "trigger"
	if state == model.AlertStateResolved {
		action = "resolve"
	}

	payload := map[string]interface{}{
		"routing_key":  n.integrationKey,
		"event_action": action,
		"dedup_key":    alert.Fingerprint,
		"payload": map[string]interface{}{
			"summary":   alert.Annotations["summary"],
			"source":    alert.Labels["service"],
			"severity":  strings.ToLower(string(alert.Severity)),
			"timestamp": alert.StartsAt.Format(time.RFC3339),
			"custom_details": map[string]interface{}{
				"value":  alert.Value,
				"labels": alert.Labels,
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal pagerduty payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, pagerDutyEventsURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create pagerduty request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send pagerduty event: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("pagerduty returned status %d", resp.StatusCode)
	}
	return nil
}

// SlackNotifier sends alert messages to Slack via an incoming webhook.
type SlackNotifier struct {
	webhookURL string
	channel    string
	httpClient *http.Client
	logger     *zap.Logger
}

// NewSlackNotifier creates a SlackNotifier targeting the given webhook URL.
func NewSlackNotifier(webhookURL, channel string, logger *zap.Logger) *SlackNotifier {
	return &SlackNotifier{
		webhookURL: webhookURL,
		channel:    channel,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		logger:     logger,
	}
}

// Name returns the notifier identifier.
func (n *SlackNotifier) Name() string { return "slack" }

// Send posts a Slack message with rich block formatting for the alert notification.
func (n *SlackNotifier) Send(ctx context.Context, notification model.AlertNotification) error { //nolint:gocritic // AlertNotification passed by value per NotificationSender interface
	color := "#e01e5a" // red for firing
	if notification.Status == model.AlertStateResolved {
		color = "#2eb67d" // green for resolved
	}

	var alertTexts []string
	for i := range notification.Alerts {
		a := notification.Alerts[i]
		alertTexts = append(alertTexts, fmt.Sprintf("*%s* — %s (value: %.4f)", a.RuleName, a.Severity, a.Value))
	}

	payload := map[string]interface{}{
		"channel": n.channel,
		"attachments": []map[string]interface{}{
			{
				"color":     color,
				"title":     fmt.Sprintf("[%s] %d alert(s)", strings.ToUpper(string(notification.Status)), len(notification.Alerts)),
				"text":      strings.Join(alertTexts, "\n"),
				"footer":    "observability-platform",
				"ts":        notification.SentAt.Unix(),
				"mrkdwn_in": []string{"text"},
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal slack payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create slack request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send slack message: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("slack returned status %d", resp.StatusCode)
	}
	return nil
}

// EmailNotifier sends alert notifications via SMTP.
type EmailNotifier struct {
	smtpAddr string
	from     string
	to       []string
	auth     smtp.Auth
	logger   *zap.Logger
}

// NewEmailNotifier creates an EmailNotifier with the given SMTP configuration.
func NewEmailNotifier(smtpAddr, from string, to []string, username, password string, logger *zap.Logger) *EmailNotifier {
	host := smtpAddr
	if idx := strings.Index(smtpAddr, ":"); idx > 0 {
		host = smtpAddr[:idx]
	}
	return &EmailNotifier{
		smtpAddr: smtpAddr,
		from:     from,
		to:       to,
		auth:     smtp.PlainAuth("", username, password, host),
		logger:   logger,
	}
}

// Name returns the notifier identifier.
func (n *EmailNotifier) Name() string { return "email" }

// Send sends an email summarising the alert notification.
func (n *EmailNotifier) Send(_ context.Context, notification model.AlertNotification) error { //nolint:gocritic // AlertNotification passed by value per NotificationSender interface
	subject := fmt.Sprintf("[%s] %d alert(s) firing", strings.ToUpper(string(notification.Status)), len(notification.Alerts))
	var body strings.Builder
	body.WriteString(subject + "\r\n\r\n")
	for i := range notification.Alerts {
		a := notification.Alerts[i]
		body.WriteString(fmt.Sprintf("Alert: %s\r\nSeverity: %s\r\nValue: %.4f\r\nStarted: %s\r\n\r\n",
			a.RuleName, a.Severity, a.Value, a.StartsAt.Format(time.RFC3339)))
	}

	msg := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s",
		n.from, strings.Join(n.to, ", "), subject, body.String())

	if err := smtp.SendMail(n.smtpAddr, n.auth, n.from, n.to, []byte(msg)); err != nil {
		return fmt.Errorf("send email: %w", err)
	}
	return nil
}

// GroupingNotifier wraps another NotificationSender and suppresses repeat notifications
// within a configurable repeat interval.
type GroupingNotifier struct {
	inner          NotificationSender
	repeatInterval time.Duration
	lastSent       map[string]time.Time
	logger         *zap.Logger
}

// NewGroupingNotifier wraps the given notifier with grouping/dedup logic.
func NewGroupingNotifier(inner NotificationSender, repeatInterval time.Duration, logger *zap.Logger) *GroupingNotifier {
	return &GroupingNotifier{
		inner:          inner,
		repeatInterval: repeatInterval,
		lastSent:       make(map[string]time.Time),
		logger:         logger,
	}
}

// Name returns the underlying notifier's name.
func (n *GroupingNotifier) Name() string { return n.inner.Name() }

// Send dispatches the notification only if enough time has passed since the last send
// for the same group key.
func (n *GroupingNotifier) Send(ctx context.Context, notification model.AlertNotification) error { //nolint:gocritic // AlertNotification passed by value per NotificationSender interface
	key := notification.GroupKey
	if last, ok := n.lastSent[key]; ok && time.Since(last) < n.repeatInterval {
		n.logger.Debug("suppressing repeat notification",
			zap.String("group_key", key),
			zap.Duration("repeat_interval", n.repeatInterval),
		)
		return nil
	}
	if err := n.inner.Send(ctx, notification); err != nil {
		return err
	}
	n.lastSent[key] = time.Now()
	return nil
}
