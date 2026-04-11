package alerting

import (
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/nicedavid98/observability-platform/internal/model"
)

// SilenceManager manages alert silences with label-matcher based filtering.
type SilenceManager struct {
	mu       sync.RWMutex
	silences map[string]*model.Silence
	logger   *zap.Logger
}

// NewSilenceManager creates an empty SilenceManager.
func NewSilenceManager(logger *zap.Logger) *SilenceManager {
	return &SilenceManager{
		silences: make(map[string]*model.Silence),
		logger:   logger,
	}
}

// Create adds a new silence and returns its assigned ID.
func (m *SilenceManager) Create(silence model.Silence) (string, error) {
	if err := validateSilence(silence); err != nil {
		return "", fmt.Errorf("invalid silence: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	silence.ID = uuid.New().String()
	silence.CreatedAt = time.Now()
	silence.UpdatedAt = time.Now()
	m.silences[silence.ID] = &silence

	m.logger.Info("silence created",
		zap.String("id", silence.ID),
		zap.Time("ends_at", silence.EndsAt),
		zap.String("created_by", silence.CreatedBy),
	)
	return silence.ID, nil
}

// Update replaces an existing silence's fields.
func (m *SilenceManager) Update(id string, updated model.Silence) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.silences[id]
	if !ok {
		return fmt.Errorf("silence %s not found", id)
	}

	updated.ID = id
	updated.CreatedAt = existing.CreatedAt
	updated.UpdatedAt = time.Now()
	m.silences[id] = &updated
	return nil
}

// Delete removes a silence by ID.
func (m *SilenceManager) Delete(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.silences[id]; !ok {
		return fmt.Errorf("silence %s not found", id)
	}
	delete(m.silences, id)
	return nil
}

// List returns all currently active silences.
func (m *SilenceManager) List() []model.Silence {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := time.Now()
	var active []model.Silence
	for _, s := range m.silences {
		if s.IsActive(now) {
			active = append(active, *s)
		}
	}
	return active
}

// IsSilenced reports whether the given alert instance is covered by an active silence.
func (m *SilenceManager) IsSilenced(alert model.AlertInstance) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := time.Now()
	for _, s := range m.silences {
		if !s.IsActive(now) {
			continue
		}
		if matchesAll(s.Matchers, alert.Labels) {
			return true
		}
	}
	return false
}

// matchesAll returns true if every matcher in the list matches the given label set.
func matchesAll(matchers []model.LabelMatcher, labels map[string]string) bool {
	for _, m := range matchers {
		val, ok := labels[m.Name]
		if !ok {
			return false
		}
		if m.IsRegex {
			matched, err := regexp.MatchString(m.Value, val)
			if err != nil || !matched {
				return false
			}
		} else if m.IsEqual {
			if val != m.Value {
				return false
			}
		} else {
			if val == m.Value {
				return false
			}
		}
	}
	return true
}

func validateSilence(s model.Silence) error {
	if len(s.Matchers) == 0 {
		return fmt.Errorf("at least one matcher is required")
	}
	if s.StartsAt.IsZero() {
		return fmt.Errorf("starts_at is required")
	}
	if s.EndsAt.IsZero() || !s.EndsAt.After(s.StartsAt) {
		return fmt.Errorf("ends_at must be after starts_at")
	}
	if s.CreatedBy == "" {
		return fmt.Errorf("created_by is required")
	}
	return nil
}
