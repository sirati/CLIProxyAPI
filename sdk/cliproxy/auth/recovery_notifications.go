package auth

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net/http"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const recoveryTicketTTL = 24 * time.Hour
const maxRecoveryTickets = 4096

type recoveryTicket struct {
	providers []string
	model     string
	expires   time.Time
}

// RecoveryStatus describes scheduling availability, not a successful upstream probe.
// It deliberately exposes no credential identity or secrets.
type RecoveryStatus struct {
	ID        string     `json:"id"`
	State     string     `json:"state"`
	ExpiresAt time.Time  `json:"expires_at"`
	RetryAt   *time.Time `json:"retry_at,omitempty"`
}

type recoveryError struct {
	error
	id string
}

func (e *recoveryError) Unwrap() error { return e.error }
func (e *recoveryError) Error() string { return appendRecoveryMarker(e.error.Error(), e.id) }

// Headers preserves passthrough headers read by executionErrorMessage.
func (e *recoveryError) Headers() http.Header {
	var source interface{ Headers() http.Header }
	if errors.As(e.error, &source) {
		return source.Headers()
	}
	return nil
}

func (e *recoveryError) ResponseBody() []byte {
	var body interface{ ResponseBody() []byte }
	if errors.As(e.error, &body) && len(body.ResponseBody()) > 0 {
		return []byte(appendRecoveryMarker(string(body.ResponseBody()), e.id))
	}
	return nil
}

// RecoveryMarker survives protocol-specific usage-limit error conversion.
func RecoveryMarker(err error) string {
	var wrapped *recoveryError
	if errors.As(err, &wrapped) {
		return " [cliproxy-recovery:" + wrapped.id + "]"
	}
	return ""
}

func appendRecoveryMarker(message, id string) string {
	marker := " [cliproxy-recovery:" + id + "]"
	if gjson.Valid(message) {
		for _, path := range []string{"error.message", "message"} {
			if value := gjson.Get(message, path); value.Type == gjson.String {
				if result, err := sjson.Set(message, path, value.String()+marker); err == nil {
					return result
				}
			}
		}
	}
	return message + marker
}

// TrackRecovery attaches a bounded, opaque ticket only when a retryable failure
// leaves the requested route unavailable. The caller controls opt-in configuration.
func (m *Manager) TrackRecovery(err error, providers []string, model string) error {
	if m == nil || err == nil || len(providers) == 0 || IsTerminalAuthError(err) {
		return err
	}
	if RecoveryMarker(err) != "" {
		return err
	}
	status := statusCodeFromError(err)
	var authErr *Error
	selection := errors.As(err, &authErr) && (authErr.Code == "auth_unavailable" || authErr.Code == "auth_not_found")
	if !selection && status != 429 && status != 502 && status != 503 && status != 504 {
		return err
	}
	now := time.Now()
	ticket := recoveryTicket{providers: append([]string(nil), providers...), model: model, expires: now.Add(recoveryTicketTTL)}
	if ready, _ := m.routeRecovery(ticket, now); ready {
		return err
	}
	var bytes [12]byte
	if _, errRandom := rand.Read(bytes[:]); errRandom != nil {
		return err
	}
	id := base64.RawURLEncoding.EncodeToString(bytes[:])
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.recoveryTickets == nil {
		m.recoveryTickets = make(map[string]recoveryTicket)
	}
	for key, existing := range m.recoveryTickets {
		if !existing.expires.After(now) {
			delete(m.recoveryTickets, key)
		}
	}
	if len(m.recoveryTickets) >= maxRecoveryTickets {
		return err
	}
	m.recoveryTickets[id] = ticket
	return &recoveryError{error: err, id: id}
}

// Recovery checks the same model support and availability rules as selection,
// without selecting a credential, reserving capacity, or issuing a model request.
func (m *Manager) Recovery(id string, now time.Time) (RecoveryStatus, bool) {
	if m == nil {
		return RecoveryStatus{}, false
	}
	m.mu.RLock()
	ticket, ok := m.recoveryTickets[id]
	m.mu.RUnlock()
	if !ok || !ticket.expires.After(now) {
		return RecoveryStatus{}, false
	}
	ready, retry := m.routeRecovery(ticket, now)
	status := RecoveryStatus{ID: id, State: "waiting", ExpiresAt: ticket.expires}
	if ready {
		status.State = "available"
	} else if !retry.IsZero() {
		status.RetryAt = &retry
	}
	return status, true
}

func (m *Manager) routeRecovery(ticket recoveryTicket, now time.Time) (bool, time.Time) {
	var earliest time.Time
	for _, auth := range m.List() {
		match := false
		for _, provider := range ticket.providers {
			if auth.Provider == provider {
				match = true
				break
			}
		}
		if !match || !m.authSupportsRouteModel(registry.GetGlobalRegistry(), auth, ticket.model) {
			continue
		}
		blocked, _, retry := isAuthBlockedForModel(auth, m.selectionModelForAuth(auth, ticket.model), now)
		if !blocked {
			return true, time.Time{}
		}
		if retry.After(now) && (earliest.IsZero() || retry.Before(earliest)) {
			earliest = retry
		}
	}
	return false, earliest
}

// RecoveryNotificationsEnabled is false by default and on Home-managed instances,
// whose authoritative routing state lives in the control plane.
func (m *Manager) RecoveryNotificationsEnabled() bool {
	if m == nil {
		return false
	}
	cfg, _ := m.runtimeConfig.Load().(*config.Config)
	return cfg != nil && cfg.Routing.RecoveryNotifications && !cfg.Home.Enabled
}
