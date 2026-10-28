package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/tidwall/gjson"
)

func TestRecoveryTracksModelAndCredentialAvailability(t *testing.T) {
	const model = "recovery-test-model"
	a, _ := guardedPair("claude", map[string]any{MetadataUseOnce: true, metadataUseOnceRetired: "stopped"})
	m, _, _ := newUsageGuardManager(t, "claude", model, a)
	original := &Error{Code: "auth_unavailable", HTTPStatus: 503, Message: `{"error":{"message":"unavailable","type":"rate_limit_error"}}`}
	tracked := m.TrackRecovery(original, []string{"claude"}, model)
	marker := RecoveryMarker(tracked)
	if marker == "" || !errors.Is(tracked, original) {
		t.Fatalf("lost marker, error identity or JSON: %v", tracked)
	}
	if !gjson.Valid(appendRecoveryMarker(original.Message, "test")) {
		t.Fatal("structured error was corrupted")
	}
	id := strings.TrimSuffix(strings.TrimPrefix(marker, " [cliproxy-recovery:"), "]")
	now := time.Now()
	status, ok := m.Recovery(id, now)
	if !ok || status.State != "waiting" || status.RetryAt != nil {
		t.Fatalf("stopped status: %+v %v", status, ok)
	}
	// Another provider and an unsupported model cannot wake this route.
	b := &Auth{ID: "recovery-other-provider", Provider: "codex", Status: StatusActive}
	if _, err := m.Register(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(b.ID, "codex", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(b.ID) })
	if status, _ := m.Recovery(id, now); status.State != "waiting" {
		t.Fatal("wrong provider woke route")
	}
	retry := now.Add(time.Hour)
	if err := m.mutateAuthAvailability(context.Background(), a.ID, func(auth *Auth, _ time.Time) bool {
		delete(auth.Metadata, metadataUseOnceRetired)
		auth.Unavailable = true
		auth.NextRetryAfter = retry
		return true
	}); err != nil {
		t.Fatal(err)
	}
	status, _ = m.Recovery(id, now)
	if status.State != "waiting" || status.RetryAt == nil || !status.RetryAt.Equal(retry) {
		t.Fatalf("cooldown status: %+v", status)
	}
	status, _ = m.Recovery(id, retry.Add(time.Second))
	if status.State != "available" {
		t.Fatalf("expired cooldown: %+v", status)
	}
	if _, ok := m.Recovery(id, now.Add(recoveryTicketTTL+time.Minute)); ok {
		t.Fatal("expired ticket remained available")
	}
	if _, ok := m.Recovery("unknown", now); ok {
		t.Fatal("unknown ticket accepted")
	}
	if trackedAgain := m.TrackRecovery(tracked, []string{"claude"}, model); trackedAgain != tracked {
		t.Fatal("duplicate ticket")
	}
}

func TestRecoverySkipsAvailableAndTerminalErrors(t *testing.T) {
	a, _ := guardedPair("claude", nil)
	m, _, _ := newUsageGuardManager(t, "claude", "recovery-ready-model", a)
	err := &Error{HTTPStatus: 429, Message: "quota"}
	if got := m.TrackRecovery(err, []string{"claude"}, "recovery-ready-model"); got != err {
		t.Fatal("available route got ticket")
	}
	terminal := NewTerminalAuthError(&Error{Code: "auth_unavailable", HTTPStatus: 503}, errors.New("revoked"))
	if got := m.TrackRecovery(terminal, []string{"claude"}, "missing"); got != terminal {
		t.Fatal("terminal error got ticket")
	}
	bad := &Error{HTTPStatus: 400, Message: "invalid request"}
	if got := m.TrackRecovery(bad, []string{"claude"}, "missing"); got != bad {
		t.Fatal("invalid request got ticket")
	}
	if m.RecoveryNotificationsEnabled() {
		t.Fatal("enabled by default")
	}
	cfg := &config.Config{Routing: config.RoutingConfig{RecoveryNotifications: true}}
	m.SetConfig(cfg)
	if !m.RecoveryNotificationsEnabled() {
		t.Fatal("opt in ignored")
	}
	cfg.Home.Enabled = true
	m.SetConfig(cfg)
	if m.RecoveryNotificationsEnabled() {
		t.Fatal("Home local ticket enabled")
	}
}

func TestUseOnceTargetZeroPreservesSavedMinimum(t *testing.T) {
	auth := &Auth{Provider: "codex", Metadata: map[string]any{MetadataUseOnce: true, MetadataMinRemaining: 25.0, MetadataUseOnceTarget: 0.0}}
	settings, _ := usageGuardSettingsOf(auth)
	if got := decideUsageGuard(settings, 10); got.action != usageGuardAllow {
		t.Fatalf("zero target held at saved minimum: %+v", got)
	}
	if got := decideUsageGuard(settings, 0); got.action != usageGuardRetire {
		t.Fatal("zero target failed to stop at zero")
	}
	delete(auth.Metadata, MetadataUseOnceTarget)
	settings, _ = usageGuardSettingsOf(auth)
	if got := decideUsageGuard(settings, 25); got.action != usageGuardRetire {
		t.Fatal("saved target failed to stop at threshold")
	}
}

func TestRecoveryTicketCapacityAndCleanup(t *testing.T) {
	m := NewManager(nil, nil, nil)
	now := time.Now()
	m.recoveryTickets = make(map[string]recoveryTicket, maxRecoveryTickets)
	for i := 0; i < maxRecoveryTickets; i++ {
		m.recoveryTickets[fmt.Sprint(i)] = recoveryTicket{expires: now.Add(time.Hour)}
	}
	err := &Error{Code: "auth_unavailable", HTTPStatus: 503, Message: "unavailable"}
	if got := m.TrackRecovery(err, []string{"claude"}, "missing"); got != err {
		t.Fatal("ticket limit exceeded")
	}
	m.recoveryTickets["0"] = recoveryTicket{expires: now.Add(-time.Second)}
	got := m.TrackRecovery(err, []string{"claude"}, "missing")
	if RecoveryMarker(got) == "" || len(m.recoveryTickets) != maxRecoveryTickets {
		t.Fatal("expired ticket did not free capacity")
	}
}
