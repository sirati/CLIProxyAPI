package auth

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	internallogging "github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type providerUsageExecutor struct {
	*usageGuardExecutor
	bodies         map[string]string
	statuses       map[string]int
	beforeResponse func()
}

func (e *providerUsageExecutor) HttpRequest(ctx context.Context, auth *Auth, req *http.Request) (*http.Response, error) {
	if e.beforeResponse != nil {
		hook := e.beforeResponse
		e.beforeResponse = nil
		hook()
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls[auth.ID]++
	status := e.statuses[auth.ID]
	if status == 0 {
		status = 200
	}
	body, ok := e.bodies[auth.ID]
	if !ok {
		return nil, fmt.Errorf("missing usage fixture for %s", auth.ID)
	}
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}, nil
}

func availableUsage(provider string) string {
	if provider == "codex" {
		return `{"rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":2,"limit_window_seconds":18000},"secondary_window":{"used_percent":10,"limit_window_seconds":604800}}}`
	}
	return `{"five_hour":{"utilization":2},"seven_day":{"utilization":10}}`
}

func usageRecoveryFixture(t *testing.T, provider string) (*Manager, *Auth, string, *providerUsageExecutor, *usageGuard) {
	t.Helper()
	model := "usage-recovery-" + provider + "-" + t.Name()
	a, _ := guardedPair(provider, map[string]any{})
	m, base, guard := newUsageGuardManager(t, provider, model, a)
	e := &providerUsageExecutor{usageGuardExecutor: base, bodies: map[string]string{a.ID: availableUsage(provider)}, statuses: map[string]int{}}
	m.RegisterExecutor(e)
	return m, a, model, e, guard
}
func exhaustUsage(t *testing.T, m *Manager, auth *Auth, model string) {
	t.Helper()
	retry := 7 * 24 * time.Hour
	failure := &Error{HTTPStatus: 429, Code: "usage_limit_reached", Message: "weekly quota exhausted"}
	if auth.Provider == "claude" {
		failure = &Error{HTTPStatus: 429, Code: "rate_limit_error", Message: "rate limit exceeded"}
	}
	m.MarkResult(context.Background(), Result{AuthID: auth.ID, Provider: auth.Provider, Model: model, CredentialScope: true,
		Error: failure, RetryAfter: &retry})
	current, _ := m.GetByID(auth.ID)
	if !current.Quota.Exceeded {
		t.Fatal("fixture did not enter cooldown")
	}
}

func TestProviderUsageRestoresUnconfiguredQuota(t *testing.T) {
	withQuotaCooldownEnabled(t)
	for _, provider := range []string{"codex", "claude"} {
		t.Run(provider, func(t *testing.T) {
			m, a, model, _, guard := usageRecoveryFixture(t, provider)
			store := NewFileCooldownStateStore(t.TempDir())
			m.SetCooldownStateStore(store)
			exhaustUsage(t, m, a, model)
			if _, err := m.SelectAuth(context.Background(), provider, model, cliproxyexecutor.Options{}); err == nil {
				t.Fatal("exhausted auth selected")
			}
			m.checkUsageGuards(context.Background(), guard)
			current, _ := m.GetByID(a.ID)
			if current.Quota.Exceeded || current.Unavailable || current.ModelStates[model].Quota.Exceeded {
				t.Fatalf("quota stayed blocked: %+v", current)
			}
			if got := selectUsageGuarded(t, m, provider, model); got != a.ID {
				t.Fatalf("scheduler not republished: %s", got)
			}
			records, err := store.Load(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(records) != 0 {
				t.Fatalf("restored quota remained on disk: %+v", records)
			}
		})
	}
}

func TestProviderUsagePreservesIndependentModelCooldown(t *testing.T) {
	withQuotaCooldownEnabled(t)
	for _, provider := range []string{"codex", "claude"} {
		t.Run(provider, func(t *testing.T) {
			m, a, model, _, guard := usageRecoveryFixture(t, provider)
			sibling := model + "-independent"
			registry.GetGlobalRegistry().RegisterClient(a.ID, provider, []*registry.ModelInfo{{ID: model}, {ID: sibling}})
			retry := 12 * time.Hour
			m.MarkResult(context.Background(), Result{AuthID: a.ID, Provider: provider, Model: sibling, Error: &Error{HTTPStatus: 429, Message: "model rate limit"}, RetryAfter: &retry})
			before, _ := m.GetByID(a.ID)
			deadline := before.ModelStates[sibling].NextRetryAfter
			exhaustUsage(t, m, a, model)
			m.checkUsageGuards(context.Background(), guard)
			current, _ := m.GetByID(a.ID)
			if blocked, _, _ := isAuthBlockedForModel(current, model, time.Now()); blocked {
				t.Fatal("credential quota not released")
			}
			if blocked, _, _ := isAuthBlockedForModel(current, sibling, time.Now()); !blocked || !current.ModelStates[sibling].NextRetryAfter.Equal(deadline) {
				t.Fatal("independent model cooldown was erased or changed")
			}
		})
	}
}

func TestProviderUsageRequiresFreshCompleteEvidence(t *testing.T) {
	withQuotaCooldownEnabled(t)
	for _, tc := range []struct {
		name, provider, body string
		status               int
	}{
		{"weekly still full", "codex", `{"rate_limit":{"allowed":true,"primary_window":{"used_percent":0},"secondary_window":{"used_percent":100}}}`, 200},
		{"short still full", "codex", `{"rate_limit":{"allowed":true,"primary_window":{"used_percent":100},"secondary_window":{"used_percent":0}}}`, 200},
		{"upstream disallowed", "codex", `{"rate_limit":{"allowed":false,"primary_window":{"used_percent":0},"secondary_window":{"used_percent":0}}}`, 200},
		{"missing short window", "claude", `{"seven_day":{"utilization":0}}`, 200},
		{"Claude short still full", "claude", `{"five_hour":{"utilization":100},"seven_day":{"utilization":0}}`, 200},
		{"malformed", "codex", `{`, 200},
		{"generic rate limit", "claude", `{"error":{"type":"rate_limit_error"}}`, 429},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, a, model, e, guard := usageRecoveryFixture(t, tc.provider)
			exhaustUsage(t, m, a, model)
			e.bodies[a.ID] = tc.body
			e.statuses[a.ID] = tc.status
			m.checkUsageGuards(context.Background(), guard)
			current, _ := m.GetByID(a.ID)
			if !current.Quota.Exceeded {
				t.Fatal("incomplete or negative observation cleared cooldown")
			}
			if claudeSubscriptionBlocked(current) {
				t.Fatal("generic rate limit classified as expired subscription")
			}
		})
	}
	t.Run("newer failure wins", func(t *testing.T) {
		m, a, model, e, guard := usageRecoveryFixture(t, "codex")
		exhaustUsage(t, m, a, model)
		e.beforeResponse = func() { exhaustUsage(t, m, a, model) }
		m.checkUsageGuards(context.Background(), guard)
		current, _ := m.GetByID(a.ID)
		if !current.Quota.Exceeded {
			t.Fatal("stale observation cleared newer failure")
		}
	})
	t.Run("use once remains parked", func(t *testing.T) {
		m, a, model, _, guard := usageRecoveryFixture(t, "codex")
		exhaustUsage(t, m, a, model)
		if err := m.mutateAuthAvailability(context.Background(), a.ID, func(auth *Auth, _ time.Time) bool {
			auth.Metadata[MetadataUseOnce] = true
			auth.Metadata[metadataUseOnceRetired] = "stopped"
			return true
		}); err != nil {
			t.Fatal(err)
		}
		m.checkUsageGuards(context.Background(), guard)
		current, _ := m.GetByID(a.ID)
		if current.Disabled || !UseOnceStopped(current) {
			t.Fatal("usage restoration changed credential's enabled/stopped state")
		}
		if _, err := m.SelectAuth(context.Background(), "codex", model, cliproxyexecutor.Options{}); err == nil {
			t.Fatal("use once stop bypassed")
		}
	})
}

func TestClaudeExpiredSubscriptionStopsAutomaticPolling(t *testing.T) {
	m, a, _, e, guard := usageRecoveryFixture(t, "claude")
	e.statuses[a.ID] = 403
	e.bodies[a.ID] = `{"error":{"type":"permission_error","message":"OAuth authentication is currently not allowed for this organization.","details":{"error_code":"oauth_not_allowed_for_organization"}}}`
	m.checkUsageGuards(context.Background(), guard)
	current, _ := m.GetByID(a.ID)
	if !claudeSubscriptionBlocked(current) || current.Metadata[metadataUsagePollingBlocked] != claudeOrganizationOAuthBlocked {
		t.Fatal("subscription failure not recorded")
	}
	for i := 0; i < 3; i++ {
		m.checkUsageGuards(context.Background(), guard)
	}
	if e.calls[a.ID] != 1 {
		t.Fatalf("expired subscription queried again: %v", e.calls)
	}
	// A persisted marker also suppresses polling after service restart.
	restarted, base, newGuard := newUsageGuardManager(t, "claude", "expired-usage-restarted", current)
	restarted.checkUsageGuards(context.Background(), newGuard)
	if base.calls[a.ID] != 0 {
		t.Fatal("restart lost polling stop")
	}
	// An already known model error suppresses the very first probe.
	delete(current.Metadata, metadataUsagePollingBlocked)
	current.ModelStates = map[string]*ModelState{"m": {LastError: &Error{HTTPStatus: 403, Message: e.bodies[a.ID]}}}
	if automaticUsageEligible(current) {
		t.Fatal("known subscription failure still polled")
	}
}

func TestUsagePollingIntervalsAndCodexResetFanout(t *testing.T) {
	m, a, model, e, guard := usageRecoveryFixture(t, "codex")
	b := &Auth{ID: "codex-peer", Provider: "codex", Metadata: map[string]any{}}
	if _, err := m.Register(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	e.bodies[b.ID] = availableUsage("codex")
	now := time.Now()
	m.pollUsageGuards(context.Background(), guard, now, false, "", false)
	m.pollUsageGuards(context.Background(), guard, now.Add(5*time.Minute), false, "", false)
	if e.calls[a.ID] != 1 || e.calls[b.ID] != 1 {
		t.Fatalf("ordinary usage polled before 15m: %v", e.calls)
	}
	// Adding a guard changes the schedule to 5m without waiting for the old 15m deadline.
	if err := m.mutateAuthAvailability(context.Background(), a.ID, func(auth *Auth, _ time.Time) bool { auth.Metadata[MetadataMinRemaining] = 10.0; return true }); err != nil {
		t.Fatal(err)
	}
	m.pollUsageGuards(context.Background(), guard, now.Add(5*time.Minute), false, "", false)
	if e.calls[a.ID] != 2 || e.calls[b.ID] != 1 {
		t.Fatalf("guard did not keep 5m interval: %v", e.calls)
	}
	// This active account resets while the peer is not due. Only a fresh peer query can release it.
	exhaustUsage(t, m, b, model)
	guard.codexUsage[a.ID] = map[string]string{"X-Codex-Secondary-Used-Percent": "99"}
	m.pollUsageGuards(context.Background(), guard, now.Add(10*time.Minute), false, "", false)
	if e.calls[b.ID] != 2 {
		t.Fatalf("reset did not check waiting peer immediately: %v", e.calls)
	}
	current, _ := m.GetByID(b.ID)
	if current.Quota.Exceeded {
		t.Fatal("reset peer remained on cooldown")
	}
	// No recursive reset fanout when all peers were just checked.
	if e.calls[a.ID] != 3 {
		t.Fatalf("source fetched twice during fanout: %v", e.calls)
	}
	if credentialUsageInterval(&Auth{Provider: "codex", Metadata: map[string]any{MetadataUseOnce: true}}) != 5*time.Minute {
		t.Fatal("use once no longer polled every 5m")
	}
}

func TestCodexPassiveResetWakesPeerChecks(t *testing.T) {
	m, a, model, _, guard := usageRecoveryFixture(t, "codex")
	guard.wake = make(chan string, 1)
	m.usageGuard = guard
	// Captured headers use canonical casing, while older snapshots may use lowercase.
	if err := m.mutateAuthAvailability(context.Background(), a.ID, func(auth *Auth, _ time.Time) bool {
		auth.Quota.Signals = map[string]string{"x-codex-secondary-used-percent": "99"}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	ctx := internallogging.WithResponseHeadersHolder(context.Background())
	internallogging.SetResponseHeaders(ctx, http.Header{"X-Codex-Secondary-Used-Percent": []string{"0"}})
	m.MarkResult(ctx, Result{AuthID: a.ID, Provider: "codex", Model: model, Success: true})
	select {
	case source := <-guard.wake:
		if source != a.ID {
			t.Fatal("wrong reset source")
		}
	default:
		t.Fatal("passive reset did not wake poller")
	}
	m.MarkResult(ctx, Result{AuthID: a.ID, Provider: "codex", Model: model, Success: true})
	select {
	case <-guard.wake:
		t.Fatal("unchanged observation caused another fanout")
	default:
	}
	if codexUsageReset(map[string]string{"X-Codex-Secondary-Reset-At": "1"}, map[string]string{"X-Codex-Secondary-Reset-At": "2"}) {
		t.Fatal("timestamp-only change caused reset storm")
	}
}

func TestProviderUsageDoesNotReleaseModelSpecificOrAuthenticationFailures(t *testing.T) {
	withQuotaCooldownEnabled(t)
	for _, code := range []int{http.StatusTooManyRequests, http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			m, a, model, _, guard := usageRecoveryFixture(t, "codex")
			retry := time.Hour
			m.MarkResult(context.Background(), Result{AuthID: a.ID, Provider: "codex", Model: model, Error: &Error{HTTPStatus: code, Code: "usage_limit_reached", Message: "model quota or authentication failure"}, RetryAfter: &retry})
			before, _ := m.GetByID(a.ID)
			deadline := before.ModelStates[model].NextRetryAfter
			m.checkUsageGuards(context.Background(), guard)
			current, _ := m.GetByID(a.ID)
			if current.ModelStates[model].LastError == nil || !current.ModelStates[model].NextRetryAfter.Equal(deadline) {
				t.Fatal("unrelated failure released by account usage probe")
			}
		})
	}
}

func TestUsagePollingSkipsDisabledAndAPIKeyCredentials(t *testing.T) {
	for _, auth := range []*Auth{
		{Provider: "codex", Disabled: true},
		{Provider: "claude", Status: StatusDisabled},
		{Provider: "codex", Attributes: map[string]string{AttributeAPIKey: "test-key"}},
		{Provider: "gemini"},
	} {
		if automaticUsageEligible(auth) {
			t.Fatalf("unexpected automatic probe for %+v", auth)
		}
	}
	for _, provider := range []string{"claude", "codex"} {
		if credentialUsageInterval(&Auth{Provider: provider}) != 15*time.Minute {
			t.Fatal("ordinary recovery schedule is not 15m")
		}
		if credentialUsageInterval(&Auth{Provider: provider, Metadata: map[string]any{MetadataMinRemaining: 10.0}}) != 5*time.Minute {
			t.Fatal("minimum remaining schedule is not 5m")
		}
	}
}

func TestClaudeSubscriptionFailureFromRequestSuppressesFirstProbe(t *testing.T) {
	m, a, model, e, guard := usageRecoveryFixture(t, "claude")
	m.MarkResult(context.Background(), Result{AuthID: a.ID, Provider: "claude", Model: model, Error: &Error{HTTPStatus: 403, Message: `{"error":{"details":{"error_code":"oauth_not_allowed_for_organization"}}}`}})
	current, _ := m.GetByID(a.ID)
	if current.Metadata[metadataUsagePollingBlocked] != claudeOrganizationOAuthBlocked {
		t.Fatal("request failure did not persist polling stop")
	}
	m.checkUsageGuards(context.Background(), guard)
	if e.calls[a.ID] != 0 {
		t.Fatal("known subscription failure still queried usage")
	}
}
