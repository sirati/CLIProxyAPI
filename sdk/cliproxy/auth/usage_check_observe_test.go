package auth

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func usageURLFor(t *testing.T, provider string) *url.URL {
	t.Helper()
	raw := claudeUsageURL
	if provider == "codex" {
		raw = codexUsageURL
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// panelUsageFixture is a cooling-down credential on a manager whose usage
// guard runs, as in the service, with a recovery ticket waiting on its route.
func panelUsageFixture(t *testing.T, provider string) (*Manager, *Auth, string, *providerUsageExecutor, *usageGuard, string) {
	t.Helper()
	withQuotaCooldownEnabled(t)
	m, a, model, e, guard := usageRecoveryFixture(t, provider)
	guard.wake = make(chan string, 1)
	m.usageGuard = guard
	exhaustUsage(t, m, a, model)
	tracked := m.TrackRecovery(&Error{Code: "auth_unavailable", HTTPStatus: http.StatusServiceUnavailable, Message: "no auth available"}, []string{provider}, model)
	id := strings.TrimSuffix(strings.TrimPrefix(RecoveryMarker(tracked), " [cliproxy-recovery:"), "]")
	if status, ok := m.Recovery(id, time.Now()); !ok || status.State != "waiting" {
		t.Fatalf("recovery ticket not waiting: %+v %v", status, ok)
	}
	return m, a, model, e, guard, id
}

// The panel's usage refresh goes through the management api-call. Usage that
// is back must lift the cooldown and release recovery waits at once, without
// waiting for the poller's next check.
func TestPanelUsageCheckLiftsQuotaCooldown(t *testing.T) {
	for _, provider := range []string{"claude", "codex"} {
		t.Run(provider, func(t *testing.T) {
			m, a, model, e, _, id := panelUsageFixture(t, provider)
			ctx := context.Background()
			before, _ := m.GetByID(a.ID)
			m.ObserveUsageCheck(ctx, before, usageURLFor(t, provider), http.StatusOK, []byte(availableUsage(provider)))

			current, _ := m.GetByID(a.ID)
			if current.Quota.Exceeded || current.Unavailable || current.ModelStates[model].Quota.Exceeded {
				t.Fatalf("usage refresh left the cooldown: %+v", current.Quota)
			}
			if got := selectUsageGuarded(t, m, provider, model); got != a.ID {
				t.Fatalf("selected %s after the cooldown was lifted", got)
			}
			if status, _ := m.Recovery(id, time.Now()); status.State != "available" {
				t.Fatalf("recovery wait not released: %+v", status)
			}
			if e.calls[a.ID] != 0 {
				t.Fatalf("panel refresh triggered extra usage requests: %v", e.calls)
			}
		})
	}
}

func TestPanelUsageCheckKeepsCooldownWithoutEvidence(t *testing.T) {
	exhausted := map[string]string{
		"claude": `{"five_hour":{"utilization":3},"seven_day":{"utilization":100}}`,
		"codex":  `{"rate_limit":{"allowed":false,"limit_reached":true,"primary_window":{"used_percent":0},"secondary_window":{"used_percent":100}}}`,
	}
	for _, provider := range []string{"claude", "codex"} {
		t.Run(provider, func(t *testing.T) {
			m, a, model, _, _, id := panelUsageFixture(t, provider)
			ctx := context.Background()
			usage := usageURLFor(t, provider)
			check := func(name string, observe func(before *Auth)) {
				t.Helper()
				before, _ := m.GetByID(a.ID)
				observe(before)
				if _, err := m.SelectAuth(ctx, provider, model, cliproxyexecutor.Options{}); err == nil {
					t.Fatalf("%s lifted the cooldown", name)
				}
				if status, _ := m.Recovery(id, time.Now()); status.State != "waiting" {
					t.Fatalf("%s released the recovery wait: %+v", name, status)
				}
			}
			check("exhausted usage", func(before *Auth) {
				m.ObserveUsageCheck(ctx, before, usage, http.StatusOK, []byte(exhausted[provider]))
			})
			check("failed usage request", func(before *Auth) {
				m.ObserveUsageCheck(ctx, before, usage, http.StatusTooManyRequests, []byte(availableUsage(provider)))
			})
			check("other endpoint", func(before *Auth) {
				other, _ := url.Parse("https://api.anthropic.com/api/oauth/profile")
				m.ObserveUsageCheck(ctx, before, other, http.StatusOK, []byte(availableUsage(provider)))
			})
			check("failure after the request was sent", func(before *Auth) {
				exhaustUsage(t, m, a, model)
				m.ObserveUsageCheck(ctx, before, usage, http.StatusOK, []byte(availableUsage(provider)))
			})
		})
	}
}

func TestPanelUsageCheckWakesCodexPeersOnReset(t *testing.T) {
	m, a, _, _, guard, _ := panelUsageFixture(t, "codex")
	ctx := context.Background()
	if err := m.mutateAuthAvailability(ctx, a.ID, func(auth *Auth, _ time.Time) bool {
		auth.Quota.Signals = map[string]string{"X-Codex-Secondary-Used-Percent": "100"}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	before, _ := m.GetByID(a.ID)
	m.ObserveUsageCheck(ctx, before, usageURLFor(t, "codex"), http.StatusOK, []byte(availableUsage("codex")))
	select {
	case source := <-guard.wake:
		if source != a.ID {
			t.Fatalf("woke peers for %s, want %s", source, a.ID)
		}
	default:
		t.Fatal("weekly reset seen by the panel did not wake the other Codex accounts")
	}
}
