package auth

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	internallogging "github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func claudeWeeklyAuth(id string, signals map[string]string) *Auth {
	return &Auth{ID: id, Provider: "claude", Quota: QuotaState{Signals: signals}}
}

func claudeWeeklySignals(weeklyReset time.Time) map[string]string {
	return map[string]string{
		"Anthropic-Ratelimit-Unified-7d-Status":      "allowed",
		"Anthropic-Ratelimit-Unified-7d-Utilization": "0.4",
		"Anthropic-Ratelimit-Unified-7d-Reset":       strconv.FormatInt(weeklyReset.Unix(), 10),
	}
}

func pickClaudeWeekly(t *testing.T, model string, auths ...*Auth) string {
	t.Helper()
	selector := NewClaudeWeeklyResetFirstSelector(&FillFirstSelector{})
	got, err := selector.Pick(context.Background(), "claude", model, cliproxyexecutor.Options{}, auths)
	if err != nil {
		t.Fatalf("Pick() error = %v", err)
	}
	return got.ID
}

func TestClaudeWeeklyResetFirst_PicksSoonestWeeklyReset(t *testing.T) {
	t.Parallel()
	now := time.Now()
	got := pickClaudeWeekly(t, "claude-opus-5-5",
		claudeWeeklyAuth("a", claudeWeeklySignals(now.Add(6*24*time.Hour))),
		claudeWeeklyAuth("b", claudeWeeklySignals(now.Add(20*time.Hour))),
		claudeWeeklyAuth("c", claudeWeeklySignals(now.Add(3*24*time.Hour))),
	)
	if got != "b" {
		t.Fatalf("picked %q, want b (soonest weekly reset)", got)
	}
}

func TestClaudeWeeklyResetFirst_RunningWindowBeatsUnobserved(t *testing.T) {
	t.Parallel()
	now := time.Now()
	got := pickClaudeWeekly(t, "claude-opus-5-5",
		claudeWeeklyAuth("a", nil),
		claudeWeeklyAuth("z", claudeWeeklySignals(now.Add(6*24*time.Hour))),
	)
	if got != "z" {
		t.Fatalf("picked %q, want z (only credential with a running weekly window)", got)
	}
}

func TestClaudeWeeklyResetFirst_PassedResetIsNotRunning(t *testing.T) {
	t.Parallel()
	now := time.Now()
	got := pickClaudeWeekly(t, "claude-opus-5-5",
		claudeWeeklyAuth("a", claudeWeeklySignals(now.Add(-time.Hour))),
		claudeWeeklyAuth("z", claudeWeeklySignals(now.Add(5*24*time.Hour))),
	)
	if got != "z" {
		t.Fatalf("picked %q, want z (a's window already reset)", got)
	}
}

func TestClaudeWeeklyResetFirst_NoRunningWindowUsesFallback(t *testing.T) {
	t.Parallel()
	now := time.Now()
	got := pickClaudeWeekly(t, "claude-opus-5-5",
		claudeWeeklyAuth("b", nil),
		claudeWeeklyAuth("a", claudeWeeklySignals(now.Add(-time.Minute))),
	)
	if got != "a" {
		t.Fatalf("picked %q, want a (fill-first fallback)", got)
	}
}

func TestClaudeWeeklyResetFirst_SkipsRejectedWindows(t *testing.T) {
	t.Parallel()
	now := time.Now()
	fiveHourOut := claudeWeeklySignals(now.Add(time.Hour))
	fiveHourOut["Anthropic-Ratelimit-Unified-5h-Status"] = "rejected"
	fiveHourOut["Anthropic-Ratelimit-Unified-5h-Reset"] = strconv.FormatInt(now.Add(2*time.Hour).Unix(), 10)
	weekOut := claudeWeeklySignals(now.Add(2 * time.Hour))
	weekOut["Anthropic-Ratelimit-Unified-7d-Status"] = "rejected"

	got := pickClaudeWeekly(t, "claude-opus-5-5",
		claudeWeeklyAuth("a", fiveHourOut),
		claudeWeeklyAuth("b", weekOut),
		claudeWeeklyAuth("c", claudeWeeklySignals(now.Add(4*24*time.Hour))),
	)
	if got != "c" {
		t.Fatalf("picked %q, want c (a and b report rejected windows)", got)
	}
}

func TestClaudeWeeklyResetFirst_FableWindowOnlyGatesFable(t *testing.T) {
	t.Parallel()
	now := time.Now()
	fableOut := claudeWeeklySignals(now.Add(time.Hour))
	fableOut["Anthropic-Ratelimit-Unified-7d_oi-Status"] = "rejected"
	fableOut["Anthropic-Ratelimit-Unified-7d_oi-Reset"] = strconv.FormatInt(now.Add(time.Hour).Unix(), 10)
	auths := []*Auth{
		claudeWeeklyAuth("a", fableOut),
		claudeWeeklyAuth("b", claudeWeeklySignals(now.Add(3*24*time.Hour))),
	}

	if got := pickClaudeWeekly(t, "claude-opus-5-5", auths...); got != "a" {
		t.Fatalf("opus picked %q, want a (Fable window does not apply)", got)
	}
	if got := pickClaudeWeekly(t, "claude-fable-5-1", auths...); got != "b" {
		t.Fatalf("fable picked %q, want b (a's Fable window is rejected)", got)
	}
}

func TestClaudeWeeklyResetFirst_HigherPriorityTierWins(t *testing.T) {
	t.Parallel()
	now := time.Now()
	high := claudeWeeklyAuth("high", claudeWeeklySignals(now.Add(6*24*time.Hour)))
	high.Attributes = map[string]string{"priority": "10"}
	got := pickClaudeWeekly(t, "claude-opus-5-5",
		high,
		claudeWeeklyAuth("low", claudeWeeklySignals(now.Add(time.Hour))),
	)
	if got != "high" {
		t.Fatalf("picked %q, want high (priority tier first)", got)
	}
}

func TestClaudeWeeklyResetFirst_IgnoresNonClaudeSignals(t *testing.T) {
	t.Parallel()
	now := time.Now()
	codex := &Auth{ID: "a", Provider: "codex", Quota: QuotaState{Signals: claudeWeeklySignals(now.Add(time.Hour))}}
	got := pickClaudeWeekly(t, "gpt-6-sol", codex, claudeWeeklyAuth("b", nil))
	if got != "a" {
		t.Fatalf("picked %q, want a (fill-first fallback, no Claude windows)", got)
	}
}

func TestClaudeWeeklyResetFirst_WeightedFallbackDropsZeroWeight(t *testing.T) {
	t.Parallel()
	now := time.Now()
	zero := claudeWeeklyAuth("a", claudeWeeklySignals(now.Add(time.Hour)))
	zero.Attributes = map[string]string{AttributeWeight: "0"}
	selector := NewClaudeWeeklyResetFirstSelector(&WeightedRoundRobinSelector{})
	got, err := selector.Pick(context.Background(), "claude", "claude-opus-5-5", cliproxyexecutor.Options{}, []*Auth{
		zero,
		claudeWeeklyAuth("b", claudeWeeklySignals(now.Add(5*24*time.Hour))),
	})
	if err != nil {
		t.Fatalf("Pick() error = %v", err)
	}
	if got.ID != "b" {
		t.Fatalf("picked %q, want b (weight 0 is excluded)", got.ID)
	}
}

// The manager learns weekly windows from real response headers via MarkResult,
// and a new session behind session affinity binds to the soonest weekly reset.
func TestManagerSelectAuth_ClaudeWeeklyResetFirstFromObservedHeaders(t *testing.T) {
	const model = "claude-weekly-test-model"
	selector := NewSessionAffinitySelector(NewClaudeWeeklyResetFirstSelector(&RoundRobinSelector{}))
	t.Cleanup(selector.Stop)
	manager := NewManager(nil, selector, nil)
	manager.RegisterExecutor(&aliasRoutingExecutor{id: "claude"})

	now := time.Now()
	resets := map[string]time.Time{
		"claude-late":  now.Add(6 * 24 * time.Hour),
		"claude-soon":  now.Add(30 * time.Hour),
		"claude-fresh": {},
	}
	reg := registry.GetGlobalRegistry()
	for id := range resets {
		if _, errRegister := manager.Register(context.Background(), &Auth{ID: id, Provider: "claude", Status: StatusActive}); errRegister != nil {
			t.Fatalf("register %s: %v", id, errRegister)
		}
		reg.RegisterClient(id, "claude", []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { reg.UnregisterClient(id) })
		manager.RefreshSchedulerEntry(id)
	}
	for id, reset := range resets {
		if reset.IsZero() {
			continue
		}
		ctx := internallogging.WithResponseHeadersHolder(context.Background())
		internallogging.SetResponseHeaders(ctx, http.Header{
			"Anthropic-Ratelimit-Unified-Status":         []string{"allowed"},
			"Anthropic-Ratelimit-Unified-7d-Status":      []string{"allowed"},
			"Anthropic-Ratelimit-Unified-7d-Utilization": []string{"0.3"},
			"Anthropic-Ratelimit-Unified-7d-Reset":       []string{strconv.FormatInt(reset.Unix(), 10)},
		})
		manager.MarkResult(ctx, Result{AuthID: id, Provider: "claude", Model: model, RouteModel: model, Success: true})
	}

	selected, errSelect := manager.SelectAuth(context.Background(), "claude", model, cliproxyexecutor.Options{
		Headers: http.Header{"X-Claude-Code-Session-Id": []string{"weekly-reset-session"}},
	})
	if errSelect != nil {
		t.Fatalf("SelectAuth() error = %v", errSelect)
	}
	if selected.ID != "claude-soon" {
		t.Fatalf("SelectAuth() = %q, want claude-soon", selected.ID)
	}
}
