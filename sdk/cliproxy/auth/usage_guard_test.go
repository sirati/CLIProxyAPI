package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func TestDecideUsageGuard(t *testing.T) {
	t.Parallel()
	minimum := func(v float64) usageGuardSettings { return usageGuardSettings{minRemaining: v, hasMinRemaining: true} }
	useOnce := func(baseline float64) usageGuardSettings {
		return usageGuardSettings{useOnce: true, baseline: baseline, hasBaseline: true}
	}
	for _, tc := range []struct {
		name         string
		settings     usageGuardSettings
		used         float64
		want         usageGuardAction
		wantBaseline float64
		setBaseline  bool
	}{
		{"min: above", minimum(20), 70, usageGuardAllow, 0, false},
		{"min: at threshold", minimum(20), 80, usageGuardAllow, 0, false},
		{"min: below", minimum(20), 80.5, usageGuardHold, 0, false},
		{"once: first reading sets baseline", usageGuardSettings{useOnce: true}, 30, usageGuardAllow, 30, true},
		{"once: within 5 points", useOnce(30), 35, usageGuardAllow, 0, false},
		{"once: more than 5 points", useOnce(30), 35.1, usageGuardRetire, 0, false},
		{"once: empty", useOnce(99), 100, usageGuardRetire, 0, false},
		{"once: window reset lowers baseline", useOnce(60), 4, usageGuardAllow, 4, true},
		{"once: below minimum retires", usageGuardSettings{useOnce: true, baseline: 85, hasBaseline: true, minRemaining: 12, hasMinRemaining: true}, 89, usageGuardRetire, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := decideUsageGuard(tc.settings, tc.used)
			if got.action != tc.want {
				t.Fatalf("action = %v (%s), want %v", got.action, got.reason, tc.want)
			}
			if got.setBaseline != tc.setBaseline || (tc.setBaseline && got.baseline != tc.wantBaseline) {
				t.Fatalf("baseline = %v/%v, want %v/%v", got.baseline, got.setBaseline, tc.wantBaseline, tc.setBaseline)
			}
		})
	}
}

func TestUsageGuardSettingsFromMetadata(t *testing.T) {
	t.Parallel()
	settings, ok := usageGuardSettingsOf(&Auth{Provider: "claude", Metadata: map[string]any{
		MetadataMinRemaining: json.Number("15"), MetadataUseOnce: "true",
	}})
	if !ok || !settings.useOnce || !settings.hasMinRemaining || settings.minRemaining != 15 {
		t.Fatalf("settings = %+v ok=%v", settings, ok)
	}
	if _, ok := usageGuardSettingsOf(&Auth{Provider: "codex", Metadata: map[string]any{MetadataUseOnce: true}}); ok {
		t.Fatal("non-Claude credential enabled the usage guard")
	}
	if _, ok := usageGuardSettingsOf(&Auth{Provider: "claude", Metadata: map[string]any{MetadataMinRemaining: 0.0}}); ok {
		t.Fatal("min_remaining 0 enabled the usage guard")
	}
}

// usageGuardExecutor answers the Claude usage endpoint with a per-credential
// weekly utilization.
type usageGuardExecutor struct {
	mu       sync.Mutex
	weekly   map[string]float64
	failures map[string]bool
	calls    map[string]int
}

func (e *usageGuardExecutor) set(authID string, used float64) {
	e.mu.Lock()
	e.weekly[authID] = used
	e.mu.Unlock()
}

func (*usageGuardExecutor) Identifier() string { return "claude" }

func (*usageGuardExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (*usageGuardExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, &Error{HTTPStatus: http.StatusNotImplemented, Message: "not implemented"}
}

func (*usageGuardExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) { return auth, nil }

func (*usageGuardExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (e *usageGuardExecutor) HttpRequest(_ context.Context, auth *Auth, req *http.Request) (*http.Response, error) {
	if req.URL.String() != claudeUsageURL || req.Header.Get("Anthropic-Beta") == "" {
		return nil, fmt.Errorf("unexpected request %s", req.URL)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls[auth.ID]++
	if e.failures[auth.ID] {
		return &http.Response{StatusCode: http.StatusBadGateway, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	}
	body := fmt.Sprintf(`{"five_hour":{"utilization":3,"resets_at":"2026-10-17T12:00:00Z"},"seven_day":{"utilization":%g,"resets_at":"2026-10-20T00:00:00Z"}}`, e.weekly[auth.ID])
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader([]byte(body)))}, nil
}

func newUsageGuardManager(t *testing.T, model string, auths ...*Auth) (*Manager, *usageGuardExecutor, *usageGuard) {
	t.Helper()
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	executor := &usageGuardExecutor{weekly: map[string]float64{}, failures: map[string]bool{}, calls: map[string]int{}}
	manager.RegisterExecutor(executor)
	reg := registry.GetGlobalRegistry()
	for _, auth := range auths {
		if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("register %s: %v", auth.ID, errRegister)
		}
		reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: model}})
		id := auth.ID
		t.Cleanup(func() { reg.UnregisterClient(id) })
		manager.RefreshSchedulerEntry(auth.ID)
	}
	return manager, executor, &usageGuard{cancel: func() {}, holds: map[string]time.Time{}}
}

func selectUsageGuarded(t *testing.T, manager *Manager, model string) string {
	t.Helper()
	selected, errSelect := manager.SelectAuth(context.Background(), "claude", model, cliproxyexecutor.Options{})
	if errSelect != nil {
		t.Fatalf("SelectAuth() error = %v", errSelect)
	}
	return selected.ID
}

func TestUsageGuard_MinRemainingHoldsAndReleases(t *testing.T) {
	const model = "usage-guard-min-model"
	guarded := &Auth{ID: "claude-a", Provider: "claude", Status: StatusActive, Metadata: map[string]any{MetadataMinRemaining: 20.0}}
	other := &Auth{ID: "claude-b", Provider: "claude", Status: StatusActive}
	manager, executor, guard := newUsageGuardManager(t, model, guarded, other)
	ctx := context.Background()

	executor.set("claude-a", 50)
	manager.checkUsageGuards(ctx, guard)
	if got := selectUsageGuarded(t, manager, model); got != "claude-a" {
		t.Fatalf("selected %q, want claude-a (50%% left is above the minimum)", got)
	}
	if executor.calls["claude-b"] != 0 {
		t.Fatal("polled a credential without use_once or min_remaining")
	}

	executor.set("claude-a", 85)
	manager.checkUsageGuards(ctx, guard)
	if got := selectUsageGuarded(t, manager, model); got != "claude-b" {
		t.Fatalf("selected %q, want claude-b (claude-a has 15%% left, below 20%%)", got)
	}

	// A failed check keeps the hold rather than guessing.
	executor.failures["claude-a"] = true
	manager.checkUsageGuards(ctx, guard)
	if got := selectUsageGuarded(t, manager, model); got != "claude-b" {
		t.Fatalf("selected %q after a failed check, want claude-b (hold kept)", got)
	}
	executor.failures["claude-a"] = false

	// The weekly window reset: usage dropped, so the hold is lifted.
	executor.set("claude-a", 2)
	manager.checkUsageGuards(ctx, guard)
	if got := selectUsageGuarded(t, manager, model); got != "claude-a" {
		t.Fatalf("selected %q, want claude-a (hold released)", got)
	}
	if auth, _ := manager.GetByID("claude-a"); auth.Disabled {
		t.Fatal("min_remaining disabled the credential")
	}
}

func TestUsageGuard_UseOnceRetiresAfterFivePoints(t *testing.T) {
	const model = "usage-guard-once-model"
	once := &Auth{ID: "claude-a", Provider: "claude", Status: StatusActive, Metadata: map[string]any{MetadataUseOnce: true}}
	other := &Auth{ID: "claude-b", Provider: "claude", Status: StatusActive}
	manager, executor, guard := newUsageGuardManager(t, model, once, other)
	ctx := context.Background()

	executor.set("claude-a", 40)
	manager.checkUsageGuards(ctx, guard)
	auth, _ := manager.GetByID("claude-a")
	if baseline, _ := metadataNumber(auth.Metadata[metadataUseOnceBaseline]); baseline != 40 {
		t.Fatalf("baseline = %v, want 40", auth.Metadata[metadataUseOnceBaseline])
	}

	executor.set("claude-a", 44)
	manager.checkUsageGuards(ctx, guard)
	if got := selectUsageGuarded(t, manager, model); got != "claude-a" {
		t.Fatalf("selected %q, want claude-a (4 points is within the limit)", got)
	}

	executor.set("claude-a", 45.5)
	manager.checkUsageGuards(ctx, guard)
	auth, ok := manager.GetByID("claude-a")
	if !ok {
		t.Fatal("retiring removed the credential; the login must be kept")
	}
	if !auth.Disabled || auth.Metadata["disabled"] != true || auth.Metadata[metadataUseOnceRetired] == nil {
		t.Fatalf("auth not retired: disabled=%v metadata=%v", auth.Disabled, auth.Metadata)
	}
	if got := selectUsageGuarded(t, manager, model); got != "claude-b" {
		t.Fatalf("selected %q, want claude-b (claude-a retired)", got)
	}

	// Retirement is permanent: a later reset does not bring it back.
	executor.set("claude-a", 0)
	calls := executor.calls["claude-a"]
	manager.checkUsageGuards(ctx, guard)
	if auth, _ := manager.GetByID("claude-a"); !auth.Disabled || executor.calls["claude-a"] != calls {
		t.Fatal("a retired credential was polled or re-enabled")
	}
}

func TestUsageGuard_ReleaseLeavesUpstreamCooldown(t *testing.T) {
	const model = "usage-guard-429-model"
	guarded := &Auth{ID: "claude-a", Provider: "claude", Status: StatusActive, Metadata: map[string]any{MetadataMinRemaining: 20.0}}
	manager, executor, guard := newUsageGuardManager(t, model, guarded)
	ctx := context.Background()

	executor.set("claude-a", 90)
	manager.checkUsageGuards(ctx, guard)
	// An upstream 429 replaces the hold with its own longer cooldown.
	upstream := time.Now().Add(3 * time.Hour)
	if errMutate := manager.mutateAuthAvailability(ctx, "claude-a", func(auth *Auth, _ time.Time) bool {
		auth.Quota.NextRecoverAt = upstream
		auth.NextRetryAfter = upstream
		return true
	}); errMutate != nil {
		t.Fatal(errMutate)
	}
	executor.set("claude-a", 10)
	manager.checkUsageGuards(ctx, guard)
	auth, _ := manager.GetByID("claude-a")
	if !auth.Quota.Exceeded || !auth.Quota.NextRecoverAt.Equal(upstream) {
		t.Fatalf("release cleared an upstream cooldown: %+v", auth.Quota)
	}
}

func TestUsageGuard_UseOnceReenabledStartsOver(t *testing.T) {
	const model = "usage-guard-reenable-model"
	once := &Auth{ID: "claude-a", Provider: "claude", Status: StatusActive, Metadata: map[string]any{MetadataUseOnce: true}}
	manager, executor, guard := newUsageGuardManager(t, model, once)
	ctx := context.Background()

	executor.set("claude-a", 10)
	manager.checkUsageGuards(ctx, guard)
	executor.set("claude-a", 20)
	manager.checkUsageGuards(ctx, guard)
	if auth, _ := manager.GetByID("claude-a"); !auth.Disabled {
		t.Fatal("precondition: credential did not retire")
	}

	// The operator re-enables it from the panel.
	if errMutate := manager.mutateAuthAvailability(ctx, "claude-a", func(auth *Auth, _ time.Time) bool {
		auth.Disabled, auth.Status = false, StatusActive
		auth.Metadata["disabled"] = false
		return true
	}); errMutate != nil {
		t.Fatal(errMutate)
	}
	executor.set("claude-a", 22)
	manager.checkUsageGuards(ctx, guard)
	auth, _ := manager.GetByID("claude-a")
	if auth.Disabled {
		t.Fatal("re-enabled credential retired again against its old baseline")
	}
	if baseline, _ := metadataNumber(auth.Metadata[metadataUseOnceBaseline]); baseline != 22 {
		t.Fatalf("baseline = %v, want 22", auth.Metadata[metadataUseOnceBaseline])
	}
	if _, ok := auth.Metadata[metadataUseOnceRetired]; ok {
		t.Fatal("retired marker kept after re-enable")
	}
	executor.set("claude-a", 27.5)
	manager.checkUsageGuards(ctx, guard)
	if auth, _ := manager.GetByID("claude-a"); !auth.Disabled {
		t.Fatal("did not retire 5.5 points above the new baseline")
	}
}
