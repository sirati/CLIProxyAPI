package auth

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

const lapsedUsageBody = `{"error":{"type":"permission_error","message":"OAuth authentication is currently not allowed for this organization.","details":{"error_code":"oauth_not_allowed_for_organization"}}}`

// lapseExecutor answers like Anthropic: lapsed logins get the 403 permission
// error, the others succeed or fail as configured.
type lapseExecutor struct {
	mu     sync.Mutex
	calls  map[string]int
	lapsed map[string]bool
	fail   map[string]*Error
}

func (e *lapseExecutor) Identifier() string { return "claude" }

func (e *lapseExecutor) Execute(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls[auth.ID]++
	if e.lapsed[auth.ID] {
		return cliproxyexecutor.Response{}, &Error{HTTPStatus: http.StatusForbidden, Message: lapsedUsageBody}
	}
	if failure := e.fail[auth.ID]; failure != nil {
		return cliproxyexecutor.Response{}, failure
	}
	return cliproxyexecutor.Response{Payload: []byte(`{}`)}, nil
}

func (e *lapseExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, &Error{HTTPStatus: http.StatusNotImplemented, Message: "not implemented"}
}

func (e *lapseExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) { return auth, nil }

func (e *lapseExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, &Error{HTTPStatus: http.StatusNotImplemented, Message: "not implemented"}
}

func (e *lapseExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func lapsedClaude(id string) *Auth {
	return &Auth{ID: id, Provider: "claude", Status: StatusActive, Metadata: map[string]any{
		metadataUsagePollingBlocked:   claudeOrganizationOAuthBlocked,
		metadataSubscriptionCheckedAt: time.Now().UTC().Format(time.RFC3339),
	}}
}

func newLapseManager(t *testing.T, model string, auths ...*Auth) (*Manager, *lapseExecutor) {
	t.Helper()
	m := NewManager(nil, &FillFirstSelector{}, nil)
	e := &lapseExecutor{calls: map[string]int{}, lapsed: map[string]bool{}, fail: map[string]*Error{}}
	m.RegisterExecutor(e)
	reg := registry.GetGlobalRegistry()
	for _, auth := range auths {
		if _, err := m.Register(context.Background(), auth); err != nil {
			t.Fatal(err)
		}
		reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: model}})
		id := auth.ID
		t.Cleanup(func() { reg.UnregisterClient(id) })
		m.RefreshSchedulerEntry(auth.ID)
	}
	return m, e
}

// The 2026-10-29 incident: the working account was briefly cooling down, the
// request went to the two lapsed accounts and Claude Code got their 403.
func TestLapsedClaudeSubscriptionsGetNoRequests(t *testing.T) {
	withQuotaCooldownEnabled(t)
	model := "lapse-model-" + t.Name()
	working := &Auth{ID: "claude-working", Provider: "claude", Status: StatusActive, Metadata: map[string]any{}}
	m, e := newLapseManager(t, model, lapsedClaude("claude-lapsed-1"), working, lapsedClaude("claude-lapsed-2"))
	e.lapsed["claude-lapsed-1"], e.lapsed["claude-lapsed-2"] = true, true
	ctx := context.Background()
	req := cliproxyexecutor.Request{Model: model}

	for i := 0; i < 4; i++ {
		if _, err := m.Execute(ctx, []string{"claude"}, req, cliproxyexecutor.Options{}); err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
	}
	if picked, err := m.SelectAuth(ctx, "claude", model, cliproxyexecutor.Options{}); err != nil || picked.ID != working.ID {
		t.Fatalf("legacy selection = %v, %v; want the working account", picked, err)
	}

	retry := time.Hour
	e.fail[working.ID] = &Error{HTTPStatus: http.StatusTooManyRequests, Message: "rate limit exceeded"}
	m.MarkResult(ctx, Result{AuthID: working.ID, Provider: "claude", Model: model, Error: e.fail[working.ID], RetryAfter: &retry})
	_, err := m.Execute(ctx, []string{"claude"}, req, cliproxyexecutor.Options{})
	var cooldown *modelCooldownError
	if !errors.As(err, &cooldown) {
		t.Fatalf("Execute() error = %v, want a cooldown of the working account", err)
	}
	if strings.Contains(err.Error(), claudeOrganizationOAuthBlocked) || strings.Contains(err.Error(), claudeOrganizationOAuthMessage) {
		t.Fatalf("lapsed account's error leaked to the client: %v", err)
	}
	if _, errSelect := m.SelectAuth(ctx, "claude", model, cliproxyexecutor.Options{}); !errors.As(errSelect, &cooldown) {
		t.Fatalf("legacy selection error = %v, want cooldown", errSelect)
	}
	if e.calls["claude-lapsed-1"] != 0 || e.calls["claude-lapsed-2"] != 0 {
		t.Fatalf("lapsed accounts received requests: %v", e.calls)
	}
}

func TestClaudeSubscriptionLapseFromRequestStopsRouting(t *testing.T) {
	model := "lapse-request-model-" + t.Name()
	fresh := &Auth{ID: "claude-fresh-lapse", Provider: "claude", Status: StatusActive, Metadata: map[string]any{}}
	working := &Auth{ID: "claude-working-2", Provider: "claude", Status: StatusActive, Metadata: map[string]any{}}
	m, e := newLapseManager(t, model, fresh, working)
	e.lapsed[fresh.ID] = true
	ctx := context.Background()

	m.MarkResult(ctx, Result{AuthID: fresh.ID, Provider: "claude", Model: model, Error: &Error{HTTPStatus: http.StatusForbidden, Message: lapsedUsageBody}})
	current, _ := m.GetByID(fresh.ID)
	if !ClaudeSubscriptionLapsed(current) || current.Disabled {
		t.Fatal("request failure did not stop routing to an enabled credential")
	}
	// Expire the 403 model cooldown: the routing stop must outlast it.
	for _, state := range current.ModelStates {
		state.NextRetryAfter = time.Now().Add(-time.Minute)
	}
	if _, err := m.Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	m.RefreshSchedulerEntry(fresh.ID)
	for i := 0; i < 4; i++ {
		if _, err := m.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{}); err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
	}
	if e.calls[fresh.ID] != 0 {
		t.Fatalf("lapsed account routed after its cooldown expired: %v", e.calls)
	}
}

func TestObserveClaudeUsageCheckSetsAndClearsLapse(t *testing.T) {
	model := "lapse-observe-model-" + t.Name()
	a := &Auth{ID: "claude-observe", Provider: "claude", Status: StatusActive, Metadata: map[string]any{}}
	m, _ := newLapseManager(t, model, a)
	ctx := context.Background()

	m.MarkResult(ctx, Result{AuthID: a.ID, Provider: "claude", Model: model, Error: &Error{HTTPStatus: http.StatusForbidden, Message: lapsedUsageBody}})
	for _, tc := range []struct {
		status int
		body   string
	}{{http.StatusTooManyRequests, `{"error":{"type":"rate_limit_error"}}`}, {http.StatusOK, `{`}, {http.StatusOK, `{"five_hour":{}}`}} {
		m.ObserveClaudeUsageCheck(ctx, a.ID, tc.status, []byte(tc.body))
		if current, _ := m.GetByID(a.ID); !ClaudeSubscriptionLapsed(current) {
			t.Fatalf("inconclusive usage response %d %s cleared the lapse", tc.status, tc.body)
		}
	}

	m.ObserveClaudeUsageCheck(ctx, a.ID, http.StatusOK, []byte(availableUsage("claude")))
	current, _ := m.GetByID(a.ID)
	if ClaudeSubscriptionLapsed(current) || current.LastError != nil {
		t.Fatalf("renewed subscription still lapsed: %+v", current)
	}
	// The 403 cooldown the lapse recorded is lifted with it.
	if picked, err := m.SelectAuth(ctx, "claude", model, cliproxyexecutor.Options{}); err != nil || picked.ID != a.ID {
		t.Fatalf("renewed subscription not routed: %v, %v", picked, err)
	}

	m.ObserveClaudeUsageCheck(ctx, a.ID, http.StatusForbidden, []byte(lapsedUsageBody))
	if current, _ = m.GetByID(a.ID); !ClaudeSubscriptionLapsed(current) {
		t.Fatal("manual usage check did not record the lapse")
	}
	if _, err := m.SelectAuth(ctx, "claude", model, cliproxyexecutor.Options{}); err == nil {
		t.Fatal("lapsed subscription routed after a manual check")
	}

	// A successful request also proves the subscription works again.
	m.MarkResult(ctx, Result{AuthID: a.ID, Provider: "claude", Model: model, Success: true})
	if current, _ = m.GetByID(a.ID); ClaudeSubscriptionLapsed(current) {
		t.Fatal("successful request did not clear the lapse")
	}
}

func TestIsClaudeUsageURL(t *testing.T) {
	for raw, want := range map[string]bool{
		"https://api.anthropic.com/api/oauth/usage":     true,
		"https://API.anthropic.com:443/api/oauth/usage": true,
		"https://api.anthropic.com/api/oauth/profile":   false,
		"https://example.com/api/oauth/usage":           false,
	} {
		u, _ := url.Parse(raw)
		if got := IsClaudeUsageURL(u); got != want {
			t.Fatalf("IsClaudeUsageURL(%s) = %v, want %v", raw, got, want)
		}
	}
}
