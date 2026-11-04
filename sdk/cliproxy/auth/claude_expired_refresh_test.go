package auth

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestClaudeExpiredRefreshTokenStopsAutomaticRetries(t *testing.T) {
	for _, validAccess := range []bool{true, false} {
		t.Run(map[bool]string{true: "unexpired access", false: "expired access"}[validAccess], func(t *testing.T) {
			ctx := context.Background()
			m := NewManager(nil, &RoundRobinSelector{}, nil)
			e := &mockOAuthErrorExecutor{id: "claude", errToReturn: errors.New(`token refresh failed with status 400: {"error":"invalid_grant","error_description":"Refresh token expired"}`)}
			m.RegisterExecutor(e)
			expiry := time.Now().Add(time.Hour)
			if !validAccess {
				expiry = time.Now().Add(-time.Hour)
			}
			a := &Auth{ID: "claude-expired-refresh", Provider: "claude", Status: StatusActive, Metadata: map[string]any{
				"access_token": "access", "refresh_token": "expired-refresh", "expired": expiry.Format(time.RFC3339),
			}}
			if _, err := m.Register(ctx, a); err != nil {
				t.Fatal(err)
			}
			m.refreshAuth(ctx, a.ID)
			current, _ := m.GetByID(a.ID)
			if current.Disabled || current.Unavailable == validAccess {
				t.Fatal("refresh failure changed access availability incorrectly")
			}
			if !hasRejectedRefreshToken(current) || !current.NextRefreshAfter.IsZero() {
				t.Fatal("expired refresh token still scheduled")
			}
			for _, now := range []time.Time{time.Now(), time.Now().Add(24 * time.Hour)} {
				if m.shouldRefresh(current, now) {
					t.Fatal("expired refresh token remains refreshable")
				}
				if _, scheduled := nextRefreshCheckAt(now, current, time.Second); scheduled {
					t.Fatal("expired refresh token remains in scheduler")
				}
			}
			m.refreshAuth(ctx, a.ID)
			if e.refreshCalls.Load() != 1 {
				t.Fatal("expired refresh token sent upstream again")
			}
			if validAccess {
				m.MarkResult(ctx, Result{AuthID: a.ID, Provider: "claude", Success: true})
				current, _ = m.GetByID(a.ID)
				if !hasRejectedRefreshToken(current) || m.shouldRefresh(current, time.Now()) {
					t.Fatal("successful inference restarted rejected refresh token retries")
				}
			}
			// File/UI updates with the same tokens must not restart the loop.
			current.rejectedRefreshToken = ""
			if _, err := m.Update(ctx, current); err != nil {
				t.Fatal(err)
			}
			current, _ = m.GetByID(a.ID)
			if !hasRejectedRefreshToken(current) {
				t.Fatal("metadata update forgot the rejected refresh token")
			}
			// A new login replaces the rejected token and resumes refresh.
			current.Metadata["refresh_token"] = "new-refresh"
			current.Metadata["access_token"] = "new-access"
			current.Metadata["expired"] = time.Now().Add(time.Hour).Format(time.RFC3339)
			if _, err := m.Update(ctx, current); err != nil {
				t.Fatal(err)
			}
			e.errToReturn = nil
			m.refreshAuth(ctx, a.ID)
			current, _ = m.GetByID(a.ID)
			if e.refreshCalls.Load() != 2 || hasRejectedRefreshToken(current) {
				t.Fatal("new login did not resume refresh")
			}
		})
	}
}

func TestClaudeExpiredRefreshAllowsExplicitRecovery(t *testing.T) {
	ctx := context.Background()
	m := NewManager(nil, &RoundRobinSelector{}, nil)
	e := &mockOAuthErrorExecutor{id: "claude", errToReturn: errors.New("invalid_grant: Refresh token expired")}
	m.RegisterExecutor(e)
	a := &Auth{ID: "claude-forced-recovery", Provider: "claude", Metadata: map[string]any{
		"access_token": "access", "refresh_token": "refresh",
	}}
	if _, err := m.Register(ctx, a); err != nil {
		t.Fatal(err)
	}
	m.refreshAuth(ctx, a.ID)
	e.errToReturn = nil
	recovered, err := m.ForceRefreshAuth(ctx, a.ID)
	if err != nil || recovered == nil {
		t.Fatalf("explicit refresh failed: %v", err)
	}
	if e.refreshCalls.Load() != 2 || hasRejectedRefreshToken(recovered) {
		t.Fatal("successful explicit refresh did not clear terminal rejection")
	}
}

func TestExpiredClaudeRefreshClassification(t *testing.T) {
	for _, tc := range []struct {
		provider, message string
		want              bool
	}{
		{"claude", `invalid_grant: Refresh token expired`, true},
		{"claude", `invalid_grant: Bad Request`, false},
		{"claude", `upstream 503: Refresh token expired`, false},
		{"codex", `invalid_grant: Refresh token expired`, false},
	} {
		if got := isExpiredClaudeRefreshToken(tc.provider, errors.New(tc.message)); got != tc.want {
			t.Fatalf("%s %q = %v, want %v", tc.provider, tc.message, got, tc.want)
		}
	}
}
