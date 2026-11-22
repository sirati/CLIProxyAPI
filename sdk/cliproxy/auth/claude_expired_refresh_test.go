package auth

import (
	"context"
	"errors"
	"testing"
	"time"
)

// An expired Claude refresh token cannot recover by retrying. The credential
// must stop refreshing until its tokens change, while an unexpired access
// token keeps serving requests.
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
			if !authHasUnchangedRejectedRefreshToken(current) {
				t.Fatal("expired refresh token not recorded as requiring sign-in")
			}
			if m.shouldRefresh(current, time.Now()) {
				t.Fatal("expired refresh token remains refreshable")
			}
			m.refreshAuth(ctx, a.ID)
			if e.refreshCalls.Load() != 1 {
				t.Fatal("expired refresh token sent upstream again")
			}
			if validAccess {
				m.MarkResult(ctx, Result{AuthID: a.ID, Provider: "claude", Success: true})
				current, _ = m.GetByID(a.ID)
				if !authHasUnchangedRejectedRefreshToken(current) || m.shouldRefresh(current, time.Now()) {
					t.Fatal("successful inference restarted rejected refresh token retries")
				}
				// Once the access token expires, at most one last attempt runs.
				current.Metadata["expired"] = time.Now().Add(-time.Minute).Format(time.RFC3339)
				if _, err := m.Update(ctx, current); err != nil {
					t.Fatal(err)
				}
				m.refreshAuth(ctx, a.ID)
				current, _ = m.GetByID(a.ID)
			}
			calls := e.refreshCalls.Load()
			if calls > 2 {
				t.Fatalf("expired refresh token sent upstream %d times", calls)
			}
			for _, now := range []time.Time{time.Now(), time.Now().Add(24 * time.Hour)} {
				if m.shouldRefresh(current, now) {
					t.Fatal("expired refresh token remains refreshable")
				}
				if _, scheduled := nextRefreshCheckAt(now, current, time.Second); scheduled {
					t.Fatal("expired refresh token remains in scheduler")
				}
			}
			// File/UI updates with the same tokens must not restart the loop. The
			// rejection is runtime state, so a reloaded file arrives without it.
			current.RejectedRefreshToken = ""
			current.RequiresSignIn = false
			if _, err := m.Update(ctx, current); err != nil {
				t.Fatal(err)
			}
			current, _ = m.GetByID(a.ID)
			if !authHasUnchangedRejectedRefreshToken(current) {
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
			if e.refreshCalls.Load() != calls+1 || authHasUnchangedRejectedRefreshToken(current) || current.RequiresSignIn {
				t.Fatal("new login did not resume refresh")
			}
		})
	}
}

// rotatingRefreshExecutor rotates the refresh token on success, as Anthropic does.
type rotatingRefreshExecutor struct{ mockOAuthErrorExecutor }

func (e *rotatingRefreshExecutor) Refresh(ctx context.Context, auth *Auth) (*Auth, error) {
	updated, err := e.mockOAuthErrorExecutor.Refresh(ctx, auth)
	if err == nil {
		updated.Metadata["refresh_token"] = "rotated-refresh"
	}
	return updated, err
}

// An explicit refresh still reaches the provider, so a renewed login can recover.
func TestClaudeExpiredRefreshAllowsExplicitRecovery(t *testing.T) {
	ctx := context.Background()
	m := NewManager(nil, &RoundRobinSelector{}, nil)
	e := &rotatingRefreshExecutor{mockOAuthErrorExecutor{id: "claude", errToReturn: errors.New("invalid_grant: Refresh token expired")}}
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
	if e.refreshCalls.Load() != 2 || authHasUnchangedRejectedRefreshToken(recovered) || recovered.RequiresSignIn {
		t.Fatal("successful explicit refresh did not clear terminal rejection")
	}
}
