package handlers

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

type retryAfterTestError struct{ after time.Duration }

func (e retryAfterTestError) Error() string              { return "usage limit" }
func (e retryAfterTestError) StatusCode() int            { return http.StatusTooManyRequests }
func (e retryAfterTestError) RetryAfter() *time.Duration { return &e.after }

func TestLimitReset(t *testing.T) {
	tooMany := func(err error) *interfaces.ErrorMessage {
		return &interfaces.ErrorMessage{StatusCode: http.StatusTooManyRequests, Error: err}
	}
	for _, tt := range []struct {
		name   string
		msg    *interfaces.ErrorMessage
		want   time.Duration
		answer LimitAnswer
	}{
		{"cooling credentials name the first reset", tooMany(coreauth.NewModelCooldownError("m", "codex", 90*time.Minute)), 90 * time.Minute, LimitReached},
		{"a reset past six hours waits six", tooMany(coreauth.NewModelCooldownError("m", "codex", 30*time.Hour)), 6 * time.Hour, LimitReached},
		{"a reset already due is retried at once", tooMany(coreauth.NewModelCooldownError("m", "codex", 0)), time.Second, RetrySoon},
		{"a 429 without a reset is left to the client", tooMany(errors.New("rate limited")), 0, NoLimit},
		{"a Codex body names the reset", tooMany(errors.New(`{"error":{"type":"usage_limit_reached","resets_in_seconds":7200}}`)), 2 * time.Hour, LimitReached},
		{"an error's retry-after names the reset", tooMany(retryAfterTestError{after: 3 * time.Hour}), 3 * time.Hour, LimitReached},
		{"a short retry-after is a retry", tooMany(retryAfterTestError{after: 20 * time.Second}), 20 * time.Second, RetrySoon},
		{"a status from the error counts", &interfaces.ErrorMessage{Error: retryAfterTestError{after: time.Hour}}, time.Hour, LimitReached},
		{"other failures are no limit", &interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: errors.New("bad gateway")}, 0, NoLimit},
		{"a direct response is kept", &interfaces.ErrorMessage{StatusCode: http.StatusTooManyRequests, DirectResponse: true}, 0, NoLimit},
		{"no message is no limit", nil, 0, NoLimit},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, answer := (&BaseAPIHandler{}).LimitReset(tt.msg, "codex")
			if answer != tt.answer || got != tt.want {
				t.Fatalf("LimitReset() = %v, %v; want %v, %v", got, answer, tt.want, tt.answer)
			}
		})
	}
}

func TestLimitResetNamesTheProvidersFirstRecovery(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	now := time.Now()
	for _, auth := range []*coreauth.Auth{
		{ID: "codex-later", Provider: "codex", Quota: coreauth.QuotaState{Exceeded: true, NextRecoverAt: now.Add(140 * time.Hour)}},
		{ID: "codex-first", Provider: "codex", Quota: coreauth.QuotaState{Exceeded: true, NextRecoverAt: now.Add(3 * time.Hour)}},
		{ID: "codex-disabled", Provider: "codex", Disabled: true, Quota: coreauth.QuotaState{Exceeded: true, NextRecoverAt: now.Add(time.Hour)}},
		{ID: "claude-sooner", Provider: "claude", Quota: coreauth.QuotaState{Exceeded: true, NextRecoverAt: now.Add(2 * time.Hour)}},
		{ID: "codex-model", Provider: "codex", ModelStates: map[string]*coreauth.ModelState{
			"gpt": {Unavailable: true, NextRetryAfter: now.Add(4 * time.Hour)},
		}},
	} {
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatalf("register %s: %v", auth.ID, err)
		}
	}
	h := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)

	got, answer := h.LimitReset(&interfaces.ErrorMessage{StatusCode: http.StatusTooManyRequests, Error: retryAfterTestError{after: 140 * time.Hour}}, "codex")
	if answer != LimitReached || got < 3*time.Hour-time.Minute || got > 3*time.Hour {
		t.Fatalf("LimitReset() = %v, %v; want about 3h", got, answer)
	}
}
