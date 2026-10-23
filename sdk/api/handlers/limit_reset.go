package handlers

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

// A client resumes a conversation stopped by a usage limit once the reset it
// was told has passed: Codex and Claude Code show the time, T3 Code continues
// the thread by itself. The proxy names the earliest reset among the
// provider's credentials, but never one more than six hours out: Codex
// windows sometimes reset early, so after six hours the client asks again and
// is told the next six.
const (
	maxLimitReset = 6 * time.Hour
	minLimitReset = time.Minute
)

// LimitReset reports how long a client should wait before it resumes a
// request that failed because provider's credentials are exhausted.
func (h *BaseAPIHandler) LimitReset(msg *interfaces.ErrorMessage, provider string) (time.Duration, bool) {
	// A direct response was written on purpose by an in-process component.
	if msg == nil || msg.DirectResponse {
		return 0, false
	}
	now := time.Now()
	reset, cooling := coreauth.ModelCooldownResetIn(msg.Error)
	status := msg.StatusCode
	if status <= 0 {
		status = statusFromError(msg.Error)
	}
	if !cooling {
		if status != http.StatusTooManyRequests {
			return 0, false
		}
		reset = maxLimitReset
	}
	earlier := func(d time.Duration) {
		if d > 0 && d < reset {
			reset = d
		}
	}

	var retryAfter interface{ RetryAfter() *time.Duration }
	if errors.As(msg.Error, &retryAfter) && retryAfter != nil {
		if d := retryAfter.RetryAfter(); d != nil {
			earlier(*d)
		}
	}
	for _, headers := range []http.Header{msg.Headers, msg.Addon} {
		if seconds, err := strconv.Atoi(strings.TrimSpace(headers.Get("Retry-After"))); err == nil {
			earlier(time.Duration(seconds) * time.Second)
		}
	}
	bodies := [][]byte{msg.Body}
	if msg.Error != nil {
		bodies = append(bodies, []byte(msg.Error.Error()))
	}
	for _, body := range bodies {
		for _, quota := range []gjson.Result{gjson.GetBytes(body, "error"), gjson.ParseBytes(body)} {
			if resetsAt := quota.Get("resets_at").Int(); resetsAt > 0 {
				earlier(time.Unix(resetsAt, 0).Sub(now))
			}
			if seconds := quota.Get("resets_in_seconds").Int(); seconds > 0 {
				earlier(time.Duration(seconds) * time.Second)
			}
		}
	}
	if h != nil && h.AuthManager != nil {
		if recovery, ok := h.AuthManager.EarliestRecovery(provider, now); ok {
			earlier(recovery.Sub(now))
		}
	}

	return min(max(reset, minLimitReset), maxLimitReset), true
}
