package auth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

const metadataUsagePollingBlocked = "usage_polling_blocked"
const claudeOrganizationOAuthBlocked = "oauth_not_allowed_for_organization"

type usageObservation struct {
	weeklyUsed float64
	available  bool
	signals    map[string]string
}

type usageEndpointError struct {
	status              int
	organizationBlocked bool
}

func (e *usageEndpointError) Error() string {
	if e.organizationBlocked {
		return "usage endpoint: " + claudeOrganizationOAuthBlocked
	}
	return fmt.Sprintf("usage endpoint returned HTTP %d", e.status)
}

func credentialUsageInterval(auth *Auth) time.Duration {
	if _, guarded := usageGuardSettingsOf(auth); guarded {
		return usageGuardInterval
	}
	return usageRecoveryInterval
}

func claudeSubscriptionBlocked(auth *Auth) bool {
	if auth == nil || !strings.EqualFold(auth.Provider, "claude") {
		return false
	}
	if auth.Metadata[metadataUsagePollingBlocked] == claudeOrganizationOAuthBlocked {
		return true
	}
	blocked := func(text string) bool {
		return strings.Contains(text, claudeOrganizationOAuthBlocked) || strings.Contains(text, "OAuth authentication is currently not allowed for this organization.")
	}
	if blocked(auth.StatusMessage) || (auth.LastError != nil && blocked(auth.LastError.Message)) {
		return true
	}
	for _, state := range auth.ModelStates {
		if state != nil && (blocked(state.StatusMessage) || (state.LastError != nil && blocked(state.LastError.Message))) {
			return true
		}
	}
	return false
}

func automaticUsageEligible(auth *Auth) bool {
	return auth != nil && usageGuardProviders[strings.ToLower(auth.Provider)] && !auth.Disabled && auth.Status != StatusDisabled &&
		auth.AuthKind() != AuthKindAPIKey && !claudeSubscriptionBlocked(auth)
}

func blockedUsageError(provider string, err error) bool {
	var endpoint *usageEndpointError
	return strings.EqualFold(provider, "claude") && errors.As(err, &endpoint) && endpoint.organizationBlocked
}

func (m *Manager) blockAutomaticUsage(ctx context.Context, authID string) {
	err := m.mutateAuthAvailability(ctx, authID, func(auth *Auth, _ time.Time) bool {
		if auth.Metadata == nil {
			auth.Metadata = make(map[string]any)
		}
		auth.Metadata[metadataUsagePollingBlocked] = claudeOrganizationOAuthBlocked
		return true
	})
	if err != nil {
		log.Warnf("usage guard: persisting polling stop for %s: %v", authID, err)
	}
}

func (m *Manager) fetchUsageObservation(ctx context.Context, auth *Auth) (usageObservation, error) {
	isCodex := strings.EqualFold(strings.TrimSpace(auth.Provider), "codex")
	usageURL := claudeUsageURL
	if isCodex {
		usageURL = codexUsageURL
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, usageURL, nil)
	if err != nil {
		return usageObservation{}, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	if isCodex {
		req.Header.Set("User-Agent", codexUsageUserAgent)
		req.Header.Set("Originator", "codex-tui")
		if accountID, ok := auth.Metadata["account_id"].(string); ok && strings.TrimSpace(accountID) != "" {
			req.Header.Set("Chatgpt-Account-Id", accountID)
		}
	} else {
		req.Header.Set("Anthropic-Beta", "oauth-2025-04-20")
		req.Header.Set("User-Agent", "claude-cli/2.1.280 (external, cli)")
	}
	resp, err := m.HttpRequest(ctx, auth, req)
	if err != nil {
		return usageObservation{}, err
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.Warnf("usage guard: close usage response: %v", errClose)
		}
	}()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return usageObservation{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		blocked := !isCodex && resp.StatusCode == http.StatusForbidden && (gjson.GetBytes(body, "error.details.error_code").String() == claudeOrganizationOAuthBlocked ||
			gjson.GetBytes(body, "error.message").String() == "OAuth authentication is currently not allowed for this organization.")
		return usageObservation{}, &usageEndpointError{status: resp.StatusCode, organizationBlocked: blocked}
	}
	return parseUsageObservation(auth.Provider, body)
}

func parseUsageObservation(provider string, body []byte) (usageObservation, error) {
	observation := usageObservation{}
	var err error
	if !strings.EqualFold(provider, "codex") {
		observation.weeklyUsed, err = parseClaudeWeeklyUsage(body)
		if err != nil {
			return observation, err
		}
		// Require both base windows; absent or malformed data cannot release a hold.
		short := gjson.GetBytes(body, "five_hour.utilization")
		long := gjson.GetBytes(body, "seven_day.utilization")
		observation.available = short.Type == gjson.Number && short.Float() >= 0 && short.Float() < 100 && long.Type == gjson.Number && long.Float() >= 0 && long.Float() < 100
		return observation, nil
	}
	observation.weeklyUsed, err = parseCodexWeeklyUsage(body)
	if err != nil {
		return observation, err
	}
	rate := firstGJSON(gjson.ParseBytes(body), "rate_limit", "rateLimit")
	observation.signals = make(map[string]string)
	observation.available = true
	for _, entry := range []struct {
		name string
		keys []string
	}{{"primary", []string{"primary_window", "primaryWindow"}}, {"secondary", []string{"secondary_window", "secondaryWindow"}}} {
		window := firstGJSON(rate, entry.keys...)
		used := firstGJSON(window, "used_percent", "usedPercent")
		if used.Type != gjson.Number || used.Float() < 0 || used.Float() >= 100 {
			observation.available = false
		}
		if used.Type == gjson.Number {
			observation.signals[http.CanonicalHeaderKey("x-codex-"+entry.name+"-used-percent")] = used.Raw
		}
		if reset := firstGJSON(window, "reset_at", "resetAt"); reset.Type == gjson.Number {
			observation.signals[http.CanonicalHeaderKey("x-codex-"+entry.name+"-reset-at")] = reset.Raw
		}
	}
	if allowed := firstGJSON(rate, "allowed"); allowed.Type == gjson.False {
		observation.available = false
	}
	if reached := firstGJSON(rate, "limit_reached", "limitReached"); reached.Type == gjson.True {
		observation.available = false
	}
	return observation, nil
}

func usageSignal(signals map[string]string, name string) string {
	for key, value := range signals {
		if strings.EqualFold(key, name) {
			return value
		}
	}
	return ""
}

func codexUsageReset(previous, current map[string]string) bool {
	// A reset must show replenished usage. Advancing a rolling reset timestamp
	// alone is insufficient evidence and would create polling storms.
	for _, window := range []string{"primary", "secondary"} {
		key := "x-codex-" + window + "-used-percent"
		before, errBefore := strconv.ParseFloat(usageSignal(previous, key), 64)
		after, errAfter := strconv.ParseFloat(usageSignal(current, key), 64)
		if errBefore == nil && errAfter == nil && before <= 100 && after >= 0 && before > after {
			return true
		}
	}
	return false
}

func (u *usageGuard) observeCodexUsage(auth *Auth, signals map[string]string) bool {
	if auth.Provider != "codex" || len(signals) == 0 {
		return false
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.codexUsage == nil {
		u.codexUsage = make(map[string]map[string]string)
	}
	previous := u.codexUsage[auth.ID]
	if previous == nil {
		previous = auth.Quota.Signals
	}
	reset := codexUsageReset(previous, signals)
	u.codexUsage[auth.ID] = signals
	return reset
}

func (u *usageGuard) usageCheckDue(auth *Auth, now time.Time) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return !u.lastChecks[auth.ID].Add(credentialUsageInterval(auth)).After(now)
}
func (u *usageGuard) recordUsageCheck(authID string, now time.Time) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.lastChecks == nil {
		u.lastChecks = make(map[string]time.Time)
	}
	u.lastChecks[authID] = now
}

// wakeCodexUsagePeers is nonblocking on the request path. The single usage
// worker coalesces reset notifications and performs the peer checks.
func (m *Manager) wakeCodexUsagePeers(authID string) {
	m.mu.RLock()
	guard := m.usageGuard
	m.mu.RUnlock()
	if guard != nil {
		select {
		case guard.wake <- authID:
		default:
		}
	}
}

// applyUsageObservation only releases quota state confirmed by this fresh
// observation. A concurrent result, disabled credential, or auth failure wins.
func (m *Manager) applyUsageObservation(ctx context.Context, before *Auth, observation usageObservation) {
	err := m.mutateAuthAvailability(ctx, before.ID, func(auth *Auth, now time.Time) bool {
		if auth.Generation != before.Generation || auth.RegistrationEpoch != before.RegistrationEpoch || auth.Disabled || auth.Status == StatusDisabled {
			return false
		}
		changed := false
		if len(observation.signals) > 0 {
			auth.Quota.Signals = observation.signals
			auth.Quota.ObservedAt = now
			changed = true
		}
		if !observation.available || hasUnauthorizedAuthFailure(auth) || strings.HasPrefix(auth.StatusMessage, "usage guard: ") {
			return changed
		}
		credentialQuota := auth.Quota.Exceeded && auth.Quota.Reason == "credential_quota"
		authQuota := auth.Quota.Exceeded && explicitUsageExhaustion(auth.LastError)
		released := false
		for _, state := range auth.ModelStates {
			if state == nil || state.Status == StatusDisabled || !state.Quota.Exceeded {
				continue
			}
			wrapped := state.credentialQuotaPropagated || state.Quota.Reason == "credential_quota"
			// Base-window usage cannot prove a model-specific quota recovered.
			if !wrapped && (!credentialQuota || state.Quota.Reason != "quota") {
				continue
			}
			if shadow := state.credentialQuotaShadow; shadow != nil && isModelStateActiveCooldown(shadow, now) {
				*state = *shadow.cloneShallow()
				state.UpdatedAt = now
			} else {
				resetModelState(state, now)
			}
			released = true
		}
		if credentialQuota || authQuota || (released && auth.Quota.Reason == "quota") {
			applyCooldownFields(&auth.Quota, QuotaState{})
			auth.Unavailable = false
			auth.NextRetryAfter = time.Time{}
			if auth.LastError == nil || statusCodeFromResult(auth.LastError) == http.StatusTooManyRequests {
				auth.LastError = nil
				auth.StatusMessage = ""
				auth.Status = StatusActive
			}
			updateAggregatedAvailability(auth, now)
			released = true
		}
		return changed || released
	})
	if err != nil {
		log.Warnf("usage guard: applying quota observation for %s: %v", before.ID, err)
	}
}

func explicitUsageExhaustion(err *Error) bool {
	if err == nil || statusCodeFromResult(err) != http.StatusTooManyRequests {
		return false
	}
	return err.Code == "usage_limit_reached" || strings.Contains(err.Message, "usage_limit_reached") || strings.Contains(strings.ToLower(err.Message), "weekly quota")
}
