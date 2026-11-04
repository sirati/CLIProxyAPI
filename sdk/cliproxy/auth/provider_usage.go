package auth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

const (
	// metadataUsagePollingBlocked marks a Claude login whose subscription has
	// lapsed. The key predates the routing stop and is kept for existing files.
	metadataUsagePollingBlocked = "usage_polling_blocked"
	// metadataSubscriptionCheckedAt is when a lapsed subscription was last
	// confirmed or probed, so restarts keep the slow probe schedule.
	metadataSubscriptionCheckedAt = "subscription_checked_at"

	claudeOrganizationOAuthBlocked = "oauth_not_allowed_for_organization"
	claudeOrganizationOAuthMessage = "OAuth authentication is currently not allowed for this organization."

	// lapsedSubscriptionInterval spaces usage probes of lapsed subscriptions.
	// Probing them often only earns rate limits.
	lapsedSubscriptionInterval = 6 * time.Hour
)

type usageObservation struct {
	weeklyUsed float64
	available  bool
	signals    map[string]string
}

type usageEndpointError struct {
	status              int
	organizationBlocked bool
	retryAfter          time.Duration
}

func (e *usageEndpointError) Error() string {
	if e.organizationBlocked {
		return "usage endpoint: " + claudeOrganizationOAuthBlocked
	}
	return fmt.Sprintf("usage endpoint returned HTTP %d", e.status)
}

func credentialUsageInterval(auth *Auth) time.Duration {
	if ClaudeSubscriptionLapsed(auth) {
		return lapsedSubscriptionInterval
	}
	if _, guarded := usageGuardSettingsOf(auth); guarded {
		return usageGuardInterval
	}
	return usageRecoveryInterval
}

// ClaudeSubscriptionLapsed reports a Claude login whose subscription has
// lapsed. The credential stays enabled for refresh and usage checks, but no
// request is routed to it until a usage check or a request succeeds again.
func ClaudeSubscriptionLapsed(auth *Auth) bool {
	return auth != nil && strings.EqualFold(auth.Provider, "claude") && auth.Metadata[metadataUsagePollingBlocked] == claudeOrganizationOAuthBlocked
}

// RoutingStopped reports an enabled credential that must not receive requests.
func RoutingStopped(auth *Auth) bool {
	return UseOnceStopped(auth) || ClaudeSubscriptionLapsed(auth)
}

func claudeSubscriptionLapseText(text string) bool {
	return strings.Contains(text, claudeOrganizationOAuthBlocked) || strings.Contains(text, claudeOrganizationOAuthMessage)
}

func isClaudeSubscriptionLapseError(provider string, err *Error) bool {
	return strings.EqualFold(provider, "claude") && err != nil && claudeSubscriptionLapseText(err.Message)
}

func markClaudeSubscriptionLapsed(auth *Auth, now time.Time) {
	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	auth.Metadata[metadataUsagePollingBlocked] = claudeOrganizationOAuthBlocked
	auth.Metadata[metadataSubscriptionCheckedAt] = now.UTC().Format(time.RFC3339)
}

// clearClaudeSubscriptionLapse lifts the routing stop and the failures the
// lapse recorded once the subscription works again.
func clearClaudeSubscriptionLapse(auth *Auth, now time.Time) bool {
	if !ClaudeSubscriptionLapsed(auth) {
		return false
	}
	delete(auth.Metadata, metadataUsagePollingBlocked)
	delete(auth.Metadata, metadataSubscriptionCheckedAt)
	reset := false
	for _, state := range auth.ModelStates {
		if state != nil && state.LastError != nil && claudeSubscriptionLapseText(state.LastError.Message) {
			resetModelState(state, now)
			reset = true
		}
	}
	if auth.LastError != nil && claudeSubscriptionLapseText(auth.LastError.Message) {
		auth.LastError = nil
		auth.StatusMessage = ""
		auth.Status = StatusActive
		reset = true
	}
	if reset {
		updateAggregatedAvailability(auth, now)
	}
	return true
}

func lapsedSubscriptionCheckDue(auth *Auth, now time.Time) bool {
	raw, _ := auth.Metadata[metadataSubscriptionCheckedAt].(string)
	checked, err := time.Parse(time.RFC3339, raw)
	return err != nil || !checked.Add(lapsedSubscriptionInterval).After(now)
}

func automaticUsageEligible(auth *Auth, now time.Time) bool {
	return auth != nil && usageGuardProviders[strings.ToLower(auth.Provider)] && !auth.Disabled && auth.Status != StatusDisabled &&
		auth.AuthKind() != AuthKindAPIKey && !hasUnauthorizedAuthFailure(auth) && auth.HasValidAccessToken(now)
}

// Usage failures affect polling only. A usage-endpoint 401 need not mean that
// inference is forbidden, so it must not reject the credential for routing.
type usageProbeFailure struct {
	credentialVersion uint64
	registrationEpoch uint64
	unauthorized      bool
	retryAt           time.Time
	failures          int
}

func (u *usageGuard) usageProbeAllowed(auth *Auth, now time.Time) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	failure, ok := u.probeFailures[auth.ID]
	if !ok {
		return true
	}
	if failure.unauthorized && (failure.credentialVersion != auth.CredentialVersion || failure.registrationEpoch != auth.RegistrationEpoch) {
		delete(u.probeFailures, auth.ID)
		return true
	}
	return !failure.unauthorized && !failure.retryAt.After(now)
}

func (u *usageGuard) recordUsageFailure(auth *Auth, err error, now time.Time) {
	var endpoint *usageEndpointError
	if !errors.As(err, &endpoint) || (endpoint.status != http.StatusUnauthorized && endpoint.status != http.StatusTooManyRequests) {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.probeFailures == nil {
		u.probeFailures = make(map[string]usageProbeFailure)
	}
	failure := u.probeFailures[auth.ID]
	if failure.unauthorized && (failure.credentialVersion != auth.CredentialVersion || failure.registrationEpoch != auth.RegistrationEpoch) {
		failure = usageProbeFailure{}
	}
	failure.credentialVersion = auth.CredentialVersion
	failure.registrationEpoch = auth.RegistrationEpoch
	failure.failures++
	failure.unauthorized = endpoint.status == http.StatusUnauthorized
	backoff := usageRecoveryInterval * time.Duration(1<<min(failure.failures-1, 5))
	backoff = min(backoff, lapsedSubscriptionInterval)
	failure.retryAt = now.Add(max(backoff, endpoint.retryAfter))
	u.probeFailures[auth.ID] = failure
}

func (u *usageGuard) clearUsageFailure(authID string) {
	u.mu.Lock()
	delete(u.probeFailures, authID)
	u.mu.Unlock()
}

func usageRetryAfter(header http.Header, now time.Time) time.Duration {
	raw := strings.TrimSpace(header.Get("Retry-After"))
	if seconds, err := strconv.ParseInt(raw, 10, 32); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if retryAt, err := http.ParseTime(raw); err == nil && retryAt.After(now) {
		return retryAt.Sub(now)
	}
	return 0
}

func blockedUsageError(provider string, err error) bool {
	var endpoint *usageEndpointError
	return strings.EqualFold(provider, "claude") && errors.As(err, &endpoint) && endpoint.organizationBlocked
}

func (m *Manager) setClaudeSubscriptionLapse(ctx context.Context, authID string, lapsed bool) {
	err := m.mutateAuthAvailability(ctx, authID, func(auth *Auth, now time.Time) bool {
		if !lapsed {
			return clearClaudeSubscriptionLapse(auth, now)
		}
		if !strings.EqualFold(auth.Provider, "claude") {
			return false
		}
		markClaudeSubscriptionLapsed(auth, now)
		return true
	})
	if err != nil {
		log.Warnf("usage guard: recording subscription state for %s: %v", authID, err)
	}
}

// ObserveClaudeUsageCheck records what a Claude usage request made outside the
// poller, such as the management panel's refresh, says about the subscription.
func (m *Manager) ObserveClaudeUsageCheck(ctx context.Context, authID string, status int, body []byte) {
	if m == nil {
		return
	}
	auth, ok := m.GetByID(authID)
	if !ok || !strings.EqualFold(auth.Provider, "claude") {
		return
	}
	if status == http.StatusForbidden && claudeUsageOrganizationBlocked(body) {
		m.setClaudeSubscriptionLapse(ctx, authID, true)
		return
	}
	if status >= 200 && status < 300 && ClaudeSubscriptionLapsed(auth) {
		if _, errParse := parseUsageObservation(auth.Provider, body); errParse == nil {
			m.setClaudeSubscriptionLapse(ctx, authID, false)
		}
	}
}

// IsClaudeUsageURL reports whether u is the Claude OAuth usage endpoint.
func IsClaudeUsageURL(u *url.URL) bool {
	return u != nil && strings.EqualFold(u.Hostname(), "api.anthropic.com") && strings.TrimSuffix(u.Path, "/") == "/api/oauth/usage"
}

func claudeUsageOrganizationBlocked(body []byte) bool {
	return gjson.GetBytes(body, "error.details.error_code").String() == claudeOrganizationOAuthBlocked ||
		gjson.GetBytes(body, "error.message").String() == claudeOrganizationOAuthMessage
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
		blocked := !isCodex && resp.StatusCode == http.StatusForbidden && claudeUsageOrganizationBlocked(body)
		return usageObservation{}, &usageEndpointError{status: resp.StatusCode, organizationBlocked: blocked, retryAfter: usageRetryAfter(resp.Header, time.Now())}
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
