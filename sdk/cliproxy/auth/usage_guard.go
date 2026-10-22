package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

// Credential metadata keys read by the usage guard. Both are set per auth file,
// for example through PATCH /auth-files/fields.
const (
	// MetadataMinRemaining is the share of the weekly window, in percent,
	// that must stay unused. Below it the credential is not used until a later
	// check finds it above again.
	MetadataMinRemaining = "min_remaining"
	// MetadataUseOnce retires the credential for good (disabled, login kept)
	// once its weekly window is empty or below MetadataMinRemaining, or once
	// weekly usage has risen more than useOnceMaxWeeklyIncrease points.
	MetadataUseOnce = "use_once"

	// metadataUseOnceBaseline is the lowest weekly usage, in percent, seen
	// since use_once was set. It is kept in the auth file across restarts.
	metadataUseOnceBaseline = "use_once_baseline"
	// metadataUseOnceRetired records why and when a use_once credential retired.
	metadataUseOnceRetired = "use_once_retired"
)

const (
	usageGuardInterval       = 5 * time.Minute
	usageGuardFetchTimeout   = 30 * time.Second
	useOnceMaxWeeklyIncrease = 5.0

	claudeUsageURL = "https://api.anthropic.com/api/oauth/usage"
	codexUsageURL  = "https://chatgpt.com/backend-api/wham/usage"

	codexUsageUserAgent = "codex-tui/0.154.0 (Mac OS 26.5.2; arm64) iTerm.app/3.6.11 (codex-tui; 0.154.0)"
	codexWeekSeconds    = 7 * 24 * 60 * 60
	codexMinMonthSecs   = 28 * 24 * 60 * 60
	codexMaxMonthSecs   = 31 * 24 * 60 * 60
)

// usageGuardProviders are the providers whose weekly usage the guard can read.
var usageGuardProviders = map[string]bool{"claude": true, "codex": true}

type usageGuardSettings struct {
	minRemaining    float64
	hasMinRemaining bool
	useOnce         bool
	baseline        float64
	hasBaseline     bool
	// retired is set when a use_once credential retired and was re-enabled since.
	retired bool
}

func usageGuardSettingsOf(auth *Auth) (usageGuardSettings, bool) {
	var settings usageGuardSettings
	if auth == nil || !usageGuardProviders[strings.ToLower(strings.TrimSpace(auth.Provider))] || len(auth.Metadata) == 0 {
		return settings, false
	}
	settings.minRemaining, settings.hasMinRemaining = metadataNumber(auth.Metadata[MetadataMinRemaining])
	settings.useOnce, _ = metadataBool(auth.Metadata[MetadataUseOnce])
	settings.baseline, settings.hasBaseline = metadataNumber(auth.Metadata[metadataUseOnceBaseline])
	_, settings.retired = auth.Metadata[metadataUseOnceRetired]
	if settings.retired {
		// Re-enabled after retiring: start over from the current usage.
		settings.hasBaseline = false
	}
	if settings.hasMinRemaining && settings.minRemaining <= 0 {
		settings.hasMinRemaining = false
	}
	return settings, settings.useOnce || settings.hasMinRemaining
}

type usageGuardAction int

const (
	usageGuardAllow usageGuardAction = iota
	usageGuardHold
	usageGuardRetire
)

type usageGuardDecision struct {
	action usageGuardAction
	reason string
	// baseline is the use_once baseline to store; set only when it changes.
	baseline    float64
	setBaseline bool
}

// decideUsageGuard applies a credential's settings to its weekly usage in
// percent (0-100).
func decideUsageGuard(settings usageGuardSettings, weeklyUsed float64) usageGuardDecision {
	var decision usageGuardDecision
	remaining := 100 - weeklyUsed
	belowMinimum := settings.hasMinRemaining && remaining < settings.minRemaining
	if !settings.useOnce {
		if belowMinimum {
			decision.action = usageGuardHold
			decision.reason = fmt.Sprintf("weekly remaining %.1f%% is below the minimum %.1f%%", remaining, settings.minRemaining)
		}
		return decision
	}

	baseline := settings.baseline
	if !settings.hasBaseline || weeklyUsed < baseline {
		// First reading, or the weekly window reset since: measure from here.
		baseline = weeklyUsed
		decision.baseline, decision.setBaseline = baseline, true
	}
	switch {
	case remaining <= 0:
		decision.action = usageGuardRetire
		decision.reason = "weekly quota is empty"
	case belowMinimum:
		decision.action = usageGuardRetire
		decision.reason = fmt.Sprintf("weekly remaining %.1f%% is below the minimum %.1f%%", remaining, settings.minRemaining)
	case weeklyUsed-baseline > useOnceMaxWeeklyIncrease:
		decision.action = usageGuardRetire
		decision.reason = fmt.Sprintf("weekly usage rose from %.1f%% to %.1f%%", baseline, weeklyUsed)
	}
	return decision
}

// parseClaudeWeeklyUsage reads seven_day.utilization (percent) from a Claude
// GET /api/oauth/usage body.
func parseClaudeWeeklyUsage(body []byte) (float64, error) {
	if !gjson.ValidBytes(body) {
		return 0, fmt.Errorf("usage body is not JSON")
	}
	used := gjson.GetBytes(body, "seven_day.utilization")
	if used.Type != gjson.Number {
		return 0, fmt.Errorf("usage body has no seven_day.utilization")
	}
	return min(max(used.Float(), 0), 100), nil
}

// parseCodexWeeklyUsage reads used_percent of the weekly window from a Codex
// GET /wham/usage body. Windows are told apart by limit_window_seconds, as the
// panel's Quota page does; team plans have a monthly window instead, which
// counts as the long window here. Without durations the secondary window is
// the long one.
func parseCodexWeeklyUsage(body []byte) (float64, error) {
	if !gjson.ValidBytes(body) {
		return 0, fmt.Errorf("usage body is not JSON")
	}
	rateLimit := firstGJSON(gjson.ParseBytes(body), "rate_limit", "rateLimit")
	if !rateLimit.IsObject() {
		return 0, fmt.Errorf("usage body has no rate_limit")
	}
	var weekly gjson.Result
	for _, keys := range [][]string{{"primary_window", "primaryWindow"}, {"secondary_window", "secondaryWindow"}} {
		window := firstGJSON(rateLimit, keys...)
		seconds := firstGJSON(window, "limit_window_seconds", "limitWindowSeconds").Int()
		if seconds == codexWeekSeconds || (seconds >= codexMinMonthSecs && seconds <= codexMaxMonthSecs) {
			weekly = window
			break
		}
	}
	if !weekly.Exists() {
		weekly = firstGJSON(rateLimit, "secondary_window", "secondaryWindow")
		if firstGJSON(weekly, "limit_window_seconds", "limitWindowSeconds").Exists() {
			// Durations are given and none is a week or a month.
			weekly = gjson.Result{}
		}
	}
	if !weekly.IsObject() {
		return 0, fmt.Errorf("usage body has no weekly window")
	}
	used := firstGJSON(weekly, "used_percent", "usedPercent")
	if used.Type != gjson.Number {
		return 0, fmt.Errorf("usage body has no weekly used_percent")
	}
	return min(max(used.Float(), 0), 100), nil
}

func firstGJSON(object gjson.Result, paths ...string) gjson.Result {
	for _, path := range paths {
		if value := object.Get(path); value.Exists() {
			return value
		}
	}
	return gjson.Result{}
}

type usageGuard struct {
	cancel context.CancelFunc

	mu sync.Mutex
	// holds maps auth ID to the recovery time of the hold this guard set, so a
	// release never clears a cooldown that came from an upstream 429.
	holds map[string]time.Time
}

// StartUsageGuard polls the provider's usage endpoint every five minutes for each
// credential with use_once or min_remaining set, and holds or retires it.
func (m *Manager) StartUsageGuard(parent context.Context) {
	if m == nil {
		return
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	guard := &usageGuard{cancel: cancel, holds: make(map[string]time.Time)}
	m.mu.Lock()
	previous := m.usageGuard
	m.usageGuard = guard
	m.mu.Unlock()
	if previous != nil {
		previous.cancel()
	}
	go func() {
		ticker := time.NewTicker(usageGuardInterval)
		defer ticker.Stop()
		for {
			m.checkUsageGuards(ctx, guard)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// StopUsageGuard stops the poller started by StartUsageGuard.
func (m *Manager) StopUsageGuard() {
	if m == nil {
		return
	}
	m.mu.Lock()
	guard := m.usageGuard
	m.usageGuard = nil
	m.mu.Unlock()
	if guard != nil {
		guard.cancel()
	}
}

func (m *Manager) checkUsageGuards(ctx context.Context, guard *usageGuard) {
	for _, auth := range m.List() {
		if ctx.Err() != nil {
			return
		}
		if auth.Disabled || auth.Status == StatusDisabled {
			guard.forget(auth.ID)
			continue
		}
		settings, enabled := usageGuardSettingsOf(auth)
		if !enabled {
			if until, held := guard.held(auth.ID); held {
				m.releaseUsageHold(ctx, guard, auth.ID, until)
			}
			continue
		}
		m.checkUsageGuard(ctx, guard, auth, settings)
	}
}

func (m *Manager) checkUsageGuard(ctx context.Context, guard *usageGuard, auth *Auth, settings usageGuardSettings) {
	weeklyUsed, errFetch := m.fetchWeeklyUsage(ctx, auth)
	if errFetch != nil {
		log.Warnf("usage guard: %s: %v", auth.ID, errFetch)
		// Unknown usage must not lift a hold: keep it until a check succeeds.
		if _, held := guard.held(auth.ID); held {
			m.applyUsageHold(ctx, guard, auth.ID, "usage check failed, keeping the hold")
		}
		return
	}
	decision := decideUsageGuard(settings, weeklyUsed)
	if decision.setBaseline {
		m.setUsageGuardMetadata(ctx, auth.ID, metadataUseOnceBaseline, decision.baseline)
	}
	if settings.retired {
		m.deleteUsageGuardMetadata(ctx, auth.ID, metadataUseOnceRetired)
	}
	switch decision.action {
	case usageGuardRetire:
		m.retireUseOnce(ctx, guard, auth.ID, decision.reason)
	case usageGuardHold:
		if _, held := guard.held(auth.ID); !held {
			log.Infof("usage guard: holding %s: %s", auth.ID, decision.reason)
		}
		m.applyUsageHold(ctx, guard, auth.ID, decision.reason)
	default:
		if until, held := guard.held(auth.ID); held {
			log.Infof("usage guard: releasing %s: weekly remaining %.1f%%", auth.ID, 100-weeklyUsed)
			m.releaseUsageHold(ctx, guard, auth.ID, until)
		}
	}
}

// fetchWeeklyUsage asks the credential's provider for its weekly usage in
// percent. The request goes through the provider executor, which adds the
// credential's token.
func (m *Manager) fetchWeeklyUsage(ctx context.Context, auth *Auth) (float64, error) {
	reqCtx, cancel := context.WithTimeout(ctx, usageGuardFetchTimeout)
	defer cancel()
	isCodex := strings.EqualFold(strings.TrimSpace(auth.Provider), "codex")
	usageURL, parse := claudeUsageURL, parseClaudeWeeklyUsage
	if isCodex {
		usageURL, parse = codexUsageURL, parseCodexWeeklyUsage
	}
	req, errReq := http.NewRequestWithContext(reqCtx, http.MethodGet, usageURL, nil)
	if errReq != nil {
		return 0, errReq
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
	resp, errDo := m.HttpRequest(reqCtx, auth, req)
	if errDo != nil {
		return 0, errDo
	}
	defer func() { _ = resp.Body.Close() }()
	body, errRead := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if errRead != nil {
		return 0, errRead
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("usage endpoint returned HTTP %d", resp.StatusCode)
	}
	return parse(body)
}

func (m *Manager) applyUsageHold(ctx context.Context, guard *usageGuard, authID, reason string) {
	until := time.Now().Add(usageGuardInterval + time.Minute)
	errMutate := m.mutateAuthAvailability(ctx, authID, func(auth *Auth, _ time.Time) bool {
		applyCooldownFields(&auth.Quota, QuotaState{
			Exceeded:      true,
			Reason:        "credential_quota",
			NextRecoverAt: until,
		})
		auth.Unavailable = true
		auth.NextRetryAfter = until
		auth.StatusMessage = "usage guard: " + reason
		return true
	})
	if errMutate != nil {
		log.Warnf("usage guard: holding %s: %v", authID, errMutate)
		return
	}
	guard.setHeld(authID, until)
}

func (m *Manager) releaseUsageHold(ctx context.Context, guard *usageGuard, authID string, until time.Time) {
	guard.forget(authID)
	errMutate := m.mutateAuthAvailability(ctx, authID, func(auth *Auth, _ time.Time) bool {
		if auth.Quota.Reason != "credential_quota" || !auth.Quota.NextRecoverAt.Equal(until) {
			// Something else replaced the hold, such as an upstream 429.
			return false
		}
		applyCooldownFields(&auth.Quota, QuotaState{})
		auth.Unavailable = false
		if auth.NextRetryAfter.Equal(until) {
			auth.NextRetryAfter = time.Time{}
		}
		if strings.HasPrefix(auth.StatusMessage, "usage guard: ") {
			auth.StatusMessage = ""
		}
		return true
	})
	if errMutate != nil {
		log.Warnf("usage guard: releasing %s: %v", authID, errMutate)
	}
}

func (m *Manager) retireUseOnce(ctx context.Context, guard *usageGuard, authID, reason string) {
	guard.forget(authID)
	now := time.Now()
	errMutate := m.mutateAuthAvailability(ctx, authID, func(auth *Auth, _ time.Time) bool {
		auth.Disabled = true
		auth.Status = StatusDisabled
		auth.StatusMessage = "use once: " + reason
		if auth.Metadata == nil {
			auth.Metadata = make(map[string]any)
		}
		auth.Metadata["disabled"] = true
		auth.Metadata[metadataUseOnceRetired] = now.UTC().Format(time.RFC3339) + " " + reason
		return true
	})
	if errMutate != nil {
		log.Warnf("usage guard: retiring %s: %v", authID, errMutate)
		return
	}
	log.Infof("usage guard: retired use-once credential %s: %s (login kept, re-enable to use again)", authID, reason)
}

func (m *Manager) setUsageGuardMetadata(ctx context.Context, authID, key string, value any) {
	errMutate := m.mutateAuthAvailability(ctx, authID, func(auth *Auth, _ time.Time) bool {
		if auth.Metadata == nil {
			auth.Metadata = make(map[string]any)
		}
		auth.Metadata[key] = value
		return true
	})
	if errMutate != nil {
		log.Warnf("usage guard: saving %s for %s: %v", key, authID, errMutate)
	}
}

// mutateAuthAvailability changes one credential's state under the manager
// lock, persists it, and republishes it to the model registry and scheduler,
// in the same order as ResetQuota.
func (m *Manager) mutateAuthAvailability(ctx context.Context, authID string, mutate func(*Auth, time.Time) bool) error {
	now := time.Now()
	releaseMutation := m.lockAuthMutation(authID)
	defer releaseMutation()
	m.mu.Lock()
	auth, ok := m.auths[authID]
	if !ok || auth == nil {
		m.mu.Unlock()
		return fmt.Errorf("auth %s not found", authID)
	}
	var cooldownRecordsBefore []CooldownStateRecord
	trackCooldownState := m.cooldownStore != nil
	if trackCooldownState {
		cooldownRecordsBefore = m.cooldownStateRecordsForAuthLocked(auth, now)
	}
	if !mutate(auth, now) {
		m.mu.Unlock()
		return nil
	}
	auth.Generation++
	auth.UpdatedAt = now
	snapshot := auth.Clone()
	cooldownStateChanged := false
	if trackCooldownState {
		cooldownStateChanged = !cooldownStateRecordsEqual(cooldownRecordsBefore, m.cooldownStateRecordsForAuthLocked(auth, now))
	}
	errPersist := m.persistLocked(ctx, auth)
	m.mu.Unlock()
	releaseMutation()

	if cooldownStateChanged {
		m.persistCooldownStates(context.Background())
	}
	m.republishAuthModels(snapshot, now)
	return errPersist
}

func (u *usageGuard) held(authID string) (time.Time, bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	until, ok := u.holds[authID]
	return until, ok
}

func (u *usageGuard) setHeld(authID string, until time.Time) {
	u.mu.Lock()
	u.holds[authID] = until
	u.mu.Unlock()
}

func (u *usageGuard) forget(authID string) {
	u.mu.Lock()
	delete(u.holds, authID)
	u.mu.Unlock()
}

func metadataNumber(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		parsed, errParse := typed.Float64()
		return parsed, errParse == nil
	case string:
		parsed, errParse := strconv.ParseFloat(strings.TrimSuffix(strings.TrimSpace(typed), "%"), 64)
		return parsed, errParse == nil
	default:
		return 0, false
	}
}

func metadataBool(value any) (bool, bool) {
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		parsed, errParse := strconv.ParseBool(strings.TrimSpace(typed))
		return parsed, errParse == nil
	default:
		return false, false
	}
}

// republishAuthModels pushes a changed credential to the model registry and the
// scheduler, as the tail of ResetQuota does.
func (m *Manager) republishAuthModels(snapshot *Auth, now time.Time) {
	if snapshot == nil {
		return
	}
	supportedModels, regEpoch := registry.GetGlobalRegistry().GetModelsAndEpochForClient(snapshot.ID)
	projections := make([]registry.ClientModelProjection, 0, len(supportedModels))
	for _, supported := range supportedModels {
		if supported == nil || strings.TrimSpace(supported.ID) == "" {
			continue
		}
		projections = append(projections, m.clientModelProjectionForAuth(snapshot, supported.ID, now))
	}
	registry.GetGlobalRegistry().ApplyClientModelProjections(snapshot.ID, regEpoch, snapshot.Generation, projections)
	if m.scheduler != nil {
		m.scheduler.upsertAuth(snapshot)
	}
}

func (m *Manager) deleteUsageGuardMetadata(ctx context.Context, authID, key string) {
	errMutate := m.mutateAuthAvailability(ctx, authID, func(auth *Auth, _ time.Time) bool {
		if _, ok := auth.Metadata[key]; !ok {
			return false
		}
		delete(auth.Metadata, key)
		return true
	})
	if errMutate != nil {
		log.Warnf("usage guard: removing %s for %s: %v", key, authID, errMutate)
	}
}
