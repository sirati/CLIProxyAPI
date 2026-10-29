package auth

import (
	"context"
	"encoding/json"
	"fmt"
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
	// MetadataUseOnce stops routing to the credential (enabled, login kept)
	// once its weekly window is empty or reaches MetadataMinRemaining, or once
	// the window resets: the weekly remainder rose more than useOnceResetRise
	// points since the previous check. It drains one window's quota, then stops.
	MetadataUseOnce = "use_once"
	// MetadataUseOnceTarget overrides the saved minimum for the current run.
	MetadataUseOnceTarget = "use_once_target"

	// metadataUseOnceLastRemaining is the weekly remainder, in percent, at the
	// previous check. It is kept in the auth file across restarts.
	metadataUseOnceLastRemaining = "use_once_last_remaining"
	// metadataUseOnceOldBaseline held weekly usage, not the remainder, in
	// earlier builds. It is dropped on the next check.
	metadataUseOnceOldBaseline = "use_once_baseline"
	// metadataUseOnceRetired records why and when a use_once credential retired.
	metadataUseOnceRetired = "use_once_retired"
)

const (
	usageGuardInterval    = 5 * time.Minute
	usageRecoveryInterval = 15 * time.Minute
	useOnceResetRise      = 5.0

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
	minRemaining     float64
	hasMinRemaining  bool
	useOnce          bool
	lastRemaining    float64
	hasLastRemaining bool
	// retired keeps the credential out of routing until explicitly restarted.
	retired bool
}

func usageGuardSettingsOf(auth *Auth) (usageGuardSettings, bool) {
	var settings usageGuardSettings
	if auth == nil || !usageGuardProviders[strings.ToLower(strings.TrimSpace(auth.Provider))] || len(auth.Metadata) == 0 {
		return settings, false
	}
	settings.minRemaining, settings.hasMinRemaining = metadataNumber(auth.Metadata[MetadataMinRemaining])
	settings.useOnce, _ = metadataBool(auth.Metadata[MetadataUseOnce])
	settings.lastRemaining, settings.hasLastRemaining = metadataNumber(auth.Metadata[metadataUseOnceLastRemaining])
	_, settings.retired = auth.Metadata[metadataUseOnceRetired]
	if settings.useOnce {
		if target, ok := metadataNumber(auth.Metadata[MetadataUseOnceTarget]); ok {
			settings.minRemaining, settings.hasMinRemaining = target, target > 0
		}
	}
	if settings.hasMinRemaining && settings.minRemaining <= 0 {
		settings.hasMinRemaining = false
	}
	return settings, settings.useOnce || settings.hasMinRemaining
}

// UseOnceStopped reports a persistent routing stop, independently of login status.
func UseOnceStopped(auth *Auth) bool {
	settings, _ := usageGuardSettingsOf(auth)
	return settings.useOnce && settings.retired
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
	// lastRemaining is the remainder to store for the next check; set only
	// when it changes.
	lastRemaining    float64
	setLastRemaining bool
}

// decideUsageGuard applies a credential's settings to the share of its
// weekly window that is left, in percent (0-100).
func decideUsageGuard(settings usageGuardSettings, remaining float64) usageGuardDecision {
	var decision usageGuardDecision
	belowMinimum := settings.hasMinRemaining && (remaining < settings.minRemaining || (settings.useOnce && remaining == settings.minRemaining))
	if !settings.useOnce {
		if belowMinimum {
			decision.action = usageGuardHold
			decision.reason = fmt.Sprintf("weekly remaining %.1f%% is below the minimum %.1f%%", remaining, settings.minRemaining)
		}
		return decision
	}

	// Compare with the previous check: using the credential lowers the
	// remainder, only a reset raises it.
	previousRemaining := settings.lastRemaining
	if !settings.hasLastRemaining {
		previousRemaining = remaining
	}
	if !settings.hasLastRemaining || remaining != settings.lastRemaining {
		decision.lastRemaining, decision.setLastRemaining = remaining, true
	}
	switch {
	case remaining <= 0:
		decision.action = usageGuardRetire
		decision.reason = "weekly quota is empty"
	case belowMinimum:
		decision.action = usageGuardRetire
		decision.reason = fmt.Sprintf("weekly remaining %.1f%% is below the minimum %.1f%%", remaining, settings.minRemaining)
	case remaining-previousRemaining > useOnceResetRise:
		decision.action = usageGuardRetire
		decision.reason = fmt.Sprintf("weekly window reset: remaining rose from %.1f%% to %.1f%%", previousRemaining, remaining)
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
	holds      map[string]time.Time
	lastChecks map[string]time.Time
	codexUsage map[string]map[string]string
	wake       chan string
}

// StartUsageGuard checks guarded credentials every five minutes and other OAuth
// credentials every fifteen minutes, revalidating upstream quota cooldowns.
func (m *Manager) StartUsageGuard(parent context.Context) {
	if m == nil {
		return
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	guard := &usageGuard{cancel: cancel, holds: make(map[string]time.Time), wake: make(chan string, 1)}
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
		m.pollUsageGuards(ctx, guard, time.Now(), false, "", false)
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				m.pollUsageGuards(ctx, guard, now, false, "", false)
			case source := <-guard.wake:
				m.pollUsageGuards(ctx, guard, time.Now(), true, source, false)
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
	m.pollUsageGuards(ctx, guard, time.Now(), false, "", true)
}

func (m *Manager) pollUsageGuards(ctx context.Context, guard *usageGuard, now time.Time, forceCodex bool, source string, forceAll bool) {
	checked := make(map[string]bool)
	reset := false
	check := func(auth *Auth) {
		checked[auth.ID] = true
		guard.recordUsageCheck(auth.ID, now)
		settings, enabled := usageGuardSettingsOf(auth)
		if !enabled {
			if until, held := guard.held(auth.ID); held {
				m.releaseUsageHold(ctx, guard, auth.ID, until)
				if current, ok := m.GetByID(auth.ID); ok {
					auth = current
				}
			}
		}
		if m.checkUsageGuard(ctx, guard, auth, settings) {
			reset = true
		}
	}
	for _, auth := range m.List() {
		if ctx.Err() != nil {
			return
		}
		if !automaticUsageEligible(auth) {
			continue
		}
		if auth.ID == source {
			continue
		}
		if ClaudeSubscriptionLapsed(auth) {
			// Probe lapsed subscriptions on their own slow, persisted schedule,
			// also on restarts and forced sweeps.
			if lapsedSubscriptionCheckDue(auth, now) {
				m.setClaudeSubscriptionLapse(ctx, auth.ID, true)
				if current, ok := m.GetByID(auth.ID); ok {
					check(current)
				}
			}
			continue
		}
		due := guard.usageCheckDue(auth, now)
		if forceCodex {
			due = strings.EqualFold(auth.Provider, "codex")
		}
		if forceAll || due {
			check(auth)
		}
	}
	if reset {
		// A reset seen by this sweep also checks peers whose normal poll is not due.
		// Already checked peers have fresh observations and are not fetched twice.
		for _, auth := range m.List() {
			if ctx.Err() != nil {
				return
			}
			if auth.Provider == "codex" && !checked[auth.ID] && auth.ID != source && automaticUsageEligible(auth) {
				check(auth)
			}
		}
	}
}

func (m *Manager) checkUsageGuard(ctx context.Context, guard *usageGuard, auth *Auth, settings usageGuardSettings) bool {
	observation, errFetch := m.fetchUsageObservation(ctx, auth)
	if errFetch != nil {
		if blockedUsageError(auth.Provider, errFetch) && !ClaudeSubscriptionLapsed(auth) {
			log.Infof("usage guard: %s subscription lapsed, stopping request routing", auth.ID)
			m.setClaudeSubscriptionLapse(ctx, auth.ID, true)
		}
		log.Warnf("usage guard: %s: %v", auth.ID, errFetch)
		// Unknown usage must not lift a hold: keep it until a check succeeds.
		if _, held := guard.held(auth.ID); held {
			m.applyUsageHold(ctx, guard, auth.ID, "usage check failed, keeping the hold")
		}
		return false
	}
	reset := guard.observeCodexUsage(auth, observation.signals)
	m.applyUsageObservation(ctx, auth, observation)
	if ClaudeSubscriptionLapsed(auth) {
		log.Infof("usage guard: %s subscription works again, resuming request routing", auth.ID)
		m.setClaudeSubscriptionLapse(ctx, auth.ID, false)
	}
	remaining := 100 - observation.weeklyUsed
	if settings.useOnce && settings.retired {
		return reset // Keep observing usage without restarting request routing.
	}
	decision := decideUsageGuard(settings, remaining)
	if decision.setLastRemaining {
		m.setUsageGuardMetadata(ctx, auth.ID, metadataUseOnceLastRemaining, decision.lastRemaining)
	}
	if _, stale := auth.Metadata[metadataUseOnceOldBaseline]; stale {
		m.deleteUsageGuardMetadata(ctx, auth.ID, metadataUseOnceOldBaseline)
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
			log.Infof("usage guard: releasing %s: weekly remaining %.1f%%", auth.ID, remaining)
			m.releaseUsageHold(ctx, guard, auth.ID, until)
		}
	}
	return reset
}

func (m *Manager) applyUsageHold(ctx context.Context, guard *usageGuard, authID, reason string) {
	var until time.Time
	errMutate := m.mutateAuthAvailability(ctx, authID, func(auth *Auth, now time.Time) bool {
		until = now.Add(credentialUsageInterval(auth) + time.Minute)
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
	if until, held := guard.held(authID); held {
		m.releaseUsageHold(ctx, guard, authID, until)
	}
	guard.forget(authID)
	now := time.Now()
	errMutate := m.mutateAuthAvailability(ctx, authID, func(auth *Auth, _ time.Time) bool {
		auth.StatusMessage = "use once: " + reason
		if auth.Metadata == nil {
			auth.Metadata = make(map[string]any)
		}
		auth.Metadata[metadataUseOnceRetired] = now.UTC().Format(time.RFC3339) + " " + reason
		return true
	})
	if errMutate != nil {
		log.Warnf("usage guard: retiring %s: %v", authID, errMutate)
		return
	}
	log.Infof("usage guard: stopped routing to use-once credential %s: %s (login enabled)", authID, reason)
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
