package auth

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

const claudeUnifiedSignalPrefix = "anthropic-ratelimit-unified-"

// ClaudeWeeklyResetFirstSelector sends a request to the Claude credential whose
// weekly quota window resets soonest, so quota that would be lost at the reset
// is spent first. It ranks only within the highest available priority tier.
//
// The weekly window is read from the last observed anthropic-ratelimit-unified-*
// headers. A credential without a running window (no observation since start,
// or its reset already passed) has no deadline, so it only takes requests
// through Fallback once no credential with a running window is usable. The same
// holds for every non-Claude credential. A credential whose last response
// reported a rejected window is skipped until that window resets.
type ClaudeWeeklyResetFirstSelector struct {
	Fallback Selector
}

// NewClaudeWeeklyResetFirstSelector wraps fallback, which picks whenever no
// Claude credential has a running weekly window.
func NewClaudeWeeklyResetFirstSelector(fallback Selector) *ClaudeWeeklyResetFirstSelector {
	return &ClaudeWeeklyResetFirstSelector{Fallback: fallback}
}

func (s *ClaudeWeeklyResetFirstSelector) fallback() Selector {
	if s == nil || s.Fallback == nil {
		return &RoundRobinSelector{}
	}
	return s.Fallback
}

// Pick returns the usable Claude credential with the soonest weekly reset, or
// defers to Fallback when none has a running weekly window.
func (s *ClaudeWeeklyResetFirstSelector) Pick(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	fallback := s.fallback()
	candidates := auths
	if selectorUsesWeights(fallback) {
		candidates = positiveWeightAuths(auths)
	}
	now := time.Now()
	available, err := getSelectorAvailableAuths(ctx, candidates, provider, model, now)
	if err != nil {
		return nil, err
	}

	var ranked, open []*Auth
	resets := make(map[string]time.Time, len(available))
	for _, auth := range available {
		signals := claudeUnifiedSignals(auth)
		if signals == nil {
			open = append(open, auth)
			continue
		}
		if claudeWindowsRejected(signals, model, now) {
			continue
		}
		resetAt, running := claudeWindowReset(signals, "7d", now)
		if !running {
			open = append(open, auth)
			continue
		}
		resets[auth.ID] = resetAt
		ranked = append(ranked, auth)
	}

	if len(ranked) > 0 {
		sort.Slice(ranked, func(i, j int) bool {
			ri, rj := resets[ranked[i].ID], resets[ranked[j].ID]
			if !ri.Equal(rj) {
				return ri.Before(rj)
			}
			return ranked[i].ID < ranked[j].ID
		})
		picked := ranked[0]
		selectorLogEntry(ctx).Debugf("claude-weekly-reset-first: picked %s, weekly window resets %s", picked.ID, resets[picked.ID].Format(time.RFC3339))
		return picked, nil
	}
	if len(open) == 0 {
		// Every candidate reported a rejected window. Their cooldowns are not
		// recorded yet, so let Fallback choose and the upstream answer decide.
		open = available
	}
	return fallback.Pick(ctx, provider, model, opts, open)
}

// claudeUnifiedSignals returns the credential's last observed Claude unified
// rate-limit signals keyed by the lower-cased header suffix, or nil when the
// credential is not Claude or has none.
func claudeUnifiedSignals(auth *Auth) map[string]string {
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "claude") || len(auth.Quota.Signals) == 0 {
		return nil
	}
	var signals map[string]string
	for key, value := range auth.Quota.Signals {
		lower := strings.ToLower(strings.TrimSpace(key))
		if !strings.HasPrefix(lower, claudeUnifiedSignalPrefix) {
			continue
		}
		if signals == nil {
			signals = make(map[string]string)
		}
		signals[strings.TrimPrefix(lower, claudeUnifiedSignalPrefix)] = strings.TrimSpace(value)
	}
	return signals
}

// claudeWindowReset reports the reset time of the named window ("5h", "7d",
// "7d_oi") and whether that reset is still ahead.
func claudeWindowReset(signals map[string]string, window string, now time.Time) (time.Time, bool) {
	seconds, errParse := strconv.ParseInt(signals[window+"-reset"], 10, 64)
	if errParse != nil || seconds <= 0 {
		return time.Time{}, false
	}
	resetAt := time.Unix(seconds, 0)
	return resetAt, resetAt.After(now)
}

// claudeWindowsRejected reports whether a window that applies to model was
// rejected in the last observation and has not reset since. The Fable weekly
// window (7d_oi) only applies to Fable models.
func claudeWindowsRejected(signals map[string]string, model string, now time.Time) bool {
	windows := []string{"5h", "7d"}
	if strings.Contains(strings.ToLower(model), "fable") {
		windows = append(windows, "7d_oi")
	}
	for _, window := range windows {
		if !strings.EqualFold(signals[window+"-status"], "rejected") {
			continue
		}
		if _, running := claudeWindowReset(signals, window, now); running {
			return true
		}
	}
	return false
}

// selectorUsesWeights reports whether selector, or the selector it wraps,
// distributes by credential weight, so weight-0 credentials must be excluded.
func selectorUsesWeights(selector Selector) bool {
	switch typed := selector.(type) {
	case *WeightedRoundRobinSelector:
		return true
	case *ClaudeWeeklyResetFirstSelector:
		return selectorUsesWeights(typed.fallback())
	default:
		return false
	}
}
