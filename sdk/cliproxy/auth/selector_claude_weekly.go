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

	fable := isClaudeFableModel(model)
	var ranked, reserved, open []*Auth
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
		if !fable && claudeWeeklyReservedForFable(signals, now) {
			reserved = append(reserved, auth)
			continue
		}
		ranked = append(ranked, auth)
	}

	// Order: unreserved running windows by reset, then credentials without a
	// running window (through Fallback), then Fable-reserved ones by reset.
	if len(ranked) == 0 && len(open) == 0 {
		ranked = reserved
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

// claudeFableReserveMargin is how close (as a share of the weekly window) the
// weekly share left may come to half the Fable share left before non-Fable
// requests stop using the credential.
const claudeFableReserveMargin = 0.05

// claudeWeeklyReservedForFable reports whether the credential's weekly window
// should be kept for Fable: half of its Fable weekly share left is reserved,
// and the weekly share left is within claudeFableReserveMargin of that reserve.
// Fable traffic also counts toward the weekly window, so spending the weekly
// window on other models below this point would strand the Fable quota.
// A credential that never reported a Fable window has nothing to reserve.
func claudeWeeklyReservedForFable(signals map[string]string, now time.Time) bool {
	fableLeft, ok := claudeWindowShareLeft(signals, "7d_oi", now)
	if !ok {
		return false
	}
	weeklyLeft, ok := claudeWindowShareLeft(signals, "7d", now)
	if !ok {
		return false
	}
	return weeklyLeft <= fableLeft/2+claudeFableReserveMargin
}

// claudeWindowShareLeft returns the share (0-1) of the named window still
// left. A window whose reset has passed is full again.
func claudeWindowShareLeft(signals map[string]string, window string, now time.Time) (float64, bool) {
	raw, hasUtilization := signals[window+"-utilization"]
	_, hasReset := signals[window+"-reset"]
	if !hasUtilization && !hasReset {
		return 0, false
	}
	if _, running := claudeWindowReset(signals, window, now); hasReset && !running {
		return 1, true
	}
	if strings.EqualFold(signals[window+"-status"], "rejected") {
		return 0, true
	}
	used, errParse := strconv.ParseFloat(raw, 64)
	if errParse != nil {
		return 0, false
	}
	return min(max(1-used, 0), 1), true
}

func isClaudeFableModel(model string) bool {
	return strings.Contains(strings.ToLower(model), "fable")
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
	if isClaudeFableModel(model) {
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
