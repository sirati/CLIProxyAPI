package auth

import (
	"errors"
	"time"
)

// ModelCooldownResetIn reports how long until the first credential of an
// all-cooling model recovers, when err says every credential is cooling.
func ModelCooldownResetIn(err error) (time.Duration, bool) {
	var cooldown *modelCooldownError
	if !errors.As(err, &cooldown) || cooldown == nil {
		return 0, false
	}
	return cooldown.resetIn, true
}

// EarliestRecovery returns when the first held-back credential of provider
// becomes usable again. It reports false when none of them is cooling.
func (m *Manager) EarliestRecovery(provider string, now time.Time) (time.Time, bool) {
	if m == nil {
		return time.Time{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var earliest time.Time
	for _, auth := range m.auths {
		if auth == nil || auth.Disabled || auth.Provider != provider {
			continue
		}
		recovery := authRecovery(auth, now)
		if !recovery.IsZero() && (earliest.IsZero() || recovery.Before(earliest)) {
			earliest = recovery
		}
	}
	return earliest, !earliest.IsZero()
}

// authRecovery is when auth stops being held back: a credential-wide block
// outlasts any model block, otherwise its first model recovers first.
func authRecovery(auth *Auth, now time.Time) time.Time {
	var credential time.Time
	if auth.Quota.Exceeded && auth.Quota.NextRecoverAt.After(now) {
		credential = auth.Quota.NextRecoverAt
	}
	if auth.Unavailable && auth.NextRetryAfter.After(credential) && auth.NextRetryAfter.After(now) {
		credential = auth.NextRetryAfter
	}
	if !credential.IsZero() {
		return credential
	}
	var model time.Time
	for _, state := range auth.ModelStates {
		if state == nil || !state.Unavailable || !state.NextRetryAfter.After(now) {
			continue
		}
		if model.IsZero() || state.NextRetryAfter.Before(model) {
			model = state.NextRetryAfter
		}
	}
	return model
}
