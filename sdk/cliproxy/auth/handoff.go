package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/handoff"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdkhandoff "github.com/router-for-me/CLIProxyAPI/v8/sdk/handoff"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

// After a binary handoff the old process is dead to decisions: it picks no
// credential, refreshes no token and writes nothing. Results of the requests
// it is still finishing go to the new process, which owns all state.

func init() {
	handoff.AddCapability(sdkhandoff.KindStateAffinity)
	handoff.AddCapability(sdkhandoff.KindStateCooldown)
	handoff.AddCapability(sdkhandoff.KindEventResult)
}

// handoffFrozen is the guard at every decision point.
func handoffFrozen(ctx context.Context) error {
	if handoff.Frozen() {
		return handoff.NoteFrozen(ctx)
	}
	return nil
}

// FreezeForHandoff stops every background owner and every write. Requests
// already running keep their in-memory state and finish.
func (m *Manager) FreezeForHandoff() {
	if m == nil {
		return
	}
	m.StopAutoRefresh()
	m.StopUsageGuard()
	m.SetStore(nil)
	m.SetCooldownStateStore(nil)
}

// handoffResult is the serialisable part of a Result.
type handoffResult struct {
	AuthID               string         `json:"auth_id"`
	Provider             string         `json:"provider,omitempty"`
	Model                string         `json:"model,omitempty"`
	RouteModel           string         `json:"route_model,omitempty"`
	Success              bool           `json:"success"`
	RetryAfter           *time.Duration `json:"retry_after,omitempty"`
	CredentialScope      bool           `json:"credential_scope,omitempty"`
	Error                *Error         `json:"error,omitempty"`
	SkipQuotaObservation bool           `json:"skip_quota_observation,omitempty"`
	Stream               bool           `json:"stream,omitempty"`
	Alt                  string         `json:"alt,omitempty"`
	Headers              http.Header    `json:"headers,omitempty"`
	Query                url.Values     `json:"query,omitempty"`
	SourceFormat         string         `json:"source_format,omitempty"`
	ResponseFormat       string         `json:"response_format,omitempty"`
	Metadata             map[string]any `json:"metadata,omitempty"`
}

// forwardHandoffResult sends a result produced after the handoff to the new
// process. It reports whether the result was taken.
func forwardHandoffResult(result Result) bool {
	if !handoff.Frozen() {
		return false
	}
	forwarded := handoffResult{
		AuthID: result.AuthID, Provider: result.Provider, Model: result.Model, RouteModel: result.RouteModel,
		Success: result.Success, RetryAfter: result.RetryAfter, CredentialScope: result.CredentialScope,
		Error: result.Error, SkipQuotaObservation: result.SkipQuotaObservation,
		Stream: result.Options.Stream, Alt: result.Options.Alt, Headers: result.Options.Headers,
		Query: result.Options.Query, SourceFormat: string(result.Options.SourceFormat),
		ResponseFormat: string(result.Options.ResponseFormat),
	}
	for key, value := range result.Options.Metadata {
		if _, errMarshal := json.Marshal(value); errMarshal == nil {
			if forwarded.Metadata == nil {
				forwarded.Metadata = make(map[string]any)
			}
			forwarded.Metadata[key] = value
		}
	}
	return handoff.Forward(sdkhandoff.KindEventResult, forwarded)
}

// ApplyHandoffResult records a result the old process forwarded.
func (m *Manager) ApplyHandoffResult(ctx context.Context, data json.RawMessage) error {
	var forwarded handoffResult
	if errDecode := json.Unmarshal(data, &forwarded); errDecode != nil {
		return errDecode
	}
	m.MarkResult(ctx, Result{
		AuthID: forwarded.AuthID, Provider: forwarded.Provider, Model: forwarded.Model,
		RouteModel: forwarded.RouteModel, Success: forwarded.Success, RetryAfter: forwarded.RetryAfter,
		CredentialScope: forwarded.CredentialScope, Error: forwarded.Error,
		SkipQuotaObservation: forwarded.SkipQuotaObservation,
		Options: cliproxyexecutor.Options{
			Stream: forwarded.Stream, Alt: forwarded.Alt, Headers: forwarded.Headers, Query: forwarded.Query,
			SourceFormat:   sdktranslator.Format(forwarded.SourceFormat),
			ResponseFormat: sdktranslator.Format(forwarded.ResponseFormat),
			Metadata:       forwarded.Metadata,
		},
	})
	return nil
}

// HandoffAffinityGroup is one session-affinity binding (state.affinity.v1).
type HandoffAffinityGroup struct {
	AuthID          string    `json:"auth_id"`
	Aliases         []string  `json:"aliases"`
	ExpiresAt       time.Time `json:"expires_at"`
	BindingSession  string    `json:"binding_session,omitempty"`
	BindingProvider string    `json:"binding_provider,omitempty"`
	BindingSequence uint64    `json:"binding_sequence,omitempty"`
}

func (m *Manager) sessionAffinityCache() *SessionCache {
	if selector, ok := m.Selector().(*SessionAffinitySelector); ok && selector != nil {
		return selector.cache
	}
	return nil
}

// ExportHandoffAffinity returns the live session bindings, oldest first.
func (m *Manager) ExportHandoffAffinity() []HandoffAffinityGroup {
	cache := m.sessionAffinityCache()
	if cache == nil {
		return nil
	}
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	if cache.evictionOrder == nil {
		return nil
	}
	now := time.Now()
	var groups []HandoffAffinityGroup
	for element := cache.evictionOrder.Front(); element != nil; element = element.Next() {
		primaryKey, _ := element.Value.(string)
		entry, ok := cache.groups[primaryKey]
		if !ok || !now.Before(entry.expiresAt) {
			continue
		}
		groups = append(groups, HandoffAffinityGroup{
			AuthID: entry.authID, Aliases: append([]string(nil), entry.aliases...), ExpiresAt: entry.expiresAt,
			BindingSession: entry.binding.sessionID, BindingProvider: entry.binding.provider,
			BindingSequence: entry.binding.sequence,
		})
	}
	return groups
}

// ImportHandoffAffinity restores bindings exported by the old process.
func (m *Manager) ImportHandoffAffinity(groups []HandoffAffinityGroup) {
	cache := m.sessionAffinityCache()
	if cache == nil {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.ensureInitializedLocked()
	now := time.Now()
	for _, group := range groups {
		if group.AuthID == "" || len(group.Aliases) == 0 || !now.Before(group.ExpiresAt) {
			continue
		}
		binding := sessionBinding{sessionID: group.BindingSession, provider: group.BindingProvider, sequence: group.BindingSequence}
		var previous []sessionEntry
		for _, alias := range group.Aliases {
			if entry, ok := cache.entries[alias]; ok {
				previous = append(previous, entry)
			}
		}
		cache.replaceAliasGroupsLocked(group.AuthID, group.ExpiresAt, group.Aliases, binding, previous...)
		if group.BindingSequence > cache.bindingSequence {
			cache.bindingSequence = group.BindingSequence
		}
	}
}

// ExportHandoffCooldown returns the runtime cooldown state of every auth.
func (m *Manager) ExportHandoffCooldown() []CooldownStateRecord {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	now := time.Now()
	var records []CooldownStateRecord
	for _, auth := range m.auths {
		records = append(records, m.cooldownStateRecordsForAuthLocked(auth, now)...)
	}
	return records
}

// ImportHandoffCooldown applies cooldown state from the old process, the same
// way a restart restores it from disk.
func (m *Manager) ImportHandoffCooldown(ctx context.Context, records []CooldownStateRecord) error {
	if m == nil || len(records) == 0 {
		return nil
	}
	m.mu.Lock()
	configured := m.cooldownStore
	m.cooldownStore = handoffCooldownRecords(records)
	m.mu.Unlock()
	errRestore := m.RestoreCooldownStates(ctx)
	m.mu.Lock()
	m.cooldownStore = configured
	m.mu.Unlock()
	return errRestore
}

// handoffCooldownRecords is a read-only CooldownStateStore over records.
type handoffCooldownRecords []CooldownStateRecord

func (r handoffCooldownRecords) Load(context.Context) ([]CooldownStateRecord, error) {
	return append([]CooldownStateRecord(nil), r...), nil
}

func (handoffCooldownRecords) Save(context.Context, []CooldownStateRecord) error { return nil }
