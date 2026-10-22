package handoff

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
)

// ErrFrozen is returned to code in the old process that would make a
// decision after the handoff: pick a credential, refresh a token. The request
// that hit it is re-run by the new process when nothing was sent yet.
var ErrFrozen = errors.New("handoff: this process has handed its work to a newer one")

// Sink delivers items and events to the new process.
type Sink interface {
	// Send transfers one item; files go with it and are closed afterwards.
	Send(kind, id string, payload any, files ...*os.File) error
	// Event reports a side effect the new process applies.
	Event(kind string, payload any)
	// Supports reports whether the new process takes an item kind.
	Supports(kind string) bool
}

var (
	frozen atomic.Bool
	sinkMu sync.RWMutex
	sink   Sink
)

// Freeze makes this process dead to decisions: from now on Frozen is true and
// everything transferable goes to s.
func Freeze(s Sink) {
	sinkMu.Lock()
	sink = s
	sinkMu.Unlock()
	frozen.Store(true)
	notifyFrozen()
}

// Unfreeze undoes Freeze when the new process failed before taking over.
func Unfreeze() {
	frozen.Store(false)
	sinkMu.Lock()
	sink = nil
	sinkMu.Unlock()
}

// Frozen reports whether this process has handed off.
func Frozen() bool { return frozen.Load() }

// CurrentSink returns the sink set by Freeze, or nil.
func CurrentSink() Sink {
	sinkMu.RLock()
	defer sinkMu.RUnlock()
	return sink
}

// Forward sends an event to the new process if this one is frozen. It
// reports whether the event was taken, so callers skip their local handling.
func Forward(kind string, payload any) bool {
	if !Frozen() {
		return false
	}
	if s := CurrentSink(); s != nil {
		s.Event(kind, payload)
	}
	return true
}

var (
	frozenHooksMu sync.Mutex
	frozenHooks   []func()
)

// OnFrozen runs fn once the process freezes (immediately if it already has).
func OnFrozen(fn func()) {
	frozenHooksMu.Lock()
	if !Frozen() {
		frozenHooks = append(frozenHooks, fn)
		frozenHooksMu.Unlock()
		return
	}
	frozenHooksMu.Unlock()
	fn()
}

func notifyFrozen() {
	frozenHooksMu.Lock()
	hooks := frozenHooks
	frozenHooks = nil
	frozenHooksMu.Unlock()
	for _, fn := range hooks {
		fn()
	}
}

// RequestMarker records that a request in the old process ran into
// ErrFrozen, so its reply can be dropped and the request re-run elsewhere.
type RequestMarker struct{ hit atomic.Bool }

type markerKey struct{}

// WithRequestMarker attaches a marker to a request context.
func WithRequestMarker(ctx context.Context) (context.Context, *RequestMarker) {
	marker := &RequestMarker{}
	return context.WithValue(ctx, markerKey{}, marker), marker
}

// HitFrozen reports whether the request ran into ErrFrozen.
func (m *RequestMarker) HitFrozen() bool { return m != nil && m.hit.Load() }

// NoteFrozen marks the request in ctx and returns ErrFrozen.
func NoteFrozen(ctx context.Context) error {
	if ctx != nil {
		if marker, ok := ctx.Value(markerKey{}).(*RequestMarker); ok {
			marker.hit.Store(true)
		}
	}
	return ErrFrozen
}

// Session is a long-lived connection that can export itself once idle.
type Session interface {
	// TryHandoff moves the session to the new process if it is idle. It
	// reports whether the session is gone from this process.
	TryHandoff() bool
}

var sessions sync.Map // Session -> struct{}

// RegisterSession tracks s until the returned function is called.
func RegisterSession(s Session) (unregister func()) {
	sessions.Store(s, struct{}{})
	return func() { sessions.Delete(s) }
}

// HandoffSessions tries every registered session once and returns how many
// are still here.
func HandoffSessions() int {
	remaining := 0
	sessions.Range(func(key, _ any) bool {
		if session, ok := key.(Session); ok && !session.TryHandoff() {
			remaining++
		}
		return true
	})
	return remaining
}

// UpstreamState describes an upstream websocket handed to the new process.
type UpstreamState struct {
	Provider    string `json:"provider"`
	SessionID   string `json:"session_id"`
	AuthID      string `json:"auth_id"`
	WSURL       string `json:"ws_url"`
	ProxyURL    string `json:"proxy_url"`
	Response101 []byte `json:"response_101"`
}

// UpstreamHooks connects a provider's websocket sessions to the handoff.
type UpstreamHooks struct {
	// Export returns the session's upstream Conn and state, if it has one.
	Export func(sessionID string) (*Conn, UpstreamState, bool)
	// Adopt installs a received upstream stream into the provider's session.
	Adopt func(ctx context.Context, state UpstreamState, file *os.File, carry []byte) error
}

var (
	upstreamMu    sync.RWMutex
	upstreamHooks = map[string]UpstreamHooks{}
)

// RegisterUpstream registers a provider's hooks.
func RegisterUpstream(provider string, hooks UpstreamHooks) {
	upstreamMu.Lock()
	upstreamHooks[provider] = hooks
	upstreamMu.Unlock()
}

// ExportUpstream finds the upstream websocket of an execution session.
func ExportUpstream(sessionID string) (*Conn, UpstreamState, bool) {
	upstreamMu.RLock()
	defer upstreamMu.RUnlock()
	for _, hooks := range upstreamHooks {
		if hooks.Export == nil {
			continue
		}
		if conn, state, ok := hooks.Export(sessionID); ok {
			return conn, state, true
		}
	}
	return nil, UpstreamState{}, false
}

// AdoptUpstream hands a received upstream stream to its provider.
func AdoptUpstream(ctx context.Context, state UpstreamState, file *os.File, carry []byte) error {
	upstreamMu.RLock()
	hooks, ok := upstreamHooks[state.Provider]
	upstreamMu.RUnlock()
	if !ok || hooks.Adopt == nil {
		_ = file.Close()
		return errors.New("handoff: no upstream adopter for " + state.Provider)
	}
	return hooks.Adopt(ctx, state, file, carry)
}

// Capabilities lists the item kinds this build understands. Packages add
// theirs from init.
var (
	capabilityMu sync.Mutex
	capabilities = map[string]struct{}{}
)

// AddCapability declares that this build can take an item kind.
func AddCapability(kind string) {
	capabilityMu.Lock()
	capabilities[kind] = struct{}{}
	capabilityMu.Unlock()
}

// Capabilities returns every declared kind.
func Capabilities() []string {
	capabilityMu.Lock()
	defer capabilityMu.Unlock()
	out := make([]string, 0, len(capabilities))
	for kind := range capabilities {
		out = append(out, kind)
	}
	return out
}
