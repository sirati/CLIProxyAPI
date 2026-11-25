package auth

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/handoff"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
)

// An overloaded model answers with a server error, sometimes for half an
// hour: Antigravity with 503 "No capacity available"
// (MODEL_CAPACITY_EXHAUSTED), OpenAI with "Our servers are currently
// overloaded" (server_is_overloaded). A client that cannot wait that out
// itself, such as the Antigravity CLI or Codex under T3 Code, marks its
// requests with WithOutageHold. Such a request is never failed for an outage:
// it waits in the proxy and is sent again on the credential it was routed to,
// until the upstream answers.
//
// Each credential and model has one gate. While it is open every request goes
// straight through. The first outage error closes it for a second; from then
// on one waiting request at a time goes out, as a probe. A refused probe
// doubles the wait, up to five minutes. A probe that gets through halves it,
// and once it falls below a second the gate opens and everything still
// waiting goes. So a long outage costs the upstream one request every five
// minutes however many requests wait, and recovery ramps up over about five
// minutes instead of releasing every waiting request at once.
var (
	outageGateFirst   = time.Second
	outageGateMax     = 5 * time.Minute
	outageGateRecheck = 5 * time.Second // a waiting request notices a handoff this fast
)

type outageHoldKey struct{}

// WithOutageHold marks requests whose client waits as long as the proxy holds
// them, so an outage is waited out instead of returned.
func WithOutageHold(ctx context.Context) context.Context {
	return context.WithValue(ctx, outageHoldKey{}, true)
}

// HoldsThroughOutages reports whether ctx belongs to a request marked with
// WithOutageHold.
func HoldsThroughOutages(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	held, _ := ctx.Value(outageHoldKey{}).(bool)
	return held
}

func holdsThroughOutages(ctx context.Context) bool { return HoldsThroughOutages(ctx) }

type outageGate struct {
	interval time.Duration // 0: open
	nextAt   time.Time     // earliest start of the next probe
	probing  bool          // a probe is out
	waiting  []uint64      // tickets of waiting requests, oldest first
	changed  chan struct{} // closed and replaced on every change
}

func (g *outageGate) broadcast() {
	close(g.changed)
	g.changed = make(chan struct{})
}

func (g *outageGate) drop(ticket uint64) {
	g.waiting = slices.DeleteFunc(g.waiting, func(t uint64) bool { return t == ticket })
}

type outageGates struct {
	mu     sync.Mutex
	gates  map[string]*outageGate
	ticket uint64
}

var outageGateSets sync.Map // *Manager -> *outageGates

func (m *Manager) outageGates() *outageGates {
	if s, ok := outageGateSets.Load(m); ok {
		return s.(*outageGates)
	}
	s, _ := outageGateSets.LoadOrStore(m, &outageGates{gates: map[string]*outageGate{}})
	return s.(*outageGates)
}

func (s *outageGates) gate(key string) *outageGate {
	g := s.gates[key]
	if g == nil {
		g = &outageGate{changed: make(chan struct{})}
		s.gates[key] = g
	}
	return g
}

func (s *outageGates) newTicket() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ticket++
	return s.ticket
}

// outageAdmission is what a request does next: go now (as the probe, if
// probe), or wait until changed fires or wait passes.
type outageAdmission struct {
	dispatch bool
	probe    bool
	wait     time.Duration
	changed  <-chan struct{}
}

func (s *outageGates) admit(key string, ticket uint64, now time.Time) outageAdmission {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.gate(key)
	if g.interval == 0 {
		g.drop(ticket)
		return outageAdmission{dispatch: true}
	}
	if !slices.Contains(g.waiting, ticket) {
		g.waiting = append(g.waiting, ticket)
	}
	if g.probing || g.waiting[0] != ticket {
		return outageAdmission{wait: outageGateMax, changed: g.changed}
	}
	if now.Before(g.nextAt) {
		return outageAdmission{wait: g.nextAt.Sub(now), changed: g.changed}
	}
	g.drop(ticket)
	g.probing = true
	return outageAdmission{dispatch: true, probe: true}
}

// failed records an outage error and puts the request first in line again. A
// request that went out while the gate was open only closes it; requests sent
// before the gate closed do not count as refused probes.
func (s *outageGates) failed(key string, ticket uint64, probe bool, now time.Time) (interval time.Duration, waiting int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.gate(key)
	switch {
	case probe:
		g.probing = false
		g.interval = min(max(2*g.interval, outageGateFirst), outageGateMax)
		g.nextAt = now.Add(g.interval)
	case g.interval == 0:
		g.interval = outageGateFirst
		g.nextAt = now.Add(g.interval)
	}
	g.drop(ticket)
	g.waiting = append([]uint64{ticket}, g.waiting...)
	g.broadcast()
	return g.interval, len(g.waiting)
}

// answered records that the upstream answered a probe: the wait halves, and
// the gate opens once it is below a second.
func (s *outageGates) answered(key string, probe bool, now time.Time) (interval time.Duration, waiting int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.gate(key)
	if !probe {
		return g.interval, len(g.waiting)
	}
	g.probing = false
	g.interval /= 2
	if g.interval < time.Second {
		g.interval = 0
	} else {
		g.nextAt = now.Add(g.interval)
	}
	g.broadcast()
	return g.interval, len(g.waiting)
}

// left removes a request that stopped waiting or gave up its probe.
func (s *outageGates) left(key string, ticket uint64, probe bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.gate(key)
	g.drop(ticket)
	if probe {
		g.probing = false
	}
	g.broadcast()
}

// isOutageError reports whether err says the upstream is overloaded or
// unreachable rather than that the request or credential is at fault. A 429
// counts only for capacity: an exhausted quota belongs to the credential, and
// the credential pool handles it.
func isOutageError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, handoff.ErrFrozen) {
		return false
	}
	switch status := statusCodeFromError(err); {
	case status == http.StatusRequestTimeout || status >= http.StatusInternalServerError:
		return true
	case status == http.StatusTooManyRequests:
		return strings.Contains(err.Error(), "CAPACITY_EXHAUSTED")
	case status != 0:
		return false
	}
	var netErr net.Error
	return errors.As(err, &netErr) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func outageGateKey(auth *Auth, opts cliproxyexecutor.Options) string {
	model, _ := opts.Metadata[cliproxyexecutor.RequestedModelMetadataKey].(string)
	return auth.ID + "\x00" + model
}

func outageLogText(err error) string {
	text := strings.Join(strings.Fields(err.Error()), " ")
	if len(text) > 160 {
		text = text[:160] + "…"
	}
	return text
}

// throughOutages runs dispatch for a request marked WithOutageHold, waiting
// out outage errors at the gate of its credential and model.
func throughOutages[T any](m *Manager, ctx context.Context, auth *Auth, opts cliproxyexecutor.Options, dispatch func(context.Context) (T, error)) (T, error) {
	var zero T
	gates := m.outageGates()
	key := outageGateKey(auth, opts)
	model, _ := opts.Metadata[cliproxyexecutor.RequestedModelMetadataKey].(string)
	ticket := gates.newTicket()
	for {
		// A process that handed its work to a newer one sends nothing more;
		// the new process re-runs a request that has not answered yet.
		if handoff.Frozen() {
			gates.left(key, ticket, false)
			return zero, handoff.NoteFrozen(ctx)
		}
		next := gates.admit(key, ticket, time.Now())
		if !next.dispatch {
			timer := time.NewTimer(min(next.wait, outageGateRecheck))
			select {
			case <-ctx.Done():
				timer.Stop()
				gates.left(key, ticket, false)
				return zero, ctx.Err()
			case <-next.changed:
			case <-timer.C:
			}
			timer.Stop()
			continue
		}
		out, err := dispatch(ctx)
		if ctx.Err() != nil {
			if next.probe {
				gates.left(key, ticket, true)
			}
			return out, err
		}
		if !isOutageError(err) {
			interval, waiting := gates.answered(key, next.probe, time.Now())
			if next.probe {
				if interval == 0 {
					log.Infof("outage gate: %s on %s answers again; releasing %d waiting requests", model, auth.ID, waiting)
				} else {
					log.Infof("outage gate: %s on %s answered a probe; next waiting request in %s (%d waiting)", model, auth.ID, interval, waiting)
				}
			}
			return out, err
		}
		if res, ok := any(out).(*cliproxyexecutor.StreamResult); ok && res != nil && res.Chunks != nil {
			discardStreamChunks(res.Chunks)
		}
		interval, waiting := gates.failed(key, ticket, next.probe, time.Now())
		log.Infof("outage gate: %s on %s failed (probe=%v: %s); next try in %s (%d waiting)", model, auth.ID, next.probe, outageLogText(err), interval, waiting)
	}
}

// SetOutageGateTimingForTest shortens the gate's waits; it returns a restore
// function.
func SetOutageGateTimingForTest(first, maxWait time.Duration) (restore func()) {
	prevFirst, prevMax := outageGateFirst, outageGateMax
	outageGateFirst, outageGateMax = first, maxWait
	return func() { outageGateFirst, outageGateMax = prevFirst, prevMax }
}

// OutageBackoffView is a gate that is not open: a credential and model whose
// held requests wait for the upstream to answer again.
type OutageBackoffView struct {
	AuthIndex string `json:"auth_index"`
	Model     string `json:"model"`
	// IntervalMs is the current wait between probes.
	IntervalMs int64 `json:"interval_ms"`
	// NextRetryInMs is the time until the next probe may start; 0 while a
	// probe is out.
	NextRetryInMs int64 `json:"next_retry_in_ms"`
	// Waiting counts the requests held for this credential and model.
	Waiting int  `json:"waiting"`
	Probing bool `json:"probing"`
}

// OutageBackoffs lists the gates that are not open and hold something back,
// ordered by credential and model.
func (m *Manager) OutageBackoffs(now time.Time) []OutageBackoffView {
	views := make([]OutageBackoffView, 0)
	if m == nil {
		return views
	}
	s := m.outageGates()
	s.mu.Lock()
	keys := make([]string, 0, len(s.gates))
	for key, g := range s.gates {
		// A gate still easing off after the outage, with nobody waiting and
		// no try due, holds nothing back.
		if g.interval > 0 && (len(g.waiting) > 0 || g.probing || g.nextAt.After(now)) {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	authIDs := make([]string, 0, len(keys))
	for _, key := range keys {
		g := s.gates[key]
		authID, model, _ := strings.Cut(key, "\x00")
		view := OutageBackoffView{
			Model:      model,
			IntervalMs: g.interval.Milliseconds(),
			Waiting:    len(g.waiting),
			Probing:    g.probing,
		}
		if !g.probing && g.nextAt.After(now) {
			view.NextRetryInMs = g.nextAt.Sub(now).Milliseconds()
		}
		views = append(views, view)
		authIDs = append(authIDs, authID)
	}
	s.mu.Unlock()
	// The panel names credentials by index; resolve outside the gate lock.
	for i, authID := range authIDs {
		if auth, ok := m.GetByID(authID); ok && auth != nil {
			views[i].AuthIndex = auth.EnsureIndex()
		}
	}
	return views
}
