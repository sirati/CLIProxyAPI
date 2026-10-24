package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

// A Codex account answers bursts with 429 {"detail":"Rate limit exceeded"}.
// That limit is no request rate, token rate or concurrency the proxy can see:
// other accounts serve ten times the load without it. So each account learns
// its pace from those answers instead. A 429 halves the start rate the
// account just saw, every quiet minute gives a little back, and requests are
// spaced to that rate. A request whose turn is at most a minute away waits
// for it, which the client sees as a slow answer; one further out is told to
// retry when its turn comes, and the turn stays reserved for it, so it is
// first in line when it returns.
const (
	codexPaceHold       = time.Minute
	codexPaceMinRate    = 4.0  // starts per minute
	codexPaceFreeRate   = 60.0 // reaching this rate lifts the throttle
	codexPaceStep       = 2.0  // starts per minute regained per quiet minute
	codexPacePause      = 15 * time.Second
	codexPaceBurst      = 10 * time.Second // 429s this close together are one burst
	codexPaceReserveTTL = 2 * time.Minute  // past its turn, an unclaimed reservation lapses
	codexPaceMaxTells   = 3                // after this many retries the client is told the reset instead
	codexPaceRetries    = 3                // upstream 429s absorbed in one dispatch
	codexPacedErrorCode = "codex_paced"
)

type codexPace struct {
	rate        float64 // starts per minute; 0 runs unthrottled
	next        time.Time
	pausedUntil time.Time
	lastLimited time.Time
	lastRaise   time.Time
	starts      []time.Time
	reserved    map[string]time.Time
	tells       map[string]int
}

type codexThrottle struct {
	mu    sync.Mutex
	paces map[string]*codexPace
}

var codexThrottles sync.Map // *Manager -> *codexThrottle

func (m *Manager) codexThrottle() *codexThrottle {
	if t, ok := codexThrottles.Load(m); ok {
		return t.(*codexThrottle)
	}
	t, _ := codexThrottles.LoadOrStore(m, &codexThrottle{paces: map[string]*codexPace{}})
	return t.(*codexThrottle)
}

func (t *codexThrottle) pace(authID string) *codexPace {
	p := t.paces[authID]
	if p == nil {
		p = &codexPace{reserved: map[string]time.Time{}, tells: map[string]int{}}
		t.paces[authID] = p
	}
	return p
}

// relax gives back rate for every quiet minute and lifts a throttle that has
// climbed back to the free rate.
func (p *codexPace) relax(now time.Time) {
	if p.rate == 0 || now.Sub(p.lastLimited) < time.Minute {
		return
	}
	if minutes := math.Floor(now.Sub(p.lastRaise).Minutes()); minutes >= 1 {
		p.rate += codexPaceStep * minutes
		p.lastRaise = p.lastRaise.Add(time.Duration(minutes) * time.Minute)
	}
	if p.rate >= codexPaceFreeRate {
		p.rate = 0
	}
}

func (p *codexPace) started(at time.Time) {
	cut := 0
	for cut < len(p.starts) && at.Sub(p.starts[cut]) > time.Minute {
		cut++
	}
	p.starts = append(p.starts[cut:], at)
}

func (p *codexPace) lapse(now time.Time) {
	for key, slot := range p.reserved {
		if now.Sub(slot) > codexPaceReserveTTL {
			delete(p.reserved, key)
			delete(p.tells, key)
		}
	}
}

// turn assigns request key its start. It reports how long to wait for it and
// whether the request is held for it; a request that may not be held, or whose
// turn is too far out, keeps the turn reserved for its retry.
func (t *codexThrottle) turn(authID, key string, now time.Time, mayHold bool) (time.Duration, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.pace(authID)
	p.relax(now)
	p.lapse(now)
	start, reserved := p.reserved[key]
	if !reserved {
		start = now
		if p.rate > 0 || now.Before(p.pausedUntil) {
			start = latest(now, p.next, p.pausedUntil)
			if p.rate > 0 {
				p.next = start.Add(time.Duration(float64(time.Minute) / p.rate))
			}
		}
	}
	wait := start.Sub(now)
	if !mayHold || wait > codexPaceHold {
		if key != "" {
			p.reserved[key] = start
			p.tells[key]++
		}
		return max(wait, 0), false
	}
	delete(p.reserved, key)
	delete(p.tells, key)
	p.started(latest(now, start))
	return max(wait, 0), true
}

func (t *codexThrottle) told(authID, key string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pace(authID).tells[key]
}

// limited records an upstream 429 on authID.
func (t *codexThrottle) limited(authID string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.pace(authID)
	p.relax(now)
	if now.Sub(p.lastLimited) >= codexPaceBurst {
		seen := 0.0
		for _, at := range p.starts {
			if now.Sub(at) <= time.Minute {
				seen++
			}
		}
		if p.rate > 0 && p.rate < seen {
			seen = p.rate
		}
		p.rate = max(codexPaceMinRate, seen/2)
		p.lastRaise = now
	}
	p.lastLimited = now
	p.pausedUntil = latest(p.pausedUntil, now.Add(codexPacePause))
	p.next = latest(p.next, p.pausedUntil)
}

func latest(times ...time.Time) time.Time {
	var out time.Time
	for _, at := range times {
		if at.After(out) {
			out = at
		}
	}
	return out
}

// codexPaceKey names one request across the client's retries: Codex keeps
// one request in flight per turn and sends the turn in every attempt.
func codexPaceKey(headers http.Header) string {
	meta := headers.Get("X-Codex-Turn-Metadata")
	thread, turn := gjson.Get(meta, "thread_id").String(), gjson.Get(meta, "turn_id").String()
	if thread == "" || turn == "" {
		return ""
	}
	return thread + "/" + turn
}

// isCodexRateLimit reports the upstream pacing 429, {"detail":"Rate limit
// exceeded"}: it names no reset, unlike an exhausted quota, which keeps to
// the cooldown.
func isCodexRateLimit(err error) bool {
	if err == nil || statusCodeFromError(err) != http.StatusTooManyRequests {
		return false
	}
	var paced *codexPacedError
	if errors.As(err, &paced) {
		return false
	}
	var reset interface{ RetryAfter() *time.Duration }
	if errors.As(err, &reset) && reset.RetryAfter() != nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "rate limit exceeded") && !strings.Contains(text, "usage_limit_reached")
}

// codexPacedError tells the client to come back when its turn comes.
type codexPacedError struct {
	err       *Error
	after     time.Duration
	exhausted bool
}

func newCodexPacedError(after time.Duration, exhausted bool) *codexPacedError {
	seconds := int(math.Ceil(after.Seconds()))
	body, _ := json.Marshal(map[string]any{"error": map[string]any{
		"type":    "rate_limit_exceeded",
		"code":    "rate_limit_exceeded",
		"message": fmt.Sprintf("The Codex account is pacing requests; this one starts in %ds.", seconds),
	}})
	return &codexPacedError{
		err:       &Error{Code: codexPacedErrorCode, Message: string(body), Retryable: true, HTTPStatus: http.StatusTooManyRequests},
		after:     after,
		exhausted: exhausted,
	}
}

func (e *codexPacedError) Error() string              { return e.err.Message }
func (e *codexPacedError) Unwrap() error              { return e.err }
func (e *codexPacedError) StatusCode() int            { return http.StatusTooManyRequests }
func (e *codexPacedError) RetryAfter() *time.Duration { return &e.after }

// CodexRetryAfter reports whether err asks the client to retry after a wait,
// and whether the client has retried so often that it should get the reset as
// a usage limit instead, before its own retries run out.
func CodexRetryAfter(err error) (after time.Duration, exhausted bool, ok bool) {
	var paced *codexPacedError
	if !errors.As(err, &paced) || paced == nil {
		return 0, false, false
	}
	return paced.after, paced.exhausted, true
}

func isCodexPacedResult(err *Error) bool {
	return err != nil && err.Code == codexPacedErrorCode
}

// paced runs dispatch for a Codex account at the account's pace: it waits
// for the request's turn, and absorbs a 429 by waiting for the next turn when
// that is close, or hands back a codexPacedError naming it.
func paced[T any](m *Manager, ctx context.Context, auth *Auth, opts cliproxyexecutor.Options, dispatch func(context.Context) (T, error)) (T, error) {
	var zero T
	if m == nil || auth == nil || auth.Provider != "codex" {
		return dispatch(ctx)
	}
	t := m.codexThrottle()
	key := codexPaceKey(opts.Headers)
	for attempt := 0; ; attempt++ {
		wait, hold := t.turn(auth.ID, key, time.Now(), attempt < codexPaceRetries)
		if !hold {
			return zero, newCodexPacedError(wait, t.told(auth.ID, key) >= codexPaceMaxTells)
		}
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return zero, ctx.Err()
			case <-timer.C:
			}
		}
		out, err := dispatch(ctx)
		if !isCodexRateLimit(err) {
			return out, err
		}
		t.limited(auth.ID, time.Now())
	}
}

// NewCodexPacedError returns the error a paced Codex request fails with.
func NewCodexPacedError(after time.Duration, exhausted bool) error {
	return newCodexPacedError(after, exhausted)
}
