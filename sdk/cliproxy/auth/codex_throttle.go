package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	internallogging "github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

// A Codex account answers bursts with 429 {"detail":"Rate limit exceeded"},
// a limit the proxy cannot predict: it is no request rate, token rate or
// concurrency, and other accounts serve ten times the load without it. It
// clears quickly, though: the first request started after a 429 succeeded
// after a median 25s (p90 55s), and requests keep getting through during the
// burst. So an account that answers 429 pauses rather than slows down.
//
// The pause lasts what the upstream's Retry-After says, otherwise what the
// account has learned it needs, starting from those 25s. Requests meanwhile
// wait in the proxy (the client sees a slow answer) or, when the pause is
// more than a minute out, are told to retry when it ends. After the pause
// one request goes first as a probe. If it gets through, everything waiting
// follows in quick succession and the next pause is a little shorter; if it
// is refused, the pause starts over and is longer. The pause therefore
// settles just above what the account needs, and a retry rarely lands in a
// burst that is still on.
const (
	codexPaceHold       = time.Minute // longest a request waits in the proxy
	codexPaceInitial    = 25 * time.Second
	codexPaceMin        = 5 * time.Second
	codexPaceMax        = 2 * time.Minute
	codexPaceGrow       = 1.25
	codexPaceShrink     = 0.9
	codexPaceSpread     = 250 * time.Millisecond // between requests released after a pause
	codexPaceSlowPoll   = 100 * time.Millisecond // re-check of a request waiting for a slow-start slot
	codexPaceTellTTL    = 5 * time.Minute
	codexPaceForget     = 10 * time.Minute // quiet this long, the account runs free again
	codexPaceMaxTells   = 3                // after this many retries the client is told the reset instead
	codexPacedErrorCode = "codex_paced"
)

type codexPace struct {
	pause       time.Duration // learned pause; 0 until the first 429
	pausedUntil time.Time
	recovering  bool          // a pause ended and no request has got through since
	probe       chan struct{} // closed when the probe after a pause resolves; nil if none is out
	release     time.Time     // next free start while waiting requests are released
	budget      int           // starts the account takes after a pause before it refuses; 0 is unknown
	preventive  bool          // the current pause was taken before a 429
	used        int           // starts since the last pause ended
	releasedAt  time.Time     // when the last pause ended
	burstAt     time.Time     // when the last burst began
	got         int           // successes sent between releasedAt and the next burst
	pending     int           // unanswered requests sent since the pause ended
	window      int           // how many of those may be out while the budget is unknown
	lastLimited time.Time
	tells       map[string]codexTell
}

type codexTell struct {
	count int
	at    time.Time
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
		p = &codexPace{tells: map[string]codexTell{}}
		t.paces[authID] = p
	}
	return p
}

// codexAdmission is what a request does next.
type codexAdmission struct {
	wait     time.Duration // sleep this long, then ask again
	probe    <-chan struct{}
	dispatch bool // dispatch now
	isProbe  bool
	tell     time.Duration // answer the client: retry after this
}

// admit decides the next step of a request for key that has waited since
// arrived.
func (t *codexThrottle) admit(authID, key string, now, arrived time.Time) codexAdmission {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.pace(authID)
	held := now.Sub(arrived)
	if now.Before(p.pausedUntil) {
		wait := p.pausedUntil.Sub(now)
		if held+wait > codexPaceHold {
			return p.tellLocked(key, wait, now)
		}
		return codexAdmission{wait: wait}
	}
	if p.recovering {
		if p.probe != nil {
			return codexAdmission{probe: p.probe}
		}
		p.probe = make(chan struct{})
		return codexAdmission{dispatch: true, isProbe: true}
	}
	if !p.releasedAt.IsZero() && now.Sub(p.lastLimited) > codexPaceForget {
		p.budget, p.releasedAt = 0, time.Time{}
	}
	// The budget is spent: pause before the account refuses, not after.
	if p.budget > 0 && p.used >= p.budget {
		p.pausedUntil = now.Add(p.pause)
		p.recovering = true
		p.preventive = true
		if held+p.pause > codexPaceHold {
			return p.tellLocked(key, p.pause, now)
		}
		return codexAdmission{wait: p.pause}
	}
	// Without a budget, a release starts slow: one more request may be out for
	// every one that got through, so a wave cannot overshoot the account before
	// its first refusal comes back.
	if p.budget == 0 && !p.releasedAt.IsZero() && p.pending >= p.window {
		if held+codexPaceSlowPoll > codexPaceHold {
			return p.tellLocked(key, codexPaceMin, now)
		}
		return codexAdmission{wait: codexPaceSlowPoll}
	}
	p.used++
	if !p.releasedAt.IsZero() {
		p.pending++
	}
	delete(p.tells, key)
	if now.Before(p.release) {
		wait := p.release.Sub(now)
		p.release = p.release.Add(codexPaceSpread)
		return codexAdmission{wait: wait, dispatch: true}
	}
	return codexAdmission{dispatch: true}
}

func (p *codexPace) tellLocked(key string, after time.Duration, now time.Time) codexAdmission {
	for k, tell := range p.tells {
		if now.Sub(tell.at) > codexPaceTellTTL {
			delete(p.tells, k)
		}
	}
	if key != "" {
		tell := p.tells[key]
		p.tells[key] = codexTell{count: tell.count + 1, at: now}
	}
	return codexAdmission{tell: after}
}

func (t *codexThrottle) told(authID, key string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pace(authID).tells[key].count
}

// limited records an upstream 429 on authID. retryAfter is the upstream's
// own wait, if it named one. A refused probe lengthens the learned pause;
// 429s from requests sent before the pause began belong to the same burst.
func (t *codexThrottle) limited(authID string, now time.Time, retryAfter time.Duration, wasProbe bool) time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.pace(authID)
	first := p.pause == 0
	if first {
		p.pause = codexPaceInitial
	}
	p.lastLimited = now
	if wasProbe {
		p.pause = min(time.Duration(float64(p.pause)*codexPaceGrow), codexPaceMax)
		p.resolveProbeLocked()
	} else if now.Before(p.pausedUntil) || p.recovering {
		if until := now.Add(retryAfter); retryAfter > 0 && until.After(p.pausedUntil) {
			p.pausedUntil = until
		}
		return p.pausedUntil.Sub(now)
	}
	// A new burst: what got through since the last pause is what the account
	// takes; successes of requests sent before now still arrive and count.
	p.burstAt = now
	if !wasProbe && !first && p.got > 0 {
		p.budget = p.got
	}
	p.preventive = false
	pause := p.pause
	if retryAfter > 0 {
		pause = retryAfter
	}
	p.pausedUntil = now.Add(pause)
	p.recovering = true
	return pause
}

// cleared records that a request sent at dispatched got through: a probe ends
// the recovery, shortens the next pause and releases everything that waited
// for it. A request sent before the pause began proves nothing: it was
// admitted before the burst, and letting it end the pause is how a 429
// backoff keeps starting over.
func (t *codexThrottle) cleared(authID string, now, dispatched time.Time, wasProbe bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.pace(authID)
	if !p.releasedAt.IsZero() && !dispatched.Before(p.releasedAt) && (p.burstAt.Before(p.releasedAt) || dispatched.Before(p.burstAt)) {
		p.got++
		p.window++
		if !p.burstAt.Before(p.releasedAt) {
			p.budget = max(p.budget, p.got)
		}
	}
	if !p.recovering || (!wasProbe && dispatched.Before(p.pausedUntil)) {
		return
	}
	if wasProbe {
		p.pause = max(time.Duration(float64(p.pause)*codexPaceShrink), codexPaceMin)
		// A pause taken early that the account did not need: try one more start
		// in the next round.
		if p.preventive && p.budget > 0 {
			p.budget++
		}
	}
	p.recovering = false
	p.preventive = false
	p.used = 1
	p.releasedAt = now
	p.got = 1
	p.pending = 0
	p.window = 2
	p.release = now.Add(codexPaceSpread)
	p.resolveProbeLocked()
}

// abandoned frees the probe slot of a probe that never got an answer.
func (t *codexThrottle) abandoned(authID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pace(authID).resolveProbeLocked()
}

func (p *codexPace) resolveProbeLocked() {
	if p.probe != nil {
		close(p.probe)
		p.probe = nil
	}
}

// upstreamRetryAfter reads the wait an upstream 429 named in its headers.
func upstreamRetryAfter(headers http.Header) time.Duration {
	if ms, err := strconv.ParseFloat(strings.TrimSpace(headers.Get("Retry-After-Ms")), 64); err == nil && ms > 0 {
		return time.Duration(ms * float64(time.Millisecond))
	}
	value := strings.TrimSpace(headers.Get("Retry-After"))
	if seconds, err := strconv.ParseFloat(value, 64); err == nil && seconds > 0 {
		return time.Duration(seconds * float64(time.Second))
	}
	if at, err := http.ParseTime(value); err == nil {
		return time.Until(at)
	}
	return 0
}

// codexRateLimitHeaders keeps the headers of a 429 that may say how long it lasts.
func codexRateLimitHeaders(headers http.Header) map[string]string {
	out := map[string]string{}
	for key, values := range headers {
		lower := strings.ToLower(key)
		if strings.Contains(lower, "retry") || strings.Contains(lower, "ratelimit") || strings.Contains(lower, "rate-limit") || strings.HasPrefix(lower, "x-codex-") {
			out[lower] = strings.Join(values, ",")
		}
	}
	return out
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// paced runs dispatch for a Codex account at the account's pace.
func paced[T any](m *Manager, ctx context.Context, auth *Auth, opts cliproxyexecutor.Options, dispatch func(context.Context) (T, error)) (T, error) {
	var zero T
	if m == nil || auth == nil || auth.Provider != "codex" {
		return dispatch(ctx)
	}
	t := m.codexThrottle()
	key := codexPaceKey(opts.Headers)
	arrived := time.Now()
	for {
		now := time.Now()
		next := t.admit(auth.ID, key, now, arrived)
		switch {
		case next.tell > 0:
			return zero, newCodexPacedError(next.tell, t.told(auth.ID, key) >= codexPaceMaxTells)
		case next.probe != nil:
			left := codexPaceHold - now.Sub(arrived)
			if left <= 0 {
				return zero, newCodexPacedError(codexPaceMin, t.told(auth.ID, key) >= codexPaceMaxTells)
			}
			timer := time.NewTimer(left)
			select {
			case <-ctx.Done():
				timer.Stop()
				return zero, ctx.Err()
			case <-next.probe:
			case <-timer.C:
			}
			timer.Stop()
			continue
		case !next.dispatch:
			if err := sleepCtx(ctx, next.wait); err != nil {
				return zero, err
			}
			continue
		}
		if err := sleepCtx(ctx, next.wait); err != nil {
			if next.isProbe {
				t.abandoned(auth.ID)
			}
			return zero, err
		}
		dispatched := time.Now()
		out, err := dispatch(ctx)
		t.settled(auth.ID, dispatched)
		if isCodexRateLimit(err) {
			headers := internallogging.GetResponseHeaders(ctx)
			pause := t.limited(auth.ID, time.Now(), upstreamRetryAfter(headers), next.isProbe)
			log.Infof("codex pace: %s answered 429 (probe=%v); pausing %s; upstream headers %v", auth.ID, next.isProbe, pause.Round(time.Second), codexRateLimitHeaders(headers))
			continue
		}
		if ctx.Err() != nil && next.isProbe {
			t.abandoned(auth.ID)
		} else {
			t.cleared(auth.ID, time.Now(), dispatched, next.isProbe)
		}
		return out, err
	}
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

// NewCodexPacedError returns the error a paced Codex request fails with.
func NewCodexPacedError(after time.Duration, exhausted bool) error {
	return newCodexPacedError(after, exhausted)
}

// settled records that a request sent at dispatched has its answer.
func (t *codexThrottle) settled(authID string, dispatched time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.pace(authID)
	if !p.releasedAt.IsZero() && !dispatched.Before(p.releasedAt) && p.pending > 0 {
		p.pending--
	}
}
