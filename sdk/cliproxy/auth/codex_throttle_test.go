package auth

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type codexRateLimitTestError struct{}

func (codexRateLimitTestError) Error() string   { return `{"detail":"Rate limit exceeded"}` }
func (codexRateLimitTestError) StatusCode() int { return http.StatusTooManyRequests }

func TestCodexPaceRunsFreeUntilLimited(t *testing.T) {
	th := &codexThrottle{paces: map[string]*codexPace{}}
	now := time.Unix(1_800_000_000, 0)
	for i := range 30 {
		if wait, hold := th.turn("a", "", now.Add(time.Duration(i)*time.Second), true); wait != 0 || !hold {
			t.Fatalf("start %d: wait %v hold %v, want an immediate start", i, wait, hold)
		}
	}
}

func TestCodexPaceHalvesTheRateItSawAndPauses(t *testing.T) {
	th := &codexThrottle{paces: map[string]*codexPace{}}
	now := time.Unix(1_800_000_000, 0)
	for i := range 20 { // 20 starts in the minute before the 429
		th.turn("a", "", now.Add(-time.Minute+time.Duration(i)*3*time.Second), true)
	}
	th.limited("a", now)
	th.limited("a", now.Add(2*time.Second)) // the same burst: no second halving

	if rate := th.paces["a"].rate; rate != 10 {
		t.Fatalf("rate = %v starts/min, want 10", rate)
	}
	if wait, hold := th.turn("a", "", now, true); wait != codexPacePause+2*time.Second || !hold {
		t.Fatalf("first start after the 429: wait %v hold %v, want the pause", wait, hold)
	}
	if wait, _ := th.turn("a", "", now, true); wait != codexPacePause+2*time.Second+6*time.Second {
		t.Fatalf("second start: wait %v, want one 6s step later", wait)
	}
}

func TestCodexPaceReservesAFarTurnForTheRetry(t *testing.T) {
	th := &codexThrottle{paces: map[string]*codexPace{}}
	now := time.Unix(1_800_000_000, 0)
	th.limited("a", now) // nothing seen: the floor rate, 4/min, one start per 15s after the 15s pause: four fit in the hold
	var waits []time.Duration
	for i := range 4 {
		wait, hold := th.turn("a", "", now, true)
		if !hold {
			t.Fatalf("start %d not held at %v", i, wait)
		}
		waits = append(waits, wait)
	}
	wait, hold := th.turn("a", "thread/turn", now, true)
	if hold || wait <= codexPaceHold {
		t.Fatalf("far turn: wait %v hold %v, want a retry later", wait, hold)
	}
	if got := th.told("a", "thread/turn"); got != 1 {
		t.Fatalf("told = %d, want 1", got)
	}
	// Another request queues behind the reservation.
	if other, _ := th.turn("a", "other/turn", now, true); other <= wait {
		t.Fatalf("a later request starts at %v, before the reserved %v", other, wait)
	}
	// Back on time, the retry starts at once on its reserved turn.
	if again, hold := th.turn("a", "thread/turn", now.Add(wait), true); again != 0 || !hold {
		t.Fatalf("returning retry: wait %v hold %v, want an immediate start", again, hold)
	}
	if got := th.told("a", "thread/turn"); got != 0 {
		t.Fatalf("told after the start = %d, want 0", got)
	}
}

func TestCodexPaceRecoversSlowlyAndLiftsAtTheFreeRate(t *testing.T) {
	th := &codexThrottle{paces: map[string]*codexPace{}}
	now := time.Unix(1_800_000_000, 0)
	th.limited("a", now)
	p := th.paces["a"]
	p.relax(now.Add(5 * time.Minute))
	if p.rate != codexPaceMinRate+5*codexPaceStep {
		t.Fatalf("rate after 5 quiet minutes = %v, want %v", p.rate, codexPaceMinRate+5*codexPaceStep)
	}
	p.relax(now.Add(time.Hour))
	if p.rate != 0 {
		t.Fatalf("rate after an hour = %v, want the throttle lifted", p.rate)
	}
}

func TestPacedAnswersAFarTurnWithoutDispatching(t *testing.T) {
	m := NewManager(nil, nil, nil)
	auth := &Auth{ID: "codex-a", Provider: "codex"}
	th := m.codexThrottle()
	th.limited(auth.ID, time.Now())
	for range 8 {
		th.turn(auth.ID, "", time.Now(), true)
	}
	opts := cliproxyexecutor.Options{Headers: http.Header{"X-Codex-Turn-Metadata": {`{"thread_id":"t","turn_id":"u"}`}}}
	dispatched := false
	_, err := paced(m, context.Background(), auth, opts, func(context.Context) (int, error) {
		dispatched = true
		return 0, nil
	})
	after, exhausted, ok := CodexRetryAfter(err)
	if dispatched || !ok || exhausted || after <= codexPaceHold {
		t.Fatalf("dispatched %v, err %v (after %v exhausted %v ok %v)", dispatched, err, after, exhausted, ok)
	}
	if !isCodexPacedResult(resultErrorFromError(err)) {
		t.Fatalf("the result error %+v is not recognised as paced, so the account would cool", resultErrorFromError(err))
	}
	for range codexPaceMaxTells - 1 {
		_, err = paced(m, context.Background(), auth, opts, func(context.Context) (int, error) { return 0, nil })
	}
	if _, exhausted, _ := CodexRetryAfter(err); !exhausted {
		t.Fatalf("after %d retries the client is not told to stop retrying", codexPaceMaxTells)
	}
}

func TestPacedLeavesOtherProvidersAlone(t *testing.T) {
	m := NewManager(nil, nil, nil)
	_, err := paced(m, context.Background(), &Auth{ID: "c", Provider: "claude"}, cliproxyexecutor.Options{}, func(context.Context) (int, error) {
		return 0, codexRateLimitTestError{}
	})
	if !errors.As(err, new(codexRateLimitTestError)) {
		t.Fatalf("err = %v, want the dispatch error unchanged", err)
	}
	if len(m.codexThrottle().paces) != 0 {
		t.Fatal("a Claude dispatch was paced")
	}
}

func TestIsCodexRateLimit(t *testing.T) {
	for _, tt := range []struct {
		err  error
		want bool
	}{
		{codexRateLimitTestError{}, true},
		{&Error{HTTPStatus: http.StatusTooManyRequests, Message: `{"error":{"type":"usage_limit_reached"}}`}, false},
		{&Error{HTTPStatus: http.StatusTooManyRequests, Message: "model is at capacity"}, false},
		{&Error{HTTPStatus: http.StatusTooManyRequests, Message: "account quota exhausted"}, false},
		{&Error{HTTPStatus: http.StatusBadGateway, Message: "Rate limit exceeded"}, false},
		{newCodexPacedError(time.Minute, false), false},
		{nil, false},
	} {
		if got := isCodexRateLimit(tt.err); got != tt.want {
			t.Errorf("isCodexRateLimit(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}
