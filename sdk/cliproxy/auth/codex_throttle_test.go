package auth

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type codexRateLimitTestError struct{}

func (codexRateLimitTestError) Error() string   { return `{"detail":"Rate limit exceeded"}` }
func (codexRateLimitTestError) StatusCode() int { return http.StatusTooManyRequests }

func newTestThrottle() *codexThrottle { return &codexThrottle{paces: map[string]*codexPace{}} }

func TestCodexPaceRunsFreeUntilLimited(t *testing.T) {
	th, now := newTestThrottle(), time.Unix(1_800_000_000, 0)
	for i := range 50 {
		if next := th.admit("a", "", now, now); !next.dispatch || next.wait != 0 {
			t.Fatalf("request %d: %+v, want an immediate dispatch", i, next)
		}
	}
}

func TestCodexPacePausesForTheLearnedTimeThenProbes(t *testing.T) {
	th, now := newTestThrottle(), time.Unix(1_800_000_000, 0)
	if pause := th.limited("a", now, 0, false); pause != codexPaceInitial {
		t.Fatalf("first pause = %v, want %v", pause, codexPaceInitial)
	}
	// 429s of requests already in flight belong to the same burst.
	if pause := th.limited("a", now.Add(time.Second), 0, false); pause != codexPaceInitial-time.Second {
		t.Fatalf("pause after a second 429 of the burst = %v, want the first pause to stand", pause)
	}
	if next := th.admit("a", "", now, now); next.dispatch || next.wait != codexPaceInitial {
		t.Fatalf("during the pause: %+v, want a %v wait", next, codexPaceInitial)
	}

	after := now.Add(codexPaceInitial)
	probe := th.admit("a", "", after, now)
	if !probe.dispatch || !probe.isProbe {
		t.Fatalf("first request after the pause: %+v, want the probe", probe)
	}
	waiting := th.admit("a", "", after, now)
	if waiting.dispatch || waiting.probe == nil {
		t.Fatalf("second request after the pause: %+v, want it to wait for the probe", waiting)
	}

	// A request sent before the pause that succeeds now proves nothing.
	th.cleared("a", after, now, false)
	select {
	case <-waiting.probe:
		t.Fatal("an old success released the waiting requests")
	default:
	}

	th.cleared("a", after.Add(time.Second), after, true)
	select {
	case <-waiting.probe:
	default:
		t.Fatal("the probe's success did not release the waiting requests")
	}
	if got := th.paces["a"].pause; got != time.Duration(float64(codexPaceInitial)*codexPaceShrink) {
		t.Fatalf("pause after a probe got through = %v, want it shortened", got)
	}
	first := th.admit("a", "", after.Add(time.Second), now)
	second := th.admit("a", "", after.Add(time.Second), now)
	if !first.dispatch || !second.dispatch || first.wait != codexPaceSpread || second.wait != 2*codexPaceSpread {
		t.Fatalf("released requests: %+v %+v, want them %v apart", first, second, codexPaceSpread)
	}
}

func TestCodexPaceLengthensAfterARefusedProbe(t *testing.T) {
	th, now := newTestThrottle(), time.Unix(1_800_000_000, 0)
	th.limited("a", now, 0, false)
	after := now.Add(codexPaceInitial)
	th.admit("a", "", after, now)
	waiting := th.admit("a", "", after, now)

	pause := th.limited("a", after, 0, true)
	if want := time.Duration(float64(codexPaceInitial) * codexPaceGrow); pause != want {
		t.Fatalf("pause after a refused probe = %v, want %v", pause, want)
	}
	select {
	case <-waiting.probe:
	default:
		t.Fatal("a refused probe left the waiting requests blocked")
	}
	if next := th.admit("a", "", after, after); next.dispatch || next.wait != pause {
		t.Fatalf("after a refused probe: %+v, want the new pause", next)
	}
}

func TestCodexPaceHonoursTheUpstreamWaitAndTellsALongOne(t *testing.T) {
	th, now := newTestThrottle(), time.Unix(1_800_000_000, 0)
	if pause := th.limited("a", now, 90*time.Second, false); pause != 90*time.Second {
		t.Fatalf("pause = %v, want the upstream's 90s", pause)
	}
	next := th.admit("a", "thread/turn", now, now)
	if next.tell != 90*time.Second {
		t.Fatalf("%+v, want the client told to retry in 90s", next)
	}
	if th.told("a", "thread/turn") != 1 {
		t.Fatal("the tell was not counted")
	}
	// A request that already waited here is told rather than held past a minute.
	th2 := newTestThrottle()
	th2.limited("a", now, 0, false)
	if next := th2.admit("a", "", now, now.Add(-50*time.Second)); next.tell != codexPaceInitial {
		t.Fatalf("%+v, want a request held 50s told to retry", next)
	}
}

func TestUpstreamRetryAfter(t *testing.T) {
	for _, tt := range []struct {
		headers http.Header
		want    time.Duration
	}{
		{http.Header{"Retry-After": {"12"}}, 12 * time.Second},
		{http.Header{"Retry-After": {"60.5"}}, 60500 * time.Millisecond},
		{http.Header{"Retry-After-Ms": {"1500"}, "Retry-After": {"9"}}, 1500 * time.Millisecond},
		{http.Header{}, 0},
	} {
		if got := upstreamRetryAfter(tt.headers); got != tt.want {
			t.Errorf("upstreamRetryAfter(%v) = %v, want %v", tt.headers, got, tt.want)
		}
	}
}

func TestPacedWaitsOutA429AndProbes(t *testing.T) {
	m := NewManager(nil, nil, nil)
	auth := &Auth{ID: "codex-a", Provider: "codex"}
	m.codexThrottle().pace(auth.ID).pause = 30 * time.Millisecond
	var calls atomic.Int32
	out, err := paced(m, context.Background(), auth, cliproxyexecutor.Options{}, func(context.Context) (int, error) {
		if calls.Add(1) == 1 {
			return 0, codexRateLimitTestError{}
		}
		return 7, nil
	})
	if err != nil || out != 7 || calls.Load() != 2 {
		t.Fatalf("out %d err %v after %d dispatches, want the retry after the pause to succeed", out, err, calls.Load())
	}
	if p := m.codexThrottle().paces[auth.ID]; p.recovering {
		t.Fatal("the successful probe did not end the recovery")
	}
}

func TestPacedTellsAFarPauseWithoutDispatching(t *testing.T) {
	m := NewManager(nil, nil, nil)
	auth := &Auth{ID: "codex-a", Provider: "codex"}
	m.codexThrottle().limited(auth.ID, time.Now(), 2*time.Minute, false)
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
